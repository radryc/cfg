package cfg

import (
"bytes"
"context"
"crypto/sha256"
"encoding/binary"
"encoding/hex"
"fmt"
"io"
"os"
"path/filepath"
"time"

gcStorage "cloud.google.com/go/storage"
"github.com/aws/aws-sdk-go-v2/aws"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/google/uuid"
"github.com/radryc/packager"
"github.com/radryc/packager/pipeline"
pkgstorage "github.com/radryc/packager/storage"
)

// Archiver builds and reads packager archives for cold-tier config versions.
type Archiver struct {
config   Config
pipeline *pipeline.Pipeline

// Cloud clients — nil when StorageType is "local".
s3Client  *s3.Client
gcsClient *gcStorage.Client
}

// newArchiver creates and initialises an Archiver.
func newArchiver(ctx context.Context, config Config) (*Archiver, error) {
if len(config.EncryptionKey) != 32 {
return nil, fmt.Errorf("cfg archiver: encryption key must be exactly 32 bytes, got %d", len(config.EncryptionKey))
}
p, err := pipeline.NewPipeline(config.EncryptionKey)
if err != nil {
return nil, fmt.Errorf("cfg archiver: create pipeline: %w", err)
}

a := &Archiver{config: config, pipeline: p}

switch config.Storage.Type {
case StorageTypeS3:
sc := config.Storage
if sc.S3Bucket == "" {
return nil, fmt.Errorf("cfg archiver: S3 bucket required when storage_type=s3")
}
client, err := pkgstorage.NewS3Client(ctx, pkgstorage.S3Config{
Region:          sc.S3Region,
Endpoint:        sc.S3Endpoint,
AccessKeyID:     sc.S3AccessKeyID,
SecretAccessKey: sc.S3SecretAccessKey,
SessionToken:    sc.S3SessionToken,
UsePathStyle:    sc.S3UsePathStyle,
})
if err != nil {
return nil, fmt.Errorf("cfg archiver: create S3 client: %w", err)
}
a.s3Client = client

case StorageTypeGCS:
sc := config.Storage
if sc.GCSBucket == "" {
return nil, fmt.Errorf("cfg archiver: GCS bucket required when storage_type=gcs")
}
client, err := pkgstorage.NewGCSClient(ctx, pkgstorage.GCSConfig{
CredentialsFile: sc.GCSCredentialsFile,
CredentialsJSON: sc.GCSCredentialsJSON,
})
if err != nil {
return nil, fmt.Errorf("cfg archiver: create GCS client: %w", err)
}
a.gcsClient = client
}

return a, nil
}

// buildArchive creates a packager archive containing all files for the given
// versions and returns an ArchiveRecord describing it.
//
// files maps sha → filePath → rawContent. The archive stores each file under
// the path archiveEntryPath(sha, filePath) so multiple versions can coexist
// within one archive.
func (a *Archiver) buildArchive(ctx context.Context, product string, shas []string, files map[string]map[string][]byte) (ArchiveRecord, error) {
archiveID := uuid.New().String()
localDir := filepath.Join(a.config.ArchiveDir, product)
if err := os.MkdirAll(localDir, 0o755); err != nil {
return ArchiveRecord{}, fmt.Errorf("cfg archiver: mkdir %s: %w", localDir, err)
}

localPath := filepath.Join(localDir, archiveID+".pack")
f, err := os.Create(localPath)
if err != nil {
return ArchiveRecord{}, fmt.Errorf("cfg archiver: create archive file: %w", err)
}

aw := packager.NewArchiveWriter(f, a.pipeline)
opts := packager.DefaultAddFileOptions()

for _, sha := range shas {
vf, ok := files[sha]
if !ok {
continue
}
for filePath, content := range vf {
entryPath := archiveEntryPath(sha, filePath)
if err := aw.AddFile(entryPath, content, opts); err != nil {
f.Close()
os.Remove(localPath)
return ArchiveRecord{}, fmt.Errorf("cfg archiver: add file %q: %w", entryPath, err)
}
}
}

if err := aw.Close(); err != nil {
f.Close()
os.Remove(localPath)
return ArchiveRecord{}, fmt.Errorf("cfg archiver: close writer: %w", err)
}

// Read the packed index size from the 8-byte LE footer.
fi, err := f.Stat()
if err != nil {
f.Close()
os.Remove(localPath)
return ArchiveRecord{}, fmt.Errorf("cfg archiver: stat archive: %w", err)
}
totalSize := fi.Size()
indexSize, err := readFooterIndexSize(f, totalSize)
if err != nil {
f.Close()
os.Remove(localPath)
return ArchiveRecord{}, fmt.Errorf("cfg archiver: read footer: %w", err)
}
f.Close()

archivePath := localPath

// Upload to cloud if configured.
switch a.config.Storage.Type {
case StorageTypeS3:
objectKey := a.s3ObjectKey(product, archiveID)
if err := a.uploadToS3(ctx, localPath, objectKey); err != nil {
os.Remove(localPath)
return ArchiveRecord{}, fmt.Errorf("cfg archiver: upload to S3: %w", err)
}
archivePath = objectKey

case StorageTypeGCS:
objectKey := a.gcsObjectKey(product, archiveID)
if err := a.uploadToGCS(ctx, localPath, objectKey); err != nil {
os.Remove(localPath)
return ArchiveRecord{}, fmt.Errorf("cfg archiver: upload to GCS: %w", err)
}
archivePath = objectKey
}

return ArchiveRecord{
ID:        archiveID,
SHAs:      shas,
Path:      archivePath,
IndexSize: indexSize,
CreatedAt: time.Now().UTC(),
}, nil
}

// readFile retrieves a single file from an archive.
func (a *Archiver) readFile(ctx context.Context, rec ArchiveRecord, sha, filePath string) ([]byte, error) {
ar, err := a.openArchive(ctx, rec)
if err != nil {
return nil, err
}
defer ar.Close()

entryPath := archiveEntryPath(sha, filePath)
data, _, err := ar.GetFile(entryPath)
if err != nil {
return nil, fmt.Errorf("cfg archiver: get file %q from archive %q: %w", entryPath, rec.ID, err)
}
return data, nil
}

// openArchive opens an ArchiveReader for rec.
// The caller must close the returned reader (which also closes the underlying storage).
func (a *Archiver) openArchive(ctx context.Context, rec ArchiveRecord) (*packager.ArchiveReader, error) {
var store pkgstorage.ObjectReader
var err error

switch a.config.Storage.Type {
case StorageTypeS3:
store, err = pkgstorage.NewS3ReaderFromConfig(ctx, pkgstorage.S3Config{
Region:          a.config.Storage.S3Region,
Endpoint:        a.config.Storage.S3Endpoint,
AccessKeyID:     a.config.Storage.S3AccessKeyID,
SecretAccessKey: a.config.Storage.S3SecretAccessKey,
SessionToken:    a.config.Storage.S3SessionToken,
UsePathStyle:    a.config.Storage.S3UsePathStyle,
}, a.config.Storage.S3Bucket, rec.Path)
if err != nil {
return nil, fmt.Errorf("cfg archiver: open S3 object %q: %w", rec.Path, err)
}

case StorageTypeGCS:
store, err = pkgstorage.NewGCSReaderFromConfig(ctx, pkgstorage.GCSConfig{
CredentialsFile: a.config.Storage.GCSCredentialsFile,
CredentialsJSON: a.config.Storage.GCSCredentialsJSON,
}, a.config.Storage.GCSBucket, rec.Path)
if err != nil {
return nil, fmt.Errorf("cfg archiver: open GCS object %q: %w", rec.Path, err)
}

default:
// Local: rec.Path is the absolute local path written during buildArchive.
// Fall back to reconstructing from ArchiveDir if the path moved.
store, err = pkgstorage.NewLocalFileReader(rec.Path)
if err != nil {
fallback := filepath.Join(a.config.ArchiveDir, rec.Path)
store, err = pkgstorage.NewLocalFileReader(fallback)
if err != nil {
return nil, fmt.Errorf("cfg archiver: open local archive %q: %w", rec.Path, err)
}
}
}

var opts []packager.OpenOption
if rec.IndexSize > 0 {
opts = append(opts, packager.WithIndexSize(rec.IndexSize))
}
ar, err := packager.OpenArchive(store, a.pipeline, opts...)
if err != nil {
store.Close()
return nil, fmt.Errorf("cfg archiver: parse archive %q: %w", rec.ID, err)
}
return ar, nil
}

// uploadToS3 streams a local file to S3.
func (a *Archiver) uploadToS3(ctx context.Context, localPath, objectKey string) error {
f, err := os.Open(localPath)
if err != nil {
return err
}
defer f.Close()
fi, err := f.Stat()
if err != nil {
return err
}
_, err = a.s3Client.PutObject(ctx, &s3.PutObjectInput{
Bucket:        aws.String(a.config.Storage.S3Bucket),
Key:           aws.String(objectKey),
Body:          f,
ContentLength: aws.Int64(fi.Size()),
})
return err
}

// uploadToGCS streams a local file to GCS.
func (a *Archiver) uploadToGCS(ctx context.Context, localPath, objectKey string) error {
f, err := os.Open(localPath)
if err != nil {
return err
}
defer f.Close()

wc := a.gcsClient.Bucket(a.config.Storage.GCSBucket).Object(objectKey).NewWriter(ctx)
if _, err := io.Copy(wc, f); err != nil {
wc.Close()
return err
}
return wc.Close()
}

func (a *Archiver) s3ObjectKey(product, archiveID string) string {
prefix := a.config.Storage.S3Prefix
if prefix != "" && prefix[len(prefix)-1] != '/' {
prefix += "/"
}
return prefix + product + "/" + archiveID + ".pack"
}

func (a *Archiver) gcsObjectKey(product, archiveID string) string {
prefix := a.config.Storage.GCSPrefix
if prefix != "" && prefix[len(prefix)-1] != '/' {
prefix += "/"
}
return prefix + product + "/" + archiveID + ".pack"
}

// fileHash returns the hex-encoded SHA-256 of data.
func fileHash(data []byte) string {
h := sha256.Sum256(data)
return hex.EncodeToString(h[:])
}

// readFooterIndexSize reads the 8-byte little-endian footer from f to obtain
// the packed index size written by ArchiveWriter.Close().
func readFooterIndexSize(f *os.File, totalSize int64) (int64, error) {
if totalSize < 8 {
return 0, nil
}
footer := make([]byte, 8)
if _, err := f.ReadAt(footer, totalSize-8); err != nil {
return 0, err
}
return int64(binary.LittleEndian.Uint64(footer)), nil
}

// buildManifest constructs a FileRecord slice from a map of filePath→content.
func buildManifest(files map[string][]byte) []FileRecord {
records := make([]FileRecord, 0, len(files))
for path, content := range files {
records = append(records, FileRecord{
Path:    path,
Size:    int64(len(content)),
ModTime: time.Now().UTC(),
Hash:    fileHash(content),
})
}
return records
}

// nopReadSeekCloser wraps a bytes.Reader as an io.ReadSeekCloser.
type nopReadSeekCloser struct{ r *bytes.Reader }

func (n *nopReadSeekCloser) Read(p []byte) (int, error)                     { return n.r.Read(p) }
func (n *nopReadSeekCloser) Seek(offset int64, whence int) (int64, error)   { return n.r.Seek(offset, whence) }
func (n *nopReadSeekCloser) Close() error                                   { return nil }
