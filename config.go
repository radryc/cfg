package cfg

import "strings"

// StorageType selects where packager archives are persisted.
type StorageType string

const (
	StorageTypeLocal StorageType = "local"
	StorageTypeS3    StorageType = "s3"
	StorageTypeGCS   StorageType = "gcs"
)

// Config controls the config store.
type Config struct {
	// RootPrefix is the KVS logical path prefix for all config entries.
	// Default: "/cfg". Must not contain a trailing slash.
	RootPrefix string

	// MaxHotVersions is the maximum number of versions per product kept
	// in KVS (hot tier) before evicting old ones to packager archives.
	// Default: 5.
	MaxHotVersions int

	// ArchiveDir is the local filesystem directory for .pack archive files.
	// Required when StorageType is "local" or as a local cache for cloud storage.
	ArchiveDir string

	// EncryptionKey is a 32-byte ChaCha20-Poly1305 key used for packager archives.
	EncryptionKey []byte

	// Storage selects where packager archives are persisted.
	Storage StorageConfig
}

// StorageConfig holds backend-specific archive storage configuration.
type StorageConfig struct {
	// Type selects the storage backend: "local" (default), "s3", or "gcs".
	Type StorageType

	// S3Region is the AWS region (e.g. "us-east-1"). Required for S3.
	S3Region string
	// S3Bucket is the bucket name for archive storage. Required for S3.
	S3Bucket string
	// S3Prefix is an optional key prefix (e.g. "cfg/archives/").
	S3Prefix string
	// S3Endpoint overrides the default S3 endpoint (for MinIO, Ceph, etc.).
	S3Endpoint string
	// S3AccessKeyID for static credentials. Empty = use default AWS chain.
	S3AccessKeyID string
	// S3SecretAccessKey for static credentials.
	S3SecretAccessKey string
	// S3SessionToken is an optional session token for temporary credentials.
	S3SessionToken string
	// S3UsePathStyle forces path-style addressing (required for most
	// S3-compatible services).
	S3UsePathStyle bool

	// GCSBucket is the GCS bucket name. Required for GCS.
	GCSBucket string
	// GCSPrefix is an optional object name prefix.
	GCSPrefix string
	// GCSCredentialsFile is the path to a service account JSON key file.
	// Empty = use Application Default Credentials.
	GCSCredentialsFile string
	// GCSCredentialsJSON is inline service account JSON key content.
	// Takes precedence over GCSCredentialsFile.
	GCSCredentialsJSON []byte
}

func (c *Config) maxHotVersions() int {
	if c.MaxHotVersions <= 0 {
		return 5
	}
	return c.MaxHotVersions
}

// rootPrefix returns the normalized root prefix, always starting with "/" and
// never ending with "/".
func (c *Config) rootPrefix() string {
	if c.RootPrefix == "" {
		return "/cfg"
	}
	return "/" + strings.Trim(c.RootPrefix, "/")
}
