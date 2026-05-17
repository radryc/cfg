package cfg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
)

const (
	// maxCompactAttempts is the maximum number of CAS retries during Compact.
	maxCompactAttempts = 5
)

// CfgStore is a two-tier versioned config store.
//
// It satisfies kvsapi.ReadStore and kvsapi.WatchStore so it can be used
// directly as the config backend in the MonoFS server via SetCfgStore.
//
// Hot-tier files live in the underlying kvsapi.Store under the path
// /{root}/{product}/{sha}/{file}. Cold-tier files are stored in packager
// archives managed by the Archiver. File manifests (name, size, hash) for
// every version live permanently in KVS at
// /{root}/{product}/manifests/{sha} so directory listings work for cold
// versions without archive access.
type CfgStore struct {
	kvs      kvsapi.Store
	archiver *Archiver
	config   Config
	root     string // normalised root prefix, e.g. "/cfg"

	// mu protects the in-memory archive record cache.
	mu      sync.RWMutex
	archRec map[string]ArchiveRecord // archiveID → record
}

// New creates a CfgStore backed by kvs.
//
// config.ArchiveDir must be writable when StorageType is "local".
// config.EncryptionKey must be exactly 32 bytes.
func New(ctx context.Context, kvs kvsapi.Store, config Config) (*CfgStore, error) {
	if kvs == nil {
		return nil, fmt.Errorf("cfg: kvs store must not be nil")
	}
	a, err := newArchiver(ctx, config)
	if err != nil {
		return nil, err
	}
	return &CfgStore{
		kvs:      kvs,
		archiver: a,
		config:   config,
		root:     config.rootPrefix(),
		archRec:  make(map[string]ArchiveRecord),
	}, nil
}

// ---------------------------------------------------------------------------
// High-level config management API
// ---------------------------------------------------------------------------

// PushVersion atomically records all files for a new config version.
//
// files maps relative file paths (e.g. "app.conf") to their raw content.
// After writing all files and their manifest to KVS, PushVersion updates the
// VersionIndex via CAS and calls Compact if the hot version count exceeds
// MaxHotVersions.
func (s *CfgStore) PushVersion(ctx context.Context, product, sha string, files map[string][]byte) error {
	if err := validateProduct(product); err != nil {
		return err
	}
	if err := validateSHA(sha); err != nil {
		return err
	}
	for path := range files {
		if err := validateFilePath(path); err != nil {
			return err
		}
	}

	// 1. Write file content to KVS (hot tier).
	writes := make([]kvsapi.PathWrite, 0, len(files)+1)
	for filePath, content := range files {
		writes = append(writes, kvsapi.PathWrite{
			LogicalPath: kvsVersionFileKey(s.root, product, sha, filePath),
			Content:     content,
		})
	}

	// 2. Write the version manifest (file list without content).
	manifest := buildManifest(files)
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("cfg: marshal manifest for %s/%s: %w", product, sha, err)
	}
	writes = append(writes, kvsapi.PathWrite{
		LogicalPath: kvsManifestKey(s.root, product, sha),
		Content:     manifestData,
	})

	if _, err := s.kvs.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  writes,
		Context: kvsapi.MutationContext{PrincipalID: "cfg-store", Reason: fmt.Sprintf("push version %s/%s", product, sha)},
	}); err != nil {
		return fmt.Errorf("cfg: write version %s/%s to KVS: %w", product, sha, err)
	}

	// 3. CAS-update the VersionIndex to register the new version.
	if err := s.addVersionToIndex(ctx, product, sha, len(files)); err != nil {
		return fmt.Errorf("cfg: update index for %s/%s: %w", product, sha, err)
	}

	// 4. Compact if hot version count exceeds the limit.
	if err := s.Compact(ctx, product); err != nil {
		// Non-fatal: compaction is best-effort. Log or expose via metrics.
		_ = err
	}
	return nil
}

// addVersionToIndex adds a new hot VersionRecord to the VersionIndex using
// a CAS loop.
func (s *CfgStore) addVersionToIndex(ctx context.Context, product, sha string, fileCount int) error {
	for attempt := 0; attempt < maxCompactAttempts; attempt++ {
		idx, versionID, err := loadIndex(ctx, s.kvs, s.root, product)
		if err != nil {
			return err
		}
		// Idempotent: skip if sha already present.
		if idx.findVersion(sha) != nil {
			return nil
		}
		// Prepend (newest first).
		rec := VersionRecord{
			SHA:       sha,
			CreatedAt: time.Now().UTC(),
			Tier:      TierHot,
			FileCount: fileCount,
		}
		idx.Versions = append([]VersionRecord{rec}, idx.Versions...)

		if err := saveIndex(ctx, s.kvs, s.root, product, idx, versionID); err != nil {
			if errors.Is(err, kvsapi.ErrConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("cfg: too many CAS conflicts adding version %s/%s", product, sha)
}

// SetCurrent updates the "current" pointer for product to sha.
// sha must already exist in the VersionIndex.
func (s *CfgStore) SetCurrent(ctx context.Context, product, sha string) error {
	if err := validateProduct(product); err != nil {
		return err
	}
	if err := validateSHA(sha); err != nil {
		return err
	}

	// Verify sha exists in the index.
	idx, _, err := loadIndex(ctx, s.kvs, s.root, product)
	if err != nil {
		return err
	}
	if idx.findVersion(sha) == nil {
		return fmt.Errorf("cfg: version %s/%s not found in index", product, sha)
	}

	// Use optimistic concurrency on the "current" file itself.
	currentKey := kvsCurrentKey(s.root, product)
	info, _ := s.kvs.Stat(ctx, currentKey)

	_, err = s.kvs.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{
			LogicalPath:       currentKey,
			Content:           []byte(sha),
			ExpectedVersionID: info.VersionID,
		}},
		Context: kvsapi.MutationContext{PrincipalID: "cfg-store", Reason: fmt.Sprintf("set current %s → %s", product, sha)},
	})
	return err
}

// GetCurrent returns the sha that the "current" pointer for product resolves to.
// Returns fs.ErrNotExist if no current has been set.
func (s *CfgStore) GetCurrent(ctx context.Context, product string) (string, error) {
	if err := validateProduct(product); err != nil {
		return "", err
	}
	data, err := s.kvs.ReadFile(ctx, kvsCurrentKey(s.root, product))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// ListProductVersions returns all known versions for product, newest first.
func (s *CfgStore) ListProductVersions(ctx context.Context, product string) ([]VersionRecord, error) {
	if err := validateProduct(product); err != nil {
		return nil, err
	}
	idx, _, err := loadIndex(ctx, s.kvs, s.root, product)
	if err != nil {
		return nil, err
	}
	out := make([]VersionRecord, len(idx.Versions))
	copy(out, idx.Versions)
	return out, nil
}

// Compact evicts the oldest hot-tier versions beyond MaxHotVersions into a
// packager archive. It uses an optimistic CAS loop on the VersionIndex to
// ensure safety under concurrent calls.
func (s *CfgStore) Compact(ctx context.Context, product string) error {
	if err := validateProduct(product); err != nil {
		return err
	}

	for attempt := 0; attempt < maxCompactAttempts; attempt++ {
		idx, versionID, err := loadIndex(ctx, s.kvs, s.root, product)
		if err != nil {
			return err
		}

		hot := idx.hotVersions()
		maxHot := s.config.maxHotVersions()
		if len(hot) <= maxHot {
			return nil // nothing to do
		}

		// Candidates are the excess oldest hot versions (at the tail of hot slice
		// since Versions is newest-first).
		candidates := hot[maxHot:]
		candidateSHAs := make([]string, len(candidates))
		for i, c := range candidates {
			candidateSHAs[i] = c.SHA
		}

		// Collect file content for all candidates from KVS.
		filesByVersion, err := s.collectVersionFiles(ctx, product, candidateSHAs)
		if err != nil {
			return err
		}

		// Build the packager archive.
		archRec, err := s.archiver.buildArchive(ctx, product, candidateSHAs, filesByVersion)
		if err != nil {
			return fmt.Errorf("cfg: compact %s: build archive: %w", product, err)
		}

		// Update VersionIndex: mark candidates as cold, add archive record.
		if idx.Archives == nil {
			idx.Archives = make(map[string]ArchiveRecord)
		}
		idx.Archives[archRec.ID] = archRec
		for _, sha := range candidateSHAs {
			idx.markCold(sha, archRec.ID)
		}

		if err := saveIndex(ctx, s.kvs, s.root, product, idx, versionID); err != nil {
			if errors.Is(err, kvsapi.ErrConflict) {
				// Another writer updated the index; orphan the archive and retry.
				continue
			}
			return fmt.Errorf("cfg: compact %s: save index: %w", product, err)
		}

		// Cache the new archive record in memory.
		s.mu.Lock()
		s.archRec[archRec.ID] = archRec
		s.mu.Unlock()

		// Delete hot content from KVS (best-effort; manifests are kept).
		for _, sha := range candidateSHAs {
			s.deleteVersionContent(ctx, product, sha, filesByVersion[sha])
		}
		return nil
	}
	return fmt.Errorf("cfg: compact %s: too many CAS conflicts", product)
}

// collectVersionFiles reads all file content for the given shas from KVS.
func (s *CfgStore) collectVersionFiles(ctx context.Context, product string, shas []string) (map[string]map[string][]byte, error) {
	result := make(map[string]map[string][]byte, len(shas))
	for _, sha := range shas {
		versionDir := kvsVersionDir(s.root, product, sha)
		entries, err := s.kvs.ListDir(ctx, versionDir)
		if err != nil {
			return nil, fmt.Errorf("cfg: list version dir %s/%s: %w", product, sha, err)
		}
		vf := make(map[string][]byte, len(entries))
		for _, e := range entries {
			if e.IsDir {
				continue
			}
			key := kvsVersionFileKey(s.root, product, sha, e.Name)
			data, err := s.kvs.ReadFile(ctx, key)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("cfg: read file %s/%s/%s: %w", product, sha, e.Name, err)
			}
			vf[e.Name] = data
		}
		result[sha] = vf
	}
	return result, nil
}

// deleteVersionContent removes hot-tier file content from KVS for a version.
// Manifests are NOT deleted — they remain for cold-tier directory listing.
func (s *CfgStore) deleteVersionContent(ctx context.Context, product, sha string, files map[string][]byte) {
	deletes := make([]kvsapi.PathDelete, 0, len(files))
	for filePath := range files {
		deletes = append(deletes, kvsapi.PathDelete{
			LogicalPath: kvsVersionFileKey(s.root, product, sha, filePath),
		})
	}
	if len(deletes) == 0 {
		return
	}
	// Best-effort deletion — ignore errors.
	_, _ = s.kvs.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: deletes,
		Context: kvsapi.MutationContext{PrincipalID: "cfg-store", Reason: fmt.Sprintf("compact evict %s/%s", product, sha)},
	})
}

// Close releases resources held by the store.
func (s *CfgStore) Close() error {
	return nil
}

// ---------------------------------------------------------------------------
// kvsapi.ReadStore implementation
// ---------------------------------------------------------------------------

// ReadFile retrieves file content from the appropriate tier.
//
// Path formats accepted:
//   - /{root}/{product}/{sha}/{file}  — specific version
//   - /{root}/{product}/current/{file} — resolved to the active version
func (s *CfgStore) ReadFile(ctx context.Context, logicalPath string) ([]byte, error) {
	p, err := s.parsePath(logicalPath)
	if err != nil {
		return nil, fs.ErrNotExist
	}
	if p.sha == "" || p.filePath == "" {
		return nil, fmt.Errorf("cfg: ReadFile called on a directory: %q", logicalPath)
	}

	sha := p.sha
	if sha == "current" {
		sha, err = s.GetCurrent(ctx, p.product)
		if err != nil {
			return nil, err
		}
	}

	// Fast path: try KVS directly (works for hot tier and orphaned hot files
	// that survived a failed compact deletion).
	data, err := s.kvs.ReadFile(ctx, kvsVersionFileKey(s.root, p.product, sha, p.filePath))
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	// Fall back to cold tier via archive.
	return s.readColdFile(ctx, p.product, sha, p.filePath)
}

// readColdFile fetches a file from the packager archive for sha.
func (s *CfgStore) readColdFile(ctx context.Context, product, sha, filePath string) ([]byte, error) {
	rec, err := s.archiveForSHA(ctx, product, sha)
	if err != nil {
		return nil, err
	}
	return s.archiver.readFile(ctx, rec, sha, filePath)
}

// archiveForSHA looks up the ArchiveRecord for a cold-tier version.
func (s *CfgStore) archiveForSHA(ctx context.Context, product, sha string) (ArchiveRecord, error) {
	idx, _, err := loadIndex(ctx, s.kvs, s.root, product)
	if err != nil {
		return ArchiveRecord{}, err
	}
	vr := idx.findVersion(sha)
	if vr == nil {
		return ArchiveRecord{}, fmt.Errorf("cfg: version %s/%s not found: %w", product, sha, fs.ErrNotExist)
	}
	if vr.Tier == TierHot {
		return ArchiveRecord{}, fmt.Errorf("cfg: version %s/%s is hot but KVS lookup failed", product, sha)
	}
	archRec, ok := idx.Archives[vr.ArchiveID]
	if !ok {
		return ArchiveRecord{}, fmt.Errorf("cfg: archive %q for %s/%s not found in index", vr.ArchiveID, product, sha)
	}
	// Cache for future reads.
	s.mu.Lock()
	s.archRec[archRec.ID] = archRec
	s.mu.Unlock()
	return archRec, nil
}

// ListDir returns directory entries for a logical directory path.
//
// Supported paths:
//   - /{root}/{product}      → version directory list + "current" synthetic entry
//   - /{root}/{product}/{sha} → files within that version (hot or cold)
//   - /{root}/{product}/current → same as listing the active sha
func (s *CfgStore) ListDir(ctx context.Context, logicalDir string) ([]kvsapi.DirEntry, error) {
	p, err := s.parsePath(logicalDir)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	if p.sha == "" {
		// Product root: list version directories + synthetic "current".
		return s.listProductDir(ctx, p.product)
	}

	sha := p.sha
	if sha == "current" {
		sha, err = s.GetCurrent(ctx, p.product)
		if err != nil {
			return nil, err
		}
	}

	return s.listVersionDir(ctx, p.product, sha)
}

// listProductDir returns the entries visible under /{root}/{product}/.
// It returns one DirEntry per known version sha plus a synthetic "current"
// entry if a current pointer is set.
func (s *CfgStore) listProductDir(ctx context.Context, product string) ([]kvsapi.DirEntry, error) {
	idx, _, err := loadIndex(ctx, s.kvs, s.root, product)
	if err != nil {
		return nil, err
	}

	var entries []kvsapi.DirEntry
	for _, vr := range idx.Versions {
		entries = append(entries, kvsapi.DirEntry{
			Name:  vr.SHA,
			IsDir: true,
		})
	}

	// Add synthetic "current" if a current pointer exists.
	if _, err := s.GetCurrent(ctx, product); err == nil {
		entries = append(entries, kvsapi.DirEntry{
			Name:  "current",
			IsDir: true,
		})
	}
	return entries, nil
}

// listVersionDir returns the files inside /{root}/{product}/{sha}/.
func (s *CfgStore) listVersionDir(ctx context.Context, product, sha string) ([]kvsapi.DirEntry, error) {
	// Try hot tier first.
	versionDir := kvsVersionDir(s.root, product, sha)
	entries, err := s.kvs.ListDir(ctx, versionDir)
	if err == nil && len(entries) > 0 {
		return entries, nil
	}

	// Fall back to manifest for cold-tier version.
	manifest, err := s.loadManifest(ctx, product, sha)
	if err != nil {
		return nil, err
	}
	dirEntries := make([]kvsapi.DirEntry, len(manifest))
	for i, fr := range manifest {
		dirEntries[i] = kvsapi.DirEntry{
			Name:  fr.Path,
			IsDir: false,
			Size:  fr.Size,
		}
	}
	return dirEntries, nil
}

// loadManifest reads the file manifest for a version from KVS.
func (s *CfgStore) loadManifest(ctx context.Context, product, sha string) ([]FileRecord, error) {
	data, err := s.kvs.ReadFile(ctx, kvsManifestKey(s.root, product, sha))
	if err != nil {
		return nil, fmt.Errorf("cfg: load manifest for %s/%s: %w", product, sha, err)
	}
	var records []FileRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("cfg: unmarshal manifest for %s/%s: %w", product, sha, err)
	}
	return records, nil
}

// Stat returns metadata for a logical path.
func (s *CfgStore) Stat(ctx context.Context, logicalPath string) (kvsapi.FileInfo, error) {
	p, err := s.parsePath(logicalPath)
	if err != nil {
		return kvsapi.FileInfo{}, fs.ErrNotExist
	}

	if p.sha == "" {
		// Product root directory.
		return kvsapi.FileInfo{Path: logicalPath, ModTime: time.Now().UTC()}, nil
	}

	sha := p.sha
	if sha == "current" {
		if p.filePath == "" {
			// stat of "current" directory itself.
			return kvsapi.FileInfo{Path: logicalPath, ModTime: time.Now().UTC()}, nil
		}
		sha, err = s.GetCurrent(ctx, p.product)
		if err != nil {
			return kvsapi.FileInfo{}, err
		}
	}

	if p.filePath == "" {
		// Version directory itself.
		return kvsapi.FileInfo{Path: logicalPath, ModTime: time.Now().UTC()}, nil
	}

	// Try hot tier.
	key := kvsVersionFileKey(s.root, p.product, sha, p.filePath)
	info, err := s.kvs.Stat(ctx, key)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return kvsapi.FileInfo{}, err
	}

	// Cold tier: look up in manifest.
	manifest, merr := s.loadManifest(ctx, p.product, sha)
	if merr != nil {
		return kvsapi.FileInfo{}, fmt.Errorf("cfg: stat %q: not found in hot tier or manifest", logicalPath)
	}
	for _, fr := range manifest {
		if fr.Path == p.filePath {
			return kvsapi.FileInfo{
				Path:    logicalPath,
				Size:    fr.Size,
				ModTime: fr.ModTime,
			}, nil
		}
	}
	return kvsapi.FileInfo{}, fs.ErrNotExist
}

// ---------------------------------------------------------------------------
// kvsapi.WatchStore implementation
// ---------------------------------------------------------------------------

// Watch delegates to the underlying KVS watch.  Clients watching a product
// root prefix will receive events for file and current-pointer changes.
func (s *CfgStore) Watch(ctx context.Context, prefixes []string) (<-chan kvsapi.ChangeEvent, error) {
	return s.kvs.Watch(ctx, prefixes)
}

// ---------------------------------------------------------------------------
// kvsapi.WriteStore passthrough (delegates to underlying KVS for callers that
// need the full Store interface — cfg-specific callers should prefer
// PushVersion / SetCurrent).
// ---------------------------------------------------------------------------

// UpsertFiles delegates to the underlying KVS. Prefer PushVersion for
// structured version pushes.
func (s *CfgStore) UpsertFiles(ctx context.Context, batch kvsapi.MutationBatch) (kvsapi.BatchRevision, error) {
	return s.kvs.UpsertFiles(ctx, batch)
}

// DeletePaths delegates to the underlying KVS.
func (s *CfgStore) DeletePaths(ctx context.Context, batch kvsapi.DeleteBatch) (kvsapi.BatchRevision, error) {
	return s.kvs.DeletePaths(ctx, batch)
}

// ListVersions delegates to the underlying KVS versioning API.
func (s *CfgStore) ListVersions(ctx context.Context, logicalPath string) ([]kvsapi.FileVersion, error) {
	return s.kvs.ListVersions(ctx, logicalPath)
}

// GetVersion delegates to the underlying KVS versioning API.
func (s *CfgStore) GetVersion(ctx context.Context, logicalPath, versionID string) (kvsapi.VersionedFile, error) {
	return s.kvs.GetVersion(ctx, logicalPath, versionID)
}

// ---------------------------------------------------------------------------
// Path parsing
// ---------------------------------------------------------------------------

// cfgPath represents a parsed logical path within the cfg namespace.
type cfgPath struct {
	product  string // e.g. "my-app"
	sha      string // e.g. "abc123" or "current" or "" (product dir)
	filePath string // e.g. "app.conf" or "" (version dir or product dir)
}

// parsePath parses a full logical path into its cfg components.
// It accepts paths of the form:
//
//	/{root}/{product}
//	/{root}/{product}/{sha}
//	/{root}/{product}/{sha}/{file...}
//	/{root}/{product}/current
//	/{root}/{product}/current/{file...}
func (s *CfgStore) parsePath(logicalPath string) (cfgPath, error) {
	// Strip root prefix.
	rel := strings.TrimPrefix(logicalPath, s.root)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return cfgPath{}, fmt.Errorf("cfg: path %q is the root, not a product path", logicalPath)
	}

	// Split into at most 3 parts: product / sha / rest
	parts := strings.SplitN(rel, "/", 3)
	product := parts[0]
	if err := validateProduct(product); err != nil {
		return cfgPath{}, err
	}
	if len(parts) == 1 {
		return cfgPath{product: product}, nil
	}

	sha := parts[1]
	// "current" is a reserved virtual directory.
	if sha != "current" {
		if err := validateSHA(sha); err != nil {
			return cfgPath{}, err
		}
	}
	if len(parts) == 2 {
		return cfgPath{product: product, sha: sha}, nil
	}

	filePath := parts[2]
	if err := validateFilePath(filePath); err != nil {
		return cfgPath{}, err
	}
	return cfgPath{product: product, sha: sha, filePath: filePath}, nil
}
