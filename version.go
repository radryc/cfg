package cfg

import "time"

// Tier constants for VersionRecord.
const (
	TierHot  = "hot"
	TierCold = "cold"
)

// VersionRecord describes one version of a product's config.
type VersionRecord struct {
	// SHA is the content-addressed identifier for this version (e.g. a git SHA).
	SHA string `json:"sha"`
	// CreatedAt is when the version was first pushed.
	CreatedAt time.Time `json:"created_at"`
	// Tier is "hot" (content in KVS) or "cold" (content in a packager archive).
	Tier string `json:"tier"`
	// ArchiveID is populated when Tier == TierCold. It references the
	// ArchiveRecord in VersionIndex.Archives that holds this version's content.
	ArchiveID string `json:"archive_id,omitempty"`
	// FileCount is the number of files in this version.
	FileCount int `json:"file_count"`
}

// ArchiveRecord describes a packager archive that holds one or more cold-tier
// versions for a product.
type ArchiveRecord struct {
	// ID is a unique identifier for this archive (UUID).
	ID string `json:"id"`
	// SHAs lists all version SHAs stored inside this archive.
	SHAs []string `json:"shas"`
	// Path is the local filesystem path or cloud object key of the .pack file.
	Path string `json:"path"`
	// IndexSize caches the packed index size for OpenArchive's WithIndexSize
	// optimisation (eliminates a footer read on every open).
	IndexSize int64 `json:"index_size"`
	// CreatedAt is when this archive was built.
	CreatedAt time.Time `json:"created_at"`
}

// FileRecord describes a single file within a version. Stored at
// /cfg/{product}/manifests/{sha} so that directory listings work for cold
// versions without reading the packager archive index.
type FileRecord struct {
	// Path is the file path within the version directory (e.g. "app.conf").
	Path string `json:"path"`
	// Size is the uncompressed file size in bytes.
	Size int64 `json:"size"`
	// ModTime is the file's modification time as recorded during ingestion.
	ModTime time.Time `json:"mod_time"`
	// Hash is the hex-encoded SHA-256 of the uncompressed content.
	Hash string `json:"hash"`
}

// VersionIndex is the authoritative catalogue for a product. It is stored as
// JSON at /cfg/{product}/_index and all writes use optimistic concurrency
// (KVS ExpectedVersionID).
type VersionIndex struct {
	// Product is the product name.
	Product string `json:"product"`
	// Versions is the ordered list of known versions, newest first.
	Versions []VersionRecord `json:"versions"`
	// Archives maps archiveID → ArchiveRecord for all cold-tier archives.
	Archives map[string]ArchiveRecord `json:"archives,omitempty"`
}

// hotVersions returns the subset of versions in the hot tier, preserving order.
func (idx *VersionIndex) hotVersions() []VersionRecord {
	var hot []VersionRecord
	for _, v := range idx.Versions {
		if v.Tier == TierHot {
			hot = append(hot, v)
		}
	}
	return hot
}

// findVersion returns a pointer to the VersionRecord for sha, or nil.
func (idx *VersionIndex) findVersion(sha string) *VersionRecord {
	for i := range idx.Versions {
		if idx.Versions[i].SHA == sha {
			return &idx.Versions[i]
		}
	}
	return nil
}

// markCold updates the tier for sha to "cold" and sets its archiveID.
func (idx *VersionIndex) markCold(sha, archiveID string) {
	for i := range idx.Versions {
		if idx.Versions[i].SHA == sha {
			idx.Versions[i].Tier = TierCold
			idx.Versions[i].ArchiveID = archiveID
			return
		}
	}
}
