// Package cfg implements a two-tier versioned config store backed by KVS (hot)
// and packager archives (cold).
//
// # Layout
//
// Each product (e.g. "my-app") is a namespace under a configurable root prefix
// (default "/cfg"). Inside that prefix, KVS holds:
//
//	/cfg/my-app/current                     → sha string (active version)
//	/cfg/my-app/_index                      → JSON VersionIndex
//	/cfg/my-app/manifests/{sha}             → JSON []FileRecord (per-version file list)
//	/cfg/my-app/{sha}/{file}                → file content (hot tier)
//
// Packager archives store cold-tier content at
// {archiveDir}/{product}/{archiveID}.pack.
//
// # Hot/cold tier management
//
// The store keeps at most MaxHotVersions (default 5) versions per product in
// KVS. When PushVersion exceeds that threshold, Compact is called automatically.
// Compact bundles the oldest excess versions into a packager archive
// (encrypted with ChaCha20-Poly1305, compressed with zstd), updates the
// VersionIndex via optimistic concurrency (CAS on the index version ID), then
// deletes the archived content from KVS. Per-version manifests (file names,
// sizes, hashes) remain in KVS forever to support directory listing of cold
// versions.
//
// # MonoFS integration
//
// CfgStore implements kvsapi.ReadStore and kvsapi.WatchStore, so it can be
// wired directly into the MonoFS server via the SetCfgStore hook in
// internal/server/cfg_backend.go.  The server routes any repository whose
// storage_backend field is "cfg" through CfgStore instead of the standard
// KVS store.
//
// Clients mount the filesystem and see:
//
//	/cfg/my-app/sha1/           → version directory
//	/cfg/my-app/sha2/           → version directory
//	/cfg/my-app/current/        → virtual directory aliasing the active version
package cfg
