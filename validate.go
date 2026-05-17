package cfg

import (
	"fmt"
	"strings"
)

// reserved is the set of path components that cannot be used as product names,
// SHAs, or filenames because they have special meaning within the store.
var reserved = map[string]bool{
	"_index":    true,
	"manifests": true,
}

// validateProduct ensures the product name is safe to use as a KVS path segment.
func validateProduct(product string) error {
	if product == "" {
		return fmt.Errorf("cfg: product name must not be empty")
	}
	if strings.ContainsAny(product, "/\x00") {
		return fmt.Errorf("cfg: product name %q must not contain '/' or null bytes", product)
	}
	if strings.HasPrefix(product, "_") || strings.HasPrefix(product, ".") {
		return fmt.Errorf("cfg: product name %q must not start with '_' or '.'", product)
	}
	if reserved[product] {
		return fmt.Errorf("cfg: product name %q is reserved", product)
	}
	return nil
}

// validateSHA ensures the sha is safe to use as a KVS path segment.
// "current" is reserved as a virtual directory name.
func validateSHA(sha string) error {
	if sha == "" {
		return fmt.Errorf("cfg: sha must not be empty")
	}
	if sha == "current" {
		return fmt.Errorf("cfg: sha %q is reserved for the current-version virtual directory", sha)
	}
	if strings.ContainsAny(sha, "/\x00") {
		return fmt.Errorf("cfg: sha %q must not contain '/' or null bytes", sha)
	}
	if strings.HasPrefix(sha, "_") || strings.HasPrefix(sha, ".") {
		return fmt.Errorf("cfg: sha %q must not start with '_' or '.'", sha)
	}
	return nil
}

// validateFilePath ensures a relative file path is safe for use inside an
// archive or as a KVS suffix. It mirrors the packager's validateArchivePath
// rules.
func validateFilePath(filePath string) error {
	if filePath == "" {
		return fmt.Errorf("cfg: file path must not be empty")
	}
	if strings.HasPrefix(filePath, "/") {
		return fmt.Errorf("cfg: file path must be relative, got absolute path %q", filePath)
	}
	for _, component := range strings.Split(filePath, "/") {
		if component == ".." {
			return fmt.Errorf("cfg: file path must not contain '..': %q", filePath)
		}
		if component == "" {
			return fmt.Errorf("cfg: file path must not contain empty components: %q", filePath)
		}
	}
	return nil
}

// kvsIndexKey returns the KVS path for the VersionIndex of a product.
// e.g. /cfg/my-app/_index
func kvsIndexKey(root, product string) string {
	return root + "/" + product + "/_index"
}

// kvsCurrentKey returns the KVS path for the current-version pointer.
// e.g. /cfg/my-app/current
func kvsCurrentKey(root, product string) string {
	return root + "/" + product + "/current"
}

// kvsManifestKey returns the KVS path for a version's file manifest.
// e.g. /cfg/my-app/manifests/sha1
func kvsManifestKey(root, product, sha string) string {
	return root + "/" + product + "/manifests/" + sha
}

// kvsVersionDir returns the KVS logical directory for all files in a version.
// e.g. /cfg/my-app/sha1
func kvsVersionDir(root, product, sha string) string {
	return root + "/" + product + "/" + sha
}

// kvsVersionFileKey returns the KVS path for a specific file within a version.
// e.g. /cfg/my-app/sha1/app.conf
func kvsVersionFileKey(root, product, sha, filePath string) string {
	return root + "/" + product + "/" + sha + "/" + filePath
}

// kvsProductDir returns the KVS logical directory for a product.
// e.g. /cfg/my-app
func kvsProductDir(root, product string) string {
	return root + "/" + product
}

// isMetaName returns true for special KVS names that must never appear in
// directory listings exposed to clients.
func isMetaName(name string) bool {
	return name == "_index" || name == "manifests" || name == "current"
}

// archiveEntryPath returns the path used when storing a file inside a packager
// archive. It encodes both the version SHA and the file path so multiple
// versions can coexist in one archive.
func archiveEntryPath(sha, filePath string) string {
	return sha + "/" + filePath
}
