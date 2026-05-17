package cfg

import (
"context"
"encoding/json"
"errors"
"fmt"
"io/fs"

"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
)

// loadIndex reads and unmarshals the VersionIndex for product from KVS.
// It returns the index, the KVS versionID of the _index file (for CAS writes),
// and any error.  If the index does not exist yet, it returns a default empty
// index with versionID == "".
func loadIndex(ctx context.Context, store kvsapi.Store, root, product string) (*VersionIndex, string, error) {
key := kvsIndexKey(root, product)
data, err := store.ReadFile(ctx, key)
if err != nil {
if errors.Is(err, fs.ErrNotExist) {
return defaultIndex(product), "", nil
}
return nil, "", fmt.Errorf("load index for %q: %w", product, err)
}

var idx VersionIndex
if err := json.Unmarshal(data, &idx); err != nil {
return nil, "", fmt.Errorf("unmarshal index for %q: %w", product, err)
}

// Fetch the KVS version ID for optimistic concurrency.
info, err := store.Stat(ctx, key)
if err != nil {
return nil, "", fmt.Errorf("stat index for %q: %w", product, err)
}
return &idx, info.VersionID, nil
}

// saveIndex marshals idx and writes it to KVS using CAS.
// expectedVersionID must match the versionID returned by loadIndex.
// If the index was modified concurrently, saveIndex returns kvsapi.ErrConflict.
func saveIndex(ctx context.Context, store kvsapi.Store, root, product string, idx *VersionIndex, expectedVersionID string) error {
data, err := json.Marshal(idx)
if err != nil {
return fmt.Errorf("marshal index for %q: %w", product, err)
}
_, err = store.UpsertFiles(ctx, kvsapi.MutationBatch{
Writes: []kvsapi.PathWrite{
{
LogicalPath:       kvsIndexKey(root, product),
Content:           data,
ExpectedVersionID: expectedVersionID,
},
},
Context: kvsapi.MutationContext{
PrincipalID: "cfg-store",
Reason:      "update version index",
},
})
return err
}

// defaultIndex returns an empty VersionIndex for a product.
func defaultIndex(product string) *VersionIndex {
return &VersionIndex{
Product:  product,
Versions: nil,
Archives: make(map[string]ArchiveRecord),
}
}
