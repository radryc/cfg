package cfg

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/radryc/packager/pipeline"
)

func randomKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, encryptionKeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func repeated(seed []byte, n int) []byte {
	out := make([]byte, 0, n)
	for len(out) < n {
		out = append(out, seed...)
	}
	return out[:n]
}

func TestValidateEncryptionKey(t *testing.T) {
	cases := []struct {
		name    string
		key     []byte
		wantErr bool
	}{
		{name: "missing", key: nil, wantErr: true},
		{name: "empty", key: []byte{}, wantErr: true},
		{name: "short", key: []byte("0123456789abcdef"), wantErr: true},
		{name: "all zero", key: make([]byte, 32), wantErr: true},
		{name: "single repeated byte", key: bytes.Repeat([]byte{0xAB}, 32), wantErr: true},
		{name: "short repeated pattern", key: repeated([]byte("0123"), 32), wantErr: true},
		{name: "random", key: randomKey(t), wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEncryptionKey(tc.key)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewArchiverRejectsWeakKeys(t *testing.T) {
	ctx := context.Background()
	weak := map[string][]byte{
		"missing":     nil,
		"short":       []byte("too-short"),
		"all-zero":    make([]byte, 32),
		"repeated":    bytes.Repeat([]byte{0x42}, 32),
		"low-entropy": repeated([]byte("abcd"), 32),
	}
	for name, key := range weak {
		t.Run(name, func(t *testing.T) {
			if _, err := newArchiver(ctx, Config{EncryptionKey: key}); err == nil {
				t.Fatal("newArchiver accepted a weak encryption key")
			}
		})
	}

	if _, err := newArchiver(ctx, Config{EncryptionKey: randomKey(t)}); err != nil {
		t.Fatalf("newArchiver rejected a valid key: %v", err)
	}
}

func TestArchiverAlwaysEncrypts(t *testing.T) {
	ctx := context.Background()
	key := randomKey(t)
	p, err := pipeline.NewPipeline(key)
	if err != nil {
		t.Fatal(err)
	}

	a := &Archiver{
		config: Config{
			ArchiveDir:    t.TempDir(),
			EncryptionKey: key,
			Storage:       StorageConfig{Type: StorageTypeLocal},
		},
		pipeline: p,
	}

	const sha = "deadbeefcafe"
	plaintext := []byte("cfg-plaintext-marker-0123456789")
	secret := []byte("db-secret-value-should-never-be-stored-in-clear")
	files := map[string]map[string][]byte{
		sha: {
			"app.conf":         plaintext,
			"nested/db.conf":   secret,
			"nested/empty.txt": {},
		},
	}

	rec, err := a.buildArchive(ctx, "my-app", []string{sha}, files)
	if err != nil {
		t.Fatalf("buildArchive: %v", err)
	}

	raw, err := os.ReadFile(rec.Path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if bytes.Contains(raw, plaintext) || bytes.Contains(raw, secret) {
		t.Fatal("archive contains plaintext config data")
	}

	ar, err := a.openArchive(ctx, rec)
	if err != nil {
		t.Fatalf("openArchive: %v", err)
	}
	defer ar.Close()

	for filePath, want := range files[sha] {
		data, entry, err := ar.GetFile(archiveEntryPath(sha, filePath))
		if err != nil {
			t.Fatalf("GetFile(%q): %v", filePath, err)
		}
		if !bytes.Equal(data, want) {
			t.Errorf("GetFile(%q) data mismatch", filePath)
		}
		if !entry.IsEncrypted {
			t.Errorf("GetFile(%q) IsEncrypted = false, want true", filePath)
		}
	}
}
