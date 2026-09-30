package trackedfs

import (
	"io/fs"
	"testing"
	"time"
)

type modeFileInfo struct{ mode fs.FileMode }

func (i modeFileInfo) Name() string       { return "file" }
func (i modeFileInfo) Size() int64        { return 1 }
func (i modeFileInfo) Mode() fs.FileMode  { return i.mode }
func (i modeFileInfo) ModTime() time.Time { return time.Time{} }
func (i modeFileInfo) IsDir() bool        { return false }
func (i modeFileInfo) Sys() any           { return nil }

func TestMetadataPreservesExactBasicPermissions(t *testing.T) {
	for _, mode := range []fs.FileMode{0, 0600, 0700, 0751, 0777, fs.ModeSetuid | 0700} {
		if got := MetadataFromInfo(modeFileInfo{mode}).ModeBits; got != int64(mode.Perm()) {
			t.Fatalf("captured mode %o from %v, want %o", got, mode, mode.Perm())
		}
	}
}
