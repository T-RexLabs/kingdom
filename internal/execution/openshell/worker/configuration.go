package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"kingdom/internal/execution/remote"
)

// ConfigurationDigest binds the assembly manifest to the exact immutable
// operator runtime configuration, excluding only the injected clock.
func (w *Worker) ConfigurationDigest() string {
	raw, _ := json.Marshal(struct {
		Target           remote.Target
		Workspace, Image string
		Profile          []byte
		Egress           Egress
	}{w.config.Target, w.config.Workspace, w.config.Image, w.config.ProfileSHA256, w.config.Egress})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
