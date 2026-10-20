package providerexecution

import (
	"fmt"
	"path/filepath"
	"strings"
)

// RequestIdentity identifies one immutable request on a shared provider service.
// It makes no assertion about the server process or workspace resource closure.
type RequestIdentity struct {
	InstanceID string `json:"instanceID"`
	Generation string `json:"generation"`
	Location   string `json:"location"`
	SessionID  string `json:"sessionID"`
	ProfileID  string `json:"profileID"`
	RequestID  string `json:"requestID"`
}

func (id RequestIdentity) Validate() error {
	for _, value := range []string{id.InstanceID, id.Generation, id.Location, id.SessionID, id.ProfileID, id.RequestID} {
		if value == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("complete provider request identity is required")
		}
	}
	if !filepath.IsAbs(id.Location) {
		return fmt.Errorf("provider request location must be absolute")
	}
	return nil
}
