package projectruntime

import (
	"context"
	"kingdom/internal/api"
	"kingdom/internal/providers/opencode"
	"testing"
)

func TestLocalManagedRequiresInitializedExplicitOwnership(t *testing.T) {
	for _, cfg := range []Config{{Mode: LocalManaged}, {Mode: LocalManaged, ProviderService: &opencode.Service{}}, {Mode: LocalManaged, ProviderService: &opencode.ManagedService{}, LocalSelection: &api.LocalSelectionConfig{}, OperatorCredential: []byte("operator")}, {Mode: Local, LocalSelection: &api.LocalSelectionConfig{}}} {
		if _, err := New(context.Background(), cfg); err == nil {
			t.Fatal("implicit local managed configuration accepted")
		}
	}
}
