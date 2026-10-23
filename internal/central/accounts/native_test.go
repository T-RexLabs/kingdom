package accounts

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestNativeAccountProcess uses a controlled credential in the real pinned
// OpenCode process. It never starts provider consent or calls model inference.
// Run the compiled test in the pinned worker image with network disabled.
func TestNativeAccountProcess(t *testing.T) {
	binary := os.Getenv("KINGDOM_TEST_ACCOUNT_OPENCODE")
	if binary == "" {
		t.Skip("requires explicit pinned OpenCode account fixture")
	}
	root := filepath.Join(t.TempDir(), "private-login")
	p, err := NewProcess(ProcessConfig{BinaryPath: binary, RuntimeRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if e := p.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	c, err := p.Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	err = c.callRaw(ctx, "POST", "/api/credential", map[string]any{"integrationID": CopilotProvider, "label": "controlled account fixture", "value": OAuthCredential{Type: "oauth", MethodID: "device", Access: "controlled-access-value", Refresh: "controlled-refresh-value", Expires: 0}}, &created)
	if err != nil {
		t.Fatal(err)
	}
	connections, err := c.Connections(ctx, CopilotProvider)
	if err != nil {
		t.Fatal(err)
	}
	if len(connections) != 1 || connections[0].ID == "" {
		t.Fatalf("expected one isolated controlled connection: %d", len(connections))
	}
	got, err := c.Resolve(ctx, CopilotProvider, connections[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Access != "controlled-access-value" || got.Refresh != "controlled-refresh-value" {
		t.Fatal("native credential differs")
	}
	if _, err = c.Resolve(ctx, CopilotProvider, "foreign-credential"); err == nil {
		t.Fatal("resolved foreign native connection")
	}
	if err = p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("native plaintext store remains after closure")
	}
}
