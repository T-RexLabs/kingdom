package workspace

import "testing"

func TestParseRunWorktreeRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "positive duration", raw: "90m"},
		{name: "never", raw: "never"},
		{name: "zero rejected", raw: "0s", wantErr: true},
		{name: "negative rejected", raw: "-5m", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseRunWorktreeRetention(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got duration %v", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse retention %q: %v", tt.raw, err)
			}
			if tt.raw == "never" && got != RunWorktreeRetentionNever {
				t.Fatalf("expected never retention sentinel, got %v", got)
			}
			if tt.raw == "90m" && got <= 0 {
				t.Fatalf("expected positive retention, got %v", got)
			}
		})
	}
}
