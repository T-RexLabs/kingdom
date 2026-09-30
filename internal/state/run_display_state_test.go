package state

import "testing"

func TestDeriveRunDisplayState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    string
		planState string
		want      string
	}{
		{name: "awaiting final decision", status: "awaiting_human_confirmation", planState: "approved", want: "awaiting_final_decision"},
		{name: "awaiting plan decision", status: "running", planState: "awaiting_plan_decision", want: "awaiting_plan_decision"},
		{name: "pending", status: "pending", planState: "draft", want: "pending"},
		{name: "running", status: "running", planState: "approved", want: "running"},
		{name: "succeeded", status: "succeeded", planState: "approved", want: "succeeded"},
		{name: "failed", status: "failed", planState: "approved", want: "failed"},
		{name: "cancelled", status: "cancelled", planState: "approved", want: "cancelled"},
		{name: "fallback", status: "unknown", planState: "unknown", want: "running"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := DeriveRunDisplayState(tt.status, tt.planState); got != tt.want {
				t.Fatalf("DeriveRunDisplayState(%q, %q) = %q, want %q", tt.status, tt.planState, got, tt.want)
			}
		})
	}
}
