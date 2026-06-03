package planning

import (
	"testing"
)

func TestStripTrailingQuestion(t *testing.T) {
	tests := []struct {
		name   string
		prose  string
		prompt string
		want   string
	}{
		{
			name:   "strips question from last paragraph",
			prose:  "Got it - cursor-based is more scalable.\n\nWhich pagination style should the endpoint use?",
			prompt: "Which pagination style should the endpoint use?",
			want:   "Got it - cursor-based is more scalable.",
		},
		{
			name:   "strips question from last line",
			prose:  "Context here.\nWhich style?",
			prompt: "Which style?",
			want:   "Context here.",
		},
		{
			name:   "returns empty when entire prose is the question",
			prose:  "Which style?",
			prompt: "Which style?",
			want:   "",
		},
		{
			name:   "handles punctuation differences",
			prose:  "Context.\n\nWhich style?",
			prompt: "Which style",
			want:   "Context.",
		},
		{
			name:   "handles bold markdown in prose",
			prose:  "Context.\n\n**Which style?**",
			prompt: "Which style?",
			want:   "Context.",
		},
		{
			name:   "handles case differences",
			prose:  "Context.\n\nwhich style?",
			prompt: "Which Style?",
			want:   "Context.",
		},
		{
			name:   "does not strip when prompt is not at the end",
			prose:  "Which style? Let me explain more context.",
			prompt: "Which style?",
			want:   "Which style? Let me explain more context.",
		},
		{
			name:   "does not strip partial matches",
			prose:  "Context.\n\nWhich pagination style should the endpoint use for best performance?",
			prompt: "Which pagination style should the endpoint use?",
			want:   "Context.\n\nWhich pagination style should the endpoint use for best performance?",
		},
		{
			name:   "returns prose unchanged when prompt is empty",
			prose:  "Hello world",
			prompt: "",
			want:   "Hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTrailingQuestion(tt.prose, tt.prompt)
			if got != tt.want {
				t.Errorf("stripTrailingQuestion(%q, %q) = %q, want %q", tt.prose, tt.prompt, got, tt.want)
			}
		})
	}
}

func TestParseStructuredQuestion_NormalizesTrailingPrompt(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantContent string
		wantPrompt  string
	}{
		{
			name: "strips trailing prompt from prose",
			input: "Got it - let me ask.\n\nWhich style?\n\n```structured_question\n" +
				`{"prompt":"Which style?","type":"single_select","options":[{"label":"A","value":"a"}]}` +
				"\n```",
			wantContent: "Got it - let me ask.",
			wantPrompt:  "Which style?",
		},
		{
			name: "returns empty content when prose is only the question",
			input: "Which style?\n\n```structured_question\n" +
				`{"prompt":"Which style?","type":"single_select","options":[{"label":"A","value":"a"}]}` +
				"\n```",
			wantContent: "",
			wantPrompt:  "Which style?",
		},
		{
			name: "preserves prose when prompt is not at the end",
			input: "Which style? Here is context.\n\n```structured_question\n" +
				`{"prompt":"Which style?","type":"single_select","options":[{"label":"A","value":"a"}]}` +
				"\n```",
			wantContent: "Which style? Here is context.",
			wantPrompt:  "Which style?",
		},
		{
			name:        "returns full text when no block present",
			input:       "Just some text without a block.",
			wantContent: "Just some text without a block.",
			wantPrompt:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content, question := parseStructuredQuestion(tt.input)
			if content != tt.wantContent {
				t.Errorf("content = %q, want %q", content, tt.wantContent)
			}
			if tt.wantPrompt == "" {
				if question != nil {
					t.Errorf("expected nil question, got %+v", question)
				}
			} else {
				if question == nil {
					t.Fatal("expected non-nil question")
				}
				if question.Prompt != tt.wantPrompt {
					t.Errorf("question.Prompt = %q, want %q", question.Prompt, tt.wantPrompt)
				}
			}
		})
	}
}

func TestProviderReceiptAcceptanceForPlannerPreservesTerminalInvalidation(t *testing.T) {
	t.Parallel()

	accepted := providerReceiptAcceptanceForPlanner(CompletionReceiptAcceptance{
		Rejected:              true,
		InvalidatesInvocation: true,
		RejectedReason:        "completion_capability_mismatch",
	})
	if !accepted.Rejected {
		t.Fatal("expected rejected receipt acceptance")
	}
	if !accepted.InvalidatesInvocation {
		t.Fatal("expected terminal receipt invalidation to be preserved for opencode")
	}
	if accepted.RejectedReason != "completion_capability_mismatch" {
		t.Fatalf("unexpected rejected reason %q", accepted.RejectedReason)
	}
}
