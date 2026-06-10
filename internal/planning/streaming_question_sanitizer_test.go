package planning

import (
	"strings"
	"testing"
)

func TestVisibleStreamingPlannerTextHidesStructuredQuestionProtocol(t *testing.T) {
	t.Parallel()

	raw := strings.Join([]string{
		"Context before the question.",
		"",
		"```structured_question",
		`{"prompt":"Choose scope","type":"single_select"}`,
		"```",
	}, "\n")

	if got := visibleStreamingPlannerText(raw); got != "Context before the question.\n\n" {
		t.Fatalf("expected protocol block to be hidden, got %q", got)
	}
}

func TestVisibleStreamingPlannerTextHidesTrailingPartialStructuredQuestionPrefix(t *testing.T) {
	t.Parallel()

	raw := "Context before the question.\n\n```str"
	if got := visibleStreamingPlannerText(raw); got != "Context before the question.\n\n" {
		t.Fatalf("expected trailing partial opener to be hidden, got %q", got)
	}
}

func TestStreamingQuestionSanitizerEmitsOnlyVisibleProse(t *testing.T) {
	t.Parallel()

	var chunks []string
	sanitizer := newStreamingQuestionSanitizer(func(text string) {
		chunks = append(chunks, text)
	})
	for _, chunk := range []string{
		"Context before the question.\n\n",
		"```structured_question\n",
		`{"prompt":"Choose scope","type":"single_select"}`,
		"\n```",
	} {
		sanitizer.Write(chunk)
	}
	got := strings.Join(chunks, "")
	if got != "Context before the question.\n\n" {
		t.Fatalf("expected sanitizer to emit only visible prose, got %q", got)
	}
	if strings.Contains(got, "structured_question") || strings.Contains(got, `{"prompt"`) {
		t.Fatalf("sanitized output leaked protocol details: %q", got)
	}
}

func TestStreamingQuestionSanitizerPreservesWhitespaceOnlyChunks(t *testing.T) {
	t.Parallel()

	var chunks []string
	sanitizer := newStreamingQuestionSanitizer(func(text string) {
		chunks = append(chunks, text)
	})
	for _, chunk := range []string{"Hello,", "\n\n", "planner."} {
		sanitizer.Write(chunk)
	}
	if got := strings.Join(chunks, ""); got != "Hello,\n\nplanner." {
		t.Fatalf("expected sanitizer to preserve whitespace-only chunks, got %q", got)
	}
}
