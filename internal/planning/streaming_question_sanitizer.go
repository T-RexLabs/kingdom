package planning

import "strings"

const structuredQuestionBlockOpen = "```structured_question"

type streamingQuestionSanitizer struct {
	onChunk func(text string)
	raw     strings.Builder
	emitted int
}

func newStreamingQuestionSanitizer(onChunk func(text string)) *streamingQuestionSanitizer {
	if onChunk == nil {
		return nil
	}
	return &streamingQuestionSanitizer{onChunk: onChunk}
}

func (s *streamingQuestionSanitizer) Write(text string) {
	if s == nil || text == "" {
		return
	}
	_, _ = s.raw.WriteString(text)
	visible := visibleStreamingPlannerText(s.raw.String())
	if len(visible) <= s.emitted {
		return
	}
	s.onChunk(visible[s.emitted:])
	s.emitted = len(visible)
}

func visibleStreamingPlannerText(raw string) string {
	if blockStart := strings.Index(raw, structuredQuestionBlockOpen); blockStart >= 0 {
		return raw[:blockStart]
	}
	if partialStart := trailingStructuredQuestionPrefixStart(raw); partialStart >= 0 {
		return raw[:partialStart]
	}
	return raw
}

func trailingStructuredQuestionPrefixStart(raw string) int {
	maxPartialLength := min(len(raw), len(structuredQuestionBlockOpen)-1)
	for length := maxPartialLength; length > 0; length-- {
		suffix := raw[len(raw)-length:]
		if strings.HasPrefix(structuredQuestionBlockOpen, suffix) {
			return len(raw) - length
		}
	}
	return -1
}
