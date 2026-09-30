package planning

import "strings"

const structuredQuestionBlockOpen = "```structured_question"

type streamingQuestionSanitizer struct {
	onChunk func(text string)
	pending string
	blocked bool
}

func newStreamingQuestionSanitizer(onChunk func(text string)) *streamingQuestionSanitizer {
	if onChunk == nil {
		return nil
	}
	return &streamingQuestionSanitizer{onChunk: onChunk}
}

func (s *streamingQuestionSanitizer) Write(text string) {
	if s == nil || text == "" || s.blocked {
		return
	}
	data := s.pending + text
	visible := visibleStreamingPlannerText(data)
	if visible != "" {
		s.onChunk(visible)
	}
	if strings.Contains(data, structuredQuestionBlockOpen) {
		s.pending = ""
		s.blocked = true
		return
	}
	// Retain only a possible fixed-token suffix, never the response history.
	s.pending = strings.Clone(data[len(visible):])
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
