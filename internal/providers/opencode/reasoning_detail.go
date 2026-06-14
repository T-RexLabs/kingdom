package opencode

import "strings"

func reasoningDetailFromMetadata(redacted string, encrypted string) string {
	if strings.TrimSpace(encrypted) != "" {
		return "The provider emitted a reasoning step here, but only encrypted reasoning metadata is available."
	}
	if strings.TrimSpace(redacted) != "" {
		return "Reasoning was emitted, but only sanitized reasoning metadata is available for this step."
	}
	return ""
}
