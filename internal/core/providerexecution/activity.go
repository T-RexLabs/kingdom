package providerexecution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"
)

const ActivityPreviewBytes = 4096

// ActivityContent carries provider-observed bytes to synchronous persistence.
// Open is process-local transport; only committed content references are replay
// authority. An empty available result is distinct from an absent result.
type ActivityContent struct {
	Availability string        `json:"availability"`
	Preview      *string       `json:"preview,omitempty"`
	ByteLength   *int64        `json:"byte_length,omitempty"`
	SHA256       string        `json:"sha256,omitempty"`
	ContentURL   string        `json:"content_url,omitempty"`
	Open         ContentOpener `json:"-"`
}

type ActivityObservation struct {
	Kind           string          `json:"kind"`
	ProviderPartID string          `json:"provider_part_id,omitempty"`
	ToolCallID     string          `json:"tool_call_id,omitempty"`
	InvocationID   string          `json:"invocation_id,omitempty"`
	OccurredAt     string          `json:"occurred_at,omitempty"`
	ToolName       string          `json:"tool_name,omitempty"`
	ToolStatus     string          `json:"tool_status,omitempty"`
	ToolTitle      string          `json:"tool_title,omitempty"`
	Input          ActivityContent `json:"input"`
	Output         ActivityContent `json:"output"`
	Reasoning      ActivityContent `json:"reasoning"`
	Message        ActivityContent `json:"message"`
	Label          string          `json:"label,omitempty"`
	Summary        string          `json:"summary,omitempty"`
}

// ContentSources must be consumed synchronously while the observation's owner
// retains its staged content. It never transfers cleanup ownership.
func (a ActivityObservation) ContentSources() map[string]ActivityContent {
	return map[string]ActivityContent{"input": a.Input, "output": a.Output, "reasoning": a.Reasoning, "message": a.Message}
}

func (c ActivityContent) PreviewText() string {
	if c.Preview == nil {
		return ""
	}
	return *c.Preview
}

func (c ActivityContent) Normalized() ActivityContent {
	if c.Availability == "" {
		c.Availability = "not_provided"
	}
	return c
}

func (c ActivityContent) Validate() error {
	c = c.Normalized()
	switch c.Availability {
	case "available":
		if c.Open == nil || c.ByteLength == nil || c.Preview == nil {
			return fmt.Errorf("available activity content requires exact bytes and a preview")
		}
		if len(*c.Preview) > ActivityPreviewBytes || !utf8.ValidString(*c.Preview) || int64(len(*c.Preview)) > *c.ByteLength {
			return fmt.Errorf("activity preview is invalid")
		}
		return (ContentReference{InvocationID: "activity", SHA256: c.SHA256, SizeBytes: *c.ByteLength}).Validate("activity")
	case "not_provided", "unavailable":
		if c.Open != nil || c.ByteLength != nil || c.Preview != nil || c.SHA256 != "" || c.ContentURL != "" {
			return fmt.Errorf("unavailable activity content cannot claim observed bytes")
		}
		return nil
	default:
		return fmt.Errorf("unknown activity content availability")
	}
}

func ActivityText(text string) ActivityContent {
	ref := ReferenceText("activity", text)
	preview := activityPreview(text)
	return ActivityContent{Availability: "available", Preview: &preview, ByteLength: &ref.SizeBytes, SHA256: ref.SHA256,
		Open: func(ctx context.Context) (io.ReadCloser, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return io.NopCloser(strings.NewReader(text)), nil
		}}
}

func activityPreview(text string) string {
	if len(text) <= ActivityPreviewBytes {
		return text
	}
	end := ActivityPreviewBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}

// StageActivityContent consumes a complete observed value with bounded memory.
// The owner must retain its cleanup until every synchronous consumer returns.
func StageActivityContent(ctx context.Context, source io.Reader) (ActivityContent, func() error, error) {
	file, err := os.CreateTemp("", "kingdom-activity-content-*")
	if err != nil {
		return ActivityContent{}, nil, err
	}
	cleanup := func() error { return errors.Join(file.Close(), os.Remove(file.Name())) }
	hash := sha256.New()
	var preview strings.Builder
	buffer := make([]byte, ContentChunkBytes)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			_ = cleanup()
			return ActivityContent{}, nil, err
		}
		n, readErr := source.Read(buffer)
		if n > 0 {
			if _, err := io.MultiWriter(file, hash).Write(buffer[:n]); err != nil {
				_ = cleanup()
				return ActivityContent{}, nil, err
			}
			size += int64(n)
			if preview.Len() < ActivityPreviewBytes+utf8.UTFMax {
				preview.Write(buffer[:min(n, ActivityPreviewBytes+utf8.UTFMax-preview.Len())])
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = cleanup()
			return ActivityContent{}, nil, readErr
		}
	}
	ref := ContentReference{InvocationID: "activity", SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: size}
	staged := StagedContent{Reference: ref, Path: file.Name()}
	text := activityPreview(preview.String())
	content := ActivityContent{Availability: "available", Preview: &text, ByteLength: &size, SHA256: ref.SHA256, Open: staged.Open}
	return content, cleanup, nil
}
