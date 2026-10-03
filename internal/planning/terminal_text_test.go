package planning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

func TestReferencedPlannerReplyParsingDoesNotMaterializeWholeResponse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "outside terminal span\n"
	io.WriteString(file, prefix)
	hash := sha256.New()
	writer := io.MultiWriter(file, hash)
	chunk := strings.Repeat("body 界 ", 8192)
	var size int64
	for i := 0; i < 256; i++ {
		n, err := io.WriteString(writer, chunk)
		if err != nil {
			t.Fatal(err)
		}
		size += int64(n)
	}
	n, err := io.WriteString(writer, "terminal-tail")
	if err != nil {
		t.Fatal(err)
	}
	size += int64(n)
	io.WriteString(file, "\noutside suffix")
	file.Close()
	ref := providerexecution.ContentReference{InvocationID: "memory-test", SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: size}
	staged := providerexecution.StagedContent{Reference: ref, Path: path, Offset: int64(len(prefix))}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	parsed, err := parsePlannerTerminal(context.Background(), ProviderExecutionMetadata{StreamObservation: providerexecution.InvocationStreamObservation{CanonicalTerminalTextRef: &ref, TerminalTextOpener: staged.Open}}, false, nil)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	defer parsed.Close()
	if parsed.body.SizeBytes != size || parsed.body.SHA256 != ref.SHA256 {
		t.Fatal("referenced body was truncated or used the wrong byte span")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 8<<20 {
		t.Fatalf("parser allocated %d bytes for %d-byte response", allocated, size)
	}
}

func plannerTextForTest(t *testing.T, inline string, source *state.PlanningTextSource) string {
	t.Helper()
	if source == nil {
		return inline
	}
	reader, err := source.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func parseStructuredQuestion(text string) (string, *StructuredQuestion) {
	parsed, err := parsePlannerTerminal(context.Background(), ProviderExecutionMetadata{FinalText: text}, false, nil)
	if err != nil {
		panic(err)
	}
	defer parsed.Close()
	reader, err := parsed.body.Open(context.Background())
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	return string(raw), parsed.question
}

func parseFormalizedOutput(text string) (string, string, error) {
	catalog, err := state.CanonicalPromptCatalog()
	if err != nil {
		return "", "", err
	}
	parsed, err := parsePlannerTerminal(context.Background(), ProviderExecutionMetadata{FinalText: text}, true, catalog.IsRecognizedPlanSectionHeading)
	if err != nil {
		return "", "", err
	}
	defer parsed.Close()
	reader, err := parsed.body.Open(context.Background())
	if err != nil {
		return "", "", err
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	return parsed.title, string(raw), err
}

func stripTrailingQuestion(prose, prompt string) string {
	file, err := newPlannerTextFile()
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if _, err := io.WriteString(file.file, prose); err != nil {
		panic(err)
	}
	span := textRange{0, int64(len(strings.TrimRight(prose, " \t\n\r")))}
	result, err := stripPlannerTrailingQuestion(context.Background(), file.file, span, prompt)
	if err != nil {
		panic(err)
	}
	if result == span {
		return prose
	}
	return prose[result.start:result.end]
}

func TestStreamedPlannerParserPreservesSingleLongProseLine(t *testing.T) {
	body := strings.Repeat("complete 界 ", 200000) + "body-tail"
	question := "Proceed with the plan?"
	input := body + "\n\n" + question + "\n\n```structured_question\n{\"prompt\":\"" + question + "\",\"type\":\"yes_no\"}\n```"
	parsed, err := parsePlannerTerminal(context.Background(), ProviderExecutionMetadata{FinalText: input}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer parsed.Close()
	if got := plannerTextForTest(t, "", parsed.body); got != body {
		t.Fatalf("long reply body changed: got %d want %d", len(got), len(body))
	}
	if parsed.question == nil || parsed.question.Prompt != question {
		t.Fatal("structured question changed")
	}
	if err := parsed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := parsed.body.Open(context.Background()); err == nil {
		t.Fatal("parsed response source outlived its owner")
	}
}

func TestStreamedPlannerParserPreservesLiteralInlineFence(t *testing.T) {
	input := "prose ```structured_question\n{\"prompt\":\"Q\",\"type\":\"yes_no\"}```"
	body, question := parseStructuredQuestion(input)
	if body != input || question != nil {
		t.Fatal("literal inline fence was treated as a structured question")
	}
}

func TestStreamedFormalizeParserPreservesLongBodyAndHeadingWhitespace(t *testing.T) {
	body := "## " + strings.Repeat(" ", 100000) + "Objective\n" + strings.Repeat("scope 界 ", 200000) + "plan-tail"
	title, text, err := parseFormalizedOutput("# Complete Plan\n\n" + body)
	if err != nil || title != "Complete Plan" || text != body {
		t.Fatalf("streamed plan changed: title=%q err=%v", title, err)
	}
}
