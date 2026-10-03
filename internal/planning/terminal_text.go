package planning

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

type plannerTextFile struct{ file *os.File }
type textRange struct{ start, end int64 }
type parsedPlannerTerminal struct {
	body     *state.PlanningTextSource
	title    string
	question *StructuredQuestion
	files    []*plannerTextFile
}

func newPlannerTextFile() (*plannerTextFile, error) {
	f, err := os.CreateTemp("", "kingdom-planner-text-*")
	if err != nil {
		return nil, err
	}
	return &plannerTextFile{file: f}, nil
}
func (f *plannerTextFile) Close() error {
	if f == nil || f.file == nil {
		return nil
	}
	file := f.file
	f.file = nil
	return errors.Join(file.Close(), os.Remove(file.Name()))
}
func (p *parsedPlannerTerminal) Close() error {
	var err error
	for _, file := range p.files {
		err = errors.Join(err, file.Close())
	}
	return err
}
func (f *plannerTextFile) size() (int64, error) {
	info, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
func (f *plannerTextFile) source(ctx context.Context, span textRange) (*state.PlanningTextSource, error) {
	hash := sha256.New()
	if err := copyPlannerRange(ctx, hash, f.file, span); err != nil {
		return nil, err
	}
	return &state.PlanningTextSource{SHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: span.end - span.start, Open: func(ctx context.Context) (io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if f.file == nil {
			return nil, os.ErrClosed
		}
		return io.NopCloser(io.NewSectionReader(f.file, span.start, span.end-span.start)), nil
	}}, nil
}
func copyPlannerRange(ctx context.Context, out io.Writer, file *os.File, span textRange) error {
	writer := &plannerPromptWriter{ctx: ctx, out: out}
	_, err := io.CopyBuffer(writer, io.NewSectionReader(file, span.start, span.end-span.start), make([]byte, 64<<10))
	return err
}
func trimPlannerRange(ctx context.Context, file *os.File, span textRange) (textRange, error) {
	summary, err := providerexecution.InspectText(ctx, io.NewSectionReader(file, span.start, span.end-span.start))
	return textRange{span.start + summary.Offset, span.start + summary.End}, err
}

// parsePlannerTerminal stages verified provider bytes once, then processes
// prose incrementally. Only structured-question metadata and the title line
// are decoded as scalar values; a long prose line never becomes a Go string.
func parsePlannerTerminal(ctx context.Context, metadata ProviderExecutionMetadata, formalize bool, isRecognizedHeading func(string) bool) (result *parsedPlannerTerminal, err error) {
	result = &parsedPlannerTerminal{}
	defer func() {
		if err != nil {
			_ = result.Close()
		}
	}()
	original, err := newPlannerTextFile()
	if err != nil {
		return result, err
	}
	result.files = append(result.files, original)
	if metadata.StreamObservation.CanonicalTerminalTextRef != nil {
		err = providerexecution.CopyTerminalText(ctx, metadata.StreamObservation, original.file)
	} else {
		_, err = io.Copy(original.file, strings.NewReader(metadata.FinalText))
	}
	if err != nil {
		return result, err
	}
	size, err := original.size()
	if err != nil {
		return result, err
	}
	prose, err := newPlannerTextFile()
	if err != nil {
		return result, err
	}
	result.files = append(result.files, prose)
	block, err := newPlannerTextFile()
	if err != nil {
		return result, err
	}
	defer block.Close()
	reader := bufio.NewReaderSize(io.NewSectionReader(original.file, 0, size), 64<<10)
	out := bufio.NewWriterSize(prose.file, 64<<10)
	firstMatch := true
	useOriginal := false
	for {
		found, readErr := copyUntilPlannerToken(ctx, reader, out, structuredQuestionBlockOpen)
		if readErr != nil {
			return result, readErr
		}
		if !found {
			break
		}
		if err := block.file.Truncate(0); err != nil {
			return result, err
		}
		if _, err := block.file.Seek(0, io.SeekStart); err != nil {
			return result, err
		}
		blockOut := bufio.NewWriterSize(block.file, 64<<10)
		if _, err := blockOut.WriteString(structuredQuestionBlockOpen); err != nil {
			return result, err
		}
		newline := false
		for {
			b, readErr := reader.ReadByte()
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return result, readErr
			}
			if !strings.ContainsRune(" \t\r\n\f", rune(b)) {
				if err := reader.UnreadByte(); err != nil {
					return result, err
				}
				break
			}
			newline = newline || b == '\n'
			if err := blockOut.WriteByte(b); err != nil {
				return result, err
			}
		}
		if err := blockOut.Flush(); err != nil {
			return result, err
		}
		headerEnd, err := block.size()
		if err != nil {
			return result, err
		}
		if !newline {
			if formalize {
				return result, fmt.Errorf("formalized output contains an unterminated structured_question block")
			}
			if err := copyPlannerRange(ctx, out, block.file, textRange{0, headerEnd}); err != nil {
				return result, err
			}
			continue
		}
		closed, readErr := copyUntilPlannerToken(ctx, reader, blockOut, "\n```")
		if readErr != nil {
			return result, readErr
		}
		if err := blockOut.Flush(); err != nil {
			return result, err
		}
		blockEnd, err := block.size()
		if err != nil {
			return result, err
		}
		if !closed {
			if formalize {
				return result, fmt.Errorf("formalized output contains an unterminated structured_question block")
			}
			if err := copyPlannerRange(ctx, out, block.file, textRange{0, blockEnd}); err != nil {
				return result, err
			}
			break
		}
		if firstMatch || formalize {
			span, err := trimPlannerRange(ctx, block.file, textRange{headerEnd, blockEnd})
			if err != nil {
				return result, err
			}
			var question StructuredQuestion
			decoder := json.NewDecoder(io.NewSectionReader(block.file, span.start, span.end-span.start))
			decodeErr := decoder.Decode(&question)
			if decodeErr == nil {
				var extra any
				if decoder.Decode(&extra) != io.EOF {
					decodeErr = fmt.Errorf("trailing structured question data")
				}
			}
			valid := decodeErr == nil && strings.TrimSpace(question.Prompt) != "" && strings.TrimSpace(question.Type) != ""
			if !valid {
				if formalize {
					return result, fmt.Errorf("formalized output contains an invalid structured_question block")
				}
				useOriginal = true
				break
			}
			if firstMatch {
				result.question = &question
			}
		}
		firstMatch = false
	}
	if err := out.Flush(); err != nil {
		return result, err
	}
	if useOriginal {
		prose = original
		result.question = nil
	}
	size, err = prose.size()
	if err != nil {
		return result, err
	}
	span, err := trimPlannerRange(ctx, prose.file, textRange{0, size})
	if err != nil {
		return result, err
	}
	if formalize {
		result.title, span, err = parsePlannerPlan(ctx, prose.file, span, isRecognizedHeading)
		if err != nil {
			return result, err
		}
		result.question = nil
	} else if result.question != nil {
		span, err = stripPlannerTrailingQuestion(ctx, prose.file, span, result.question.Prompt)
		if err != nil {
			return result, err
		}
	}
	result.body, err = prose.source(ctx, span)
	return result, err
}

// ReadSlice emits a long delimiter-free prefix in bounded fragments. Peeking
// only the fixed token suffix handles fences split across any read boundary.
func copyUntilPlannerToken(ctx context.Context, r *bufio.Reader, out io.Writer, token string) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		part, err := r.ReadSlice(token[0])
		if len(part) > 0 && part[len(part)-1] == token[0] {
			peek, _ := r.Peek(len(token) - 1)
			if string(peek) == token[1:] {
				if _, err := out.Write(part[:len(part)-1]); err != nil {
					return false, err
				}
				_, err := r.Discard(len(token) - 1)
				return true, err
			}
		}
		if len(part) > 0 {
			if _, writeErr := out.Write(part); writeErr != nil {
				return false, writeErr
			}
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil && err != bufio.ErrBufferFull {
			return false, err
		}
	}
}

func visitPlannerLines(ctx context.Context, file *os.File, span textRange, visit func(textRange) error) error {
	r := bufio.NewReaderSize(io.NewSectionReader(file, span.start, span.end-span.start), 64<<10)
	start, offset := span.start, span.start
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		part, err := r.ReadSlice('\n')
		offset += int64(len(part))
		if err == bufio.ErrBufferFull {
			continue
		}
		end := offset
		if len(part) > 0 && part[len(part)-1] == '\n' {
			end--
		}
		if err != nil && err != io.EOF {
			return err
		}
		if end > start {
			if err := visit(textRange{start, end}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		start = offset
	}
}

func plannerRangePrefix(file *os.File, span textRange, n int) ([]byte, error) {
	n = min(n, int(span.end-span.start))
	raw := make([]byte, n)
	_, err := file.ReadAt(raw, span.start)
	return raw, err
}

func parsePlannerPlan(ctx context.Context, file *os.File, span textRange, isRecognizedHeading func(string) bool) (string, textRange, error) {
	var title string
	var titleSpan textRange
	body := textRange{span.end, span.end}
	first, active, content := true, false, false
	contentFound := errors.New("plan section content found")
	err := visitPlannerLines(ctx, file, span, func(line textRange) error {
		trimmed, err := trimPlannerRange(ctx, file, line)
		if err != nil {
			return err
		}
		if trimmed.start == trimmed.end {
			return nil
		}
		prefix, err := plannerRangePrefix(file, trimmed, 3)
		if err != nil {
			return err
		}
		if first {
			first = false
			if !bytes.HasPrefix(prefix, []byte("# ")) {
				return fmt.Errorf("formalized output must start with a level-one Markdown title")
			}
			titleSpan, err = trimPlannerRange(ctx, file, textRange{trimmed.start + 2, trimmed.end})
			if err != nil {
				return err
			}
			if titleSpan.start == titleSpan.end {
				return fmt.Errorf("formalized output title is empty")
			}
			body.start = min(line.end+1, span.end)
			return nil
		}
		if bytes.HasPrefix(prefix, []byte("## ")) {
			headingSpan, err := trimPlannerRange(ctx, file, textRange{trimmed.start + 3, trimmed.end})
			if err != nil {
				return err
			}
			// Only the finite section names are buffered; an arbitrary long
			// heading remains complete in the body and simply is not recognized.
			r := bufio.NewReader(io.NewSectionReader(file, headingSpan.start, headingSpan.end-headingSpan.start))
			var heading strings.Builder
			for heading.Len() <= 64 {
				ch, _, err := r.ReadRune()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				heading.WriteRune(unicode.ToLower(ch))
			}
			active = isRecognizedHeading != nil && isRecognizedHeading(heading.String())
			return nil
		}
		if active && !bytes.HasPrefix(prefix, []byte("#")) {
			content = true
			return contentFound
		}
		return nil
	})
	if errors.Is(err, contentFound) {
		err = nil
	}
	if err != nil {
		return "", span, err
	}
	if first {
		return "", span, fmt.Errorf("formalized output is empty")
	}
	if !content {
		return "", span, fmt.Errorf("formalized output must include a recognized plan section with content")
	}
	raw, err := io.ReadAll(io.NewSectionReader(file, titleSpan.start, titleSpan.end-titleSpan.start))
	if err != nil {
		return "", span, err
	}
	title = string(raw)
	body, err = trimPlannerRange(ctx, file, body)
	return title, body, err
}

func trimPlannerASCIIEnd(file *os.File, span textRange, cutset string) (textRange, error) {
	buffer := make([]byte, 64<<10)
	for span.end > span.start {
		n := min(int64(len(buffer)), span.end-span.start)
		raw := buffer[:n]
		if _, err := file.ReadAt(raw, span.end-n); err != nil {
			return span, err
		}
		i := len(raw)
		for i > 0 && strings.ContainsRune(cutset, rune(raw[i-1])) {
			i--
		}
		span.end -= int64(len(raw) - i)
		if i > 0 {
			break
		}
	}
	return span, nil
}

func plannerRangeMatchesQuestion(ctx context.Context, file *os.File, span textRange, normalized string) (bool, error) {
	span, err := trimPlannerRange(ctx, file, span)
	if err != nil {
		return false, err
	}
	if span.end-span.start >= 2 {
		first, err := plannerRangePrefix(file, span, 2)
		if err != nil {
			return false, err
		}
		last := make([]byte, 2)
		if _, err := file.ReadAt(last, span.end-2); err != nil {
			return false, err
		}
		if span.end-span.start >= 4 && string(first) == "**" && string(last) == "**" {
			span.start += 2
			span.end -= 2
		} else if (first[0] == '*' && last[1] == '*') || (first[0] == '_' && last[1] == '_') {
			span.start++
			span.end--
		}
	}
	span, err = trimPlannerASCIIEnd(file, span, "?.!:;")
	if err != nil {
		return false, err
	}
	span, err = trimPlannerRange(ctx, file, span)
	if err != nil {
		return false, err
	}
	r := bufio.NewReader(io.NewSectionReader(file, span.start, span.end-span.start))
	expected := strings.NewReader(normalized)
	for {
		a, _, ea := r.ReadRune()
		b, _, eb := expected.ReadRune()
		if ea == io.EOF || eb == io.EOF {
			return ea == io.EOF && eb == io.EOF, nil
		}
		if ea != nil {
			return false, ea
		}
		if eb != nil {
			return false, eb
		}
		if unicode.ToLower(a) != b {
			return false, nil
		}
	}
}

func stripPlannerTrailingQuestion(ctx context.Context, file *os.File, span textRange, prompt string) (textRange, error) {
	normalized := normalizeForPromptMatch(prompt)
	if normalized == "" {
		return span, nil
	}
	match, err := plannerRangeMatchesQuestion(ctx, file, span, normalized)
	if err != nil {
		return span, err
	}
	if match {
		return textRange{span.start, span.start}, nil
	}
	lastNL, lastParagraph := int64(-1), int64(-1)
	r := bufio.NewReaderSize(io.NewSectionReader(file, span.start, span.end-span.start), 64<<10)
	offset := span.start
	for {
		part, err := r.ReadSlice('\n')
		offset += int64(len(part))
		if len(part) > 0 && part[len(part)-1] == '\n' {
			at := offset - 1
			if at == lastNL+1 {
				lastParagraph = lastNL
			}
			lastNL = at
		}
		if err == io.EOF {
			break
		}
		if err != nil && err != bufio.ErrBufferFull {
			return span, err
		}
	}
	for _, candidate := range []struct{ at, skip int64 }{{lastParagraph, 2}, {lastNL, 1}} {
		if candidate.at < 0 {
			continue
		}
		match, err := plannerRangeMatchesQuestion(ctx, file, textRange{candidate.at + candidate.skip, span.end}, normalized)
		if err != nil {
			return span, err
		}
		if match {
			return trimPlannerASCIIEnd(file, textRange{span.start, candidate.at}, " \t\n\r")
		}
	}
	return span, nil
}
