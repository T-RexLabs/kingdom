package orchestration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/workflow"
	"kingdom/internal/securefs"
)

// promptDocument is a small composition tree over replayable sources. Only
// bounded configuration/labels live in strings; content is prepared on disk.
type promptDocument struct {
	text, title, separator, fallback string
	open                             providerexecution.ContentOpener
	children                         []promptDocument
	strip, sanitize, dropComposed    bool
}

func promptLiteral(text string) promptDocument { return promptDocument{text: strings.TrimSpace(text)} }
func promptSource(open providerexecution.ContentOpener) promptDocument {
	return promptDocument{open: open}
}
func promptJoin(separator string, parts ...promptDocument) promptDocument {
	return promptDocument{separator: separator, children: parts}
}
func promptSection(title string, body promptDocument) promptDocument { body.title = title; return body }
func promptInstructions(parts ...promptDocument) promptDocument {
	return promptJoin("\n\n"+instructionSectionSeparator+"\n\n", parts...)
}
func promptFiltered(body promptDocument) promptDocument { body.strip = true; return body }

type promptFile struct {
	*os.File
	size int64
	sha  string
}

func (f *promptFile) Close() error { return errors.Join(f.File.Close(), os.Remove(f.Name())) }
func (f *promptFile) source() providerexecution.ContentOpener {
	return func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(io.NewSectionReader(f.File, 0, f.size)), nil
	}
}

func preparePromptDocument(ctx context.Context, doc promptDocument) (result *promptFile, err error) {
	f, err := os.CreateTemp("", "kingdom-prompt-*")
	if err != nil {
		return nil, err
	}
	result = &promptFile{File: f}
	defer func() {
		if err != nil {
			result.Close()
		}
	}()
	if _, err = renderPromptDocument(ctx, f, doc); err != nil {
		return result, err
	}
	result.size, err = f.Seek(0, io.SeekCurrent)
	if err != nil {
		return result, err
	}
	h := sha256.New()
	if _, err = securefs.Copy(ctx, h, io.NewSectionReader(f, 0, result.size)); err != nil {
		return result, err
	}
	result.sha = hex.EncodeToString(h.Sum(nil))
	return result, nil
}

func renderPromptDocument(ctx context.Context, dst *os.File, doc promptDocument) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	start, err := dst.Seek(0, io.SeekCurrent)
	if err != nil {
		return false, err
	}
	rollback := func(offset int64) error {
		if err := dst.Truncate(offset); err != nil {
			return err
		}
		_, err := dst.Seek(offset, io.SeekStart)
		return err
	}
	if doc.title != "" {
		if _, err := io.WriteString(dst, strings.TrimSpace(doc.title)+":\n"); err != nil {
			return false, err
		}
	}
	bodyStart, err := dst.Seek(0, io.SeekCurrent)
	if err != nil {
		return false, err
	}
	present := false
	if doc.open != nil {
		reader, err := doc.open(ctx)
		if err != nil {
			return false, err
		}
		_, copyErr := securefs.Copy(ctx, dst, reader)
		closeErr := reader.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return false, err
		}
	} else if doc.text != "" {
		if _, err := io.WriteString(dst, doc.text); err != nil {
			return false, err
		}
	}
	for _, child := range doc.children {
		before, err := dst.Seek(0, io.SeekCurrent)
		if err != nil {
			return false, err
		}
		if present {
			if _, err := io.WriteString(dst, doc.separator); err != nil {
				return false, err
			}
		}
		nonempty, err := renderPromptDocument(ctx, dst, child)
		if err != nil {
			return false, err
		}
		if nonempty {
			present = true
		} else if err := rollback(before); err != nil {
			return false, err
		}
	}
	end, err := dst.Seek(0, io.SeekCurrent)
	if err != nil {
		return false, err
	}
	// The same Unicode TrimSpace rule applies to every body, before and after
	// control filtering. Sections are omitted only after the source is verified.
	summary, err := providerexecution.InspectText(ctx, io.NewSectionReader(dst, bodyStart, end-bodyStart))
	if err != nil {
		return false, err
	}
	if doc.strip || doc.sanitize || doc.dropComposed || summary.Offset > 0 || summary.End < end-bodyStart {
		filtered, err := os.CreateTemp("", "kingdom-prompt-filter-*")
		if err != nil {
			return false, err
		}
		defer func() { filtered.Close(); os.Remove(filtered.Name()) }()
		if err := copyPromptTransform(ctx, filtered, dst, bodyStart+summary.Offset, summary.End-summary.Offset, doc); err != nil {
			return false, err
		}
		info, err := filtered.Stat()
		if err != nil {
			return false, err
		}
		trimmed, err := providerexecution.InspectText(ctx, io.NewSectionReader(filtered, 0, info.Size()))
		if err != nil {
			return false, err
		}
		if err := rollback(bodyStart); err != nil {
			return false, err
		}
		if _, err := securefs.Copy(ctx, dst, io.NewSectionReader(filtered, trimmed.Offset, trimmed.End-trimmed.Offset)); err != nil {
			return false, err
		}
		end, err = dst.Seek(0, io.SeekCurrent)
		if err != nil {
			return false, err
		}
	}
	if end == bodyStart && doc.fallback != "" {
		if _, err := io.WriteString(dst, doc.fallback); err != nil {
			return false, err
		}
		end, err = dst.Seek(0, io.SeekCurrent)
		if err != nil {
			return false, err
		}
	}
	if end == bodyStart {
		return false, rollback(start)
	}
	return true, nil
}

func copyPromptTransform(ctx context.Context, dst io.Writer, source io.ReaderAt, offset, size int64, doc promptDocument) error {
	if doc.strip {
		f, err := os.CreateTemp("", "kingdom-prompt-lines-*")
		if err != nil {
			return err
		}
		defer func() { f.Close(); os.Remove(f.Name()) }()
		if err := stripPromptControlLines(ctx, f, source, offset, size); err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			return err
		}
		source = f
		offset = 0
		size = info.Size()
	}
	if doc.dropComposed {
		found, err := promptContains(ctx, io.NewSectionReader(source, offset, size), []string{"Primary Rework Plan:", "Approved Plan Baseline:", "Failure Context:", "Scoped Delegator Instruction:"})
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}
	if doc.sanitize {
		return sanitizePromptTokens(ctx, dst, source, offset, size)
	}
	_, err := securefs.Copy(ctx, dst, io.NewSectionReader(source, offset, size))
	return err
}

type promptControlPrefix struct {
	prefix        []byte
	word          string
	decided, drop bool
}

func asciiPromptSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\f' || b == '\n'
}
func (p *promptControlPrefix) accept(data []byte) {
	for _, b := range data {
		if p.decided {
			return
		}
		if len(p.prefix) == 0 && asciiPromptSpace(b) {
			continue
		}
		if p.word != "" {
			if asciiPromptSpace(b) {
				continue
			}
			p.drop = b == ':'
			p.decided = true
			return
		}
		if b >= 'a' && b <= 'z' {
			b -= 32
		}
		p.prefix = append(p.prefix, b)
		candidate := string(p.prefix)
		if candidate == "VERDICT" || candidate == "ROYAL_VERDICT" {
			p.word = candidate
			continue
		}
		if !strings.HasPrefix("VERDICT", candidate) && !strings.HasPrefix("ROYAL_VERDICT", candidate) {
			p.decided = true
			return
		}
	}
}
func stripPromptControlLines(ctx context.Context, dst io.Writer, source io.ReaderAt, offset, size int64) error {
	r := bufio.NewReaderSize(io.NewSectionReader(source, offset, size), 64*1024)
	out := bufio.NewWriterSize(dst, 64*1024)
	var lineStart, lineSize, written int64
	prefix := promptControlPrefix{}
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		part, err := r.ReadSlice('\n')
		previousSize := lineSize
		prefix.accept(part)
		lineSize += int64(len(part))
		if !prefix.drop && (prefix.decided || err != bufio.ErrBufferFull) {
			if written == 0 && previousSize > 0 {
				if _, copyErr := io.CopyBuffer(out, promptContextReader{ctx, io.NewSectionReader(source, offset+lineStart, previousSize)}, buffer); copyErr != nil {
					return copyErr
				}
			}
			if _, writeErr := out.Write(part); writeErr != nil {
				return writeErr
			}
			written = lineSize
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		lineStart += lineSize
		lineSize = 0
		written = 0
		prefix = promptControlPrefix{}
		if err == io.EOF {
			return out.Flush()
		}
	}
}
func promptContains(ctx context.Context, r io.Reader, needles []string) (bool, error) {
	buf := make([]byte, 64*1024)
	carry := []byte{}
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		n, err := r.Read(buf)
		data := append(carry, buf[:n]...)
		for _, needle := range needles {
			if bytes.Contains(data, []byte(needle)) {
				return true, nil
			}
		}
		keep := min(len(data), 64)
		carry = append(carry[:0], data[len(data)-keep:]...)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}
func asciiPromptWord(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
func sanitizePromptTokens(ctx context.Context, dst io.Writer, source io.ReaderAt, offset, size int64) error {
	out := bufio.NewWriterSize(dst, 64*1024)
	r := bufio.NewReaderSize(io.NewSectionReader(source, offset, size), 64*1024)
	var position, copied int64
	previousWord := false
	buffer := make([]byte, 64*1024)
	for {
		if position%(64*1024) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		ch, err := r.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		start := position
		position++
		upper := ch
		if upper >= 'a' && upper <= 'z' {
			upper -= 32
		}
		if previousWord || (upper != 'V' && upper != 'R') {
			previousWord = asciiPromptWord(ch)
			continue
		}
		word := "VERDICT"
		replacement := "verdict token "
		if upper == 'R' {
			word = "ROYAL_VERDICT"
			replacement = "royal closure verdict token "
		}
		matched := true
		for i := 1; i < len(word); i++ {
			next, err := r.ReadByte()
			if err == io.EOF {
				matched = false
				break
			}
			if err != nil {
				return err
			}
			position++
			previousWord = asciiPromptWord(next)
			if next >= 'a' && next <= 'z' {
				next -= 32
			}
			if next != word[i] {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		previousWord = true
		for {
			if position%(64*1024) == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			next, err := r.ReadByte()
			if err == io.EOF {
				matched = false
				break
			}
			if err != nil {
				return err
			}
			position++
			if asciiPromptSpace(next) {
				previousWord = false
				continue
			}
			if next != ':' {
				_ = r.UnreadByte()
				position--
				matched = false
			}
			break
		}
		if !matched {
			continue
		}
		if _, err := io.CopyBuffer(out, promptContextReader{ctx, io.NewSectionReader(source, offset+copied, start-copied)}, buffer); err != nil {
			return err
		}
		if _, err := io.WriteString(out, replacement); err != nil {
			return err
		}
		copied = position
		previousWord = false
	}
	_, err := io.CopyBuffer(out, promptContextReader{ctx, io.NewSectionReader(source, offset+copied, size-copied)}, buffer)
	if err != nil {
		return err
	}
	return out.Flush()
}

type promptContextReader struct {
	ctx context.Context
	io.Reader
}

func (r promptContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func (o *orchestrationLoop) buildNodeContextDocument(ctx context.Context, runID, nodeID, nodeClass, delegatorRole, finalChecks string, workOrder, plan, guidance promptDocument) promptDocument {
	classID := resolvedNodeClass(nodeID, nodeClass)
	workOrder = promptFiltered(workOrder)
	workOrder.fallback = "No work order text was provided."
	blocks := []promptDocument{
		promptSection("Current Assignment", promptLiteral(currentAssignmentForNodeClass(classID, delegatorRole))),
		promptSection("Current Work Order", workOrder),
	}
	if classID != workflow.NodeClassDelegatedExecution {
		blocks = append(blocks, promptSection("Approved Plan", promptFiltered(plan)))
	}
	builderEvidence := ""
	if classID == workflow.NodeClassExecutionReview || classID == workflow.NodeClassValidationGates {
		builderEvidence = o.builderEvidenceManifestForRun(ctx, runID)
	}
	blocks = append(blocks,
		promptSection("Upstream Guidance", promptFiltered(guidance)),
		promptSection("Builder Evidence Summary", promptLiteral(builderEvidence)),
	)
	return promptInstructions(
		promptSection("Run Context", promptJoin("\n\n", blocks...)),
		promptLiteral(finalChecks),
	)
}
