package planning

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

type OpenCodeAgent struct {
	executor    plannerExecutor
	idGenerator func(prefix string) string
	limits      requestLimits
}

type requestLimits struct{ firstEvent, inactivity time.Duration }

// AgentOption configures an OpenCodeAgent.
type AgentOption func(*OpenCodeAgent)

// WithRequestLimits interrupts a request that stays silent: before its first
// event for first, and between events for inactivity. Zero disables a limit.
func WithRequestLimits(first, inactivity time.Duration) AgentOption {
	return func(a *OpenCodeAgent) { a.limits = requestLimits{firstEvent: first, inactivity: inactivity} }
}

// plannerExecutor runs a request in two phases: Prepare fixes the provider
// identity without sending work, and Execute sends it only after the caller has
// bound that identity to the canonical invocation.
type plannerExecutor interface {
	Prepare(context.Context, opencode.ExecuteRequest) (opencode.Prepared, error)
	Execute(context.Context, opencode.ExecuteRequest) (opencode.ExecuteResponse, error)
	Discard(context.Context, string) error
}

// NewOpenCodeAgent borrows the executor, which is shared with run execution and
// model discovery. The assembly that created it closes it after all consumers
// have drained.
func NewOpenCodeAgent(executor plannerExecutor, idGenerator func(prefix string) string, options ...AgentOption) *OpenCodeAgent {
	agent := &OpenCodeAgent{executor: executor, idGenerator: idGenerator}
	for _, option := range options {
		option(agent)
	}
	return agent
}

func (a *OpenCodeAgent) ContinueTurnStream(ctx context.Context, input AgentContext, onChunk func(text string)) (AgentTurn, error) {
	prompts := promptWriters{bootstrap: writeContinuePrompt, continuation: writeContinuationPrompt}
	sanitizer := newStreamingQuestionSanitizer(onChunk)
	streamChunk := onChunk
	if sanitizer != nil {
		streamChunk = sanitizer.Write
	}
	metadata, err := a.execute(ctx, input, prompts, input.ForceNewSession, "reply", streamChunk)
	if err != nil {
		return AgentTurn{}, err
	}
	parsed, err := parsePlannerTerminal(ctx, metadata, false, nil)
	if err != nil {
		return AgentTurn{Metadata: metadata}, executionError(metadata, err)
	}
	return AgentTurn{
		ContentSource: parsed.body,
		closeContent:  parsed.Close,
		Question:      parsed.question,
		Metadata:      metadata,
	}, nil
}

func (a *OpenCodeAgent) GenerateTitle(ctx context.Context, input AgentContext, onChunk func(text string)) (GeneratedTitle, error) {
	metadata, err := a.execute(ctx, input, promptWriters{bootstrap: writeTitlePrompt}, true, "generate_title", onChunk)
	if err != nil {
		return GeneratedTitle{}, fmt.Errorf("generate title: %w", err)
	}
	titleText := metadata.FinalText
	if metadata.StreamObservation.CanonicalTerminalTextRef != nil {
		var out strings.Builder
		if err := providerexecution.CopyTerminalText(ctx, metadata.StreamObservation, &out); err != nil {
			return GeneratedTitle{Metadata: metadata}, executionError(metadata, err)
		}
		titleText = out.String()
	}
	title, err := normalizeGeneratedTitle(titleText)
	if err != nil {
		return GeneratedTitle{Metadata: metadata}, executionError(metadata, err)
	}
	return GeneratedTitle{Title: title, Metadata: metadata}, nil
}

func (a *OpenCodeAgent) FormalizePlanStream(ctx context.Context, input AgentContext, onChunk func(text string)) (FormalizedPlan, error) {
	prompts := promptWriters{bootstrap: writeFormalizePrompt, continuation: writeFormalizeContinuationPrompt}
	sanitizer := newStreamingQuestionSanitizer(onChunk)
	streamChunk := onChunk
	if sanitizer != nil {
		streamChunk = sanitizer.Write
	}
	metadata, err := a.execute(ctx, input, prompts, input.ForceNewSession, "formalize", streamChunk)
	if err != nil {
		return FormalizedPlan{}, err
	}

	parsed, err := parsePlannerTerminal(ctx, metadata, true, input.PromptCatalog.IsRecognizedPlanSectionHeading)
	if err != nil {
		return FormalizedPlan{Metadata: metadata}, executionError(metadata, fmt.Errorf("parse formalized plan: %w", err))
	}
	return FormalizedPlan{
		WorkOrderTitle: parsed.title,
		TextSource:     parsed.body,
		closeContent:   parsed.Close,
		Metadata:       metadata,
	}, nil
}

// promptWriters render the two inputs a request can submit: the complete
// bootstrap for a new provider session, and only the new input for a session
// that already holds the earlier transcript.
type promptWriters struct {
	bootstrap    func(context.Context, io.Writer, AgentContext) error
	continuation func(context.Context, io.Writer, AgentContext) error
}

func (a *OpenCodeAgent) execute(ctx context.Context, input AgentContext, prompts promptWriters, forceNew bool, mode string, onChunk func(text string)) (ProviderExecutionMetadata, error) {
	if a.executor == nil {
		return ProviderExecutionMetadata{}, fmt.Errorf("opencode executor not available")
	}
	if input.PromptCatalog.Version != 1 {
		return ProviderExecutionMetadata{}, fmt.Errorf("pinned planner prompt catalog is required")
	}
	providerInvocation := input.ProviderInvocation
	if providerInvocation == nil || providerInvocation.Activate == nil {
		return ProviderExecutionMetadata{}, fmt.Errorf("a durable planner provider invocation is required")
	}
	systemPrompt, err := input.PromptCatalog.PlanningSystemPrompt(input.VoiceInstruction, input.GuidanceInstruction, mode)
	if err != nil {
		return ProviderExecutionMetadata{}, fmt.Errorf("build planner system prompt: %w", err)
	}
	attemptID := a.id("planner_attempt")
	if strings.TrimSpace(providerInvocation.AttemptID) != "" {
		attemptID = strings.TrimSpace(providerInvocation.AttemptID)
	}
	invocationID := strings.TrimSpace(providerInvocation.InvocationID)
	if invocationID == "" {
		invocationID = a.id("planner_invocation")
	}
	// A linked issue is the subject the operator wants to work through, not a
	// hostile payload, and understanding one needs the planner's ordinary
	// capabilities: reading the repository, researching references, and talking
	// it over. Turns carrying external context therefore run under the same
	// session policy as every other planner turn — no per-turn override here.
	//
	// The binding check stays: external text without its immutable snapshot
	// bindings (or the reverse) means the operation was assembled wrong, and
	// that fails closed regardless of policy.
	hasExternalContext := strings.TrimSpace(input.ExternalContext) != "" || input.WriteExternalContext != nil
	hasExternalSnapshots := len(input.ExternalSnapshotIDs) > 0
	if hasExternalContext != hasExternalSnapshots {
		return ProviderExecutionMetadata{}, fmt.Errorf("external planner context requires matching immutable snapshot bindings")
	}
	reusable := strings.TrimSpace(providerInvocation.ReusableProviderSessionID)
	if forceNew {
		reusable = ""
	}
	// Complete and verify immutable inputs before provider dispatch. Prompt
	// staging is disk-backed and replayable; no final aggregate string is made.
	bootstrap, err := stagePrompt(ctx, prompts.bootstrap, input)
	if err != nil {
		return ProviderExecutionMetadata{}, fmt.Errorf("stage planner prompt: %w", err)
	}
	defer bootstrap.Close()
	request := opencode.ExecuteRequest{
		CompletionMode:          opencode.CompletionModeServerResponse,
		AttemptID:               attemptID,
		InvocationID:            invocationID,
		Model:                   input.ModelSelection.Model,
		Variant:                 input.ModelSelection.ThinkingEffort,
		Agent:                   "planning-assistant",
		System:                  systemPrompt,
		KeepSession:             mode != "generate_title",
		ProviderSessionID:       reusable,
		ForceNewSession:         reusable == "",
		WorkspacePath:           input.ProjectRoot,
		FirstEventTimeout:       a.limits.firstEvent,
		StreamInactivityTimeout: a.limits.inactivity,
		OpenPrompt:              bootstrap.Open,
		OnProgress:              onChunk,
		OnActivity: func(observation opencode.ExecuteActivityObservation) {
			if providerInvocation.OnActivity != nil {
				providerInvocation.OnActivity(observation)
			}
		},
	}
	if reusable != "" {
		continuation, err := stagePrompt(ctx, prompts.continuation, input)
		if err != nil {
			return ProviderExecutionMetadata{}, fmt.Errorf("stage planner continuation prompt: %w", err)
		}
		defer continuation.Close()
		request.OpenContinuationPrompt = continuation.Open
	}

	prepared, err := a.executor.Prepare(ctx, request)
	if err != nil {
		return ProviderExecutionMetadata{}, fmt.Errorf("prepare opencode request: %w", err)
	}
	if err := providerInvocation.Activate(ctx, prepared); err != nil {
		discardCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return ProviderExecutionMetadata{}, errors.Join(fmt.Errorf("activate planner provider invocation: %w", err), a.executor.Discard(discardCtx, invocationID))
	}
	resp, err := a.executor.Execute(ctx, request)
	metadata := ProviderExecutionMetadata{
		FinalText:               strings.TrimSpace(resp.OutputText),
		ProviderSessionID:       strings.TrimSpace(resp.ProviderSessionID),
		ProviderDiagnosticsJSON: strings.TrimSpace(resp.ProviderDiagnosticsJSON),
		OutcomeState:            strings.TrimSpace(resp.Outcome.State),
		FailureReasonCode:       strings.TrimSpace(resp.Outcome.ReasonCode),
		FailureReasonText:       strings.TrimSpace(resp.Outcome.FailureText),
		StreamObservation:       resp.StreamObservation,
	}
	if resp.StreamObservation.CanonicalTerminalTextRef != nil {
		metadata.FinalText = ""
	}
	if err != nil {
		return metadata, executionError(metadata, fmt.Errorf("opencode execute: %w", err))
	}
	if err := providerexecution.ValidateServerResponseSuccess(resp.StreamObservation); err != nil {
		return metadata, executionError(metadata, fmt.Errorf("invalid planner terminal observation: %w", err))
	}
	if err := requirePlannerTurnSettled(metadata); err != nil {
		return metadata, executionError(metadata, err)
	}
	return metadata, nil
}

// stagedPrompt is a prompt held on disk so Execute can read it as many times as
// it needs without a final aggregate string.
type stagedPrompt struct{ file *os.File }

func stagePrompt(ctx context.Context, write func(context.Context, io.Writer, AgentContext) error, input AgentContext) (*stagedPrompt, error) {
	if write == nil {
		return nil, fmt.Errorf("planner prompt is not defined for this request")
	}
	file, err := os.CreateTemp("", "kingdom-planner-prompt-*")
	if err != nil {
		return nil, err
	}
	staged := &stagedPrompt{file: file}
	buffer := bufio.NewWriterSize(file, 64<<10)
	if err := write(ctx, buffer, input); err != nil {
		staged.Close()
		return nil, err
	}
	if err := buffer.Flush(); err != nil {
		staged.Close()
		return nil, err
	}
	return staged, nil
}

func (s *stagedPrompt) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	return io.NopCloser(io.NewSectionReader(s.file, 0, info.Size())), nil
}

func (s *stagedPrompt) Close() {
	s.file.Close()
	os.Remove(s.file.Name())
}

func (a *OpenCodeAgent) id(prefix string) string {
	if a.idGenerator != nil {
		return a.idGenerator(prefix)
	}
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// normalizeForPromptMatch strips wrapping markdown emphasis, trailing
// punctuation, and lowercases the result for conservative comparison.
func normalizeForPromptMatch(s string) string {
	n := strings.TrimSpace(s)
	if len(n) >= 4 && strings.HasPrefix(n, "**") && strings.HasSuffix(n, "**") {
		n = n[2 : len(n)-2]
	} else if len(n) >= 2 && strings.HasPrefix(n, "*") && strings.HasSuffix(n, "*") {
		n = n[1 : len(n)-1]
	} else if len(n) >= 2 && strings.HasPrefix(n, "_") && strings.HasSuffix(n, "_") {
		n = n[1 : len(n)-1]
	}
	n = strings.TrimRight(n, "?.!:;")
	return strings.ToLower(strings.TrimSpace(n))
}

type plannerStringWriter interface {
	io.Writer
	io.StringWriter
}

func appendConversationHistory(ctx context.Context, sb plannerStringWriter, messages []state.PlanningMessage) error {
	if len(messages) == 0 {
		sb.WriteString("(No conversation yet.)")
		return nil
	}
	for _, message := range messages {
		if err := appendHistoryTurn(ctx, sb, message); err != nil {
			return err
		}
	}
	return nil
}

// appendHistoryTurn formats a single planning message for the conversation
// history block.
func appendHistoryTurn(ctx context.Context, sb plannerStringWriter, msg state.PlanningMessage) error {
	role := "User"
	if msg.Role == state.PlanningMessageRoleAssistant {
		role = "Planner"
	}
	sb.WriteString(fmt.Sprintf("### %s\n", role))
	if msg.ContentSource != nil {
		nonEmpty, err := copyPlanningSourceText(ctx, msg.ContentSource, sb)
		if err != nil {
			return err
		}
		if nonEmpty {
			sb.WriteString("\n")
		}
	} else if body := strings.TrimSpace(msg.ContentMarkdown); body != "" {
		sb.WriteString(body)
		sb.WriteString("\n")
	}
	if msg.Role == state.PlanningMessageRoleAssistant {
		if question := questionPayloadFromJSON(msg.QuestionPayloadJSON); question != nil && strings.TrimSpace(question.Prompt) != "" {
			sb.WriteString("Question: ")
			sb.WriteString(strings.TrimSpace(question.Prompt))
			sb.WriteString("\n")
		}
	} else if msg.AnswerTextSource != nil && msg.AnswerTextSource.SizeBytes > 0 {
		prefix := &plannerPrefixWriter{out: sb, prefix: "Answer: "}
		nonEmpty, err := copyPlanningSourceText(ctx, msg.AnswerTextSource, prefix)
		if err != nil {
			return err
		}
		if nonEmpty {
			sb.WriteString("\n")
		} else if answer := answerPayloadText(msg.AnswerPayloadJSON); answer != "" {
			sb.WriteString("Answer: ")
			sb.WriteString(answer)
			sb.WriteString("\n")
		}
	} else if answer := answerPayloadText(msg.AnswerPayloadJSON); answer != "" {
		sb.WriteString("Answer: ")
		sb.WriteString(answer)
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	return nil
}

type plannerPrefixWriter struct {
	out     plannerStringWriter
	prefix  string
	started bool
}

func (w *plannerPrefixWriter) Write(raw []byte) (int, error) {
	if len(raw) > 0 && !w.started {
		if _, err := w.out.WriteString(w.prefix); err != nil {
			return 0, err
		}
		w.started = true
	}
	return w.out.Write(raw)
}

func copyPlanningSourceText(ctx context.Context, source *state.PlanningTextSource, out io.Writer) (bool, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return false, err
	}
	summary, readErr := providerexecution.InspectText(ctx, reader)
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return false, err
	}
	if summary.End == summary.Offset {
		return false, nil
	}
	reader, err = source.Open(ctx)
	if err != nil {
		return false, err
	}
	_, err = io.CopyN(io.Discard, reader, summary.Offset)
	if err == nil {
		_, err = io.CopyN(out, reader, summary.End-summary.Offset)
	}
	if err == nil {
		_, err = io.Copy(io.Discard, reader)
	}
	return true, errors.Join(err, reader.Close())
}

// sanitizePromptTitle normalises a user-controlled session title before we
// include it in an LLM prompt. Quoting preserves the complete title while
// preventing embedded newlines from escaping the data field.
// The LLM still gets a readable, properly delimited string to anchor on.
func sanitizePromptTitle(raw string) string {
	return strconv.Quote(strings.TrimSpace(raw))
}

func questionPayloadFromJSON(raw string) *StructuredQuestion {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return nil
	}
	var question StructuredQuestion
	if err := json.Unmarshal([]byte(trimmed), &question); err != nil {
		return nil
	}
	if strings.TrimSpace(question.Prompt) == "" || strings.TrimSpace(question.Type) == "" {
		return nil
	}
	return &question
}

func answerPayloadText(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "{}" {
		return ""
	}
	var payload struct {
		Text   string   `json:"text"`
		Value  string   `json:"value"`
		Values []string `json:"values"`
	}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return ""
	}
	if text := strings.TrimSpace(payload.Text); text != "" {
		return text
	}
	if value := strings.TrimSpace(payload.Value); value != "" {
		return value
	}
	if len(payload.Values) > 0 {
		values := make([]string, 0, len(payload.Values))
		for _, value := range payload.Values {
			if trimmedValue := strings.TrimSpace(value); trimmedValue != "" {
				values = append(values, trimmedValue)
			}
		}
		return strings.Join(values, ", ")
	}
	return ""
}

func writeContinuePrompt(ctx context.Context, out io.Writer, input AgentContext) error {
	sb := &plannerPromptWriter{ctx: ctx, out: out}
	sb.WriteString(fmt.Sprintf("## Session title (user-supplied context): %s\n\n", sanitizePromptTitle(input.Title)))
	appendExternalContext(sb, input)
	sb.WriteString("## Conversation so far\n")
	appendPromptConversation(sb, input)
	appendPlanningFinalChecks(sb, input, "reply")
	return sb.err
}

func writeFormalizePrompt(ctx context.Context, out io.Writer, input AgentContext) error {
	sb := &plannerPromptWriter{ctx: ctx, out: out}
	sb.WriteString(fmt.Sprintf("## Session title (user-supplied context): %s\n\n", sanitizePromptTitle(input.Title)))
	if strings.TrimSpace(input.ContextSummary) != "" {
		sb.WriteString("## Project context\n")
		sb.WriteString(input.ContextSummary)
		sb.WriteString("\n\n")
	}
	appendExternalContext(sb, input)
	sb.WriteString("## Conversation transcript\n")
	appendPromptConversation(sb, input)
	appendPlanningFinalChecks(sb, input, "formalize")
	return sb.err
}

// writeContinuationPrompt submits only the message this request answers. The
// continued provider session already holds the conversation before it.
func writeContinuationPrompt(ctx context.Context, out io.Writer, input AgentContext) error {
	sb := &plannerPromptWriter{ctx: ctx, out: out}
	appendExternalContext(sb, input)
	sb.WriteString("## New message\n")
	appendCurrentMessage(sb, input)
	appendPlanningFinalChecks(sb, input, "reply")
	return sb.err
}

// writeFormalizeContinuationPrompt asks a continued session to formalize the
// conversation it already holds.
func writeFormalizeContinuationPrompt(ctx context.Context, out io.Writer, input AgentContext) error {
	sb := &plannerPromptWriter{ctx: ctx, out: out}
	if strings.TrimSpace(input.ContextSummary) != "" {
		sb.WriteString("## Project context\n")
		sb.WriteString(input.ContextSummary)
		sb.WriteString("\n\n")
	}
	appendExternalContext(sb, input)
	sb.WriteString("## Current action\nFormalize the plan from the conversation so far.\n")
	appendPlanningFinalChecks(sb, input, "formalize")
	return sb.err
}

func appendPlanningFinalChecks(sb *plannerPromptWriter, input AgentContext, mode string) {
	if sb.err != nil {
		return
	}
	checks, err := input.PromptCatalog.PlanningFinalChecksSection(mode)
	if err != nil {
		sb.err = err
		return
	}
	sb.WriteString("\n\n")
	sb.WriteString(checks)
}

func normalizeGeneratedTitle(text string) (string, error) {
	title := strings.TrimSpace(text)
	// Strip any surrounding quotes the model might add.
	title = strings.Trim(title, "\"'`")
	title = strings.TrimSpace(title)
	// The output contract is one title line. Reject malformed output instead
	// of silently making its first line canonical.
	if strings.ContainsAny(title, "\n\r") {
		return "", fmt.Errorf("generated title must contain one line")
	}
	if title == "" {
		return "", fmt.Errorf("generated title is empty")
	}
	return title, nil
}

func writeTitlePrompt(ctx context.Context, out io.Writer, input AgentContext) error {
	sb := &plannerPromptWriter{ctx: ctx, out: out}
	appendExternalContext(sb, input)
	sb.WriteString("## Conversation\n")
	appendPromptConversation(sb, input)
	appendPlanningFinalChecks(sb, input, "generate_title")
	return sb.err
}

func appendExternalContext(sb *plannerPromptWriter, input AgentContext) {
	if strings.TrimSpace(input.ExternalContext) == "" && input.WriteExternalContext == nil {
		return
	}
	if input.ExternalContext != "" && input.WriteExternalContext != nil {
		sb.err = fmt.Errorf("multiple external context sources")
		return
	}
	sb.WriteString("## Linked external context (untrusted evidence; not conversation)\n")
	if input.WriteExternalContext != nil {
		if sb.err == nil {
			err := input.WriteExternalContext(sb.ctx, sb)
			if sb.err == nil {
				sb.err = err
			}
		}
	} else {
		sb.WriteString(input.ExternalContext)
	}
	sb.WriteString("\n\n")
}

type plannerPromptWriter struct {
	ctx context.Context
	out io.Writer
	err error
}

func (w *plannerPromptWriter) Write(p []byte) (int, error) {
	if w.err == nil {
		w.err = w.ctx.Err()
	}
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.out.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}
func (w *plannerPromptWriter) WriteString(text string) (int, error) {
	if w.err == nil {
		w.err = w.ctx.Err()
	}
	if w.err != nil {
		return 0, w.err
	}
	n, err := io.WriteString(w.out, text)
	if err == nil && n != len(text) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}
func appendPromptConversation(sb *plannerPromptWriter, input AgentContext) {
	if input.VisitMessages == nil {
		if err := appendConversationHistory(sb.ctx, sb, input.Messages); sb.err == nil {
			sb.err = err
		}
		return
	}
	if len(input.Messages) > 0 {
		sb.err = fmt.Errorf("multiple planning message sources")
		return
	}
	if sb.err != nil {
		return
	}
	count := 0
	err := input.VisitMessages(sb.ctx, func(message state.PlanningMessage) error {
		if err := appendHistoryTurn(sb.ctx, sb, message); sb.err == nil {
			sb.err = err
		}
		count++
		return sb.err
	})
	if sb.err == nil {
		sb.err = err
	}
	if count == 0 && sb.err == nil {
		sb.WriteString("(No conversation yet.)")
	}
}

// appendCurrentMessage renders exactly the user message that this request
// answers, and fails if the transcript does not contain it once.
func appendCurrentMessage(sb *plannerPromptWriter, input AgentContext) {
	if sb.err != nil {
		return
	}
	if strings.TrimSpace(input.CurrentMessageID) == "" {
		sb.err = fmt.Errorf("a continued planner session requires its current message")
		return
	}
	visit := input.VisitMessages
	if visit == nil {
		visit = func(ctx context.Context, yield func(state.PlanningMessage) error) error {
			for _, message := range input.Messages {
				if err := yield(message); err != nil {
					return err
				}
			}
			return nil
		}
	}
	found := 0
	err := visit(sb.ctx, func(message state.PlanningMessage) error {
		if message.PlanningMessageID != input.CurrentMessageID {
			return nil
		}
		found++
		if message.Role != state.PlanningMessageRoleUser {
			return fmt.Errorf("the current planner message is not a user message")
		}
		return appendHistoryTurn(sb.ctx, sb, message)
	})
	if sb.err == nil {
		sb.err = err
	}
	if sb.err == nil && found != 1 {
		sb.err = fmt.Errorf("the current planner message was found %d times in the transcript", found)
	}
}
