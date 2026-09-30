package opencode

import "kingdom/internal/core/providerexecution"

// Provisional steps are discarded as soon as they finish. Only a stop step can
// supply terminal text; failed writes remain fatal even if a later step succeeds.
type stepOutputAssembler struct {
	currentStep   textSpool
	terminal      textSpool
	terminalCache string
	stepOpen      bool
	err           error
}

func (a *stepOutputAssembler) startStep() {
	if a.currentStep.err != nil {
		a.err = a.currentStep.err
	}
	a.currentStep.close()
	a.currentStep = textSpool{}
	a.clearTerminal()
	a.stepOpen = true
}

func (a *stepOutputAssembler) clearTerminal() {
	if a.terminal.err != nil {
		a.err = a.terminal.err
	}
	a.terminal.close()
	a.terminal = textSpool{}
	a.terminalCache = ""
}

func (a *stepOutputAssembler) appendChunk(text string, first bool) {
	if !a.stepOpen {
		a.startStep()
	}
	if first {
		a.currentStep.append(text)
	} else {
		a.currentStep.write(text)
	}
	if a.currentStep.err != nil {
		a.err = a.currentStep.err
	}
}

func (a *stepOutputAssembler) finishStep(class stepFinishTerminalClass) {
	a.clearTerminal()
	if class == stepFinishTerminalSuccessCandidate {
		a.terminal = a.currentStep
		a.currentStep = textSpool{}
	} else {
		a.currentStep.close()
	}
	a.stepOpen = false
}

// terminalOutput is the bounded display preview. Complete terminal bytes are
// available only through terminalContent's verified replay source.
func (a *stepOutputAssembler) terminalOutput() string {
	if a.terminalCache == "" {
		a.terminalCache = a.terminal.text()
	}
	return a.terminalCache
}

func (a *stepOutputAssembler) terminalContent(invocationID string) (*providerexecution.StagedContent, error) {
	return a.terminal.content(invocationID)
}
func (a *stepOutputAssembler) close() { a.currentStep.close(); a.terminal.close() }
