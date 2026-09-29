package opencode

import "strings"

// stepOutputAssembler keeps provisional provider text separate from the text
// that can settle a server-response turn. A tool-call step is real progress, but its
// prose is not the terminal assistant answer.
type stepOutputAssembler struct {
	currentStep  []string
	terminalText string
	stepOpen     bool
}

func (a *stepOutputAssembler) startStep() {
	a.currentStep = nil
	a.stepOpen = true
}

func (a *stepOutputAssembler) appendText(text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	if !a.stepOpen {
		a.startStep()
	}
	a.currentStep = append(a.currentStep, text)
}

func (a *stepOutputAssembler) finishStep(class stepFinishTerminalClass) {
	if class == stepFinishTerminalSuccessCandidate {
		a.terminalText = strings.TrimSpace(strings.Join(a.currentStep, "\n"))
	}
	a.currentStep = nil
	a.stepOpen = false
}

func (a *stepOutputAssembler) terminalOutput() string {
	return strings.TrimSpace(a.terminalText)
}
