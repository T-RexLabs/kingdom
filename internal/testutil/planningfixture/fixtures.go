package planningfixture

type Question struct {
	Prompt string
	Type   string
}

type Script struct {
	OpeningContent   string
	OpeningQuestion  Question
	ContinueContent  string
	ContinueQuestion Question
	GeneratedTitle   string
	FormalizedText   string
}

func DefaultServiceScript() Script {
	return Script{
		OpeningContent: openingQuestionPrompt,
		OpeningQuestion: Question{
			Prompt: openingQuestionPrompt,
			Type:   "long_text",
		},
		ContinueContent: continueQuestionPrompt,
		ContinueQuestion: Question{
			Prompt: continueQuestionPrompt,
			Type:   "long_text",
		},
		GeneratedTitle: "Mock Generated Title",
		FormalizedText: "## Objective\n\nFormalized from mock agent conversation.\n\n## Scope\n\nMock scope.",
	}
}

func DefaultAPIScript() Script {
	return Script{
		OpeningContent: apiOpeningQuestionPrompt,
		OpeningQuestion: Question{
			Prompt: apiOpeningQuestionPrompt,
			Type:   "long_text",
		},
		ContinueContent: continueQuestionPrompt,
		ContinueQuestion: Question{
			Prompt: continueQuestionPrompt,
			Type:   "long_text",
		},
		GeneratedTitle: "Mock Generated Title",
		FormalizedText: "## Objective\n\nFormalized from test agent conversation.\n\n## Scope\n\nTest scope.",
	}
}

const (
	openingQuestionPrompt    = "How would you like to start?"
	apiOpeningQuestionPrompt = "What is the primary objective for this work order?"
	continueQuestionPrompt   = "What constraints should shape the plan?"
)
