package api

import (
	"context"
	"io"

	"kingdom/internal/state"
)

func planningRequestText(ctx context.Context, text *RequestText, trim bool) (*state.PlanningTextSource, error) {
	if text == nil {
		return nil, nil
	}
	var err error
	if trim {
		text, err = text.Trimmed(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &state.PlanningTextSource{SHA256: text.SHA256, SizeBytes: text.SizeBytes, Open: func(ctx context.Context) (io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return text.Open()
	}}, nil
}
