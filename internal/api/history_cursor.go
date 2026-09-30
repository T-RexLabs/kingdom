package api

import (
	"encoding/json"
	"fmt"
)

// History cursors carry the ordering values, rather than requiring the anchor
// row to still exist. The cursor codec authenticates both resource and filters.
type historyCursor struct {
	OrderedAt string `json:"ordered_at"`
	ID        string `json:"id"`
}

func (s *Server) encodeHistoryCursor(kind, scope, orderedAt, id string) string {
	payload, _ := json.Marshal(historyCursor{OrderedAt: orderedAt, ID: id})
	return s.cursors.Encode(kind, scope, string(payload))
}

func (s *Server) decodeHistoryCursor(kind, scope, raw string) (historyCursor, error) {
	if raw == "" {
		return historyCursor{}, nil
	}
	payload, err := s.cursors.Decode(kind, scope, raw)
	if err != nil {
		return historyCursor{}, err
	}
	var cursor historyCursor
	if err := json.Unmarshal([]byte(payload), &cursor); err != nil || cursor.OrderedAt == "" || cursor.ID == "" {
		return historyCursor{}, fmt.Errorf("invalid history cursor")
	}
	return cursor, nil
}
