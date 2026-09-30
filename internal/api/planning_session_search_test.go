package api

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"kingdom/internal/state"
)

func TestPlanningSessionTitleSearchUsesCanonicalTextAndScopedPages(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	store := state.NewPlanningStore(db)
	// Matching sessions remain searchable beyond every initially loaded page.
	for index := 0; index < 205; index++ {
		_, err := store.CreateSession(t.Context(), state.CreatePlanningSessionRequest{
			PlanningSessionID: fmt.Sprintf("search-unrelated-%03d", index), TitleSource: state.PlanningTextFromString("A different topic"), TitleState: "custom",
			LifecycleState: state.PlanningSessionLifecycleStateActive, ConversationState: state.PlanningSessionConversationStateReady,
			ProjectRoot: "/tmp", ModelSelectionJSON: "{}", CreatedAt: "2026-09-30T00:00:00Z", UpdatedAt: "2026-09-30T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct{ id, title string }{
		{"search-1", "Översikt Needle 100%"},
		{"search-2", "Unrelated"},
		{"search-3", "Second NEEDLE"},
		{"search-4", "Needle newest"},
	} {
		_, err := store.CreateSession(t.Context(), state.CreatePlanningSessionRequest{
			PlanningSessionID: item.id, TitleSource: state.PlanningTextFromString(item.title), TitleState: "custom",
			LifecycleState: state.PlanningSessionLifecycleStateActive, ConversationState: state.PlanningSessionConversationStateReady,
			ProjectRoot: "/tmp", ModelSelectionJSON: "{}", CreatedAt: "2026-09-30T00:00:00Z", UpdatedAt: "2026-09-30T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	first := readHistoryTestPage(t, handler, "/api/v1/planning-sessions?limit=2&q=needle")
	if len(first.Items) != 2 || first.Items[0]["planning_session_id"] != "search-4" || first.Items[1]["planning_session_id"] != "search-3" || first.NextCursor == "" {
		t.Fatalf("unexpected first search page: %#v", first)
	}
	second := readHistoryTestPage(t, handler, "/api/v1/planning-sessions?limit=2&q=NEEDLE&cursor="+url.QueryEscape(first.NextCursor))
	if len(second.Items) != 1 || second.Items[0]["planning_session_id"] != "search-1" || second.NextCursor != "" {
		t.Fatalf("unexpected second search page: %#v", second)
	}
	wrongScope := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/planning-sessions?q=unrelated&cursor=" + url.QueryEscape(first.NextCursor)})
	if wrongScope.Code != http.StatusBadRequest {
		t.Fatalf("cross-query cursor status=%d body=%s", wrongScope.Code, wrongScope.Body.String())
	}
	for _, query := range []string{"öVERSIKT", "100%"} {
		page := readHistoryTestPage(t, handler, "/api/v1/planning-sessions?q="+url.QueryEscape(query))
		if len(page.Items) != 1 || page.Items[0]["planning_session_id"] != "search-1" {
			t.Fatalf("literal case-insensitive query %q: %#v", query, page)
		}
	}
}
