package store

import (
	"context"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
)

func TestFailedHandoffWithoutProviderAnswerAbandonsOnlyClosedRequest(t *testing.T) {
	for _, variant := range []string{"closed", "open command", "success", "stale lease", "foreign session", "settlement success", "atomic rollback", "changed observation", "unverified response"} {
		t.Run(variant, func(t *testing.T) {
			s, db, req := unfinishedPhase2ProviderSettlementFixture(t)
			req.Settlement.Outcome = ProviderCompletionOutcomeFailed
			req.Settlement.FinalText = ""
			req.Terminal.TerminalState = "committed_failure"
			observation := providerexecution.InvocationStreamObservation{}
			req.StreamObservation = &observation
			handoff := terminalHandoffRequest(req)
			handoff.PayloadJSON = `{"outcome":{"state":"failed","failure_text":"OpenCode message returned HTTP 400"}}`
			switch variant {
			case "open command":
				claimBoundCommands(t, s, req.Settlement.ProviderInvocationID, 1)
			case "success":
				handoff.PayloadJSON = `{"outcome":{"state":"success"}}`
			case "stale lease":
				handoff.ExpectedLeaseVersion++
			case "foreign session":
				handoff.StreamObservation.ProviderSessionID = "foreign"
			case "unverified response":
				handoff.StreamObservation.FinalTurnID = "unverified"
				handoff.StreamObservation.RawFinishReason = "stop"
				handoff.StreamObservation.CanonicalTerminalText = "This text must not certify success."
				*req.StreamObservation = handoff.StreamObservation
			}
			persisted, err := s.PersistProviderTerminalHandoff(context.Background(), handoff)
			if variant == "success" || variant == "stale lease" || variant == "foreign session" {
				if err == nil {
					t.Fatal("accepted unbound or successful handoff without provider evidence")
				}
				return
			}
			if err != nil {
				t.Fatalf("persist failed handoff: %v", err)
			}
			if persisted.Handoff.PayloadJSON != handoff.PayloadJSON {
				t.Fatal("failure diagnostics changed")
			}
			req.TerminalHandoffID = persisted.Handoff.HandoffID
			if variant == "settlement success" {
				req.Settlement.Outcome = ProviderCompletionOutcomeSuccess
				req.Terminal.TerminalState = "committed_success"
			}
			if variant == "changed observation" {
				req.StreamObservation.FinalTurnID = "changed"
			}
			if variant == "atomic rollback" {
				mustExec(t, db, `CREATE TRIGGER fail_abandoned_settlement BEFORE INSERT ON provider_completion_evidence BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END`)
			}
			first, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			if variant == "open command" || variant == "settlement success" || variant == "atomic rollback" || variant == "changed observation" {
				if err == nil {
					t.Fatal("invalid or interrupted failure settlement was accepted")
				}
				b, found, bindingErr := providerrequest.ForOwner(context.Background(), db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.Settlement.ProviderInvocationID})
				if bindingErr != nil || !found {
					t.Fatalf("missing request after failed settlement: %v", bindingErr)
				}
				if b.TerminalReference != nil {
					t.Fatal("failed settlement left abandonment evidence")
				}
				if _, found, err := s.GetProviderTerminalHandoff(context.Background(), req.TerminalHandoffID); err != nil || !found {
					t.Fatalf("failed settlement lost durable handoff: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("settle failed handoff: %v", err)
			}
			b, _, err := providerrequest.ForOwner(context.Background(), db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.Settlement.ProviderInvocationID})
			want, _ := providerexecution.AbandonedRequestTerminal(req.Settlement.ProviderInvocationID).Reference()
			if err != nil || b.TerminalReference == nil || *b.TerminalReference != want {
				t.Fatalf("missing authoritative abandonment: %+v %v", b, err)
			}
			replay, err := s.SettleProviderCompletionAndTerminal(context.Background(), req)
			if err != nil || replay != first {
				t.Fatalf("replay changed settlement: %+v %v", replay, err)
			}
			evidence, err := s.ListProviderCompletionEvidence(context.Background(), req.Settlement.ProviderInvocationID)
			if err != nil || len(evidence) != 1 || evidence[0].PayloadJSON != persisted.Handoff.StreamObservationJSON {
				t.Fatalf("original failed observation changed: %+v %v", evidence, err)
			}
			if countPhase2Table(t, db, "provider_completion_settlements") != 1 {
				t.Fatal("replay duplicated settlement")
			}
		})
	}
}

func TestFailedHandoffCannotReplaceVerifiedTerminalOnReplay(t *testing.T) {
	s, _, req := phase2ProviderSettlementFixture(t)
	req.Settlement.Outcome = ProviderCompletionOutcomeFailed
	req.Terminal.TerminalState = "committed_failure"
	attachTerminalHandoffToSettlement(t, s, &req)
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.StreamObservation = &providerexecution.InvocationStreamObservation{}
	if _, err := s.SettleProviderCompletionAndTerminal(context.Background(), req); err == nil {
		t.Fatal("replaced verified provider evidence with abandonment")
	}
}
