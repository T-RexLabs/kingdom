package workflow

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSchedulerDispatchOrderIsDeterministicAcrossRuns(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	scheduler := NewScheduler(SchedulerConfig{QueueAgingBoost: 24 * time.Hour})
	ready := []ReadyNode{
		{RunID: "run-b", NodeID: "node-b", Attempt: 1, RunPriority: 3, CriticalPath: false, ReadyAt: now.Add(-2 * time.Minute)},
		{RunID: "run-a", NodeID: "node-c", Attempt: 1, RunPriority: 3, CriticalPath: true, ReadyAt: now.Add(-90 * time.Second)},
		{RunID: "run-a", NodeID: "node-a", Attempt: 2, RunPriority: 3, CriticalPath: true, ReadyAt: now.Add(-90 * time.Second)},
		{RunID: "run-a", NodeID: "node-a", Attempt: 1, RunPriority: 3, CriticalPath: true, ReadyAt: now.Add(-90 * time.Second)},
		{RunID: "run-z", NodeID: "node-z", Attempt: 1, RunPriority: 4, CriticalPath: false, ReadyAt: now.Add(-30 * time.Second)},
	}

	baseline := scheduler.DispatchOrder(now, ready)
	for i := 0; i < 20; i++ {
		got := scheduler.DispatchOrder(now, ready)
		if !reflect.DeepEqual(got, baseline) {
			t.Fatalf("dispatch order changed on iteration %d\nwant: %+v\n got: %+v", i, baseline, got)
		}
	}

	assertDispatchIDs(t, baseline, []string{
		"run-z/node-z/1",
		"run-a/node-a/1",
		"run-a/node-a/2",
		"run-a/node-c/1",
		"run-b/node-b/1",
	})
}

func TestSchedulerAgingPreventsStarvation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	scheduler := NewScheduler(SchedulerConfig{
		QueueAgingBoost:          30 * time.Second,
		ReadyEscalationThreshold: 5 * time.Minute,
	})

	ready := []ReadyNode{
		{RunID: "run-low", NodeID: "long-waiting", Attempt: 1, RunPriority: 1, CriticalPath: false, ReadyAt: now.Add(-330 * time.Second)},
		{RunID: "run-high", NodeID: "new-high-pri", Attempt: 1, RunPriority: 10, CriticalPath: false, ReadyAt: now.Add(-10 * time.Second)},
	}

	ordered := scheduler.DispatchOrder(now, ready)
	assertDispatchIDs(t, ordered, []string{
		"run-low/long-waiting/1",
		"run-high/new-high-pri/1",
	})
}

func TestSchedulerEscalatesWhenReadyThresholdBreached(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	scheduler := NewScheduler(SchedulerConfig{
		QueueAgingBoost:          30 * time.Second,
		ReadyEscalationThreshold: 5 * time.Minute,
	})

	ready := []ReadyNode{
		{RunID: "run-1", NodeID: "at-threshold", Attempt: 1, RunPriority: 1, CriticalPath: false, ReadyAt: now.Add(-5 * time.Minute)},
		{RunID: "run-2", NodeID: "below-threshold", Attempt: 1, RunPriority: 1, CriticalPath: false, ReadyAt: now.Add(-4*time.Minute - 59*time.Second)},
	}

	escalations := scheduler.Escalations(now, ready)
	if len(escalations) != 1 {
		t.Fatalf("expected exactly one escalation, got %d (%+v)", len(escalations), escalations)
	}

	escalation := escalations[0]
	if escalation.EventType != "escalation.raised" {
		t.Fatalf("unexpected event type: %s", escalation.EventType)
	}
	if escalation.TargetRole != "chancellor" {
		t.Fatalf("unexpected escalation target: %s", escalation.TargetRole)
	}
	if escalation.ReasonCode != "ready_threshold_breached" {
		t.Fatalf("unexpected reason code: %s", escalation.ReasonCode)
	}
	if escalation.RunID != "run-1" || escalation.NodeID != "at-threshold" || escalation.Attempt != 1 {
		t.Fatalf("unexpected escalation node identity: %+v", escalation)
	}
}

func TestSchedulerAppliesDefaultConfigValues(t *testing.T) {
	t.Parallel()

	scheduler := NewScheduler(SchedulerConfig{})
	if scheduler.queueAgingBoost != DefaultQueueAgingBoost {
		t.Fatalf("expected default queue aging boost %s, got %s", DefaultQueueAgingBoost, scheduler.queueAgingBoost)
	}
	if scheduler.readyEscalationThreshold != DefaultReadyEscalationThreshold {
		t.Fatalf("expected default ready escalation threshold %s, got %s", DefaultReadyEscalationThreshold, scheduler.readyEscalationThreshold)
	}
}

func assertDispatchIDs(t *testing.T, ordered []ReadyNode, want []string) {
	t.Helper()

	got := make([]string, 0, len(ordered))
	for _, node := range ordered {
		got = append(got, node.RunID+"/"+node.NodeID+"/"+strconv.Itoa(node.Attempt))
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected dispatch order\nwant: %v\n got: %v", want, got)
	}
}
