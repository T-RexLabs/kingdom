package workflow

import (
	"sort"
	"time"
)

const (
	DefaultQueueAgingBoost          = 30 * time.Second
	DefaultReadyEscalationThreshold = 5 * time.Minute
)

type ReadyNode struct {
	RunID        string
	NodeID       string
	Attempt      int
	RunPriority  int
	CriticalPath bool
	ReadyAt      time.Time
}

type EscalationEvent struct {
	RunID      string
	NodeID     string
	Attempt    int
	EventType  string
	TargetRole string
	ReasonCode string
	WaitFor    time.Duration
}

type SchedulerConfig struct {
	QueueAgingBoost          time.Duration
	ReadyEscalationThreshold time.Duration
}

type Scheduler struct {
	queueAgingBoost          time.Duration
	readyEscalationThreshold time.Duration
}

func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.QueueAgingBoost <= 0 {
		cfg.QueueAgingBoost = DefaultQueueAgingBoost
	}
	if cfg.ReadyEscalationThreshold <= 0 {
		cfg.ReadyEscalationThreshold = DefaultReadyEscalationThreshold
	}

	return &Scheduler{
		queueAgingBoost:          cfg.QueueAgingBoost,
		readyEscalationThreshold: cfg.ReadyEscalationThreshold,
	}
}

func (s *Scheduler) DispatchOrder(now time.Time, ready []ReadyNode) []ReadyNode {
	ordered := make([]ReadyNode, len(ready))
	copy(ordered, ready)

	now = now.UTC()

	sort.Slice(ordered, func(i, j int) bool {
		left := ordered[i]
		right := ordered[j]

		leftPriority := left.RunPriority + s.agingBoost(now, left.ReadyAt)
		rightPriority := right.RunPriority + s.agingBoost(now, right.ReadyAt)
		if leftPriority != rightPriority {
			return leftPriority > rightPriority
		}

		if left.CriticalPath != right.CriticalPath {
			return left.CriticalPath
		}

		leftReady := left.ReadyAt.UTC()
		rightReady := right.ReadyAt.UTC()
		if !leftReady.Equal(rightReady) {
			return leftReady.Before(rightReady)
		}

		if left.RunID != right.RunID {
			return left.RunID < right.RunID
		}

		if left.NodeID != right.NodeID {
			return left.NodeID < right.NodeID
		}

		return left.Attempt < right.Attempt
	})

	return ordered
}

func (s *Scheduler) Escalations(now time.Time, ready []ReadyNode) []EscalationEvent {
	ordered := s.DispatchOrder(now, ready)
	escalations := make([]EscalationEvent, 0)

	now = now.UTC()

	for _, node := range ordered {
		wait := readyWait(now, node.ReadyAt.UTC())
		if wait < s.readyEscalationThreshold {
			continue
		}

		escalations = append(escalations, EscalationEvent{
			RunID:      node.RunID,
			NodeID:     node.NodeID,
			Attempt:    node.Attempt,
			EventType:  "escalation.raised",
			TargetRole: "coordinator",
			ReasonCode: "ready_threshold_breached",
			WaitFor:    wait,
		})
	}

	return escalations
}

func (s *Scheduler) agingBoost(now, readyAt time.Time) int {
	if s.queueAgingBoost <= 0 {
		return 0
	}
	wait := readyWait(now, readyAt.UTC())
	return int(wait / s.queueAgingBoost)
}

func readyWait(now, readyAt time.Time) time.Duration {
	if readyAt.After(now) {
		return 0
	}
	return now.Sub(readyAt)
}
