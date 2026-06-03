package state

import "time"

const (
	DefaultHeartbeatInterval          = 10 * time.Second
	DefaultStaleHeartbeatThreshold    = 35 * time.Second
	DefaultReassignCandidateThreshold = 60 * time.Second
	DefaultQueueAgingBoost            = 30 * time.Second
	DefaultReadyEscalationThreshold   = 5 * time.Minute

	DefaultMaxSpawnDepth         = 3
	DefaultMaxActiveAgentsPerRun = 12
	DefaultMaxAttemptsPerNode    = 5

	DefaultNightravenConcurrencyCap = 1
)
