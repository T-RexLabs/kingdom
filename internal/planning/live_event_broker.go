package planning

import "sync"

type LiveOperationEvent struct {
	EventType string
	Payload   string
}

type liveOperationEventBroker struct {
	mu          sync.Mutex
	nextSubID   uint64
	subscribers map[string]map[uint64]chan LiveOperationEvent
}

func newLiveOperationEventBroker() *liveOperationEventBroker {
	return &liveOperationEventBroker{subscribers: make(map[string]map[uint64]chan LiveOperationEvent)}
}

func (b *liveOperationEventBroker) Subscribe(operationID string) (<-chan LiveOperationEvent, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextSubID++
	subID := b.nextSubID
	ch := make(chan LiveOperationEvent, 8)
	if b.subscribers[operationID] == nil {
		b.subscribers[operationID] = make(map[uint64]chan LiveOperationEvent)
	}
	b.subscribers[operationID][subID] = ch

	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if subs := b.subscribers[operationID]; subs != nil {
			if existing, ok := subs[subID]; ok {
				delete(subs, subID)
				close(existing)
			}
			if len(subs) == 0 {
				delete(b.subscribers, operationID)
			}
		}
	}
}

func (b *liveOperationEventBroker) Publish(operationID string, event LiveOperationEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subscribers[operationID] {
		select {
		case ch <- event:
		default:
		}
	}
}
