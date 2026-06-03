package planning

import (
	"sync"
)

type operationEventBroker struct {
	mu          sync.Mutex
	nextSubID   uint64
	subscribers map[string]map[uint64]chan struct{}
}

func newOperationEventBroker() *operationEventBroker {
	return &operationEventBroker{subscribers: make(map[string]map[uint64]chan struct{})}
}

func (b *operationEventBroker) Subscribe(operationID string) (<-chan struct{}, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextSubID++
	subID := b.nextSubID
	ch := make(chan struct{}, 1)
	if b.subscribers[operationID] == nil {
		b.subscribers[operationID] = make(map[uint64]chan struct{})
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

func (b *operationEventBroker) Publish(operationID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subscribers[operationID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
