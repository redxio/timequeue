package delayqueue

import (
	"container/list"
	"sync"
	"time"
)

type dqItem struct {
	value  interface{}
	expire time.Time
}

type semaphore struct{}

var greenLight, yellowLight semaphore

// DelayQueue represents a delay queue
type DelayQueue struct {
	rw            sync.RWMutex
	queue         *list.List
	size          int
	worker        chan semaphore
	reconsumption chan semaphore
	delay         chan interface{}
}

// TravFunc is used for traversing delay queue, the argument of TravFunc is value stored in queue item
type TravFunc func(interface{})

func (dq *DelayQueue) delayService() {
	var (
		elem, end    *list.Element
		item         *dqItem
		releaseRLock bool
	)

	for {
		dq.rw.RLock()

		if releaseRLock {
			releaseRLock = false
		}

		if dq.queue.Len() == 0 {
			dq.rw.RUnlock()
			releaseRLock = true
			<-dq.worker
			continue
		}

		if len(dq.reconsumption) > 0 {
			<-dq.reconsumption
		}

		elem = dq.queue.Front()
		end = elem.Next()
		item = elem.Value.(*dqItem)

		if time.Now().Before(item.expire) {
			dq.rw.RUnlock()
			releaseRLock = true

			select {
			case <-dq.reconsumption:
				if time.Now().Before(dq.queue.Front().Value.(*dqItem).expire) {
					continue
				} else if time.Now().Before(item.expire) {
					end = elem
				}
			case <-time.After(item.expire.Sub(time.Now())):
				if len(dq.reconsumption) > 0 {
					<-dq.reconsumption
				}
			}
		}

		if !releaseRLock {
			dq.rw.RUnlock()
		}

		dq.rw.Lock()

		if len(dq.reconsumption) > 0 {
			<-dq.reconsumption
		}

		for front := dq.queue.Front(); front != end; front = dq.queue.Front() {
			if dq.delay != nil {
				dq.delay <- front.Value.(*dqItem).value
			}
			dq.queue.Remove(front)
		}

		dq.rw.Unlock()
	}
}

// New returns a initialized delay queue
func New() *DelayQueue {
	dq := &DelayQueue{
		queue:         list.New(),
		worker:        make(chan semaphore, 1),
		reconsumption: make(chan semaphore, 1),
	}
	go dq.delayService()
	return dq
}

// EnQueue enters delay queue, stay in queue for delay milliseconds then leave immediately, it will leave immediately if delay less or equal than 0.
func (dq *DelayQueue) EnQueue(value interface{}, delay int64) {
	expireTime := time.Now().Add(time.Millisecond * time.Duration(delay))

	if delay <= 0 {
		if dq.delay != nil {
			dq.delay <- value
		}
		return
	}

	dq.rw.Lock()

	elem := dq.queue.Back()
	var mark *list.Element

	for elem != nil && expireTime.Before(elem.Value.(*dqItem).expire) {
		mark = elem
		elem = elem.Prev()
	}

	item := &dqItem{value, expireTime}

	if elem == nil && mark == nil {
		dq.queue.PushFront(item)
	} else if mark == nil {
		dq.queue.PushBack(item)
	} else if mark == dq.queue.Front() && elem == nil {
		dq.queue.PushFront(item)
		dq.reconsumption <- yellowLight
	} else {
		dq.queue.InsertBefore(item, mark)
	}

	if dq.queue.Len() == 1 {
		dq.worker <- greenLight
	}

	dq.rw.Unlock()
}

// Receive returns a received only channel, which can be used for receiving value left from queue
func (dq *DelayQueue) Receive() <-chan interface{} {
	if dq.delay == nil {
		dq.delay = make(chan interface{})
	}
	return dq.delay
}

// Trav returns a received only channel, which can be used to receive traversing result
func (dq *DelayQueue) Trav() <-chan interface{} {
	ch := make(chan interface{})

	go func() {
		dq.rw.RLock()
		defer dq.rw.RUnlock()
		defer close(ch)

		for elem := dq.queue.Front(); elem != nil; elem = elem.Next() {
			ch <- elem.Value.(*dqItem).value
		}

	}()

	return ch
}

// TravWithFunc traverses delay queue dq with function f
func (dq *DelayQueue) TravWithFunc(f TravFunc) {
	dq.rw.RLock()
	defer dq.rw.RUnlock()

	if f == nil {
		return
	}

	for elem := dq.queue.Front(); elem != nil; elem = elem.Next() {
		f(elem.Value.(*dqItem).value)
	}
}
