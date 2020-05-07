package timequeue

import (
	"container/list"
	"errors"
	"math"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type item struct {
	Value  interface{}
	Expire time.Time
}

// TimeQueue represents a time queue
type TimeQueue struct {
	lock            sync.RWMutex
	queue           *list.List
	worker          *semaphore
	leave           chan interface{}
	persistent      bool
	persistence     *persistence
	stopTime        time.Time
	stopPersistence bool
	osSignal        chan os.Signal
	receiveBuffer   int
}

// TravFunc is used for traversing time queue, the argument of TravFunc is Value stored in queue item
type TravFunc func(interface{})

func (tq *TimeQueue) service() {
	var releaseRLock bool

	for {
		tq.lock.RLock()

		if releaseRLock {
			releaseRLock = false
		}

		if tq.queue.Len() == 0 {
			tq.lock.RUnlock()
			releaseRLock = true

			tq.worker.peek(consumption)

			continue
		}

		tq.worker.processAvailableSignal()

		if time.Now().Before(tq.queue.Front().Value.(*node).item.Expire) {
			tq.lock.RUnlock()
			releaseRLock = true

		here:
			select {
			case *tq.worker.addr() = <-tq.worker.channel():

				switch tq.worker.view() {
				case reconsumption:
					if time.Now().Before(tq.queue.Front().Value.(*node).item.Expire) {
						continue
					}

				case stop, resume:
					tq.worker.doAction()
					goto here
				}

			case <-time.After(tq.queue.Front().Value.(*node).item.Expire.Sub(time.Now())):
				tq.worker.processAvailableSignal()
			}
		}

		if !releaseRLock {
			tq.lock.RUnlock()
		}

		tq.lock.Lock()

		tq.worker.processAvailableSignal()

		for tmp, iter := (*node)(nil), tq.queue.Front(); iter != nil; iter = tq.queue.Front() {
			tmp = iter.Value.(*node)

			if tmp.item.Expire.After(time.Now()) {
				break
			}

			if tq.leave != nil {
				tq.leave <- tmp.item.Value
			}

			select {
			case <-tq.osSignal:
				return
			default:
				if tq.persistent || (!tq.stopTime.IsZero() && (tmp.item.Expire.Before(tq.stopTime) || tmp.item.Expire.Equal(tq.stopTime))) {
					tq.persistence.expired <- tmp.length
				} else if !tq.stopPersistence {
					tq.persistence.worker.send(stop)
					tq.stopPersistence = true
				}
			}

			tmp = nil
			tq.queue.Remove(iter)
		}

		tq.lock.Unlock()
	}
}

// New returns a initialized time queue
func New() *TimeQueue {
	tq := &TimeQueue{
		queue:    list.New(),
		worker:   newSemaphore(1),
		osSignal: make(chan os.Signal, 1),
	}

	signal.Notify(tq.osSignal, syscall.SIGINT, syscall.SIGTERM)

	go tq.service()
	return tq
}

// Persist enable persistence for time queue
func (tq *TimeQueue) Persist(filename string, maxExpired int64, value interface{}, registry map[string]interface{}) (*TimeQueue, error) {
	if tq.persistent {
		return tq, errors.New("persistence already enabled")
	}

	return tq, tq.withPersistence(filename, maxExpired, value, registry)
}

// MustPersist must enable persistence for time queue, otherwise panics if there are any error occurred
func (tq *TimeQueue) MustPersist(filename string, maxExpired int64, value interface{}, registry map[string]interface{}) *TimeQueue {
	if tq.persistent {
		panic("persistence already enabled")
	}

	if err := tq.withPersistence(filename, maxExpired, value, registry); err != nil {
		panic(err)
	}

	return tq
}

func (tq *TimeQueue) insertAndCalculateOffset(n *node, offset *int64) {
	if front, back := tq.queue.Front(), tq.queue.Back(); front == nil {
		tq.queue.PushFront(n)
		tq.worker.send(consumption)
	} else if math.Abs(float64(n.item.Expire.UnixNano()-front.Value.(*node).item.Expire.UnixNano())) <=
		math.Abs(float64(n.item.Expire.UnixNano()-back.Value.(*node).item.Expire.UnixNano())) {
		iter := front
		for tmp := (*node)(nil); iter != nil; iter = iter.Next() {
			tmp = iter.Value.(*node)

			if n.item.Expire.Before(tmp.item.Expire) {
				break
			}
			*offset += tmp.length
		}

		if iter != nil {
			tq.queue.InsertBefore(n, iter)
			if iter == front {
				tq.worker.send(reconsumption)
			}
		} else {
			tq.queue.PushBack(n)
		}
	} else {
		iter := back
		for tmp := (*node)(nil); iter != nil; iter = iter.Prev() {
			tmp = iter.Value.(*node)

			if n.item.Expire.After(tmp.item.Expire) {
				break
			}
			*offset += tmp.length
		}

		*offset = atomic.LoadInt64(&tq.persistence.validLength) - *offset

		if iter != nil {
			tq.queue.InsertAfter(n, iter)
		} else {
			tq.queue.PushFront(n)
			tq.worker.send(reconsumption)
		}
	}
}

func (tq *TimeQueue) insert(n *node) {
	if front, back := tq.queue.Front(), tq.queue.Back(); front == nil {
		tq.queue.PushFront(n)
		tq.worker.send(consumption)
	} else if math.Abs(float64(n.item.Expire.UnixNano()-front.Value.(*node).item.Expire.UnixNano())) <
		math.Abs(float64(n.item.Expire.UnixNano()-back.Value.(*node).item.Expire.UnixNano())) {
		iter := front
		for iter != nil && n.item.Expire.After(iter.Value.(*node).item.Expire) {
			iter = iter.Next()
		}

		if iter != nil {
			tq.queue.InsertBefore(n, iter)
			if iter == front {
				tq.worker.send(reconsumption)
			}
		} else {
			tq.queue.PushBack(n)
		}
	} else {
		iter := back
		for iter != nil && n.item.Expire.Before(iter.Value.(*node).item.Expire) {
			iter = iter.Prev()
		}

		if iter != nil {
			tq.queue.InsertAfter(n, iter)
		} else {
			tq.queue.PushFront(n)
			tq.worker.send(reconsumption)
		}
	}
}

func (tq *TimeQueue) enqueue(n *node) {
	if tq.persistent {
		tq.persistence.encoder.in <- &n.item
		block := &blockinfo{}

		tq.insertAndCalculateOffset(n, &block.offset)

		select {
		case <-tq.osSignal:
			return
		default:
			block.data = <-tq.persistence.encoder.out
			n.length = int64(len(block.data))
			tq.persistence.stream <- block
		}
	} else {
		tq.insert(n)
	}
}

// EnQueue enters time queue, stay in queue for duration delay then leave immediately, it will leave immediately if delay less or equal than 0.
func (tq *TimeQueue) EnQueue(value interface{}, delay time.Duration) {
	expireTime := time.Now().Add(delay)
	n := &node{item: item{value, expireTime}}

	tq.lock.Lock()

	if delay <= 0 {
		if tq.leave != nil {
			tq.leave <- value
		}

		tq.lock.Unlock()
		return
	}

	if tq.persistent && reflect.TypeOf(value).Kind() == reflect.Func {
		panic("unsupported persistent type")
	}
	tq.enqueue(n)

	tq.lock.Unlock()
}

// SetReceiveBuffer sets buffer size for receiving
func (tq *TimeQueue) SetReceiveBuffer(buf int) *TimeQueue {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	if tq.receiveBuffer != buf {
		if tq.leave != nil {
			tq.leave = make(chan interface{}, buf)
		} else {
			tq.receiveBuffer = buf
		}
	}

	return tq
}

// Receive returns a received only channel, which can be used for receiving Value left from queue
func (tq *TimeQueue) Receive() <-chan interface{} {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	tq.leave = make(chan interface{}, tq.receiveBuffer)

	return tq.leave
}

// Traverse returns a received only channel, which can be used to receive traversing result
func (tq *TimeQueue) Traverse() <-chan interface{} {
	ch := make(chan interface{})

	go func() {
		tq.lock.RLock()
		defer tq.lock.RUnlock()
		defer close(ch)

		for elem := tq.queue.Front(); elem != nil; elem = elem.Next() {
			ch <- elem.Value.(*node).item.Value
		}
	}()

	return ch
}

// TraverseF traverses time queue tq with function f
func (tq *TimeQueue) TraverseF(f TravFunc) {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	for elem := tq.queue.Front(); elem != nil; elem = elem.Next() {
		f(elem.Value.(*node).item.Value)
	}
}

// SetMaxExpiredStorage sets maximum expired data in bytes
func (tq *TimeQueue) SetMaxExpiredStorage(maxExpired int64) {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	if !tq.persistent {
		panic("persistence not enabled")
	}
	tq.persistence.clean <- maxExpired
}

// Persistent reports whether persistence is enabled
func (tq *TimeQueue) Persistent() bool {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	return tq.persistent
}

// Len returns the length of time queue
func (tq *TimeQueue) Len() int {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	return tq.queue.Len()
}

// PersistOff turns persistence off
func (tq *TimeQueue) PersistOff() *TimeQueue {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	if !tq.persistent {
		return tq
	}
	tq.worker.send(stop)

	return tq
}

// PersistOn turns persistence on
func (tq *TimeQueue) PersistOn() *TimeQueue {
	tq.lock.RLock()
	defer tq.lock.RUnlock()

	if tq.persistent {
		return tq
	}
	tq.worker.send(resume)

	return tq
}
