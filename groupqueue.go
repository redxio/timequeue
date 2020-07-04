package timequeue

import (
	"container/heap"
	"container/list"
	"sort"
	"sync"
)

type GroupQueue struct {
	groupSize      int
	groups         *list.List
	groupSpacePool *sync.Pool
	worker         *semaphore
	groupIndex     *priorityQueue
	orderlyIndex   bool
	lock           sync.RWMutex
}

type group struct {
	element           *list.Element
	queue             *priorityQueue
	maxPriorityMember *priorityQueueItem
	length            int64
	orderlyQueue      bool
	lock              sync.RWMutex
}

func (s *group) enter(member *priorityQueueItem) {
	heap.Push(s.queue, member)
	s.length += member.data.(*node).length

	if member.priority > s.maxPriorityMember.priority {
		s.maxPriorityMember = member
	}

	if s.full() && !s.orderlyQueue {
		sort.Sort(s.queue)
		s.orderlyQueue = true
	}
}

func (s *group) full() bool {
	return len(s.queue.items) == cap(s.queue.items)
}

// NewGroupQueue ...
func NewGroupQueue(groupSize int) *GroupQueue {
	return &GroupQueue{
		groupSize:      groupSize,
		groups:         list.New(),
		groupSpacePool: &sync.Pool{},
		worker:         newSemaphore(1),
		groupIndex: &priorityQueue{
			allocation: true,
		},
	}
}

func makeDecision(s1, s2 *group) *group {
	return nil
}

func (gq *GroupQueue) getGroupSpace() *group {
	var newGroup *group

	if idle := gq.groupSpacePool.Get(); idle != nil {
		newGroup = idle.(*group)
		if newGroup.orderlyQueue {
			newGroup.orderlyQueue = false
		}
	} else {
		newGroup = &group{
			queue: &priorityQueue{
				items: make([]*priorityQueueItem, 0, gq.groupSize),
			},
		}
	}

	return newGroup
}

func (gq *GroupQueue) setupGroup(front, back *group, member *priorityQueueItem) {
	newGroup := gq.getGroupSpace()

	newGroup.maxPriorityMember = member
	newGroup.length = member.data.(*node).length

	heap.Push(newGroup.queue, member)

	if front == nil && back != nil {
		newGroup.element = gq.groups.PushFront(newGroup)
		gq.worker.send(reconsumption)

	} else if front != nil && back == nil {
		newGroup.element = gq.groups.PushBack(newGroup)

	} else if front != nil && back != nil {
		newGroup.element = gq.groups.InsertAfter(newGroup, front.element)

	} else {
		newGroup.element = gq.groups.PushFront(newGroup)
		gq.worker.send(consumption)
	}

	heap.Push(gq.groupIndex, newGroup.queue.items[0])
}

func (gq *GroupQueue) selectGroup(priority int64) *group {
	low, high := 0, len(gq.groupIndex.items)-1
	var middle int
	var target *group

	for low <= high {
		middle = low + (high-low)/2
		target = gq.groupIndex.items[middle].data.(*group)

		if (priority >= gq.groupIndex.items[middle].priority && priority <= target.maxPriorityMember.priority) ||
			(priority >= gq.groupIndex.items[middle].priority && !target.full()) {
			return target
		} else if priority < gq.groupIndex.items[middle].priority {
			high = middle - 1
		} else {
			low = middle + 1
		}
	}
	return nil
}

func (gq *GroupQueue) enterQueue(n *node) {
	member := &priorityQueueItem{
		data:     n,
		priority: n.item.Expire.UnixNano(),
	}

	if gq.groups.Len() > 0 {
		front, back := gq.groups.Front(), gq.groups.Back()
		frontGroup, backGroup := front.Value.(*group), back.Value.(*group)

		// optimize for big scale data
		if frontGroup.lock.Lock(); member.priority <= frontGroup.queue.items[0].priority && !frontGroup.full() {
			frontGroup.enter(member)
			frontGroup.lock.Unlock()

		} else if member.priority <= frontGroup.queue.items[0].priority && frontGroup.full() {
			frontGroup.lock.Unlock()
			gq.setupGroup(nil, frontGroup, member)

		} else if backGroup.lock.Lock(); member.priority >= backGroup.queue.items[0].priority && !backGroup.full() {
			frontGroup.lock.Unlock()
			backGroup.enter(member)
			backGroup.lock.Unlock()

		} else if member.priority >= backGroup.queue.items[0].priority && backGroup.full() {
			frontGroup.lock.Unlock()
			backGroup.lock.Unlock()
			gq.setupGroup(backGroup, nil, member)

		} else {
			frontGroup.lock.Unlock()
			backGroup.lock.Unlock()

			gq.lock.Lock()

			if !gq.orderlyIndex {
				sort.Sort(gq.groupIndex)
			}

			gq.lock.Unlock()

		}
	} else {
		gq.setupGroup(nil, nil, member)
	}
}

func (gq *GroupQueue) leaveHead() *node {
	return nil
}

func (gq *GroupQueue) peekHead() *node {
	return nil
}
