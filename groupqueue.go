package timequeue

import (
	"container/list"
	"sync"
)

type GroupQueue struct {
	swarmPerGroup int
	totalGroup    int64
	groupIndex    priorityQueue
	groups        *list.List
	reusePool     *sync.Pool
}

type swarm struct {
	swarmIndex      priorityQueue
	isFull          bool
	length          int64
	nextExpiredTime int64
	lastExpiredTime int64
	ordered         bool
}

// NewGroupQueue ...
func NewGroupQueue(swarmPerGroup int) *GroupQueue {
	return &GroupQueue{
		swarmPerGroup: swarmPerGroup,
		groupIndex:    make(priorityQueue, 0),
		groups:        list.New(),
		reusePool:     &sync.Pool{},
	}
}

func (gq *GroupQueue) enterQueue(n *node) {
	return
}

func (gq *GroupQueue) leaveHead() *node {
	return nil
}

func (gq *GroupQueue) peekHead() *node {
	return nil
}
