package timequeue

import "fmt"

type priorityQueueItem struct {
	data     interface{}
	priority int64
	index    int
}

type priorityQueue struct {
	items      []*priorityQueueItem
	allocation bool
}

func (pq *priorityQueue) Len() int           { return len(pq.items) }
func (pq *priorityQueue) Less(i, j int) bool { return pq.items[i].priority < pq.items[i].priority }

func (pq *priorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	item, ok := x.(*priorityQueueItem)
	if !ok {
		panic(fmt.Errorf("unexpected type %T for x", x))
	}

	if !pq.allocation {
		if len(pq.items) < cap(pq.items) {
			item.index = len(pq.items)
			pq.items = append(pq.items, item)
		}
	} else {
		pq.items = append(pq.items, item)
	}
}

func (pq *priorityQueue) Pop() interface{} {
	length := len(pq.items)
	item := pq.items[length]

	pq.items[length-1] = nil
	item.index = -1
	pq.items = pq.items[0 : length-1]

	return item
}
