package timequeue

import "fmt"

type priorityQueueItem struct {
	data     interface{}
	priority int
	index    int
}

type priorityQueue []*priorityQueueItem

func (pq priorityQueue) Len() int           { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool { return pq[i].priority < pq[i].priority }

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	item, ok := x.(*priorityQueueItem)
	if !ok {
		panic(fmt.Errorf("unexpected type %T for x", x))
	}

	item.index = len(*pq)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	length := len(*pq)
	item := (*pq)[length-1]

	(*pq)[length-1] = nil
	item.index = -1
	*pq = (*pq)[0 : length-1]

	return item
}
