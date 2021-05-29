package queue

import "container/heap"

// PriorityQueue queue has priority, the first element has the highest priority
type PriorityQueue struct {
	queue *priorityHeap
	dict  map[interface{}]*Entry
}

// NewPriorityQueue construct of priority queue
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		queue: &priorityHeap{},
		dict:  make(map[interface{}]*Entry),
	}
}

// Push push element into pq with priority
func (pq *PriorityQueue) Push(priority int, x interface{}) {
	v, ok := pq.dict[x]
	if !ok {
		v = &Entry{Value: x, Priority: priority}
		pq.queue.Push(v)
	} else {
		pq.Update(v, priority)
	}
	pq.dict[x] = v
}

// Pop pop the high Priority element from queue
func (pq *PriorityQueue) Pop() *Entry {
	entry, ok := pq.queue.Pop().(*Entry)
	if !ok || entry == nil {
		return nil
	}
	delete(pq.dict, entry.Value)
	return entry
}

// Peek return the highest priority element
func (pq *PriorityQueue) Peek() *Entry {
	return pq.queue.Peek()
}

// Update update the priority of the given element
func (pq *PriorityQueue) Update(old *Entry, priority int) {
	pq.queue.update(old, old.Value, priority)
}

// GetAll return limited Entries, if limit = -1 ,it will return all entries
func (pq *PriorityQueue) GetAll(limit int) []*Entry {
	result := pq.queue.GetAll(limit)
	return result
}

// RemoveValues remove elements
func (pq *PriorityQueue) RemoveValues(values []interface{}) {
	for _, v := range values {
		if entry, ok := pq.dict[v]; ok {
			pq.queue.remove(entry.Index)
			delete(pq.dict, v)
		}
	}

}

// Has it will return true if queue has the value
func (pq *PriorityQueue) Has(v interface{}) bool {
	if value, ok := pq.dict[v]; ok && value.Index != -1 {
		return true
	}
	return false
}

// Size return the size of queue
func (pq *PriorityQueue) Size() int {
	return pq.queue.Len()
}

// Entry internal struct for record element
type Entry struct {
	Value    interface{}
	Priority int
	Index    int
}

// priorityHeap implements heap.Interface and holds entries.
type priorityHeap []*Entry

// Len
func (pq priorityHeap) Len() int { return len(pq) }

// Lss
func (pq priorityHeap) Less(i, j int) bool {
	return pq[i].Priority >= pq[j].Priority
}

// Swap
func (pq priorityHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push push element to heap
func (pq *priorityHeap) Push(x interface{}) {
	entry := x.(*Entry)
	entry.Index = len(*pq)
	*pq = append(*pq, entry)
	pq.fix(entry)
}

// Pop pop the highest Priority element from heap
func (pq *priorityHeap) Pop() interface{} {
	return pq.remove(0)
}

// Peek peek the highest priority element from heap
func (pq *priorityHeap) Peek() *Entry {
	old := *pq
	return old[0]
}

// GetAll return the limited size of elements
func (pq *priorityHeap) GetAll(limit int) []*Entry {
	if limit == -1 {
		limit = len(*pq)
	}
	tmp := *pq
	return tmp[:limit]
}

// remove remove index from heap
// it will swap the lowest priority element of the target ,and then fix the index
func (pq *priorityHeap) remove(id int) interface{} {
	old := *pq
	result := old[id]
	pq.Swap(id, len(old)-1)
	*pq = old[:len(old)-1]
	pq.fix(old[id])
	result.Index = -1 // safe mark
	return result
}

// fix the entry index
func (pq *priorityHeap) fix(entry *Entry) {
	heap.Fix(pq, entry.Index)
}

// update modifies the priority and value of an entry in the queue
func (pq *priorityHeap) update(entry *Entry, value interface{}, priority int) {
	entry.Value = value
	entry.Priority = priority
	heap.Fix(pq, entry.Index)
}
