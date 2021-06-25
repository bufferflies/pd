package cache

import (
	"fmt"
	"time"

	"github.com/tikv/pd/pkg/btree"
)

// defaultDegree default btree degree, the depth is h<log(degree)(capacity+1)/2
const defaultDegree = 4

// PriorityQueue queue has priority  and preempt
type PriorityQueue struct {
	items    map[interface{}]*Entry
	btree    *btree.BTree
	capacity int
}

// NewPriorityQueue construct of priority queue
func NewPriorityQueue(capacity int) *PriorityQueue {
	return &PriorityQueue{
		items:    make(map[interface{}]*Entry),
		btree:    btree.New(defaultDegree),
		capacity: capacity,
	}
}

// Put put value into queue
func (pq *PriorityQueue) Put(priority int, value interface{}) bool {
	entry, ok := pq.items[value]
	if !ok {
		entry = &Entry{Priority: priority, Value: value}
		if pq.Len() >= pq.capacity {
			min := pq.btree.Min()
			// avoid to capacity equal 0
			if min == nil || entry.Less(min) {
				return false
			}
			pq.Remove(min.(*Entry).Value)
		}
	} else {
		// delete before update
		if entry.Priority != priority {
			pq.btree.Delete(entry)
			entry.Priority = priority
			entry.Retry = 0
		} else {
			entry.Retry = entry.Retry + 1
		}
	}
	entry.Last = time.Now()
	pq.btree.ReplaceOrInsert(entry)
	pq.items[value] = entry
	return true
}

// Get get value from queue
func (pq *PriorityQueue) Get(value interface{}) *Entry {
	return pq.items[value]

}

// Peek return the highest priority entry
func (pq *PriorityQueue) Peek() *Entry {
	if max, ok := pq.btree.Max().(*Entry); ok {
		return max
	}
	return nil
}

// Tail return the lowest priority entry
func (pq *PriorityQueue) Tail() *Entry {
	if min, ok := pq.btree.Min().(*Entry); ok {
		return min
	}
	return nil
}

// Elems return all elements in queue
func (pq *PriorityQueue) Elems() []*Entry {
	rs := make([]*Entry, pq.Len())
	count := 0
	pq.btree.Descend(func(i btree.Item) bool {
		rs[count] = i.(*Entry)
		count++
		return true
	})
	return rs
}

// Remove remove value from queue
func (pq *PriorityQueue) Remove(value interface{}) {
	if v, ok := pq.items[value]; ok {
		if i := pq.btree.Delete(v); i == nil {
			fmt.Printf("error:%v \n", pq.items)
		}
		delete(pq.items, value)
	}
}

// Len return queue size
func (pq *PriorityQueue) Len() int {
	return pq.btree.Len()
}

// Entry a record in PriorityQueue
type Entry struct {
	Priority int
	Value    interface{}
	Last     time.Time
	Retry    int
}

// Less return true if the entry has smaller priority
func (r *Entry) Less(other btree.Item) bool {
	left := r.Priority
	right := other.(*Entry).Priority
	return left > right
}
