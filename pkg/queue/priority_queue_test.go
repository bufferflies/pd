package queue

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testQueueSuite{})

type testQueueSuite struct {
}

func (t *testQueueSuite) TestPriorityQueue(c *C) {
	pq := NewPriorityQueue()
	pq.Push(1, 1)
	pq.Push(3, 3)
	pq.Push(5, 3)
	pq.Push(2, 2)

	// case1 test getAll ,the highest element should be the first
	entries := pq.queue.GetAll(-1)
	c.Assert(len(entries), Equals, 3)
	c.Assert(entries[0].Priority, Equals, 5)
	c.Assert(entries[0].Value, Equals, 3)

	// case2 test remove the high element, and the second element should be the first
	pq.RemoveValues([]interface{}{3})
	c.Assert(pq.Size(), Equals, 2)
	entry := pq.Peek()
	c.Assert(entry.Priority, Equals, 2)
	c.Assert(entry.Index, Equals, 0)
	c.Assert(entry.Value, Equals, 2)

	// case3 update entry should be fix
	pq.Update(entry, 3)
	entry = pq.Pop()
	c.Assert(entry.Priority, Equals, 3)
	c.Assert(entry.Index, Equals, -1)
	c.Assert(entry.Value, Equals, 2)

	// case4 remove all element
	pq.RemoveValues([]interface{}{1})
	c.Assert(pq.Size(), Equals, 0)
	c.Assert(len(pq.dict), Equals, 0)
	c.Assert(pq.queue.Len(), Equals, 0)
}
