package jobqueue

import (
	"fmt"
	"slices"
	"sync"
)

type JobQueue struct {
	items    []any
	size     int
	capacity int
	first    int
	lock     sync.RWMutex
}

// balloon doubles the queue's backing storage and preserves FIFO order.
func (q *JobQueue) balloon() {

	new_cap := q.capacity * 2
	new_queue := make([]any, new_cap)

	for i := range q.capacity {
		idx := (q.first + i) % q.capacity
		new_queue[i] = q.items[idx]
	}

	q.items = new_queue
	q.capacity = new_cap
	q.first = 0

}

// isEmpty reports whether the queue contains no items.
func (q *JobQueue) isEmpty() bool {
	return q.size == 0
}

// shrink reduces the backing storage when the queue is mostly empty.
func (q *JobQueue) shrink() {

	if q.capacity <= 1 || q.size > q.capacity/4 {
		return
	}

	new_capacity := max(q.capacity/2, 1)

	new_q := make([]any, new_capacity)

	for i := range q.size {
		index := (q.first + i) % q.capacity
		new_q[i] = q.items[index]
	}

	q.items = new_q
	q.capacity = new_capacity
	q.first = 0
}

// Clear removes all items from the queue while preserving its capacity.
func (q *JobQueue) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items = make([]any, q.capacity)
	q.size = 0
	q.first = 0
}

// Contains reports whether item is present in the queue.
func (q *JobQueue) Contains(item any) bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return slices.Contains(q.ToSlice(), item)
}

// Dequeue removes and returns the oldest item in the queue. It returns false
// when the queue is empty.
func (q *JobQueue) Dequeue() (any, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.isEmpty() {
		return nil, false
	}

	item := q.items[q.first]
	q.items[q.first] = nil

	q.first = (q.first + 1) % q.capacity
	q.size--

	q.shrink()

	return item, true
}

// Enqueue adds item to the back of the queue.
func (q *JobQueue) Enqueue(item any) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.size == q.capacity {
		q.balloon()
	}

	idx := (q.first + q.size) % q.capacity
	q.items[idx] = item
	q.size++
}

// IsEmpty reports whether the queue contains no items.
func (q *JobQueue) IsEmpty() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.size == 0
}

// IsFull reports whether the queue has reached its current capacity.
func (q *JobQueue) IsFull() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.size == q.capacity
}

// Peek returns the oldest item without removing it, or nil when the queue is empty.
func (q *JobQueue) Peek() any {
	q.lock.RLock()
	defer q.lock.RUnlock()
	if q.size > 0 {
		return q.items[q.first]
	}
	return nil
}

// ToSlice returns the queue's items in FIFO order as an independent slice.
func (q *JobQueue) ToSlice() []any {
	q.lock.RLock()
	defer q.lock.RUnlock()
	temp_arr := make([]any, q.size)

	for i := range q.size {
		idx := (q.first + i) % q.capacity
		temp_arr[i] = q.items[idx]
	}

	return temp_arr
}

// String returns the queue's items formatted as a slice.
func (q *JobQueue) String() string {
	return fmt.Sprint(q.ToSlice())
}

// NewJobQueue creates an empty queue with the requested initial capacity.
// Capacities less than one are normalized to one.
func NewJobQueue(JobCapacity int) *JobQueue {

	if JobCapacity < 1 {
		JobCapacity = 1
	}

	jq := JobQueue{
		items:    make([]any, JobCapacity),
		size:     0,
		capacity: JobCapacity,
		first:    0,
	}

	return &jq
}
