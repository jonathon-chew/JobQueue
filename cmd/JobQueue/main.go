package jobqueue

import (
	"fmt"
	"slices"
)

type JobQueue struct {
	items    []any
	size     int
	capacity int
	first    int
}

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

func (q *JobQueue) shrink() {

	if q.capacity <= 1 || q.size > q.capacity/4 {
		return
	}

	new_capacity := q.capacity / 2

	if new_capacity < 1 {
		new_capacity = 1
	}

	new_q := make([]any, new_capacity)

	for i := range q.size {
		index := (q.first + i) % q.capacity
		new_q[i] = q.items[index]
	}

	q.items = new_q
	q.capacity = new_capacity
	q.first = 0
}

func (q *JobQueue) Clear() {
	q.items = make([]any, q.capacity)
	q.size = 0
	q.first = 0
}

func (q *JobQueue) Contains(item any) bool {
	return slices.Contains(q.items[q.first:], item)
}

func (q *JobQueue) Dequeue() (any, bool) {
	if q.IsEmpty() {
		return nil, false
	}

	item := q.items[q.first]
	q.items[q.first] = nil

	q.first = (q.first + 1) % q.capacity
	q.size--

	q.shrink()

	return item, true
}

func (q *JobQueue) Enqueue(item any) {

	if q.IsFull() {
		q.balloon()
	}

	idx := (q.first + q.size) % q.capacity
	q.items[idx] = item
	q.size++
}

func (q *JobQueue) IsEmpty() bool {
	return q.size == 0
}

func (q *JobQueue) IsFull() bool {
	return q.size == q.capacity
}

func (q *JobQueue) Peek() any {
	if !q.IsEmpty() {
		return q.items[q.first]
	}
	return nil
}

func (q *JobQueue) ToSlice() []any {
	temp_arr := make([]any, q.size)

	for i := range q.size {
		idx := (q.first + i) % q.capacity
		temp_arr[i] = q.items[idx]
	}

	return temp_arr
}

func (q *JobQueue) String() string {
	return fmt.Sprint(q.ToSlice())
}

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
