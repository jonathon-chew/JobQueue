package jobqueue

import (
	"reflect"
	"sync"
	"testing"
)

// TestQueue verifies basic FIFO behaviour across enqueue and dequeue operations.
func TestQueue(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	item, _ := q.Dequeue()

	if item != "A" {
		t.Fatalf("expected A, got %v", item)
	}

	q.Enqueue("D")

	item, _ = q.Dequeue()

	if item != "B" {
		t.Fatalf("expected B, got %v", item)
	}

	item, _ = q.Dequeue()

	if item != "C" {
		t.Fatalf("expected C, got %v", item)
	}

	item, _ = q.Dequeue()

	if item != "D" {
		t.Fatalf("expected D, got %v", item)
	}

	if !q.IsEmpty() {
		t.Fatal("expected queue to be empty")
	}
}

// TestNewJobQueue verifies that a new queue has the requested capacity and is empty.
func TestNewJobQueue(t *testing.T) {
	q := NewJobQueue(5)

	if q.capacity != 5 {
		t.Errorf("expected capacity 5, got %d", q.capacity)
	}

	if q.size != 0 {
		t.Errorf("expected size 0, got %d", q.size)
	}

	if !q.IsEmpty() {
		t.Error("new queue should be empty")
	}
}

// TestEnqueue verifies that items are appended in order and capacity is reported correctly.
func TestEnqueue(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	expected := []any{"A", "B", "C"}

	if got := q.ToSlice(); !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	if q.size != 3 {
		t.Errorf("expected size 3, got %d", q.size)
	}

	if !q.IsFull() {
		t.Error("queue should be full")
	}
}

// TestDequeue verifies that items are removed in FIFO order.
func TestDequeue(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	expected := []any{"A", "B", "C"}

	for _, want := range expected {
		got, ok := q.Dequeue()

		if !ok {
			t.Fatal("expected successful dequeue")
		}

		if got != want {
			t.Errorf("expected %v, got %v", want, got)
		}
	}

	if !q.IsEmpty() {
		t.Error("queue should be empty")
	}
}

// TestDequeueEmptyQueue verifies dequeue behaviour when no items are available.
func TestDequeueEmptyQueue(t *testing.T) {
	q := NewJobQueue(3)

	item, ok := q.Dequeue()

	if ok {
		t.Error("dequeue should fail on empty queue")
	}

	if item != nil {
		t.Errorf("expected nil, got %v", item)
	}
}

// TestPeek verifies that peeking returns the oldest item without removing it.
func TestPeek(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")

	if got := q.Peek(); got != "A" {
		t.Errorf("expected A, got %v", got)
	}

	if q.size != 2 {
		t.Error("Peek should not modify queue size")
	}
}

// TestPeekEmptyQueue verifies that peeking an empty queue returns nil.
func TestPeekEmptyQueue(t *testing.T) {
	q := NewJobQueue(3)

	if got := q.Peek(); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

// TestCircularQueue verifies FIFO order after the circular buffer wraps once.
func TestCircularQueue(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	q.Dequeue()
	q.Dequeue()

	q.Enqueue("D")
	q.Enqueue("E")

	expected := []any{"C", "D", "E"}

	if got := q.ToSlice(); !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

// TestCircularQueueMultipleWraps verifies FIFO order across repeated wraparounds.
func TestCircularQueueMultipleWraps(t *testing.T) {
	q := NewJobQueue(4)

	for i := range 20 {
		q.Enqueue(i)

		if i >= 4 {
			got, ok := q.Dequeue()

			if !ok {
				t.Fatal("expected successful dequeue")
			}

			if got != i-4 {
				t.Errorf("expected %d, got %v", i-4, got)
			}
		}
	}
}

// TestBalloon verifies that a full queue grows when another item is enqueued.
func TestBalloon(t *testing.T) {
	q := NewJobQueue(2)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	if q.capacity != 4 {
		t.Errorf("expected capacity 4, got %d", q.capacity)
	}

	expected := []any{"A", "B", "C"}

	if got := q.ToSlice(); !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

// TestBalloonAfterWraparound verifies growth preserves order after wraparound.
func TestBalloonAfterWraparound(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	q.Dequeue()

	q.Enqueue("D")

	// Queue is logically B, C, D.
	// Enqueueing E should trigger growth.

	q.Enqueue("E")

	expected := []any{"B", "C", "D", "E"}

	if got := q.ToSlice(); !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	if q.capacity != 6 {
		t.Errorf("expected capacity 6, got %d", q.capacity)
	}
}

// TestShrink verifies that a mostly empty queue reduces its capacity.
func TestShrink(t *testing.T) {
	q := NewJobQueue(8)

	q.Enqueue("A")
	q.Enqueue("B")

	for range 6 {
		q.Dequeue()
	}

	if q.capacity != 2 {
		t.Errorf("expected capacity 4, got %d", q.capacity)
	}
}

// TestShrinkAfterWraparound verifies shrinking preserves order after wraparound.
func TestShrinkAfterWraparound(t *testing.T) {
	q := NewJobQueue(8)

	for i := range 8 {
		q.Enqueue(i)
	}

	for range 5 {
		q.Dequeue()
	}

	// Remaining: 5, 6, 7

	q.Dequeue()

	// Remaining: 6, 7
	// Shrinking should preserve FIFO order.

	expected := []any{6, 7}

	if got := q.ToSlice(); !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

// TestClear verifies that clearing removes items and preserves capacity.
func TestClear(t *testing.T) {
	q := NewJobQueue(5)

	q.Enqueue("A")
	q.Enqueue("B")

	q.Clear()

	if !q.IsEmpty() {
		t.Error("queue should be empty after Clear")
	}

	if q.size != 0 {
		t.Errorf("expected size 0, got %d", q.size)
	}

	if q.first != 0 {
		t.Errorf("expected first 0, got %d", q.first)
	}

	if q.capacity != 5 {
		t.Errorf("Clear should preserve capacity")
	}
}

// TestContains verifies membership checks before and after dequeue operations.
func TestContains(t *testing.T) {
	q := NewJobQueue(4)

	q.Enqueue("A")
	q.Enqueue("B")
	q.Enqueue("C")

	if !q.Contains("B") {
		t.Error("expected queue to contain B")
	}

	if q.Contains("D") {
		t.Error("queue should not contain D")
	}

	q.Dequeue()

	if q.Contains("A") {
		t.Error("queue should no longer contain A")
	}
}

// TestToSliceReturnsCopy verifies that ToSlice does not expose queue storage.
func TestToSliceReturnsCopy(t *testing.T) {
	q := NewJobQueue(3)

	q.Enqueue("A")
	q.Enqueue("B")

	result := q.ToSlice()
	result[0] = "CHANGED"

	if q.Peek() != "A" {
		t.Error("ToSlice should return an independent copy")
	}
}

// TestConcurrent verifies that concurrent enqueues retain every item exactly once.
func TestConcurrent(t *testing.T) {
	q := NewJobQueue(100)
	var producers sync.WaitGroup

	for i := range 100 {
		producers.Add(1)
		go func(i int) {
			defer producers.Done()
			q.Enqueue(i)
		}(i)
	}
	producers.Wait()

	seen := make(map[int]bool, 100)
	for range 100 {
		number, ok := q.Dequeue()
		if !ok {
			t.Fatal("queue became empty before all items were dequeued")
		}
		n, ok := number.(int)
		if !ok || n < 0 || n >= 100 || seen[n] {
			t.Fatalf("unexpected or duplicate item: %v", number)
		}
		seen[n] = true
	}

	for i := range 100 {
		if !seen[i] {
			t.Errorf("item %d was not enqueued", i)
		}
	}
}
