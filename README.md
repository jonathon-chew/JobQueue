# JobQueue

`JobQueue` is a generic, in-memory FIFO queue implemented in Go. It uses a circular buffer and grows or shrinks its backing storage as the number of queued items changes.

## Features

- FIFO `Enqueue` and `Dequeue` operations
- `Peek` without removing the next item
- `IsEmpty` and `IsFull` state checks
- `Contains`, `Clear`, and `ToSlice` helpers
- Circular storage so dequeue/enqueue cycles reuse capacity
- Automatic growth when the queue is full
- Automatic shrinking when the queue is mostly empty
- Stores values as `any`, allowing different Go value types

This is a data-structure project rather than a concurrent worker pool. Queue operations are protected by a mutex, but the queue does not provide job execution, persistence, or retry handling.

## Installation

Clone the repository and run the tests from its root:

```bash
git clone https://github.com/jonathon-chew/JobQueue.git
cd JobQueue
go test ./...
```

The implementation is in `cmd/JobQueue/main.go` and is currently packaged as `jobqueue`.

## Example

```go
queue := NewJobQueue(3)

queue.Enqueue("send email")
queue.Enqueue("refresh cache")

next := queue.Peek() // "send email"
job, ok := queue.Dequeue()
if ok {
	// process job
	_ = job
}
_ = next
```

## Testing

```bash
go test ./...
```

The test suite covers FIFO ordering, empty-queue behaviour, wraparound, growth, and queue inspection helpers.

## License

See [LICENSE](./LICENSE) for details.
