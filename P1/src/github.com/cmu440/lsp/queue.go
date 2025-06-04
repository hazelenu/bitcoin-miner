package lsp

import "container/heap"

/*
Queue

A standard FIFO queue struct with thread-UNSAFE accessors.
Though the implementation is simple, the underlying array may be growing indefinitely and cause memory issues.
This abstraction allows us to swap in a more sophisticated implementation (e.g. deque) if problems do arise.
*/
type Queue[T any] struct {
	data []T
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		data: make([]T, 0),
	}
}

func (q *Queue[T]) Push(v T) {
	q.data = append(q.data, v)
}

func (q *Queue[T]) Pop() T {
	q.assertNonEmpty()
	ret := q.data[0]
	q.data = q.data[1:]
	return ret
}

func (q *Queue[T]) Len() int {
	return len(q.data)
}

func (q *Queue[T]) Empty() bool {
	return q.Len() == 0
}

func (q *Queue[T]) Front() T {
	q.assertNonEmpty()
	return q.data[0]
}

func (q *Queue[T]) assertNonEmpty() {
	if q.Empty() {
		panic("Queue is empty")
	}
}

/*
Priority Queue

Useful for sorting read but pending delivery messages
*/

type PriorityQueue[T any] struct {
	internal *priorityQueueInternal[T]
}

type priorityQueueInternal[T any] struct {
	data []any
	less func(T, T) bool
}

func NewPriorityQueue[T any](less func(T, T) bool) *PriorityQueue[T] {
	pq := &PriorityQueue[T]{
		internal: &priorityQueueInternal[T]{
			data: make([]any, 0),
			less: less,
		},
	}
	heap.Init(pq.internal)
	return pq
}

func (pq *priorityQueueInternal[T]) Less(i, j int) bool {
	return pq.less(pq.data[i].(T), pq.data[j].(T))
}

func (pq *priorityQueueInternal[T]) Swap(i, j int) {
	pq.data[i], pq.data[j] = pq.data[j], pq.data[i]
}

func (pq *priorityQueueInternal[T]) Push(v any) {
	pq.data = append(pq.data, v)
}

func (pq *priorityQueueInternal[T]) Pop() any {
	pq.assertNonEmpty()
	n := pq.Len()
	item := pq.data[n-1]
	pq.data[n-1] = nil
	pq.data = pq.data[:n-1]

	return item
}

func (pq *priorityQueueInternal[T]) Len() int {
	return len(pq.data)
}

func (pq *priorityQueueInternal[T]) Empty() bool {
	return pq.Len() == 0
}

func (pq *priorityQueueInternal[T]) Top() any {
	pq.assertNonEmpty()
	return pq.data[0]
}

func (pq *priorityQueueInternal[T]) assertNonEmpty() {
	if pq.Empty() {
		panic("Priority Queue is empty")
	}
}

func (pq *PriorityQueue[T]) Push(v T) {
	heap.Push(pq.internal, v)
}

func (pq *PriorityQueue[T]) Pop() T {
	return heap.Pop(pq.internal).(T)
}

func (pq *PriorityQueue[T]) Top() T {
	return pq.internal.Top().(T)
}

func (pq *PriorityQueue[T]) Len() int {
	return pq.internal.Len()
}

func (pq *PriorityQueue[T]) Empty() bool {
	return pq.internal.Empty()
}
