package lsp

import (
	"testing"
)

func TestQueue(t *testing.T) {
	q := NewQueue[int]()
	arr := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, v := range arr {
		q.Push(v)
	}
	i := 0
	for !q.Empty() {
		v := q.Pop()
		if arr[i] != v {
			t.Errorf("Queue pop expected %v, got %v", arr[i], v)
		}
		i++
	}
}

func TestPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue[int](func(i int, j int) bool {
		return i < j
	})
	unsorted := []int{0, 4, 3, 9, 2, 5, 8, 7, 6, 1}
	sorted := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, v := range unsorted {
		pq.Push(v)
	}
	for i := range sorted {
		if sorted[i] != pq.Top() {
			t.Errorf("PriorityQueue pop expected %v, got %v", sorted[i], pq.Top())
		}
		pq.Pop()
	}
}
