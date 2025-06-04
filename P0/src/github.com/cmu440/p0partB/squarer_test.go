// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

func TestYourFirstGoTest(t *testing.T) {
	fmt.Println("Running second test.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		for i := 1; i <= 3; i++ {
			input <- i
		}
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	expectedResults := []int{1, 4, 9}
	received := []int{}
	for i := 0; i < len(expectedResults); i++ {
		select {
		case <-timeoutChan:
			t.Error("Test timed out.")
		case result := <-squares:
			received = append(received, result)
		}
	}
	for i, expected := range expectedResults {
		if received[i] != expected {
			t.Errorf("Error: got %d, expected %d", received[i], expected)
		}
	}

	sq.Close()
	go func() {
		input <- 1
	}()
	timeoutChan = time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		return
	case <-squares:
		t.Error("Close failed")
	}
}
