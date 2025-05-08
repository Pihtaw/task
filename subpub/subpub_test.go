package subpub

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPublishOrder(t *testing.T) {
	sp := NewSubPub()
	var mu sync.Mutex
	var received []int
	done := make(chan struct{})
	sub, err := sp.Subscribe("test", func(msg interface{}) {
		mu.Lock()
		received = append(received, msg.(int))
		mu.Unlock()
		if msg.(int) == 5 {
			close(done)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		err = sp.Publish("test", i)
		if err != nil {
			t.Errorf("Publish error: %v", err)
		}
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for messages")
	}
	sub.Unsubscribe()

	expected := []int{1, 2, 3, 4, 5}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(expected) {
		t.Errorf("Expected %d messages, got %d", len(expected), len(received))
	}
	for i, v := range expected {
		if received[i] != v {
			t.Errorf("At index %d: expected %d, got %d", i, v, received[i])
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	sp := NewSubPub()
	var count int
	var mu sync.Mutex
	sub, err := sp.Subscribe("test", func(msg interface{}) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	sp.Publish("test", "message")
	time.Sleep(50 * time.Millisecond)
	sub.Unsubscribe()
	sp.Publish("test", "message2")
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if count != 1 {
		t.Errorf("Expected count 1 after unsubscribe, got %d", count)
	}
	mu.Unlock()
}

func TestSlowSubscriber(t *testing.T) {
	sp := NewSubPub()
	var mu sync.Mutex
	var received []int
	sub, err := sp.Subscribe("test", func(msg interface{}) {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		received = append(received, msg.(int))
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	for i := 1; i <= 5; i++ {
		err = sp.Publish("test", i)
		if err != nil {
			t.Errorf("Publish error: %v", err)
		}
	}
	time.Sleep(600 * time.Millisecond)
	sub.Unsubscribe()

	expected := []int{1, 2, 3, 4, 5}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(expected) {
		t.Errorf("Expected %d messages, got %d", len(expected), len(received))
	}
	for i, v := range expected {
		if received[i] != v {
			t.Errorf("At index %d: expected %d, got %d", i, v, received[i])
		}
	}
}

func TestClose(t *testing.T) {
	sp := NewSubPub()
	var count int
	sub, err := sp.Subscribe("test", func(msg interface{}) {
		time.Sleep(50 * time.Millisecond)
		count++
	})
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	for i := 0; i < 5; i++ {
		sp.Publish("test", i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = sp.Close(ctx)
	if err == nil {
		t.Errorf("Expected context deadline exceeded error due to slow processing")
	}

	time.Sleep(300 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel2()
	err = sp.Close(ctx2)
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
	sub.Unsubscribe()
}
