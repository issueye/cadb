package client

import (
	"testing"
	"time"
)

func TestGRPCClient(t *testing.T) {
	client, err := NewCadbClient("localhost:50051")
	if err != nil {
		t.Errorf("Error creating client: %v", err)
		return
	}

	key := "queue-sign-inc-num"
	stream, err := client.Watch(key)
	if err != nil {
		t.Errorf("Error watching key: %v", err)
		return
	}

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					return
				}
				t.Errorf("Error receiving message: %v", err)
				return
			} else {
				t.Logf("Received message: %v", msg)
			}
		}
	}()

	err = client.Set(key, "testdata", 0)
	if err != nil {
		t.Errorf("Error setting key: %v", err)
		return
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Error getting key: %v", err)
		return
	}

	t.Logf("Value: %v", value)

	err = client.AddExpire(key, 5)
	if err != nil {
		t.Errorf("Error adding expire: %v", err)
	}

	// sleep
	time.Sleep(7 * time.Second)
	err = client.CloseWatch(key)
	if err != nil {
		t.Errorf("Error closing watch: %v", err)
		return
	}

	client.Close()
}
