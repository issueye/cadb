package client

import (
	"testing"

	"github.com/r3labs/sse/v2"
)

func TestClient(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}

	client.WatchCallBak("test-sss", func(value []byte) {
		t.Log("watched value:", string(value))
	})

	client.WatchCallBak("test-sss---", func(value []byte) {
		t.Log("watched value---:", string(value))
	})

	err = client.Set("test-sss", "aasdafsad")
	if err != nil {
		t.Fatal(err)
	}

	value, err := client.Get("test-sss")
	if err != nil || value.Value != "aasdafsad" {
		t.Fatal(err)
	}

	err = client.Set("test-sss---", "----12www--")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Set("test-sss", "------")
	if err != nil {
		t.Fatal(err)
	}

	err = client.Set("test-sss", "------2")
	if err != nil {
		t.Fatal(err)
	}

	// delete
	err = client.Delete("test-sss")
	if err != nil {
		t.Fatal(err)
	}

	value, err = client.Get("test-sss")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("value:", value.Value)
}

func New() (*Client, error) {
	url := "http://localhost:8080"
	return NewClient(url)
}

func TestClient2(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}

	client.WatchCallBak("test-sss", func(value []byte) {
		t.Log("watched value:", string(value))
	})

	err = client.Set("test-sss", "------2")
	if err != nil {
		t.Fatal(err)
	}

	client.SSEClient.OnDisconnect(func(c *sse.Client) {
		t.Log("disconnected")
	})

	// delete
	// err = client.Delete("test-sss")
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
