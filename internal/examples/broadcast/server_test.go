package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Test_broadcastServer subscribes clients with and without compression,
// publishes messages below and above the compression threshold and ensures
// every client receives all of them.
func Test_broadcastServer(t *testing.T) {
	t.Parallel()

	s := httptest.NewServer(newBroadcastServer(t.Logf))
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var clients []*websocket.Conn
	for i := 0; i < 4; i++ {
		mode := websocket.CompressionDisabled
		if i%2 == 0 {
			mode = websocket.CompressionNoContextTakeover
		}
		c, _, err := websocket.Dial(ctx, s.URL+"/subscribe", &websocket.DialOptions{
			CompressionMode: mode,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(websocket.StatusInternalError, "the sky is falling")
		clients = append(clients, c)
	}

	msgs := []string{
		"hello",
		strings.Repeat("hello world ", 512),
	}
	for _, msg := range msgs {
		resp, err := http.Post(s.URL+"/publish", "text/plain", strings.NewReader(msg))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("expected status %v but got %v", http.StatusAccepted, resp.StatusCode)
		}
	}

	for i, c := range clients {
		for _, exp := range msgs {
			_, b, err := c.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != exp {
				t.Fatalf("client %v: expected %q but got %q", i, exp, b)
			}
		}
		c.Close(websocket.StatusNormalClosure, "")
	}
}
