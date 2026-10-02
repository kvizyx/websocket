//go:build !js

package websocket

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/coder/websocket/internal/test/assert"
)

type bufConn struct {
	bytes.Buffer
}

func (*bufConn) Close() error {
	return nil
}

// TestWritePreparedWire verifies that WritePrepared puts the same bytes on the
// wire as Write, including for messages written after it, and that the
// compressed payload is shared only when the connection compresses the
// message without context takeover.
func TestWritePreparedWire(t *testing.T) {
	t.Parallel()

	largeMsg := strings.Repeat("prepared message ", 100)
	smallMsg := "small"
	nextMsg := []byte(strings.Repeat("next message ", 100))

	testCases := []struct {
		name   string
		copts  *compressionOptions
		msg    string
		shared bool // Whether the compressed payload of the message is shared.
	}{
		{"Disabled", nil, largeMsg, false},
		{"ContextTakeover/AboveThreshold", CompressionContextTakeover.opts(), largeMsg, false},
		{"ContextTakeover/BelowThreshold", CompressionContextTakeover.opts(), smallMsg, false},
		{"NoContextTakeover/AboveThreshold", CompressionNoContextTakeover.opts(), largeMsg, true},
		{"NoContextTakeover/AtThreshold", CompressionNoContextTakeover.opts(), strings.Repeat("a", 512), true},
		{"NoContextTakeover/BelowThreshold", CompressionNoContextTakeover.opts(), strings.Repeat("a", 511), false},
		// Only the server side of the negotiated options applies to server writes.
		{"ServerNoContextTakeover", &compressionOptions{serverNoContextTakeover: true}, largeMsg, true},
		{"ClientNoContextTakeover", &compressionOptions{clientNoContextTakeover: true}, largeMsg, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			newServerConn := func() (*Conn, *bufConn) {
				rwc := &bufConn{}
				c := newConn(connConfig{
					rwc:   rwc,
					copts: tc.copts,
					br:    bufio.NewReader(rwc),
					bw:    bufio.NewWriter(rwc),
				})
				t.Cleanup(func() {
					c.CloseNow()
				})
				return c, rwc
			}

			ctx := context.Background()

			data := []byte(tc.msg)
			pm := NewPreparedMessage(MessageText, data)
			copy(data, "modified") // NewPreparedMessage must copy data.

			want, wantBuf := newServerConn()
			got, gotBuf := newServerConn()
			for range 2 {
				assert.Success(t, want.Write(ctx, MessageText, []byte(tc.msg)))
				assert.Success(t, want.Write(ctx, MessageBinary, nextMsg))

				assert.Success(t, got.WritePrepared(ctx, pm))
				assert.Success(t, got.Write(ctx, MessageBinary, nextMsg))
			}

			assert.Equal(t, "wire bytes", wantBuf.Bytes(), gotBuf.Bytes())
			assert.Equal(t, "shared", tc.shared, pm.compressed != nil)
		})
	}
}
