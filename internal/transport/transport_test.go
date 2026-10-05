package transport

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/protocol"
)

func echoServer(t *testing.T) *Server {
	h := func(ctx context.Context, hdr protocol.RequestHeader, body []byte) protocol.Message {
		var req protocol.HeartbeatRequest
		if err := protocol.Unmarshal(body, &req); err != nil {
			return &protocol.SimpleResponse{Err: protocol.ErrInvalidRequest}
		}
		if req.Group == "slow" {
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}
		}
		return &protocol.SimpleResponse{ErrMsg: "echo:" + req.Group}
	}
	s, err := Listen("127.0.0.1:0", h, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPoolCall(t *testing.T) {
	s := echoServer(t)
	p := NewPool(nil)
	defer p.Close()
	for i := 0; i < 5; i++ {
		var resp protocol.SimpleResponse
		if err := p.Call(context.Background(), s.Addr(), protocol.APIHeartbeat, &protocol.HeartbeatRequest{Group: "g"}, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.ErrMsg != "echo:g" {
			t.Fatalf("got %q", resp.ErrMsg)
		}
	}
	if n := len(p.idle[s.Addr()]); n != 1 {
		t.Fatalf("connection not reused: %d idle", n)
	}
}

func TestCallRespectsContextDeadline(t *testing.T) {
	s := echoServer(t)
	p := NewPool(nil)
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	var resp protocol.SimpleResponse
	err := p.Call(ctx, s.Addr(), protocol.APIHeartbeat, &protocol.HeartbeatRequest{Group: "slow"}, &resp)
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("expected timeout, got %v after %v", err, time.Since(start))
	}
}

func TestFaultsBlockAndHeal(t *testing.T) {
	s := echoServer(t)
	f := NewFaults()
	p := NewPool(&Dialer{Self: "a", Faults: f})
	defer p.Close()
	call := func() error {
		var resp protocol.SimpleResponse
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return p.Call(ctx, s.Addr(), protocol.APIHeartbeat, &protocol.HeartbeatRequest{Group: "g"}, &resp)
	}
	if err := call(); err != nil {
		t.Fatal(err)
	}
	f.Block("a", s.Addr())
	if err := call(); err == nil {
		t.Fatal("call succeeded across a blocked link")
	}
	f.Heal("a", s.Addr())
	if err := call(); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}
