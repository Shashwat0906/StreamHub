package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/broker"
	"github.com/Shashwat0906/StreamHub/internal/storage"
)

func runBroker(args []string) error {
	fs := flag.NewFlagSet("broker", flag.ExitOnError)
	id := fs.Int("id", 1, "broker ID (unique, > 0)")
	listen := fs.String("listen", "127.0.0.1:9092", "protocol listen address")
	advertise := fs.String("advertise", "", "address other brokers/clients use (default: listen address)")
	httpAddr := fs.String("http", "127.0.0.1:8080", "HTTP address for /metrics, /healthz, /v1/state (empty = off)")
	advertiseHTTP := fs.String("advertise-http", "", "HTTP address published in cluster metadata (default: --http, with 0.0.0.0 replaced by the --advertise host)")
	dataDir := fs.String("data-dir", "./data", "data directory")
	peers := fs.String("peers", "", "metadata quorum: id=host:port,... including this broker (empty = standalone)")
	minISR := fs.Int("min-insync-replicas", 1, "default min.insync.replicas for acks=all")
	defRF := fs.Int("default-replication-factor", 1, "replication factor when CreateTopic passes 0")
	defParts := fs.Int("default-partitions", 3, "partitions when CreateTopic passes 0")
	segBytes := fs.Int64("segment-bytes", 64<<20, "log segment size")
	retMs := fs.Int64("retention-ms", 7*24*3600*1000, "default retention by time (-1 = forever)")
	retBytes := fs.Int64("retention-bytes", -1, "default retention by size per partition (-1 = unlimited)")
	fsync := fs.Bool("fsync", false, "fsync after every append (slower; protects against power loss of all replicas)")
	session := fs.Duration("session-timeout", 3*time.Second, "controller fences a broker after this long without heartbeats")
	lag := fs.Duration("replica-lag-time", 5*time.Second, "follower leaves the ISR after lagging this long")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	logFormat := fs.String("log-format", "json", "json|text")
	fs.Parse(args)

	logger := newLogger(*logLevel, *logFormat)
	peerMap, err := parsePeers(*peers)
	if err != nil {
		return err
	}
	cfg := broker.Config{
		ID:                       int32(*id),
		ListenAddr:               *listen,
		AdvertisedAddr:           *advertise,
		HTTPAddr:                 *httpAddr,
		AdvertisedHTTPAddr:       *advertiseHTTP,
		DataDir:                  *dataDir,
		Peers:                    peerMap,
		MinInsyncReplicas:        *minISR,
		DefaultReplicationFactor: int16(*defRF),
		DefaultPartitions:        int32(*defParts),
		SessionTimeout:           *session,
		ReplicaLagTime:           *lag,
		Log: storage.Config{
			SegmentBytes:       *segBytes,
			IndexIntervalBytes: 4096,
			RetentionMs:        *retMs,
			RetentionBytes:     *retBytes,
			FsyncEveryAppend:   *fsync,
		},
		Logger: logger,
	}
	b, err := broker.New(cfg)
	if err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	logger.Info("shutting down", "signal", s.String())
	return b.Close()
}

func newLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

func parsePeers(s string) (map[int32]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[int32]string{}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad peer %q (want id=host:port)", part)
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("bad peer id %q", kv[0])
		}
		out[int32(id)] = kv[1]
	}
	return out, nil
}
