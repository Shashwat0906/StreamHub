package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shashwat0906/StreamHub/internal/dashboard"
)

// runDashboard serves the web dashboard.
//
//	streamhub dashboard --managed            launch a local 3-broker cluster and own it (enables broker failure simulation)
//	streamhub dashboard --bootstrap h:9092   attach to an existing cluster
func runDashboard(args []string) error {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8090", "dashboard HTTP address")
	def := os.Getenv("STREAMHUB_BOOTSTRAP")
	bootstrap := fs.String("bootstrap", def, "attach to this cluster (comma-separated broker addresses)")
	managed := fs.Bool("managed", false, "launch and own a local broker cluster (enables broker kill/stop/start)")
	brokers := fs.Int("brokers", 3, "managed mode: number of brokers")
	dataDir := fs.String("data-dir", "./dashboard-data", "managed mode: data directory")
	basePort := fs.Int("base-port", 19092, "managed mode: first broker protocol port")
	baseHTTP := fs.Int("base-http", 18081, "managed mode: first broker HTTP port")
	demo := fs.Bool("demo", true, "managed mode: create topic 'orders', 2 consumers in group 'order-processor' and start traffic")
	demoRate := fs.Int("demo-rate", 20, "demo traffic rate (messages/second, 0 = do not start traffic)")
	demoFail := fs.Float64("demo-failure-rate", 0.05, "fraction of demo messages marked to fail processing (go to the DLQ)")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	fs.Parse(args)

	logger := newLogger(*logLevel, "text")
	cfg := dashboard.Config{UI: dashboard.UI(), Logger: logger}
	switch {
	case *managed:
		cfg.Managed = &dashboard.ManagedConfig{Brokers: *brokers, DataDir: *dataDir, BasePort: *basePort, BaseHTTP: *baseHTTP}
	case *bootstrap != "":
		cfg.Bootstrap = strings.Split(*bootstrap, ",")
	default:
		return errors.New("use --managed (launch a local cluster) or --bootstrap host:port,... (attach)")
	}
	g, err := dashboard.New(cfg)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		g.Close()
		return err
	}
	srv := &http.Server{Handler: g.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go srv.Serve(ln)
	fmt.Fprintf(os.Stderr, "StreamHub dashboard: http://%s  (mode: %s)\n", ln.Addr(), map[bool]string{true: "managed", false: "attached"}[*managed])

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *managed && *demo {
		go func() {
			sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			if err := g.SeedDemo(sctx, "orders", "order-processor", 2, *demoRate, *demoFail); err != nil && ctx.Err() == nil {
				logger.Warn("demo setup failed", "err", err)
			}
		}()
	}
	<-ctx.Done()
	fmt.Fprintln(os.Stderr, "shutting down dashboard")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	srv.Shutdown(sctx)
	cancel()
	g.Close()
	return nil
}
