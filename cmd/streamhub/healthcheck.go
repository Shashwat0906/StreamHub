package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"
)

// runHealthcheck exits non-zero unless the broker's /readyz returns 200.
// It exists so container images built FROM scratch (no curl/wget) can
// still have a HEALTHCHECK.
func runHealthcheck(args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)
	addr := fs.String("http", "127.0.0.1:8080", "broker HTTP address")
	path := fs.String("path", "/readyz", "endpoint to check")
	fs.Parse(args)
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + *addr + *path)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %d", *path, resp.StatusCode)
	}
	return nil
}
