//go:build integration

// Package testdocker owns disposable backend containers for integration tests.
package testdocker

import (
	"context"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	postgresImage  = "postgres:16-alpine@sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea"
	redisImage     = "redis:7-alpine@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499"
	startupTimeout = 2 * time.Minute
	commandTimeout = 30 * time.Second
	pollInterval   = 100 * time.Millisecond
)

// Postgres starts an isolated PostgreSQL server and returns its connection string.
func Postgres(t *testing.T) string {
	t.Helper()
	address := start(
		t,
		postgresImage,
		"5432",
		[]string{"-e", "POSTGRES_PASSWORD=flowy", "-e", "POSTGRES_USER=flowy", "-e", "POSTGRES_DB=flowy_test"},
	)
	return "postgres://flowy:flowy@" + address + "/flowy_test?sslmode=disable"
}

// Redis starts an isolated Redis server and returns its address.
func Redis(t *testing.T) string {
	t.Helper()
	return start(t, redisImage, "6379", nil)
}

func start(t *testing.T, image, port string, env []string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), startupTimeout)
	defer cancel()
	platform := run(ctx, t, "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}")
	// Register cleanup before starting the container, including failed startup.
	name := "flowy-" + strings.ToLower(
		strings.ReplaceAll(time.Now().UTC().Format("20060102t150405.000000000"), ".", "-"),
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), commandTimeout)
		defer cleanupCancel()
		output, err := exec.CommandContext(cleanupCtx, "docker", "rm", "-f", "-v", name).CombinedOutput()
		if err != nil && !strings.Contains(string(output), "No such container") {
			t.Errorf("remove container %s: %v: %s", name, err, output)
		}
	})
	dataMount := "/data:rw,size=64m"
	if image == postgresImage {
		dataMount = "/var/lib/postgresql/data:rw,size=512m"
	}
	args := append(
		[]string{"run", "-d", "--platform", platform, "--name", name, "--tmpfs", dataMount, "-p", "0:" + port},
		env...)
	run(ctx, t, append(args, image)...)
	published := waitPort(ctx, t, name, port)
	_, mapped, err := net.SplitHostPort(strings.Split(published, "\n")[0])
	if err != nil {
		t.Fatalf("parse Docker port %q: %v", published, err)
	}
	host := "127.0.0.1"
	// Remote daemons publish on their own host, including Linux checks in Docker.
	if endpoint := os.Getenv("DOCKER_HOST"); strings.HasPrefix(endpoint, "tcp://") {
		parsed, parseErr := url.Parse(endpoint)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		host = parsed.Hostname()
	}
	address := net.JoinHostPort(host, mapped)
	waitReady(ctx, t, name, image, address)
	return address
}

func waitPort(ctx context.Context, t *testing.T, name, port string) string {
	t.Helper()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		output, err := exec.CommandContext(ctx, "docker", "port", name, port+"/tcp").CombinedOutput()
		if err == nil && strings.TrimSpace(string(output)) != "" {
			return strings.TrimSpace(string(output))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("container %s port %s not published: %v: %s", name, port, ctx.Err(), output)
		case <-ticker.C:
		}
	}
}

func waitReady(ctx context.Context, t *testing.T, name, image, address string) {
	t.Helper()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		ready := exec.CommandContext(ctx, "docker", "exec", name, "redis-cli", "ping")
		if image == postgresImage {
			ready = exec.CommandContext(
				ctx,
				"docker",
				"exec",
				name,
				"pg_isready",
				"-h",
				"127.0.0.1",
				"-U",
				"flowy",
				"-d",
				"flowy_test",
			)
		}
		output, readyErr := ready.CombinedOutput()
		if readyErr == nil {
			var dialer net.Dialer
			dialer.Timeout = time.Second
			connection, dialErr := dialer.DialContext(ctx, "tcp", address)
			if dialErr == nil {
				if closeErr := connection.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("container %s not ready at %s: %v: %s", name, address, ctx.Err(), output)
		case <-ticker.C:
		}
	}
}

func run(ctx context.Context, t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}
