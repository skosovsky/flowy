// PostgreSQL blueprint: host ports and orchestration, no provider SDK in core.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("FLOWY_TEST_DATABASE_URL")
	if dsn == "" {
		return errors.New("FLOWY_TEST_DATABASE_URL is required (PostgreSQL, schema CREATE/DROP privilege)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	now := time.Now().UTC()
	h := &host{model: &fakeModel{}, tool: newFakeTool(), clock: &hostClock{at: now}, deadline: now.Add(time.Hour),
		barrier: &childBarrier{ready: make(chan struct{})}}
	report, err := runScenario(ctx, dsn, h)
	if err != nil {
		return err
	}
	fmt.Printf("completed: values=%v token=%+v child resolutions=%d\n",
		report.Final.State.Values, report.Final.ResumeToken, report.Resolved)
	return nil
}
