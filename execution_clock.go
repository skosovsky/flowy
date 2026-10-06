package flowy

import "time"

// ExecutionClock supplies activity deadlines and durable wait arbitration time.
// Lease expiry is enforced independently by the storage server clock.
type ExecutionClock interface {
	Now() time.Time
}

type wallExecutionClock struct{}

func (wallExecutionClock) Now() time.Time { return time.Now().UTC() }
