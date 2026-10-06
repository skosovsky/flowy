package flowy

import "context"

// WaitCapabilityProfile assigns exactly one owner to each runtime role. Labels
// are host deployment contracts, not inferred from the presence of an adapter.
type WaitCapabilityProfile struct {
	Label         string `json:"label"`
	JournalOwner  string `json:"journal_owner"`
	LeaseOwner    string `json:"lease_owner"`
	TimerOwner    string `json:"timer_owner"`
	ClockOwner    string `json:"clock_owner"`
	RetryOwner    string `json:"retry_owner"`
	RecoveryOwner string `json:"recovery_owner"`
}

func (p WaitCapabilityProfile) Validate() error {
	if p.Label == "" || p.JournalOwner == "" || p.LeaseOwner == "" || p.TimerOwner == "" ||
		!validRuntimeText(
			p.Label,
			p.JournalOwner,
			p.LeaseOwner,
			p.TimerOwner,
			p.ClockOwner,
			p.RetryOwner,
			p.RecoveryOwner,
		) ||
		p.ClockOwner == "" || p.RetryOwner == "" || p.RecoveryOwner == "" {
		return ErrExecutionCapability
	}
	return nil
}

// DurableWaitBackend is optional and must be implemented by the execution store
// itself. RegisterWait verifies an already committed armed generation before
// acknowledging registration. It must replay registration idempotently and may
// not fabricate an arm/deadline, execute a node, or resolve the wait.
type DurableWaitBackend interface {
	WaitCapabilities() WaitCapabilityProfile
	RegisterWait(context.Context, DurableWaitRecord) error
}

func configuredWaitBackend(store ExecutionStore, options *DurableOptions) (DurableWaitBackend, error) {
	if options.WaitProfile == nil {
		return nil, ErrExecutionCapability
	}
	profile := *options.WaitProfile
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	backend, ok := store.(DurableWaitBackend)
	if !ok || backend.WaitCapabilities() != profile {
		return nil, ErrExecutionCapability
	}
	options.WaitProfile = &profile
	return backend, nil
}
