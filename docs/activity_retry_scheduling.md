# Persisted activity retry scheduling

This contract replaces the single `ActivityRetryPolicy.Delay` field. The runtime remains the only retry owner; host transports disable competing retry loops and host schedulers redeliver work when its persisted deadline is due. A downstream `SafeRetryContract` label is required for multiple attempts. No policy establishes downstream idempotency by itself.

An all-zero policy disables retry. Every nonzero policy has a compatibility label, positive MaxAttempts, downstream contract when MaxAttempts > 1, and an explicit `ActivityRetrySchedule`. Label and complete schedule configuration participate in equality for the same activity identity. Changing policy cannot create a new identity or erase previous attempts.

## Schedule configuration

`ActivityRetrySchedule` carries Kind (`fixed` or `exponential`), InitialDelay, MaxDelay, Multiplier, JitterPermille and optional HintLabel. Durations are nonnegative, jitter is 0..1000, MaxDelay >= InitialDelay. Fixed requires Multiplier == 0 and MaxDelay == InitialDelay, including a zero-delay fixed schedule. Exponential requires positive InitialDelay and Multiplier >= 2. Validate configuration before any dispatch.

For failure of dispatch attempt k (initial dispatch k=1), fixed base delay is InitialDelay. Exponential base is min(MaxDelay, InitialDelay * Multiplier^(k-1)); multiplication saturates at the configured bound before overflow. Jitter width is floor(base * JitterPermille / 1000), computed without overflowing duration arithmetic. One injected random sample selects a reduction in [0,width]; selected delay lies in [base-width,base]. No randomness is consumed when jitter width is zero.

`DurableOptions.RetryRandom` supplies a pure uint64 sampling function; nil uses a concurrency-safe standard random source. This is scheduling randomness, not a security primitive. The existing ExecutionClock supplies time. Production clock and randomness belong to the host and must return promptly; tests inject controlled sources. These sources never dispatch work.

## Host hints and persisted decisions

`ActivityRequest.Classify` returns `ActivityFailureDecision{Class, NotBefore}`. Adapter code translates a provider hint into this generic UTC time; core does not parse HTTP Retry-After or provider exceptions. A hint used for preparing a retry requires a nonempty valid HintLabel in the persisted schedule compatibility configuration. The selected deadline is max(failure time + selected delay, hint, failure time). A stale hint cannot move a deadline backwards. Times must be nonzero where required and serializable in UTC years1..9999; durations/deadline overflow and invalid configuration are rejected.

A retry decision records attempt, preparation time, base/selected delay, hint and the chosen deadline in the failed attempt's RetrySchedule provenance. The prepared record stores the same NextAttemptAt. Both commit atomically with failure classification. After restart the journal supplies the chosen deadline: replay, inspection and discovery do not run the classifier, recompute backoff or reroll jitter. Dispatch before that deadline is forbidden and does not increment attempts. Pending work releases the worker and lease.

Unrepresentable hints are recorded by the rejection reason without serializing an invalid time value. A clock outside its declared representable range violates the host clock contract and cannot establish valid journal timestamps.

An invalid live scheduling input after a known returned failure commits a failed attempt with explicit rejected scheduling provenance and ErrActivityScheduleInvalid; it does not create a prepared retry, silently use a default deadline or reinterpret the outcome as unknown. Replay reports the recorded scheduling rejection without another classifier call. Storage uncertainty still uses the existing journal-unavailable/unknown outcome contract; a failed write never reports a committed scheduling decision.

Unknown/abandoned remote outcomes never enter ordinary retry classification. Only addressed reconciliation or an explicit operator safe-retry decision may resolve them. Manual safe retry selects its deadline once from the same persisted policy and records the scheduling decision in ActivityResolutionRecord; invalid scheduling input rejects the manual decision before commit. Old ambiguous attempt history remains intact. Completing reconciliation does not rewrite old attempts as live successes.

## Format transition

Old active retry policies containing Delay without an explicit supported Schedule.Kind are rejected when reading or preparing the activity. There is no implicit fixed-delay fallback. Drain old executions before upgrade, or apply an explicit host-owned offline conversion that preserves identity, attempt history and already chosen absolute deadlines while supplying supported compatibility-labelled schedule configuration and complete provenance. Absent evidence requires rejection. Disabled all-zero policies retain their unambiguous meaning and do not acquire a retry policy automatically.
