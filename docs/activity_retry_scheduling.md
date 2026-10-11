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

## Host callbacks and recovery

CallActivity belongs to a node activation. Dispatch, Reconcile and Classify must
not recursively call CallActivity, even using captured node contexts. The supplied
dispatch/reconciliation context rejects nested calls before journal mutation.
Classify has no context parameter; the host must enforce the same restriction.
Sequential calls from the owning node, with separate keys or a due retry of the
same key, are supported. A single CallActivity performs at most one dispatch.

Reconcile is a read-only, idempotent, concurrency-safe probe: concurrent recovery
can invoke overlapping probes, and an unacknowledged result can be probed again.
It must return promptly and must not redispatch the operation. Dispatch/classifier
panics propagate through the established node panic boundary; they are not known
retryable failures. Unknown classification values become ambiguous, discard hints,
and emit only the bounded `invalid_classification` diagnostic. No raw provider
value or error is put in observation. Nil RetryRandom retains standard randomness;
the sampled persisted deadline is authoritative after recovery.

ResolveActivity rejects duplicate DecisionID, including exact repetition. After
an error/ACK loss, load the latest envelope from ExecutionStore, validate its
integrity and inspect JournalPayload as map[string]ActivityRecord. Locate the exact
identity and Resolutions entry by DecisionID; compare action, reason/evidence,
prior-state/attempt and safe-retry provenance against the submitted command.
The current format is not a full command digest: a found decision confirms a
recorded decision, not equality of every submitted outcome byte. Inspect the
record's outcome and subsequent history when needed. If confirmed, use a fresh
ResumeToken{ThreadID: head.ExecutionID, SnapshotRevision: head.Revision} to resume;
do not resubmit with a fresh DecisionID or redispatch. A missing decision after a
successful authoritative reload permits retrying the original command against the
current validated revision only if its original target is still eligible. An
unavailable read leaves the outcome unknown. Changed/stale targets require host
review. This contract does not add automatic exact-command replay.

Record State is the current outcome, while Classification describes the last
failed/unknown attempt. Completed reconciliation can retain unknown classification
and an unknown historical attempt. Origin identifies live/reconciled/manual outcome;
replay returns ActivityReplayed without rewriting persisted provenance. Manual
resolution retains an independent Resolutions entry and emits LifecycleReconcile
with DecisionID and current state code (started/committed/failed); observation is not a commit receipt. A retryable failure
with an all-zero policy returns ErrActivityAttemptsExhausted: its sole attempt has
used the available dispatch opportunity, even though MaxAttempts is zero. Other
known nonretryable failure returns ErrActivityFailed. Unknown never enters retry.

## Construction and attempt counts

Use NewFixedActivityRetrySchedule(delay) or
NewExponentialActivityRetrySchedule(initial, maximum, multiplier), checking the
returned error, then supply the persisted policy explicitly. For example, a fixed
schedule of one second with MaxAttempts:3, Label:"fixed-v1" and
SafeRetryContract:"downstream-idempotency-v1" admits three total dispatches.
An exponential schedule of one second, maximum one minute, multiplier two uses
1s, 2s, 4s between the first four attempts, before any optional jitter/hint.
Setting jitter or HintLabel still goes through policy validation on CallActivity.

| Policy | Total dispatch limit | Automatic retries |
|---|---:|---:|
| All zero | 1 | 0 |
| MaxAttempts=1, explicit schedule | 1 | 0 |
| MaxAttempts=3, safe contract | 3 | 2 |

A zero-delay fixed schedule makes the next attempt immediately due; it does not
execute a hidden loop. The node can invoke CallActivity again after the pending
result. Transport retries remain disabled.

## Compatibility ownership

| Changed host behavior | Version to change before deployment |
|---|---|
| Dispatch semantics, activity result/input codecs, classifier | ActivityRequest.Implementation; execution descriptor/codec versions as applicable |
| Retry algorithm/configuration or hint interpretation | Retry.Label and schedule HintLabel/configuration |
| Wait matcher/decoder/continuation | MatcherLabel, PayloadCodec, ContinuationLabel |
| Child codec/dispatcher/merge/cancellation/budget projection | Child implementation and corresponding plan labels |
| Fork/import/rollover projection | ProjectionLabel plus relevant descriptor |

Compatibility labels attest host behavior; core cannot derive them from Go function
pointers. Existing records reject incompatible replacements. Drain or migrate with
explicit historical provenance instead of silently bumping a label during replay.

If a step commit fails and committed-state decode also fails, the error joins
ErrDurableStateUnavailable with both causes. RunResult.Reason explicitly marks
local state as diagnostic; State, Effects and RunMeta must not be treated as a
restored committed boundary. Correct the codec/store issue, inspect the persisted
head/history, and reconstruct the exact validated resume token. Do not save the
local diagnostic value as recovered state.

## Aggregate rewrite measurements (Task28, 2026-10-06)

Fresh one-iteration measurements on Go1.27.1 darwin/arm64, Apple M1 Max, memory
store, no telemetry exporter. These measure serialized aggregates written by the
fixture, excluding backend indexes/WAL, retention reads and physical disk overhead.
They are growth evidence, not a stable latency baseline or production capacity.

| Workload | Records | Maximum aggregate bytes | Cumulative rewrite bytes |
|---|---:|---:|---:|
| Separate activities, 512-byte input/result | 16 / 64 / 256 | 30,232 / 118,677 / 472,401 | 780,949 / 11,609,832 / 182,263,872 |
| Child fanout, 512-byte input/result | 16 / 64 / 256 | 51,697 / 203,058 / 808,498 | 2,240,154 / 33,560,891 / 528,036,987 |
| Attempts of one zero-delay retrying activity | 16 / 64 | 8,518 / 30,150 | 174,426 / 2,071,701 |
| Unique unmatched wait deliveries | 16 / 64 | 7,108 / 23,108 | 73,885 / 807,005 |

Activities/children used BenchmarkExecutionGrowth with benchtime=1x. Attempts and
waits used deterministic cardinality measurement tests under race; wait rewrite
bytes cover only delivery commits, and its armed record deliberately remains
unresolved. Timestamps and serialization may slightly vary exact byte counts.
The amplification supports bounded activation batches and explicit rollover at a
resolved, committed boundary. An unresolved wait/unknown attempt cannot be made
rollover eligible by deleting records. Use immutable host payload references with
an explicit missing-reference error for large data; retain identities, receipts,
fences and outcome provenance. Set aggregate/target limits and measure native
storage separately. Rollover preserves accounting; it does not erase permanent
identity metadata. Existing rollover benchmarks compare matched workloads, while
fresh native and final performance gates remain separate acceptance work.
