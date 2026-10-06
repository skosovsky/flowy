package flowy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
)

// ErrExecutionIncompatible rejects a missing or unsupported execution descriptor.
var ErrExecutionIncompatible = errors.New("flowy: incompatible execution descriptor")

// ErrMigrationMissing rejects an incomplete migration chain.
var ErrMigrationMissing = errors.New("flowy: migration chain missing")

// ErrMigrationInvalid rejects an ambiguous chain, invalid target or failed transform.
var ErrMigrationInvalid = errors.New("flowy: invalid migration")

// ExecutionDescriptor identifies semantic compatibility, independently of digest.
// Labels are assigned by the host; empty labels never mean the current contract.
type ExecutionDescriptor struct {
	GraphID           string           `json:"graph_id"`
	GraphRevision     string           `json:"graph_revision"`
	StateCodec        string           `json:"state_codec"`
	ExecutionContract string           `json:"execution_contract"`
	ReplayPolicy      StepReplayPolicy `json:"replay_policy"`
}

// StepReplayMode identifies the explicit step recovery contract.
type StepReplayMode string

// StepReplaySafe declares that graph computations/routing may repeat after an
// uncommitted step. External effects must use the durable activity boundary.
const StepReplaySafe StepReplayMode = "replay_safe"

// StepReplayPolicy is a host declaration for the entire graph, not a runtime
// proof of purity. Empty/unsupported policies never default to replay safety.
type StepReplayPolicy struct {
	Label string         `json:"label"`
	Mode  StepReplayMode `json:"mode"`
}

// Validate requires every explicit compatibility label.
func (d ExecutionDescriptor) Validate() error {
	if d.GraphID == "" || d.GraphRevision == "" || d.StateCodec == "" || d.ExecutionContract == "" ||
		!validRuntimeText(d.GraphID, d.GraphRevision, d.StateCodec, d.ExecutionContract, d.ReplayPolicy.Label) ||
		d.ReplayPolicy.Label == "" ||
		d.ReplayPolicy.Mode != StepReplaySafe {
		return ErrExecutionIncompatible
	}
	return nil
}

// Check rejects incompatibility before the host decodes state or executes nodes.
func (d ExecutionDescriptor) Check(target ExecutionDescriptor) error {
	if err := d.Validate(); err != nil {
		return err
	}
	if err := target.Validate(); err != nil {
		return err
	}
	if d != target {
		return ErrExecutionIncompatible
	}
	return nil
}

// MigrationState contains only the runtime fields a pure migration may change.
// Activities and persisted outcomes are intentionally not part of this input.
type MigrationState struct {
	StatePayload     []byte                      `json:"state_payload"`
	ExecutionPointer ExecutionPointer            `json:"execution_pointer"`
	ChildCursors     map[string]ExecutionPointer `json:"child_cursors,omitempty"`
	// JournalReferences maps current logical activity keys to immutable source
	// identities within this activation. Migration supplies explicit bindings.
	JournalReferences map[string]string `json:"journal_references,omitempty"`
	// ChildGroupReferences binds current logical group keys to immutable groups
	// in this activation; migrations do not relabel children or replenish budgets.
	ChildGroupReferences map[string]string `json:"child_group_references,omitempty"`
}

// MigrationProvenance links a migrated checkpoint to its immutable source.
type MigrationProvenance struct {
	SourceRevision uint64   `json:"source_revision"`
	SourceDigest   string   `json:"source_digest"`
	Chain          []string `json:"chain"`
	Digest         string   `json:"digest"`
}

// ExecutionEnvelope can be loaded without invoking a domain codec. Persistence
// must keep source history and atomically commit this envelope against revision
// and a live lease incarnation. It does not promise exactly-once external I/O.
type ExecutionEnvelope struct {
	ExecutionID     string                 `json:"execution_id"`
	Revision        uint64                 `json:"revision"`
	Digest          string                 `json:"digest"`
	Descriptor      ExecutionDescriptor    `json:"descriptor"`
	RuntimeProfile  *WaitCapabilityProfile `json:"runtime_profile,omitempty"`
	Progress        MigrationState         `json:"progress"`
	EffectsPayload  []byte                 `json:"effects_payload,omitempty"`
	JournalPayload  []byte                 `json:"journal_payload,omitempty"`
	ChildrenPayload []byte                 `json:"children_payload,omitempty"`
	WaitsPayload    []byte                 `json:"waits_payload,omitempty"`
	RunMeta         RunMetadata            `json:"run_meta"`
	Activation      uint64                 `json:"activation"`
	Terminal        *ExecutionTerminal     `json:"terminal,omitempty"`
	Migration       *MigrationProvenance   `json:"migration,omitempty"`
	Import          *ImportProvenance      `json:"import,omitempty"`
	Fork            *ForkLineage           `json:"fork,omitempty"`
}

// ExecutionTerminal records a durable terminal outcome independently of domain codecs.
type ExecutionTerminal struct {
	Status  RunStatus         `json:"status"`
	Reason  string            `json:"reason"`
	Failure *ExecutionFailure `json:"failure,omitempty"`
}

// ExecutionFailure stores a terminal error without pretending to serialize an
// arbitrary Go error type or reconstruct host sentinel identities.
type ExecutionFailure struct {
	Message string `json:"message"`
}

// ErrExecutionFailed identifies a persisted definitive execution failure.
var ErrExecutionFailed = errors.New("flowy: persisted execution failed")

// PersistedExecutionError is the recovered terminal failure. Original host error
// identities are available only during live execution, not inferred from text.
type PersistedExecutionError struct {
	Message string
	Reason  string
}

func (e *PersistedExecutionError) Error() string { return e.Message }
func (*PersistedExecutionError) Unwrap() error   { return ErrExecutionFailed }

func terminalFailureError(terminal *ExecutionTerminal) error {
	if terminal == nil {
		return nil
	}
	if terminal.Status == RunStatusCompleted && terminal.Failure == nil {
		return nil
	}
	if terminal.Status != RunStatusFailed || terminal.Failure == nil {
		return ErrInvalidSnapshot
	}
	return &PersistedExecutionError{Message: terminal.Failure.Message, Reason: terminal.Reason}
}

// ExecutionMigration is a pure source-to-target transformation registered by the
// host. Transform must not call external services. State bytes are detached from
// the source, and successful activity outcomes cannot be rewritten by it.
type ExecutionMigration struct {
	ID        string
	Source    ExecutionDescriptor
	Target    ExecutionDescriptor
	Transform func(MigrationState) (MigrationState, error)
}

// EnvelopeDigest is the integrity digest of the complete raw envelope. It is not
// a substitute for the explicit semantic descriptor.
func EnvelopeDigest(envelope ExecutionEnvelope) (string, error) {
	envelope.Digest = ""
	data, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// PrepareExecutionMigration builds a detached target without persistence or
// dispatch. validatePointer must validate the final cursor against the target
// graph. Caller commits with the source revision and fencing token; returning
// this value alone is never a committed migration.
func PrepareExecutionMigration(
	source ExecutionEnvelope,
	target ExecutionDescriptor,
	registry []ExecutionMigration,
	validatePointer func(ExecutionPointer) error,
) (ExecutionEnvelope, error) {
	if source.ExecutionID == "" || source.Revision == 0 || validatePointer == nil {
		return ExecutionEnvelope{}, ErrMigrationInvalid
	}
	if err := source.Descriptor.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err := target.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	chain, err := executionMigrationChain(source.Descriptor, target, registry)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	result := cloneExecutionEnvelope(source)
	ids := make([]string, 0, len(chain))
	for _, migration := range chain {
		progress, transformErr := migration.Transform(cloneMigrationState(result.Progress))
		if transformErr != nil {
			return ExecutionEnvelope{}, fmt.Errorf("%w: %s: %w", ErrMigrationInvalid, migration.ID, transformErr)
		}
		result.Progress = cloneMigrationState(progress)
		result.Descriptor = migration.Target
		ids = append(ids, migration.ID)
	}
	if result.Progress.ExecutionPointer == "" || !validMigrationText(result.Progress) {
		return ExecutionEnvelope{}, ErrMigrationInvalid
	}
	if pointerErr := validatePointer(result.Progress.ExecutionPointer); pointerErr != nil {
		return ExecutionEnvelope{}, fmt.Errorf("%w: target pointer: %w", ErrMigrationInvalid, pointerErr)
	}
	if len(chain) == 0 {
		return result, nil
	}
	digest, err := EnvelopeDigest(source)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	result.Migration = &MigrationProvenance{
		SourceRevision: source.Revision,
		SourceDigest:   digest,
		Chain:          ids,
		Digest:         "",
	}
	result.Migration.Digest, err = EnvelopeDigest(result)
	if err != nil {
		return ExecutionEnvelope{}, err
	}
	return result, nil
}

func executionMigrationChain(
	source, target ExecutionDescriptor,
	registry []ExecutionMigration,
) ([]ExecutionMigration, error) {
	edges := make(map[ExecutionDescriptor]ExecutionMigration, len(registry))
	ids := make(map[string]bool, len(registry))
	for _, migration := range registry {
		if migration.ID == "" || !validRuntimeText(migration.ID) || ids[migration.ID] || migration.Transform == nil {
			return nil, ErrMigrationInvalid
		}
		if migration.Source.Validate() != nil || migration.Target.Validate() != nil {
			return nil, ErrMigrationInvalid
		}
		if _, exists := edges[migration.Source]; exists {
			return nil, ErrMigrationInvalid
		}
		edges[migration.Source], ids[migration.ID] = migration, true
	}
	visited := make(map[ExecutionDescriptor]bool)
	chain := make([]ExecutionMigration, 0)
	for source != target {
		if visited[source] {
			return nil, ErrMigrationInvalid
		}
		visited[source] = true
		migration, ok := edges[source]
		if !ok {
			return nil, ErrMigrationMissing
		}
		chain = append(chain, migration)
		source = migration.Target
	}
	return chain, nil
}

func cloneMigrationState(state MigrationState) MigrationState {
	state.StatePayload = bytes.Clone(state.StatePayload)
	state.ChildCursors = maps.Clone(state.ChildCursors)
	state.JournalReferences = maps.Clone(state.JournalReferences)
	state.ChildGroupReferences = maps.Clone(state.ChildGroupReferences)
	return state
}

func cloneExecutionEnvelope(envelope ExecutionEnvelope) ExecutionEnvelope {
	if envelope.Fork != nil {
		lineage := *envelope.Fork
		envelope.Fork = &lineage
	}
	if envelope.RuntimeProfile != nil {
		profile := *envelope.RuntimeProfile
		envelope.RuntimeProfile = &profile
	}
	envelope.Progress = cloneMigrationState(envelope.Progress)
	envelope.EffectsPayload = bytes.Clone(envelope.EffectsPayload)
	envelope.JournalPayload = bytes.Clone(envelope.JournalPayload)
	envelope.ChildrenPayload = bytes.Clone(envelope.ChildrenPayload)
	envelope.WaitsPayload = bytes.Clone(envelope.WaitsPayload)
	envelope.RunMeta.RetryCounts = maps.Clone(envelope.RunMeta.RetryCounts)
	envelope.RunMeta.BudgetCounts = maps.Clone(envelope.RunMeta.BudgetCounts)
	envelope.RunMeta.TelemetryContext = maps.Clone(envelope.RunMeta.TelemetryContext)
	if envelope.Terminal != nil {
		terminal := *envelope.Terminal
		if terminal.Failure != nil {
			failure := *terminal.Failure
			terminal.Failure = &failure
		}
		envelope.Terminal = &terminal
	}
	if envelope.Migration != nil {
		provenance := *envelope.Migration
		provenance.Chain = append([]string(nil), provenance.Chain...)
		envelope.Migration = &provenance
	}
	if envelope.Import != nil {
		provenance := *envelope.Import
		provenance.Source.Payload = bytes.Clone(provenance.Source.Payload)
		envelope.Import = &provenance
	}
	return envelope
}
