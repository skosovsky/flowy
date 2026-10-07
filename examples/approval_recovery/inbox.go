package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/skosovsky/flowy"
)

// savedDecision preserves the exact authenticated event before grant issuance.
// Redelivery after a process crash must not invent new timestamps under an old ID.
func (h *host) savedDecision(
	ctx context.Context,
	store flowy.ExecutionStore,
	id, kind, dir string,
) (signedDecision, error) {
	s, _, err := inspectState(ctx, store, id)
	if err != nil {
		return signedDecision{}, err
	}
	path := filepath.Join(dir, fmt.Sprintf("decision-%d-%s.json", s.Round, kind))
	raw, err := os.ReadFile(path) // #nosec G703 -- Trusted host directory and fixed phase filename, never model input.
	if err == nil {
		var d signedDecision
		if err = json.Unmarshal(raw, &d); err != nil {
			return signedDecision{}, err
		}
		if err = h.authenticate(d); err != nil {
			return signedDecision{}, err
		}
		return d, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return signedDecision{}, err
	}
	d, err := h.request(ctx, store, id, kind, input{Value: "edited value"})
	if err != nil {
		return signedDecision{}, err
	}
	if err = persistDecision(path, mustJSON(d)); err != nil {
		return signedDecision{}, err
	}
	return d, nil
}

//nolint:gosec // Private host-generated filenames in a constructor-validated local directory; no model or transport paths.
func persistDecision(path string, raw []byte) (err error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".decision-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer func() {
		removeErr := os.Remove(name) // #nosec G703 -- Generated private temporary filename.
		if !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}()
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	if err = os.Rename(
		name,
		path,
	); err != nil { // #nosec G703 -- Both filenames are in the trusted host-owned directory.
		return err
	}
	directory, err := os.Open(
		filepath.Dir(path),
	) // #nosec G703 -- Trusted directory must be synced for process recovery.
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
