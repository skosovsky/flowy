package main

import (
	"bytes"
	"encoding/json"
	"io"
)

type completeReceipt struct {
	OperationID *string `json:"operation_id"`
	Value       *string `json:"value"`
	Accepted    *bool   `json:"accepted"`
}

func decodeReceipt(raw []byte) (receipt, error) {
	var complete completeReceipt
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&complete); err != nil {
		return receipt{}, errEvidence
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || complete.OperationID == nil || *complete.OperationID == "" ||
		complete.Value == nil ||
		complete.Accepted == nil {
		return receipt{}, errEvidence
	}
	return receipt{OperationID: *complete.OperationID, Value: *complete.Value, Accepted: *complete.Accepted}, nil
}

// validateStoredResult checks field presence before the backend's typed decoder
// can turn absent JSON fields into Go zero values.
func validateStoredResult(raw []byte) error {
	var wire struct {
		Result         json.RawMessage `json:"result"`
		EnvelopeResult json.RawMessage `json:"envelope_result"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return errEvidence
	}
	typed, err := decodeReceipt(wire.Result)
	if err != nil {
		return err
	}
	envelope, err := decodeReceipt(wire.EnvelopeResult)
	if err != nil || typed != envelope {
		return errEvidence
	}
	return nil
}
