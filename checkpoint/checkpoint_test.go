package checkpoint_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skosovsky/flowy/checkpoint"
)

func TestEncodeStateData_JSONRoundTrip(t *testing.T) {
	payload := []byte(`{"value":"init_done"}`)
	enc, err := checkpoint.EncodeStateData(payload)
	require.NoError(t, err)
	require.True(t, json.Valid(enc))

	raw, err := checkpoint.DecodeStateData(enc)
	require.NoError(t, err)
	assert.JSONEq(t, string(payload), string(raw))
}

func TestEncodeStateData_NonJSONUsesBase64Envelope(t *testing.T) {
	payload := []byte("bin:not-json")
	enc, err := checkpoint.EncodeStateData(payload)
	require.NoError(t, err)

	var envelope map[string]string
	require.NoError(t, json.Unmarshal(enc, &envelope))
	assert.Equal(t, "flowy-checkpoint-state/v2", envelope["$flowy_checkpoint_state"])
	assert.Equal(t, "base64", envelope["encoding"])
	assert.NotEmpty(t, envelope["payload_base64"])

	dec, err := checkpoint.DecodeStateData(enc)
	require.NoError(t, err)
	assert.Equal(t, payload, dec)
}
