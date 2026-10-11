package flowy

import (
	"encoding/json"
	"testing"
	"time"
)

func BenchmarkChildGroupCopy(b *testing.B) {
	fixture := childCloneOwnershipFixture()
	fixture.Plan.Label = "valid"
	fixture.CancelRequest.At = time.Date(2026, time.October, 6, 0, 0, 0, 0, time.UTC)
	for _, useJSON := range []bool{false, true} {
		name := "detached"
		if useJSON {
			name = "JSON-roundtrip"
		}
		b.Run(name, func(b *testing.B) { benchmarkChildGroupCopy(b, fixture, useJSON) })
	}
}

func benchmarkChildGroupCopy(b *testing.B, fixture ChildGroupRecord, useJSON bool) {
	b.ReportAllocs()
	var copied ChildGroupRecord
	for b.Loop() {
		if useJSON {
			encoded, err := json.Marshal(fixture)
			if err != nil {
				b.Fatal(err)
			}
			copied = ChildGroupRecord{}
			if err = json.Unmarshal(encoded, &copied); err != nil {
				b.Fatal(err)
			}
		} else {
			copied = detachedChildGroup(fixture)
		}
	}
	if len(copied.Children) != 1 || string(copied.Children[0].Result) != "result" {
		b.Fatal("copy missing")
	}
}
