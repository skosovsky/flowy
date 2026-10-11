package checkpoint

import "testing"

func TestSanitizerDetachesReferenceValues(t *testing.T) {
	// Arrange.
	type state struct {
		Values  map[string]int
		Items   []int
		Pointer *int
	}
	p := 7
	input := state{Values: map[string]int{"v": 7}, Items: []int{7}, Pointer: &p}
	codec, constructorErr := WithSanitizer(
		JSONSerializer[state]{},
		func(s *state) { s.Values["v"] = 0; s.Items[0] = 0; *s.Pointer = 0 },
	)
	if constructorErr != nil {
		t.Fatal(constructorErr)
	}
	// Act.
	payload, err := codec.Marshal(input)
	// Assert.
	if err != nil {
		t.Fatal(err)
	}
	if input.Values["v"] != 7 || input.Items[0] != 7 || p != 7 {
		t.Fatalf("input mutated: %+v", input)
	}
	decoded, err := JSONSerializer[state]{}.Unmarshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Values["v"] != 0 || decoded.Items[0] != 0 || *decoded.Pointer != 0 {
		t.Fatalf("not sanitized %+v", decoded)
	}
}
