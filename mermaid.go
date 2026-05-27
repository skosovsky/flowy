package flowy

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Multiplier for hashing empty-base Mermaid node IDs (non-cryptographic, stable string hash).
const mermaidEmptyIDHashMul = 31

// ExportMermaid returns a Mermaid flowchart (TD) representation of the graph.
// Output is deterministic (keys sorted) for stable snapshots and documentation.
// Node names that sanitize to the same ID get unique suffixes to avoid diagram collisions.
func (g *Graph[T]) ExportMermaid() string {
	idMap := g.buildMermaidIDMap()
	var b strings.Builder
	b.WriteString("flowchart TD\n")

	edgeKeys := make([]string, 0, len(g.edges))
	for from := range g.edges {
		edgeKeys = append(edgeKeys, from)
	}
	slices.Sort(edgeKeys)
	for _, from := range edgeKeys {
		fmt.Fprintf(&b, "  %s --> %s\n", idMap[from], idMap[g.edges[from]])
	}

	routerKeys := make([]string, 0, len(g.conditionalEdges))
	for from := range g.conditionalEdges {
		routerKeys = append(routerKeys, from)
	}
	slices.Sort(routerKeys)
	for _, from := range routerKeys {
		choiceNode := "__route_" + idMap[from]
		fmt.Fprintf(&b, "  %s --> %s\n", idMap[from], choiceNode)
		fmt.Fprintf(&b, "  %s -->|conditional| %s\n", choiceNode, idMap[EndNode])
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// buildMermaidIDMap returns a map from every node/routing name in the graph to a unique Mermaid-safe ID.
// Names that sanitize to the same base ID get deterministic suffixes (_0, _1, ...) so the diagram has no collisions.
func (g *Graph[T]) buildMermaidIDMap() map[string]string {
	names := make(map[string]struct{})
	for name := range g.nodes {
		names[name] = struct{}{}
	}
	for from, to := range g.edges {
		names[from] = struct{}{}
		names[to] = struct{}{}
	}
	for from := range g.conditionalEdges {
		names[from] = struct{}{}
	}
	names[EndNode] = struct{}{}

	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	slices.Sort(ordered)

	baseCount := make(map[string]int)
	idMap := make(map[string]string, len(ordered))
	for _, name := range ordered {
		base := mermaidSanitize(name)
		if base == "" {
			var h uint64
			// Hash UTF-8 bytes (not runes) for a stable non-cryptographic Mermaid node ID; avoids G115 on rune casts.
			for i := range len(name) {
				h = h*mermaidEmptyIDHashMul + uint64(name[i])
			}
			base = "_n" + strconv.FormatUint(h, 16)
		}
		n := baseCount[base]
		baseCount[base]++
		if n == 0 {
			idMap[name] = base
		} else {
			idMap[name] = base + "_" + strconv.Itoa(n-1)
		}
	}
	return idMap
}

// mermaidSanitize returns a Mermaid-safe ID from a node name (alphanumeric + underscore).
// Different names may produce the same result; use buildMermaidIDMap for unique IDs.
func mermaidSanitize(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		if r == ' ' || r == '-' {
			return '_'
		}
		return -1
	}, name)
}
