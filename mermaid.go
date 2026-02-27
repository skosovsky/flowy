package flowy

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ExportMermaid returns a Mermaid flowchart (TD) representation of the graph.
// Output is deterministic (keys sorted) for stable snapshots and documentation.
func (g *Graph[T]) ExportMermaid() string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")

	edgeKeys := make([]string, 0, len(g.edges))
	for from := range g.edges {
		edgeKeys = append(edgeKeys, from)
	}
	slices.Sort(edgeKeys)
	for _, from := range edgeKeys {
		fmt.Fprintf(&b, "  %s --> %s\n", mermaidID(from), mermaidID(g.edges[from]))
	}

	condKeys := make([]string, 0, len(g.conditionalEdges))
	for from := range g.conditionalEdges {
		condKeys = append(condKeys, from)
	}
	slices.Sort(condKeys)
	for _, from := range condKeys {
		fmt.Fprintf(&b, "  %s -->|conditional| __dynamic__\n", mermaidID(from))
	}

	fanKeys := make([]string, 0, len(g.fanOuts))
	for from := range g.fanOuts {
		fanKeys = append(fanKeys, from)
	}
	slices.Sort(fanKeys)
	for _, from := range fanKeys {
		fo := g.fanOuts[from]
		targets := make([]string, len(fo.targets))
		copy(targets, fo.targets)
		slices.Sort(targets)
		for _, t := range targets {
			fmt.Fprintf(&b, "  %s --> %s\n", mermaidID(from), mermaidID(t))
		}
		for _, t := range targets {
			fmt.Fprintf(&b, "  %s --> %s\n", mermaidID(t), mermaidID(fo.joinNode))
		}
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func mermaidID(name string) string {
	// Mermaid node IDs must be alphanumeric or underscore; replace spaces and special chars.
	id := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		if r == ' ' || r == '-' {
			return '_'
		}
		return -1
	}, name)
	if id == "" {
		// Fallback for names that become empty (e.g. all special chars) so Mermaid output is valid.
		var h uint32
		for _, r := range name {
			h = h*31 + uint32(r)
		}
		id = "_n" + strconv.FormatUint(uint64(h), 16)
	}
	return id
}
