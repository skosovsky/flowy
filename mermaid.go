package flowy

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

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

	condKeys := make([]string, 0, len(g.conditionalEdges))
	for from := range g.conditionalEdges {
		condKeys = append(condKeys, from)
	}
	slices.Sort(condKeys)
	for _, from := range condKeys {
		placeholder := "__cond_" + idMap[from]
		fmt.Fprintf(&b, "  %s -->|conditional| %s\n", idMap[from], placeholder)
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
			fmt.Fprintf(&b, "  %s --> %s\n", idMap[from], idMap[t])
		}
		for _, t := range targets {
			fmt.Fprintf(&b, "  %s --> %s\n", idMap[t], idMap[fo.joinNode])
		}
	}

	dynFanKeys := make([]string, 0, len(g.dynamicFanOuts))
	for from := range g.dynamicFanOuts {
		dynFanKeys = append(dynFanKeys, from)
	}
	slices.Sort(dynFanKeys)
	for _, from := range dynFanKeys {
		dfo := g.dynamicFanOuts[from]
		placeholder := "__dyn_" + idMap[from]
		fmt.Fprintf(&b, "  %s -->|dynamic fan-out| %s\n", idMap[from], placeholder)
		fmt.Fprintf(&b, "  %s --> %s\n", placeholder, idMap[dfo.joinNode])
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
	for from, fo := range g.fanOuts {
		names[from] = struct{}{}
		names[fo.joinNode] = struct{}{}
		for _, t := range fo.targets {
			names[t] = struct{}{}
		}
	}
	for from, dfo := range g.dynamicFanOuts {
		names[from] = struct{}{}
		names[dfo.joinNode] = struct{}{}
	}

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
			var h uint32
			for _, r := range name {
				h = h*31 + uint32(r)
			}
			base = "_n" + strconv.FormatUint(uint64(h), 16)
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
