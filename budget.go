package flowy

import "errors"

// ErrBudgetExceeded is returned when a named budget is exhausted.
var ErrBudgetExceeded = errors.New("flowy: named budget exceeded")

func checkBudgetLimits(meta RunMetadata, limits map[string]int) error {
	if len(limits) == 0 {
		return nil
	}
	for name, limit := range limits {
		if limit <= 0 {
			continue
		}
		if meta.BudgetCounts[name] > limit {
			return ErrBudgetExceeded
		}
	}
	return nil
}
