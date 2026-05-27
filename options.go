package flowy

const defaultMaxSteps = 1000

// runConfig holds options for a run.
type runConfig struct {
	maxSteps int
}

// buildOpts holds compile-time options.
type buildOpts struct {
	run runConfig
}

// BuildOption configures the graph at compile time.
type BuildOption func(*buildOpts)

// WithMaxSteps sets the maximum number of steps (prevents infinite loops). Default is 1000 if <= 0.
func WithMaxSteps(limit int) BuildOption {
	return func(o *buildOpts) {
		o.run.maxSteps = limit
	}
}

// applyBuildOptions applies opts and returns buildOpts; used in Compile.
func applyBuildOptions(opts []BuildOption) buildOpts {
	o := buildOpts{run: runConfig{maxSteps: defaultMaxSteps}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.run.maxSteps <= 0 {
		o.run.maxSteps = defaultMaxSteps
	}
	return o
}
