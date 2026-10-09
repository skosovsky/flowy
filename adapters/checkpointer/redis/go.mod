module github.com/skosovsky/flowy/adapters/checkpointer/redis

go 1.27.2

require (
	github.com/alicebob/miniredis/v2 v2.39.0
	github.com/redis/go-redis/v9 v9.22.0
	github.com/skosovsky/flowy v0.0.0
	github.com/skosovsky/flowy/adapters/lease/redis v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace (
	github.com/skosovsky/flowy => ../../..
	github.com/skosovsky/flowy/adapters/lease/redis => ../../lease/redis
)
