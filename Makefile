GO      := go
MODULES := $(shell find . -type d \( -name ".*" -not -name "." -o -name "vendor" \) -prune -o -type f -name "go.mod" -exec dirname {} \;)

.PHONY: lint fix test test-race test-goleak verify-stress bench bench-hotpath fuzz cover release-patch release-break

lint:
	@for dir in $(MODULES); do \
		echo "golangci-lint - $$dir"; \
		(cd "$$dir" && golangci-lint run ./...) || exit 1; \
	done

fix:
	@if [ -f "go.work" ]; then $(GO) work sync; fi
	@for dir in $(MODULES); do \
		echo "fix & tidy - $$dir"; \
		(cd "$$dir" && $(GO) fix ./... && $(GO) mod tidy) || exit 1; \
		(cd "$$dir" && golangci-lint run --fix ./...) || exit 1; \
	done

test:
	@for dir in $(MODULES); do \
		echo "test - $$dir"; \
		timeout=120s; \
		case "$$dir" in ./adapters/*) timeout=3m ;; esac; \
		(cd "$$dir" && $(GO) test -timeout $$timeout ./...) || exit 1; \
	done

test-race:
	@for dir in $(MODULES); do \
		echo "test-race - $$dir"; \
		(cd "$$dir" && $(GO) test -v -race -timeout 5m ./...) || exit 1; \
	done

test-goleak:
	@for dir in $(MODULES); do \
		echo "test-goleak - $$dir"; \
		timeout=120s; \
		case "$$dir" in ./adapters/*) timeout=3m ;; esac; \
		(cd "$$dir" && $(GO) test -v -count=1 -timeout $$timeout ./...) || exit 1; \
	done

verify-stress:
	@echo "verify-stress - root handoff/resume/orphan contracts"
	@(cd . && $(GO) test -count=20 -timeout 10m -run 'Handoff|Resume|Orphan' .)

bench:
	@for dir in $(MODULES); do \
		echo "bench - $$dir"; \
		(cd "$$dir" && $(GO) test -bench=. -run=^$$ ./...) || exit 1; \
	done

fuzz:
	@for dir in $(MODULES); do \
		echo "fuzz - $$dir"; \
		(cd "$$dir" && \
			for pkg in $$($(GO) list -tags=fuzz ./...); do \
				if $(GO) test -tags=fuzz -list . "$$pkg" 2>/dev/null | grep -q '^Fuzz'; then \
					$(GO) test -tags=fuzz -fuzz=. -fuzztime=30s "$$pkg" || exit 1; \
				fi; \
			done \
		) || exit 1; \
	done

cover:
	@for dir in $(MODULES); do \
		echo "cover - $$dir"; \
		(cd "$$dir" && $(GO) test -coverprofile=coverage.out ./... && $(GO) tool cover -func=coverage.out) || exit 1; \
	done

release-patch: lint test ## v0.5.0 -> v0.5.1
	@chmod +x ./scripts/release.sh
	@./scripts/release.sh patch "$(MODULES)"

release-break: lint test ## v0.5.1 -> v0.6.0
	@chmod +x ./scripts/release.sh
	@./scripts/release.sh break "$(MODULES)"
