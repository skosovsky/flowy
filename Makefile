export GOWORK := off
MODULES := $(sort $(patsubst ./%,%,$(shell find . -type d \( -name '.*' ! -name '.' -o -name vendor \) -prune -o -type f -name go.mod -exec dirname {} \;)))

.PHONY: test test-integration test-e2e test-live cover bench fuzz
.PHONY: lint fix modules
.PHONY: release-patch release-break release-inspect release-resume release-finish

# Run fresh unit tests with the race detector.
test:
	@for module in $(MODULES); do \
		printf '\n[test] %s\n' "$$module"; \
		(cd "$$module" && go test -race -count=1 ./...) || exit $$?; \
	done

# Run integration tests (integration tag, TestIntegration prefix).
test-integration:
	@for module in $(MODULES); do \
		printf '\n[integration] %s\n' "$$module"; \
		(cd "$$module" && go test -race -count=1 -timeout=30m -tags=integration -run='^TestIntegration' ./...) || exit $$?; \
	done

# Run end-to-end tests (e2e tag, TestE2E prefix).
test-e2e:
	@for module in $(MODULES); do \
		printf '\n[e2e] %s\n' "$$module"; \
		(cd "$$module" && go test -race -count=1 -timeout=30m -tags=e2e -run='^TestE2E' ./...) || exit $$?; \
	done

# Run live-provider tests; may incur API charges.
test-live:
	@for module in $(MODULES); do \
		printf '\n[live] %s\n' "$$module"; \
		(cd "$$module" && go test -race -count=1 -tags=live -run='^TestLive' ./...) || exit $$?; \
	done

# Write coverage.out and print coverage for each module.
cover:
	@for module in $(MODULES); do \
		printf '\n[cover] %s\n' "$$module"; \
		(cd "$$module" && go test -count=1 -coverprofile=coverage.out ./... && go tool cover -func=coverage.out) || exit $$?; \
	done

# Run benchmarks with allocation statistics.
bench:
	@for module in $(MODULES); do \
		printf '\n[bench] %s\n' "$$module"; \
		(cd "$$module" && go test -run='^$$' -bench=. -benchmem ./...) || exit $$?; \
	done

# Fuzz each discovered target for 30 seconds.
fuzz:
	@for module in $(MODULES); do \
		printf '\n[fuzz] %s\n' "$$module"; \
		(cd "$$module" || exit $$?; \
			packages=$$(go list -tags=fuzz ./...) || exit $$?; \
			for package in $$packages; do \
				names=$$(go test -tags=fuzz -list='^Fuzz' "$$package") || exit $$?; \
				printf '%s\n' "$$names" | while IFS= read -r name; do \
					case "$$name" in Fuzz*) ;; *) continue;; esac; \
					case "$$name" in *[[:space:]]*) continue;; esac; \
					go test -tags=fuzz -run='^$$' -fuzz="^$${name}$$" -fuzztime=30s -timeout=90s "$$package" || exit $$?; \
				done || exit $$?; \
			done) || exit $$?; \
	done

# Check formatting and lint without modifying files.
lint:
	@golangci-lint config verify
	@for module in $(MODULES); do \
		printf '\n[lint] %s\n' "$$module"; \
		(cd "$$module" && golangci-lint fmt --diff && golangci-lint run --allow-serial-runners ./...) || exit $$?; \
	done

# Apply Go fixes, formatting and automatic lint fixes.
fix:
	@for module in $(MODULES); do \
		printf '\n[fix] %s\n' "$$module"; \
		(cd "$$module" && go fix ./... && golangci-lint fmt && golangci-lint run --fix ./...) || exit $$?; \
	done

# List all discovered modules used by tests and releases.
modules:
	@printf '%s\n' $(MODULES)

# Prepare and publish the next patch release after confirmation.
release-patch:
	@./scripts/release.sh patch '$(RELEASE_SOURCE)'

# Prepare a breaking release; v2+ requires an import-path migration.
release-break:
	@./scripts/release.sh break '$(RELEASE_SOURCE)'

# Inspect the saved release and remote refs.
release-inspect:
	@./scripts/release.sh inspect

# Resume the saved release without changing its version or candidate.
release-resume:
	@./scripts/release.sh resume

# Archive a successfully published and verified release.
release-finish:
	@./scripts/release.sh finish

# Optional long-running handoff/resume/orphan campaign; outside release gates.
.PHONY: verify-stress
verify-stress:
	@go test -race -count=20 -timeout=10m -run='Handoff|Resume|Orphan' .
