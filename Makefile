GO ?= go
COREPACK ?= corepack
PNPM ?= $(COREPACK) pnpm
WEB_PNPM = cd web && $(PNPM)
GOBIN ?= $(shell $(GO) env GOBIN)

ifeq ($(strip $(GOBIN)),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

.DEFAULT_GOAL := help

GO_VERSION ?= 1.27.0
NODE_VERSION ?= 26.2.0
PNPM_VERSION ?= 12.3.4
OAPI_CODEGEN_VERSION ?= v2.4.1
GOLANG_MIGRATE_VERSION ?= v4.18.2

OPENAPI_SPEC ?= contracts/openapi/openapi.yaml
OPENAPI_GENERATED ?= internal/apigen/types.gen.go
WEB_OPENAPI_GENERATED ?= web/src/api/schema.ts
CORE_COVERAGE_MIN ?= 72.0

WORKDIR_ID := $(shell printf '%s' '$(CURDIR)' | cksum | awk '{print $$1}')
WEB_NODE_MODULES_STAMP := web/node_modules/.pnpm-ready-$(PNPM_VERSION)-$(WORKDIR_ID).stamp
WEB_DIST_INDEX := web/dist/index.html
KD_BINARY := bin/kd

WEB_BUILD_INPUTS := $(shell find web/src web/public -type f) \
	$(shell find web -maxdepth 1 -type f \( -name '*.html' -o -name '*.json' -o -name '*.js' -o -name '*.ts' \)) \
	Makefile
KD_GO_SOURCES := $(shell find cmd internal web -type f -name '*.go' ! -name '*_test.go')
BOOTSTRAP_ASSETS := $(shell find internal/bootstrapassets/files -type f)
DB_MIGRATIONS := $(shell find internal/dbmigrations -maxdepth 1 -type f -name '*.sql')

.PHONY: help install build dev-web test test-web lint-web test-race test-integration test-e2e test-github-smoke test-opencode-security-smoke vet coverage-core scripts-check verify contracts-generate contracts-check openapi-lint tool-versions

##@ Setup and Build
help: ## Show supported make targets
	@awk 'BEGIN {FS = ":.*## "} /^##@/ {if (printed++) printf "\n"; print substr($$0, 5) ":"; next} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: $(WEB_DIST_INDEX) ## Install `kd` with the embedded web UI
	$(GO) install ./cmd/kd
	@printf 'Installed kd to %s\nNext steps:\n  export PATH="$$PATH:%s"\n  kd init\n  kd summon\nPrerequisite: ensure opencode is installed and authenticated.\n' "$(GOBIN)" "$(GOBIN)"

build: $(KD_BINARY) ## Build the local `kd` binary into `bin/`

dev-web: $(WEB_NODE_MODULES_STAMP) ## Start the web app in Vite dev mode
	$(WEB_PNPM) dev

##@ Verification
test: $(WEB_DIST_INDEX) ## Run `go test ./...`
	$(GO) test ./...

test-web: $(WEB_NODE_MODULES_STAMP) ## Run the frontend unit test suite
	$(WEB_PNPM) test

lint-web: $(WEB_NODE_MODULES_STAMP) ## Run frontend lint with zero warnings
	$(WEB_PNPM) lint

# test-race exercises the planning + state packages under the race detector.
# Critical for the streaming sequence cursor and lease-fenced terminal writes -
# both are concurrent paths where a missed race would silently violate the
# UNIQUE(operation_id, seq) constraint or let a stale worker mark a terminal
# state. Scoped to the planning subtree to keep CI wall-clock reasonable;
# extend to ./... if you suspect regressions outside this area.
test-race: $(WEB_DIST_INDEX) ## Run the targeted race-enabled Go test suite
	$(GO) test -race ./internal/state/... ./internal/planning/... ./internal/api/...

test-integration: $(WEB_DIST_INDEX) ## Run the integration test suite
	$(GO) test ./tests/integration/...

test-e2e: $(WEB_DIST_INDEX) ## Run the end-to-end test suite
	$(GO) test ./tests/e2e/...

test-github-smoke: ## Run the opt-in live GitHub App smoke test
	KINGDOM_GITHUB_LIVE_SMOKE=1 $(GO) test ./internal/external/github -run '^TestLiveGitHubAppSmoke$$' -count=1 -v

test-opencode-security-smoke: ## Run the opt-in live adversarial planner containment test
	KINGDOM_LIVE_OPENCODE_SECURITY_TEST=1 $(GO) test ./internal/planning -run '^TestLiveAdversarialExternalPlannerContainment$$' -count=1 -v

vet: $(WEB_DIST_INDEX) ## Run `go vet ./...`
	$(GO) vet ./...

coverage-core: ## Check the core coverage gate
	./scripts/check_core_coverage.sh $(CORE_COVERAGE_MIN)

scripts-check: ## Run lightweight script verification checks
	bash -n scripts/*.sh
	python3 -m unittest discover -s scripts/tests -p "test_*.py"

verify: test test-web lint-web test-race vet coverage-core contracts-check ## Run the main verification sweep

##@ Contracts
contracts-check: $(WEB_NODE_MODULES_STAMP) ## Verify generated OpenAPI types are up to date
	@tmp_dir="$$(mktemp -d)"; trap 'rm -rf "$$tmp_dir"' EXIT; \
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) -generate types,client,spec -package apigen -o "$$tmp_dir/types.gen.go" $(OPENAPI_SPEC); \
	($(WEB_PNPM) exec openapi-typescript ../$(OPENAPI_SPEC) -o "$$tmp_dir/schema.ts"); \
	cmp -s "$$tmp_dir/types.gen.go" $(OPENAPI_GENERATED) || { diff -u $(OPENAPI_GENERATED) "$$tmp_dir/types.gen.go"; exit 1; }; \
	cmp -s "$$tmp_dir/schema.ts" $(WEB_OPENAPI_GENERATED) || { diff -u $(WEB_OPENAPI_GENERATED) "$$tmp_dir/schema.ts"; exit 1; }; \
	printf 'Generated OpenAPI contracts are current.\n'

openapi-lint: $(WEB_NODE_MODULES_STAMP) ## Lint the OpenAPI spec with Redocly
	$(WEB_PNPM) exec redocly lint ../$(OPENAPI_SPEC)

##@ Utilities
tool-versions: ## Print the pinned local tool versions
	@printf "Go: %s\nNode: %s\npnpm: %s\noapi-codegen: %s\ngolang-migrate: %s\n" \
		"$(GO_VERSION)" "$(NODE_VERSION)" "$(PNPM_VERSION)" "$(OAPI_CODEGEN_VERSION)" "$(GOLANG_MIGRATE_VERSION)"

# Internal targets
contracts-generate: $(OPENAPI_GENERATED) $(WEB_OPENAPI_GENERATED)

$(WEB_NODE_MODULES_STAMP): web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml Makefile
	rm -rf web/node_modules
	$(WEB_PNPM) install --frozen-lockfile
	touch $@

$(WEB_DIST_INDEX): $(WEB_BUILD_INPUTS) $(WEB_NODE_MODULES_STAMP)
	$(WEB_PNPM) build

$(KD_BINARY): $(KD_GO_SOURCES) $(BOOTSTRAP_ASSETS) $(DB_MIGRATIONS) go.mod go.sum $(WEB_DIST_INDEX)
	mkdir -p bin
	$(GO) build -o $@ ./cmd/kd

$(OPENAPI_GENERATED): $(OPENAPI_SPEC) Makefile
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) -generate types,client,spec -package apigen -o $(OPENAPI_GENERATED) $(OPENAPI_SPEC)

$(WEB_OPENAPI_GENERATED): $(OPENAPI_SPEC) $(WEB_NODE_MODULES_STAMP)
	$(WEB_PNPM) exec openapi-typescript ../$(OPENAPI_SPEC) -o src/api/schema.ts
