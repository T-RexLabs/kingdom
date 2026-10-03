GO ?= go
NODE ?= node
PNPM ?= pnpm
WEB_PNPM = cd web && $(PNPM)
GOBIN ?= $(shell $(GO) env GOBIN)

ifeq ($(strip $(GOBIN)),)
GOBIN := $(shell $(GO) env GOPATH)/bin
endif

.DEFAULT_GOAL := help

GO_VERSION := $(shell awk '$$1 == "go" {print $$2}' .tool-versions)
NODE_VERSION := $(shell awk '$$1 == "nodejs" {print $$2}' .tool-versions)
PYTHON_VERSION := $(shell awk '$$1 == "python" {print $$2}' .tool-versions)
PNPM_VERSION := $(shell awk '$$1 == "pnpm" {print $$2}' .tool-versions)
OAPI_CODEGEN_VERSION ?= v2.4.1

OPENAPI_SPEC ?= contracts/openapi/openapi.yaml
OPENAPI_GENERATED ?= internal/apigen/types.gen.go
WEB_OPENAPI_GENERATED ?= web/src/api/schema.ts
CORE_COVERAGE_MIN ?= 72.0

WORKDIR_ID := $(shell printf '%s' '$(CURDIR)' | cksum | awk '{print $$1}')
WEB_NODE_MODULES_STAMP := web/node_modules/.pnpm-ready-$(PNPM_VERSION)-$(WORKDIR_ID).stamp
WEB_DIST_INDEX := web/dist/index.html
KD_BINARY := bin/kd

.PHONY: help install build dev-web test test-web lint-web test-race test-integration test-e2e test-github-smoke test-opencode-security-smoke vet coverage-core scripts-check verify contracts-generate contracts-check openapi-lint tool-versions tools-check web-build assets-check assets-generate

##@ Setup and Build
help: ## Show supported make targets
	@awk 'BEGIN {FS = ":.*## "} /^##@/ {if (printed++) printf "\n"; print substr($$0, 5) ":"; next} /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: $(WEB_DIST_INDEX) ## Install `kd` with the embedded web UI
	$(GO) install ./cmd/kd
	@printf 'Installed kd to %s\nNext steps:\n  export PATH="$$PATH:%s"\n  kd init\n  kd summon\nPrerequisite: ensure opencode is installed and authenticated.\n' "$(GOBIN)" "$(GOBIN)"

build: $(WEB_DIST_INDEX) ## Build the local `kd` binary into `bin/`
	mkdir -p bin
	$(GO) build -o $(KD_BINARY) ./cmd/kd

dev-web: tools-check $(WEB_NODE_MODULES_STAMP) ## Start the web app in Vite dev mode
	$(WEB_PNPM) dev

##@ Verification
test: $(WEB_DIST_INDEX) ## Run `go test ./...`
	$(GO) test ./...

test-web: tools-check $(WEB_NODE_MODULES_STAMP) ## Run the frontend unit test suite
	$(WEB_PNPM) test

lint-web: tools-check $(WEB_NODE_MODULES_STAMP) ## Run frontend lint with zero warnings
	$(WEB_PNPM) lint

# Check the concurrent state, planning, API, dispatch, and provider lifecycle paths.
test-race: $(WEB_DIST_INDEX) ## Run the targeted race-enabled Go test suite
	$(GO) test -race ./internal/state/... ./internal/planning/... ./internal/api/... ./internal/orchestration/... ./internal/providers/opencode/... ./internal/processguardian/... ./internal/execution/store/...

test-integration: $(WEB_DIST_INDEX) ## Run the integration test suite
	$(GO) test ./tests/integration/...

test-e2e: $(WEB_DIST_INDEX) ## Run the end-to-end test suite
	$(GO) test ./tests/e2e/...

test-github-smoke: tools-check ## Check GitHub and uninstall the controlled test installation
	KINGDOM_GITHUB_LIVE_SMOKE=1 $(GO) test ./internal/external/github -run '^TestLiveGitHubAppSmoke$$' -count=1 -v

test-opencode-security-smoke: tools-check ## Run live planner containment and capability controls
	KINGDOM_LIVE_OPENCODE_SECURITY_TEST=1 $(GO) test ./internal/planning -run '^TestLive(AdversarialExternalPlannerContainment|ExternalPlannerKeepsWorkspaceTools)$$' -count=1 -v

vet: $(WEB_DIST_INDEX) ## Run `go vet ./...`
	$(GO) vet ./...

coverage-core: tools-check $(WEB_DIST_INDEX) ## Check the core coverage gate
	GO="$(GO)" ./scripts/check_core_coverage.sh $(CORE_COVERAGE_MIN)

scripts-check: ## Run lightweight script verification checks
	@set -e; for script in scripts/*.sh; do bash -n "$$script"; done
	python3 -m unittest discover -s scripts/tests -p "test_*.py"
	$(NODE) --test scripts/tests/*.test.mjs

verify: tools-check test test-web lint-web test-race vet coverage-core contracts-check scripts-check openapi-lint assets-check ## Run the main verification sweep

##@ Contracts
contracts-check: tools-check $(WEB_NODE_MODULES_STAMP) ## Verify generated OpenAPI types are up to date
	$(NODE) scripts/generate_theme_contract.mjs --check
	@set -eu; tmp_dir="$$(mktemp -d)"; trap 'rm -rf "$$tmp_dir"' EXIT; \
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) -generate types,client,spec -package apigen -o "$$tmp_dir/types.gen.go" $(OPENAPI_SPEC); \
	($(WEB_PNPM) exec openapi-typescript ../$(OPENAPI_SPEC) -o "$$tmp_dir/schema.ts"); \
	cmp -s "$$tmp_dir/types.gen.go" $(OPENAPI_GENERATED) || { diff -u $(OPENAPI_GENERATED) "$$tmp_dir/types.gen.go"; exit 1; }; \
	cmp -s "$$tmp_dir/schema.ts" $(WEB_OPENAPI_GENERATED) || { diff -u $(WEB_OPENAPI_GENERATED) "$$tmp_dir/schema.ts"; exit 1; }; \
	printf 'Generated OpenAPI contracts are current.\n'

openapi-lint: tools-check $(WEB_NODE_MODULES_STAMP) ## Lint the OpenAPI spec with Redocly
	$(WEB_PNPM) exec redocly lint ../$(OPENAPI_SPEC)

##@ Utilities
tool-versions: ## Print the pinned local tool versions
	@printf "Go: %s\nNode: %s\nPython: %s\npnpm: %s\noapi-codegen: %s\n" \
		"$(GO_VERSION)" "$(NODE_VERSION)" "$(PYTHON_VERSION)" "$(PNPM_VERSION)" "$(OAPI_CODEGEN_VERSION)"

tools-check: ## Check active tools against .tool-versions
	python3 scripts/check_tool_versions.py --go "$(GO)" --node "$(NODE)" --pnpm "$(PNPM)"

assets-generate: tools-check ## Generate bundled policies and schemas from canonical files
	$(NODE) scripts/generate_bootstrap_assets.mjs

assets-check: tools-check ## Check bundled policies and schemas against canonical files
	$(NODE) scripts/generate_bootstrap_assets.mjs --check

# Internal targets
contracts-generate: tools-check $(OPENAPI_GENERATED) $(WEB_OPENAPI_GENERATED)

$(OPENAPI_SPEC): contracts/theme/theme.schema.json scripts/generate_theme_contract.mjs
	$(NODE) scripts/generate_theme_contract.mjs

$(WEB_NODE_MODULES_STAMP): web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml .tool-versions Makefile | tools-check
	rm -rf web/node_modules
	$(WEB_PNPM) install --frozen-lockfile
	touch $@

# The phony prerequisite handles deleted sources and assets, and Make shares it
# across all requested targets. Go's build cache handles unchanged Go inputs.
web-build: tools-check assets-check $(WEB_NODE_MODULES_STAMP)
	$(WEB_PNPM) build

$(WEB_DIST_INDEX): web-build
	@test -f $@

$(OPENAPI_GENERATED): $(OPENAPI_SPEC) Makefile
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) -generate types,client,spec -package apigen -o $(OPENAPI_GENERATED) $(OPENAPI_SPEC)

$(WEB_OPENAPI_GENERATED): $(OPENAPI_SPEC) $(WEB_NODE_MODULES_STAMP)
	$(WEB_PNPM) exec openapi-typescript ../$(OPENAPI_SPEC) -o src/api/schema.ts
