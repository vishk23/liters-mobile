# liters build helpers.
#
# `make reference` fetches the pinned benbjohnson/litestream checkout into
# reference/litestream, which is not in the repo (reference/.gitignore keeps
# the directory but ignores its contents). Two things need it: the wal-reader
# fixture tests, which read testdata straight out of it, and `make oracle`,
# which builds the litestream CLI from it. Git is the only prerequisite.
#
# `make oracle` builds the Go reference binaries used by the interop test
# suite ("the oracle"): the litestream CLI from reference/litestream and the
# ltx CLI at the exact version litestream pins. Tests locate them via
# LITERS_ORACLE_DIR (defaults to target/oracle) and skip if absent.

ORACLE_DIR := $(CURDIR)/target/oracle
LITESTREAM := $(ORACLE_DIR)/litestream
LTX_CLI    := $(ORACLE_DIR)/ltx
HELPER     := $(ORACLE_DIR)/oracle-helper
LTX_VERSION := v0.5.1

REFERENCE_DIR := $(CURDIR)/reference/litestream
# Commit of benbjohnson/litestream the suite is validated against (v0.5.14 + 1).
# Defined here only: .github/workflows/test.yml fetches the checkout by running
# `make reference`, so CI and a local clone can never drift to different refs.
LITESTREAM_REF := c96c0f42a51bf48a40e13a1569a46312e957b429

.PHONY: oracle reference test test-system-sqlite clean-oracle clean-reference

# Idempotent: re-fetches only when the checkout is missing or off the pin,
# so bumping LITESTREAM_REF above is enough to move it.
reference:
	@if [ "$$(git -C $(REFERENCE_DIR) rev-parse HEAD 2>/dev/null)" != "$(LITESTREAM_REF)" ]; then \
		echo "fetching litestream $(LITESTREAM_REF) into $(REFERENCE_DIR)"; \
		git init -q $(REFERENCE_DIR); \
		git -C $(REFERENCE_DIR) fetch -q --depth 1 https://github.com/benbjohnson/litestream $(LITESTREAM_REF); \
		git -C $(REFERENCE_DIR) checkout -q FETCH_HEAD; \
	fi

oracle: $(LITESTREAM) $(LTX_CLI) $(HELPER)

$(HELPER): tests/oracle-helper/main.go tests/oracle-helper/go.mod
	mkdir -p $(ORACLE_DIR)
	cd tests/oracle-helper && go build -o $(HELPER) .

# Order-only dep on the (phony) fetch: the checkout must exist first, but its
# presence must not relink the binary on every invocation. The wildcard is
# empty before the first fetch and picks up the sources after it.
$(LITESTREAM): $(wildcard reference/litestream/*.go reference/litestream/go.mod) | reference
	mkdir -p $(ORACLE_DIR)
	cd reference/litestream && go build -o $(LITESTREAM) ./cmd/litestream

$(LTX_CLI):
	mkdir -p $(ORACLE_DIR)
	GOBIN=$(ORACLE_DIR) go install github.com/superfly/ltx/cmd/ltx@$(LTX_VERSION)

# The reference checkout is mandatory (git-only, and the fixture tests read it
# directly); the oracle is best-effort, so that `make test` on a machine
# without Go still runs the suite with the oracle-backed tests skipping, as
# documented. `make oracle` on its own stays strict.
test: reference
	@if command -v go >/dev/null 2>&1; then \
		$(MAKE) oracle; \
	else \
		echo "note: no Go toolchain; skipping the oracle build (oracle-backed tests will print SKIP)"; \
	fi
	LITERS_ORACLE_DIR=$(ORACLE_DIR) cargo test --workspace

# The other SQLite linkage. `bundled-sqlite` is on by default, so `test` above
# compiles the amalgamation and nothing in it exercises the platform
# libsqlite3 that an embedder sharing a process with another SQLite -- an iOS
# app on GRDB -- is required to link instead.
#
# The package selection is load-bearing, not a shortcut. `ltx`, `liters-wal`
# and `liters-storage` each dev-depend on `rusqlite = { features =
# ["bundled"] }` to keep their own fixtures hermetic, and cargo unions
# features across the whole build: pull any of them into the selection and
# `bundled` comes back on for `liters` itself. `cargo tree --workspace
# --no-default-features -e features -i rusqlite` shows `feature "bundled"`
# twice; the two-package selection below shows it zero times. So
# `make test-system-sqlite` is the unbundled run and `--workspace
# --no-default-features` is the bundled one wearing its flag.
#
# The oracle is a hard prerequisite here, unlike `test`, and it matters more:
# the oracle-gated tests return early and still report `ok`, so a bare
# `cargo test -p liters -p liters-ffi --no-default-features` on a tree with no
# `target/oracle` goes green having silently skipped the entire litestream
# comparison, with nothing in the output to say so.
test-system-sqlite: reference oracle
	LITERS_ORACLE_DIR=$(ORACLE_DIR) cargo test -p liters -p liters-ffi --no-default-features

clean-oracle:
	rm -rf $(ORACLE_DIR)

clean-reference:
	rm -rf $(REFERENCE_DIR)
