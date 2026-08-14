# List available targets
list:
	@just --list --unsorted

build:
	cargo build --workspace

test:
	cargo test --workspace

check:
	cargo clippy --workspace --all-targets -- -D warnings
	cargo fmt --check

# End-to-end gate: push a real store path and substitute it back out
e2e:
	nix develop --command scripts/e2e.sh

# Load-test a running deployment (all scenarios) and record results
bench pusher="http://127.0.0.1:8080" puller="http://127.0.0.1:8081":
	cargo run --release -p garret-bench -- all --endpoint {{pusher}} --puller-endpoint {{puller}} --json bench-results.json

# Self-provisioned benchmark: throwaway Garage + services, all scenarios,
# pusher RSS sampled against the spec's 2x in-flight-cap budget
bench-local:
	nix develop --command scripts/bench-local.sh

# Stepped-concurrency pull sweep (ticket 28): c=20/100/300, keep-alive on
# and off, reported-only — the curve that would reopen ticket 26
bench-pull-sweep:
	nix develop --command scripts/bench-pull-sweep.sh

# Diff the latest results against the checked-in baseline
bench-compare baseline="benchmarks/baseline.json" current="bench-results.json":
	python3 scripts/bench-diff.py {{baseline}} {{current}}

# Promote the latest results to the checked-in baseline
bench-baseline:
	cp bench-results.json benchmarks/baseline.json

# In-process micro-benchmarks: zstd, sha256, preamble framing
microbench:
	cargo bench -p garret-bench
