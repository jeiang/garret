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

# Load-test a running Pusher and record results for comparison
bench endpoint="http://127.0.0.1:8080":
	cargo run --release -p garret-bench -- --endpoint {{endpoint}} --json bench-results.json
