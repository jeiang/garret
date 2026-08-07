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
