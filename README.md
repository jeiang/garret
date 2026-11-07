# garret

A single-tenant Nix binary cache: OIDC-protected push, standard substituter
pull, SQLite + S3.

An authenticated **Pusher** accepts store paths over a purpose-built push
protocol. A public **Puller** serves them to any machine's `nix.conf` as a
standard substituter. Both run on one host over SQLite and an S3-compatible
bucket.

## Install

Add the cache to `nix.conf`:

```
extra-substituters = https://cache.jeiang.dev
extra-trusted-public-keys = cache.jeiang.dev-1:owXJK5/UX9NSf1lhmDDT3QTxMtbVk9YfHhjvOXyPhpA=
```

Then install the client:

```
nix profile install github:jeiang/garret#garret
```

## Quickstart

```
garret login https://push.cache.example   # writes the config, then signs you in
garret use                                # points nix at the cache
garret push ./result                      # uploads the closure
garret watch-store                        # keeps pushing what you build
```

## Documentation

The book is at <https://garret.docs.jeiang.dev>. Agents can read
<https://garret.docs.jeiang.dev/llms.txt> for an index, or
<https://garret.docs.jeiang.dev/llms-full.txt> for the whole book in one file.

It is built from `docs/` with mdbook; `just docs` builds it into `docs/book`.

## Development

`nix develop` provides the toolchain and the development dependencies.

```
just build       # cargo build --workspace
just test        # unit tests
just check       # clippy (-D warnings) + rustfmt
just e2e         # end-to-end gate against a throwaway Garage
just docs        # build the documentation book
just bench-local # self-provisioned benchmark, all scenarios + RSS check
just microbench  # in-process criterion benches
just kani        # Kani proofs
just tla         # TLC model check of the push/GC protocol
```

## License

[MIT](LICENSE)
