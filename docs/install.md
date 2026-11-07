# Installation

## Use the cache

Add the cache to `nix.conf`:

```
extra-substituters = https://cache.jeiang.dev
extra-trusted-public-keys = cache.jeiang.dev-1:owXJK5/UX9NSf1lhmDDT3QTxMtbVk9YfHhjvOXyPhpA=
```

On NixOS, set the same values through `nix.settings`. On a multi-user install,
`--option` on the command line works only for trusted users.

## Install the client

```
nix profile install github:jeiang/garret#garret
```

Pin a revision with `github:jeiang/garret/<rev>#garret` to keep a fixed version.

## Flake outputs

| Output | Contents |
|---|---|
| `default` | The workspace build |
| `garret-all` | The workspace build |
| `garret` | The client |
| `garret-pusher` | The Pusher service |
| `garret-puller` | The Puller service |
| `garret-admin` | The admin CLI |
| `garret-bench` | The load harness |
| `garret-deps` | Compiled third-party dependencies; a build-time input only |

The NixOS modules `services.garret-pusher`, `services.garret-puller` and
`services.garret-watcher` default to these packages, so a host that imports them
substitutes the binaries too — as long as it does not override garret's
`nixpkgs` input with `follows`.

## Prebuilt binaries

CI builds on x86_64-linux and aarch64-darwin and publishes the closures of
`garret-all`, `garret`, `garret-pusher`, `garret-puller`, `garret-admin` and
`garret-deps`, plus the development shell, to garret's own cache. Publication
happens on pushes to `main`, on `v*` tags, and on manual dispatch, and only when
the `GARRET_ENDPOINT` repository variable is set.

Next: [Quickstart](quickstart.md) to push your first path, or
[Deployment](deployment.md) to run the server.
