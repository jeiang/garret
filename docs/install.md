# Installation

Garret is not a hosted service. You run the Pusher and the Puller on your own
host, and point your machines at the Puller. This page installs the client;
[Deployment](deployment.md) covers the server.

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
`services.garret-watcher` default to these packages.

## Point nix at your cache

Once your Puller is running, add it to `nix.conf`:

```
extra-substituters = https://cache.example
extra-trusted-public-keys = cache.example-1:<base64 public key>
```

`garret use` writes both entries for you. `garret-admin key show` prints the
public key of the signing key your Pusher uses.

On NixOS, set the same values through `nix.settings`. On a multi-user install,
`--option` on the command line works only for trusted users.

Next: [Quickstart](quickstart.md) to push your first path.
