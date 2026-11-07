# Quickstart

Four commands take you from nothing to a cache your machines substitute from.

```
garret login https://push.cache.example   # writes the config, then signs you in
garret use                                # points nix at the cache
garret push ./result                      # uploads the closure
garret watch-store                        # keeps pushing what you build
```

`garret login` takes the Pusher URL and fetches the rest — Puller URL, signing
keys, OIDC issuer and client id — from the server. It creates the config
directory if it is absent.

## Commands without a config

Exactly these run without one: `garret login <url>`, `garret logout`,
`garret completions <shell>` and `garret enqueue`. `garret login` without a URL
needs an existing config, since there is nothing to re-authenticate against.

## Machines without a human

Build hosts and CI skip `login`. Set `[oidc] client_secret_file` to a file
holding only the client secret, with `client_id` and `issuer` alongside it, or
set `GARRET_TOKEN`. A GitHub Actions runner with `id-token: write` needs
neither. `GARRET_ENDPOINT` overrides `endpoint`.

## Pointing nix at the cache

`garret use` appends the cache to the user's `nix.conf`. Substituters in a
user's `nix.conf` are ignored unless that user is in `trusted-users`, so the
command checks `nix config show substituters` afterwards and exits 1 when nix
ignores the file, printing the `nix.settings` snippet to use instead.

The full reference — every command, config key and default — is in the
[Client reference](client.md).
