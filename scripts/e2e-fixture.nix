# Test store paths for the end-to-end gate.
#
# Built with the flake's own locked nixpkgs rather than a hand-rolled
# `derivation`: naming a bash store path as `builder` declares that one path as
# an input, but not its closure, so the sandbox has the binary and not its
# dynamic loader. That passes on macOS only because builds there run with the
# sandbox off. `runCommand` gets the closure right everywhere.
let
  flake = builtins.getFlake (builtins.getEnv "GARRET_E2E_FLAKE");
  pkgs = flake.inputs.nixpkgs.legacyPackages.${builtins.currentSystem};

  # Fresh on every run, so each e2e pushes paths the cache has never seen.
  stamp = builtins.getEnv "GARRET_E2E_STAMP";

  leaf = pkgs.runCommand "garret-e2e-leaf" { } "echo leaf ${stamp} > $out";
in
{
  # `closure` embeds leaf's path, so nix records a real reference between them:
  # that is what exercises closure walking, the narinfo References line, and
  # the signature computed over it.
  #
  # It writes its own $out too, so the root *self-references*. Most compiled
  # store paths do, and a self-reference is covered by the signature — a server
  # that stores or renders it inconsistently produces a narinfo whose
  # fingerprint nix cannot reproduce. Without this the whole suite passed while
  # every real-world push was unverifiable.
  closure = pkgs.runCommand "garret-e2e-root" { } "echo ${leaf} $out > $out";

  # A path the watcher should notice and push without being asked.
  watched = pkgs.runCommand "garret-e2e-watched" { } "echo watched ${stamp} > $out";
}
