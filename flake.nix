{
  description = "garret — a single-tenant Nix binary cache";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # One derivation builds the workspace; the per-binary outputs are thin
      # wrappers over it, so each unit depends on exactly the binary it runs.
      garret = pkgs: pkgs.rustPlatform.buildRustPackage {
        pname = "garret";
        version = "0.1.0";
        src = ./.;
        cargoLock.lockFile = ./Cargo.lock;
        # rusqlite is vendored (its `bundled` feature), so no system sqlite.
        nativeBuildInputs = [ pkgs.installShellFiles ];
        # Generating completions means *running* the binary we just built, which
        # only works when the build machine can execute the host platform's
        # binaries. Without the guard a `pkgsCross` build fails here with an
        # exec-format error that says nothing about completions.
        postInstall = pkgs.lib.optionalString
          (pkgs.stdenv.buildPlatform.canExecute pkgs.stdenv.hostPlatform) ''
          installShellCompletion --cmd garret \
            --bash <($out/bin/garret completions bash) \
            --zsh <($out/bin/garret completions zsh) \
            --fish <($out/bin/garret completions fish)
        '';
        meta.description = "A single-tenant Nix binary cache";
      };

      # mainProgram is required, not cosmetic: the derivation is named
      # `garret-${name}` while the binary inside it is `${name}`, so without it
      # `nix run .#garret-admin` looks for a `garret-garret-admin` that does not
      # exist and fails with "unable to execute".
      #
      # `share/` is linked too, not just `bin/`: the shell completions installed
      # by postInstall live there, and a wrapper that dropped them would leave
      # `nix profile install .#garret` with no completions at all.
      only = pkgs: name: pkgs.runCommand "garret-${name}" { meta.mainProgram = name; } ''
        mkdir -p $out/bin
        ln -s ${garret pkgs}/bin/${name} $out/bin/${name}
        if [ -d ${garret pkgs}/share ]; then
          ln -s ${garret pkgs}/share $out/share
        fi
      '';
    in
    {
      packages = forAllSystems (pkgs: {
        default = garret pkgs;
        garret-all = garret pkgs;
        garret = only pkgs "garret";
        garret-pusher = only pkgs "garret-pusher";
        garret-puller = only pkgs "garret-puller";
        garret-admin = only pkgs "garret-admin";
        garret-bench = only pkgs "garret-bench";
      });

      nixosModules = {
        pusher = import ./nix/pusher.nix self;
        puller = import ./nix/puller.nix self;
        watcher = import ./nix/watcher.nix self;
      };

      checks = forAllSystems (pkgs: {
        build = garret pkgs;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            rust-analyzer
            sqlite
            just
            garage
            zstd
            curl
            lsof
            openssl
            (python3.withPackages (ps: [ ps.pyjwt ps.cryptography ]))
          ];
        };
      });
    };
}
