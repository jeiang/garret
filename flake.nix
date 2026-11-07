{
  description = "garret — a single-tenant Nix binary cache";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.crane.url = "github:ipetkov/crane";

  outputs = { self, nixpkgs, crane }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Crane over nixpkgs' own rustc, so the toolchain is the devshell's.
      craneLib = pkgs: crane.mkLib pkgs;

      # Everything shared by the dependency build, the workspace and the
      # checks. Nothing here may vary per commit (GARRET_REVISION lives on the
      # workspace derivation only), or the dependency build would be redone
      # on every change instead of substituted.
      commonArgs = pkgs: {
        pname = "garret";
        version = "0.1.0";
        # Cargo sources only: the crates read no other file at build or test
        # time, and docs, scripts or nix/ edits then rebuild nothing.
        src = pkgs.lib.fileset.toSource {
          root = ./.;
          fileset = (craneLib pkgs).fileset.commonCargoSources ./.;
        };
        strictDeps = true;
        # rusqlite is vendored (its `bundled` feature), so no system sqlite;
        # aws-lc-sys and zstd-sys build with cc alone.
      };

      # The workspace's third-party dependencies, compiled once. Only a
      # Cargo.lock or manifest change rebuilds this; a source edit reuses it.
      garretDeps = pkgs: (craneLib pkgs).buildDepsOnly (commonArgs pkgs);

      # The documentation book. `docs/` is the mdbook source root; the
      # derivation also validates every relative link and emits llms.txt and
      # llms-full.txt for agents.
      docs = pkgs: pkgs.stdenvNoCC.mkDerivation {
        name = "garret-docs";
        src = pkgs.lib.fileset.toSource {
          root = ./docs;
          fileset = pkgs.lib.fileset.unions [
            ./docs/book.toml
            ./docs/img
            (pkgs.lib.fileset.fileFilter (f: f.hasExt "md") ./docs)
          ];
        };
        nativeBuildInputs = [ pkgs.mdbook pkgs.python3 ];
        dontConfigure = true;
        buildPhase = ''
          runHook preBuild
          mdbook build . --dest-dir "$out"
          rm "$out/book.toml"
          python3 ${./scripts/gen-llms.py} . "$out"
          runHook postBuild
        '';
        dontInstall = true;
      };

      # One derivation builds the workspace; the per-binary outputs are thin
      # wrappers over it, so each unit depends on exactly the binary it runs.
      garret = pkgs: (craneLib pkgs).buildPackage (commonArgs pkgs // {
        cargoArtifacts = garretDeps pkgs;
        # The tests are the `test` check; running them here again would only
        # make every build slower.
        doCheck = false;
        # Read by `option_env!("GARRET_REVISION")` for the reported build revision.
        env.GARRET_REVISION = self.shortRev or self.dirtyShortRev or "unknown";
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
      });

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
        # Build-time only: nothing at run time references it, but pushing it
        # lets a source-only change start from compiled dependencies.
        garret-deps = garretDeps pkgs;
        docs = docs pkgs;
      });

      nixosModules = {
        pusher = import ./nix/pusher.nix self;
        puller = import ./nix/puller.nix self;
        watcher = import ./nix/watcher.nix self;
      };

      # The same gates as `just check` and `just test`, over the shared
      # dependency build.
      checks = forAllSystems (pkgs: let
        craneLib' = craneLib pkgs;
        args = commonArgs pkgs // { cargoArtifacts = garretDeps pkgs; };
      in {
        clippy = craneLib'.cargoClippy (args // {
          cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
        });
        fmt = craneLib'.cargoFmt { inherit (args) pname version src; };
        test = craneLib'.cargoTest (args // {
          cargoTestExtraArgs = "--workspace";
        });
        # The shipped alert rules must parse, and the dashboard must be JSON.
        monitoring = pkgs.runCommand "garret-monitoring-check"
          { nativeBuildInputs = [ pkgs.prometheus.cli pkgs.jq ]; } ''
          promtool check rules ${./contrib/monitoring/garret.rules.yml}
          jq empty ${./contrib/monitoring/garret-dashboard.json}
          touch $out
        '';
        # The book builds and every relative link resolves.
        docs = docs pkgs;
      } // nixpkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
        # The modules on a booted host: service users, file modes, sockets.
        module = pkgs.testers.runNixOSTest (import ./nix/test.nix self);
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
            tlaplus
            mdbook
            (python3.withPackages (ps: [ ps.pyjwt ps.cryptography ]))
          ];
        };
      });
    };
}
