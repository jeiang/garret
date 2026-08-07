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
        meta.description = "A single-tenant Nix binary cache";
      };

      only = pkgs: name: pkgs.runCommand "garret-${name}" { } ''
        mkdir -p $out/bin
        ln -s ${garret pkgs}/bin/${name} $out/bin/${name}
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
            openssl
            (python3.withPackages (ps: [ ps.pyjwt ps.cryptography ]))
          ];
        };
      });
    };
}
