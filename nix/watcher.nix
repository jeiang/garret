# NixOS module for the store watcher, which runs on build machines rather than
# the cache host (spec 06-client, 10-packaging).
self: { config, lib, pkgs, ... }:

let
  cfg = config.services.garret.watcher;
  inherit (lib) mkEnableOption mkOption mkIf types;

  settings = {
    endpoint = cfg.endpoint;
    jobs = cfg.jobs;
    zstd_level = cfg.zstdLevel;
    oidc = {
      inherit (cfg.oidc) issuer audience;
      client_id = cfg.oidc.clientId;
    };
    watch = {
      credentials_file = cfg.credentialsFile;
      nix_db = cfg.nixDb;
      cursor_path = cfg.cursorPath;
      poll_interval_secs = cfg.pollIntervalSeconds;
      upstream_keys = cfg.filters.upstreamKeys;
      exclude_patterns = cfg.filters.excludePatterns;
    };
  };

  configFile = (pkgs.formats.toml { }).generate "garret-watcher.toml" settings;
in
{
  options.services.garret.watcher = {
    enable = mkEnableOption "the garret store watcher";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.garret;
      description = "The garret client package to run.";
    };

    endpoint = mkOption { type = types.str; description = "Pusher base URL."; };

    oidc = {
      issuer = mkOption { type = types.str; description = "Pocket ID issuer URL."; };
      clientId = mkOption { type = types.str; description = "This machine's confidential client id."; };
      audience = mkOption { type = types.str; description = "Audience identifying garret."; };
    };

    credentialsFile = mkOption {
      type = types.str;
      description = ''
        Root-owned file holding `client_id:client_secret` for this machine's
        confidential client. A path, so the secret never enters the nix store.
      '';
    };

    nixDb = mkOption { type = types.str; default = "/nix/var/nix/db/db.sqlite"; description = "The local nix database."; };
    cursorPath = mkOption { type = types.str; default = "/var/lib/garret/watcher-cursor"; description = "Persisted ValidPaths.id cursor."; };
    pollIntervalSeconds = mkOption { type = types.int; default = 30; description = "Seconds between polls of the nix database."; };
    jobs = mkOption { type = types.int; default = 8; description = "Concurrent uploads."; };
    zstdLevel = mkOption { type = types.int; default = 3; description = "Client-side zstd level."; };

    fullSync = mkOption {
      type = types.bool;
      default = false;
      description = ''
        Walk the whole store history on first start instead of beginning at the
        newest path. Only meaningful before a cursor exists.
      '';
    };

    filters = {
      upstreamKeys = mkOption {
        type = types.listOf types.str;
        default = [ "cache.nixos.org-1" ];
        description = "Paths already signed by these keys are served elsewhere; skip them.";
      };
      excludePatterns = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Substring patterns whose matching store paths are never pushed.";
      };
    };
  };

  config = mkIf cfg.enable {
    systemd.services.garret-watcher = {
      description = "garret store watcher";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" "nix-daemon.service" ];
      wants = [ "network-online.target" ];

      serviceConfig = {
        ExecStart = lib.concatStringsSep " " ([
          "${cfg.package}/bin/garret"
          "--config ${configFile}"
          "watch-store"
        ] ++ lib.optional cfg.fullSync "--full-sync");
        Restart = "always";
        RestartSec = 10;
        StateDirectory = "garret";
        # Root: reading the nix database and the credentials file both need it,
        # and there is no unprivileged mode in v1 (spec 06-client).
        User = "root";
        ProtectSystem = "strict";
        ProtectHome = true;
        NoNewPrivileges = true;
        ReadWritePaths = [ (builtins.dirOf cfg.cursorPath) ];
      };
    };
  };
}
