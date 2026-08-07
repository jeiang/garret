# NixOS module for the Puller (spec 10-packaging).
self: { config, lib, pkgs, ... }:

let
  cfg = config.services.garret.puller;
  inherit (lib) mkEnableOption mkOption mkIf types;

  settings = {
    listen = cfg.listen;
    metrics_listen = cfg.metricsListen;
    db_path = cfg.dbPath;
    store_dir = cfg.storeDir;
    presign_ttl_secs = cfg.presignTtlSeconds;
    bump_debounce_secs = cfg.bumpDebounceSeconds;
    s3 = {
      inherit (cfg.s3) bucket region;
      endpoint_url = cfg.s3.endpointUrl;
      path_style = cfg.s3.pathStyle;
    };
  } // lib.optionalAttrs (cfg.browseOidc != null) { browse_oidc = cfg.browseOidc; };

  configFile = (pkgs.formats.toml { }).generate "garret-puller.toml" settings;
in
{
  options.services.garret.puller = {
    enable = mkEnableOption "the garret Puller";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.garret-puller;
      description = "The garret-puller package to run.";
    };

    listen = mkOption { type = types.str; default = "0.0.0.0:8081"; description = "Public substituter listener."; };
    metricsListen = mkOption { type = types.str; default = "127.0.0.1:9092"; description = "Internal metrics listener."; };
    dbPath = mkOption { type = types.str; default = "/var/lib/garret/garret.db"; description = "The Pusher's database."; };
    storeDir = mkOption { type = types.str; default = "/nix/store"; description = "Reported in nix-cache-info."; };

    presignTtlSeconds = mkOption {
      type = types.int;
      default = 3600;
      description = "Lifetime of the presigned URLs NAR requests redirect to.";
    };

    bumpDebounceSeconds = mkOption {
      type = types.int;
      default = 86400;
      description = "Skip a last-accessed write if the stored value is newer than this.";
    };

    s3 = {
      bucket = mkOption { type = types.str; description = "Bucket name."; };
      endpointUrl = mkOption { type = types.nullOr types.str; default = null; description = "S3 endpoint."; };
      region = mkOption { type = types.nullOr types.str; default = null; description = "S3 region."; };
      pathStyle = mkOption { type = types.bool; default = true; description = "Path-style addressing."; };
      credentialsFile = mkOption { type = types.path; description = "EnvironmentFile with AWS_* credentials."; };
    };

    browseOidc = mkOption {
      type = types.nullOr (types.submodule {
        options = {
          issuer = mkOption { type = types.str; description = "Pocket ID issuer URL."; };
          audience = mkOption { type = types.str; description = "Audience identifying garret."; };
          jwks_url = mkOption { type = types.nullOr types.str; default = null; description = "Skips discovery when set."; };
          allowed_groups = mkOption { type = types.listOf types.str; default = [ ]; description = "Optional group allowlist."; };
        };
      });
      default = null;
      description = ''
        Auth for the browse routes only; narinfo and NAR stay anonymous so any
        machine's nix.conf works untouched. Null means no browse API is served.
      '';
    };
  };

  config = mkIf cfg.enable {
    systemd.services.garret-puller = {
      description = "garret Puller";
      wantedBy = [ "multi-user.target" ];
      # The Pusher owns the schema and must create the database first.
      after = [ "network-online.target" "garret-pusher.service" ];
      wants = [ "network-online.target" ];

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/garret-puller ${configFile}";
        EnvironmentFile = cfg.s3.credentialsFile;
        Restart = "on-failure";
        StateDirectory = "garret";
        User = "garret";
        Group = "garret";
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        NoNewPrivileges = true;
        ReadWritePaths = [ (builtins.dirOf cfg.dbPath) ];
      };
    };

    users.users.garret = {
      isSystemUser = true;
      group = "garret";
    };
    users.groups.garret = { };
  };
}
