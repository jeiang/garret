# NixOS module for the Pusher (spec 10-packaging).
self: { config, lib, pkgs, ... }:

let
  cfg = config.services.garret.pusher;
  inherit (lib) mkEnableOption mkOption mkIf types;

  # Secrets stay out of the store: the unit renders the config at start-up,
  # substituting AWS credentials from an EnvironmentFile (agenix/sops-friendly).
  settings = {
    listen = cfg.listen;
    metrics_listen = cfg.metricsListen;
    db_path = cfg.dbPath;
    store_dir = cfg.storeDir;
    signing_key_files = cfg.signingKeyFiles;
    admin_socket = cfg.adminSocketPath;
    s3 = {
      inherit (cfg.s3) bucket region;
      endpoint_url = cfg.s3.endpointUrl;
      path_style = cfg.s3.pathStyle;
    };
    limits = {
      max_concurrent_uploads = cfg.limits.maxConcurrentUploads;
      max_in_flight_bytes = cfg.limits.maxInFlightBytes;
      part_size = cfg.limits.partSize;
      max_parts_in_flight = cfg.limits.maxPartsInFlight;
    };
    oidc = cfg.oidc;
  } // lib.optionalAttrs (cfg.quotaBytes != null) {
    gc = {
      quota_bytes = cfg.quotaBytes;
      high_watermark = cfg.watermarks.high;
      low_watermark = cfg.watermarks.low;
      interval_secs = cfg.gcIntervalSeconds;
    };
  };

  configFile = (pkgs.formats.toml { }).generate "garret-pusher.toml" settings;
in
{
  options.services.garret.pusher = {
    enable = mkEnableOption "the garret Pusher";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.garret-pusher;
      description = "The garret-pusher package to run.";
    };

    listen = mkOption {
      type = types.str;
      default = "127.0.0.1:8080";
      description = "Address for the push API. Front it with TLS.";
    };

    metricsListen = mkOption {
      type = types.str;
      default = "127.0.0.1:9091";
      description = "Internal metrics listener. Never expose this publicly.";
    };

    dbPath = mkOption {
      type = types.str;
      default = "/var/lib/garret/garret.db";
      description = "SQLite database, shared with the Puller on this host.";
    };

    storeDir = mkOption {
      type = types.str;
      default = "/nix/store";
      description = "Store directory the signed fingerprints refer to.";
    };

    s3 = {
      bucket = mkOption { type = types.str; description = "Bucket name."; };
      endpointUrl = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "S3 endpoint, e.g. MEGA S4's regional URL.";
      };
      region = mkOption { type = types.nullOr types.str; default = null; description = "S3 region."; };
      pathStyle = mkOption { type = types.bool; default = true; description = "Path-style addressing."; };
      credentialsFile = mkOption {
        type = types.path;
        description = ''
          EnvironmentFile providing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
          A file path, so the secret never enters the nix store.
        '';
      };
    };

    signingKeyFiles = mkOption {
      type = types.listOf types.str;
      description = ''
        Nix-format secret keys. List several during an overlap rotation: every
        object is signed with all of them, and `garret-admin resign` backfills.
      '';
    };

    quotaBytes = mkOption {
      type = types.nullOr types.int;
      default = null;
      description = "Storage budget. Null means no quota and no eviction.";
    };

    watermarks = {
      high = mkOption { type = types.float; default = 0.95; description = "Eviction starts here."; };
      low = mkOption { type = types.float; default = 0.85; description = "And runs until here."; };
    };

    gcIntervalSeconds = mkOption { type = types.int; default = 300; description = "Seconds between GC ticks."; };

    limits = {
      maxConcurrentUploads = mkOption { type = types.int; default = 32; description = "Past this, uploads are shed with 429."; };
      maxInFlightBytes = mkOption { type = types.int; default = 2147483648; description = "Cap on buffered upload bytes."; };
      partSize = mkOption { type = types.int; default = 67108864; description = "Multipart part size; also the single-PutObject threshold."; };
      maxPartsInFlight = mkOption { type = types.int; default = 4; description = "Concurrent parts per upload."; };
    };

    oidc = mkOption {
      type = types.listOf (types.submodule {
        options = {
          issuer = mkOption { type = types.str; description = "Issuer URL."; };
          audience = mkOption { type = types.str; description = "RFC 8707 audience identifying garret."; };
          jwks_url = mkOption { type = types.nullOr types.str; default = null; description = "Skips discovery when set."; };
          github_owner_id = mkOption { type = types.nullOr types.str; default = null; description = "GitHub: immutable owner id."; };
          ref_patterns = mkOption { type = types.listOf types.str; default = [ ]; description = "GitHub: allowed refs."; };
          allowed_groups = mkOption { type = types.listOf types.str; default = [ ]; description = "Optional group allowlist."; };
        };
      });
      description = ''
        Trusted OIDC issuers. At least one is required — the Pusher refuses to
        start without one, and there is deliberately no auth-disable flag.
      '';
    };

    adminSocketPath = mkOption {
      type = types.nullOr types.str;
      default = "/run/garret/admin.sock";
      description = "Root-only socket for garret-admin.";
    };
  };

  config = mkIf cfg.enable {
    assertions = [{
      assertion = cfg.oidc != [ ];
      message = "services.garret.pusher.oidc must list at least one issuer.";
    }];

    systemd.services.garret-pusher = {
      description = "garret Pusher";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/garret-pusher ${configFile}";
        EnvironmentFile = cfg.s3.credentialsFile;
        Restart = "on-failure";
        StateDirectory = "garret";
        RuntimeDirectory = "garret";
        # The Pusher reads signing keys and writes only its own state.
        DynamicUser = false;
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
