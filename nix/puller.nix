# NixOS module for the Puller (spec 10-packaging).
self: { config, lib, pkgs, ... }:

let
  cfg = config.services.garret.puller;
  # The database, store directory and bucket are the Pusher's on this host
  # (ADR-0001, ADR-0009); the Puller takes them rather than repeating them.
  pusher = config.services.garret.pusher;
  inherit (lib) mkEnableOption mkOption mkIf types;

  common = import ./common.nix { inherit lib pkgs; };

  settings = {
    listen = cfg.listen;
    metrics_listen = cfg.metricsListen;
    db_path = pusher.dbPath;
    store_dir = pusher.storeDir;
    presign_ttl_secs = cfg.presignTtlSeconds;
    bump_debounce_secs = cfg.bumpDebounceSeconds;
    db_wait_timeout_secs = cfg.dbWaitTimeoutSeconds;
    db_read_budget_ms = cfg.dbReadBudgetMs;
    presign_budget_ms = cfg.presignBudgetMs;
    s3 = {
      inherit (pusher.s3) bucket region;
      endpoint_url = pusher.s3.endpointUrl;
      path_style = pusher.s3.pathStyle;
    };
    telemetry = {
      enabled = cfg.telemetry.enable;
    } // lib.optionalAttrs (cfg.telemetry.endpoint != null) {
      endpoint = cfg.telemetry.endpoint;
    } // lib.optionalAttrs (cfg.telemetry.serviceName != null) {
      service_name = cfg.telemetry.serviceName;
    } // lib.optionalAttrs (cfg.telemetry.sampleRatio != null) {
      sample_ratio = cfg.telemetry.sampleRatio;
    };
  } // lib.optionalAttrs (cfg.browseOidc != null) { browse_oidc = cfg.browseOidc; };

  configFile = common.configFile "garret-puller.toml" settings cfg.extraSettings;
in
{
  # The Pusher's option declarations, so this module evaluates on its own;
  # its config stays off unless services.garret.pusher.enable is set.
  imports = [
    (import ./pusher.nix self)
    (lib.mkRemovedOptionModule [ "services" "garret" "puller" "s3" "operationTimeoutSeconds" ]
      "The Puller makes no S3 requests (presigning is local); the timeout is services.garret.pusher.s3.operationTimeoutSeconds.")
  ];

  options.services.garret.puller = {
    enable = mkEnableOption "the garret Puller";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.garret-puller;
      description = "The garret-puller package to run.";
    };

    extraSettings = common.extraSettings;

    listen = mkOption { type = types.nullOr types.str; default = null; description = "Public substituter listener. Front it with TLS. Null: loopback."; };
    metricsListen = mkOption { type = types.nullOr types.str; default = null; description = "Internal metrics listener. Never expose this publicly. Null: loopback."; };

    presignTtlSeconds = mkOption {
      type = types.nullOr (types.ints.between 1 604800);
      default = null;
      description = "Lifetime of the presigned URLs NAR requests redirect to.";
    };

    bumpDebounceSeconds = mkOption {
      type = types.nullOr types.ints.unsigned;
      default = null;
      description = "Skip a last-accessed write if the stored value is newer than this.";
    };

    dbWaitTimeoutSeconds = mkOption {
      type = types.nullOr types.ints.positive;
      default = null;
      description = "How long to wait for the Pusher to create the database before exiting.";
    };

    dbReadBudgetMs = mkOption {
      type = types.nullOr types.ints.positive;
      default = null;
      description = "Pull-path database read budget before the request degrades to a 404 miss.";
    };

    presignBudgetMs = mkOption {
      type = types.nullOr types.ints.positive;
      default = null;
      description = "Presign budget before a NAR request degrades to a 404 miss.";
    };

    s3 = {
      # Bucket, endpoint, region and addressing come from the Pusher.
      credentialsFile = mkOption {
        type = types.path;
        description = ''
          EnvironmentFile providing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
          Give the Puller a key of its own limited to GetObject: it only
          presigns GETs, and a presigned URL carries exactly its key's
          authority. The Pusher's key works too, but hands the public-facing
          process write access to the bucket.
        '';
      };
    };

    telemetry = {
      enable = mkEnableOption "OpenTelemetry span export to an OTLP/HTTP collector";

      endpoint = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          OTLP/HTTP base URL; spans are posted to `<endpoint>/v1/traces`.
          Null: `http://127.0.0.1:4318`, the collector's default receiver.
        '';
      };

      serviceName = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "`service.name` on exported spans. Null: `puller`, the service's own name.";
      };

      sampleRatio = mkOption {
        type = types.nullOr (types.numbers.between 0.0 1.0);
        default = null;
        description = ''
          Fraction of new traces sampled, 0.0..=1.0. Children keep their
          parent's sampling decision, so a trace is sampled or not as a
          whole and a caller's decision is never overridden. Null: 1.0.
        '';
      };
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

  config = mkIf cfg.enable (lib.mkMerge [ (common.serviceUser "garret-puller") {
    assertions = [{
      assertion = pusher.enable;
      message = "services.garret.puller needs services.garret.pusher on the same host: the Puller serves the Pusher's database and bucket, and takes dbPath, storeDir and s3 from it.";
    }];

    systemd.services.garret-puller = {
      description = "garret Puller";
      wantedBy = [ "multi-user.target" ];
      # The Pusher owns the schema and must create the database first; its
      # start is also what makes the database group-writable for us.
      after = [ "network-online.target" "garret-pusher.service" ];
      wants = [ "network-online.target" ];

      serviceConfig = common.daemonServiceConfig // {
        ExecStart = "${cfg.package}/bin/garret-puller ${configFile}";
        EnvironmentFile = cfg.s3.credentialsFile;
        # Its own user, in group `garret` only to share the database
        # (ADR-0009): the signing keys and the admin socket are owner-only, so
        # the public-facing process can reach neither. No StateDirectory: the
        # directory is the Pusher's, and systemd would chown it to this user.
        User = "garret-puller";
        Group = "garret";
        ReadWritePaths = [ (builtins.dirOf pusher.dbPath) ];
      };
    };
  } ]);
}
