# NixOS module for the Puller (spec 10-packaging).
self: { config, lib, pkgs, ... }:

let
  cfg = config.services.garret.puller;
  # The database, store directory and bucket are the Pusher's on this host
  # (ADR-0001, ADR-0009); the Puller takes them rather than repeating them.
  pusher = config.services.garret.pusher;
  inherit (lib) mkEnableOption mkOption mkIf types;

  # `pkgs.formats.toml` has no representation for null, and every nullable
  # value here (the Pusher's `s3.region` and `s3.endpointUrl`, `browseOidc.jwks_url`) reaches
  # this attrset as `null` when unset -- which fails the build with
  # "unsupported unit type" rather than being omitted. Serde already defaults
  # every one of these on the Rust side, so dropping the key is exactly right.
  stripNulls = value:
    if lib.isAttrs value
    then lib.mapAttrs (_: stripNulls) (lib.filterAttrs (_: v: v != null) value)
    else if lib.isList value
    then map stripNulls value
    else value;

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
      operation_timeout_secs = cfg.s3.operationTimeoutSeconds;
    };
  } // lib.optionalAttrs (cfg.browseOidc != null) { browse_oidc = cfg.browseOidc; };

  configFile = (pkgs.formats.toml { }).generate "garret-puller.toml" (lib.recursiveUpdate (stripNulls settings) cfg.extraSettings);
in
{
  # The Pusher's option declarations, so this module evaluates on its own;
  # its config stays off unless services.garret.pusher.enable is set.
  imports = [ (import ./pusher.nix self) ];

  options.services.garret.puller = {
    enable = mkEnableOption "the garret Puller";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.garret-puller;
      description = "The garret-puller package to run.";
    };

    extraSettings = mkOption {
      type = (pkgs.formats.toml { }).type;
      default = { };
      description = ''
        Raw TOML settings merged over the generated config (recursively), for
        keys that have no typed option here.
      '';
    };

    listen = mkOption { type = types.str; default = "127.0.0.1:8081"; description = "Public substituter listener. Front it with TLS."; };
    metricsListen = mkOption { type = types.str; default = "127.0.0.1:9092"; description = "Internal metrics listener. Never expose this publicly."; };

    presignTtlSeconds = mkOption {
      type = types.ints.between 1 604800;
      default = 3600;
      description = "Lifetime of the presigned URLs NAR requests redirect to.";
    };

    bumpDebounceSeconds = mkOption {
      type = types.ints.unsigned;
      default = 86400;
      description = "Skip a last-accessed write if the stored value is newer than this.";
    };

    dbWaitTimeoutSeconds = mkOption {
      type = types.ints.positive;
      default = 300;
      description = "How long to wait for the Pusher to create the database before exiting.";
    };

    dbReadBudgetMs = mkOption {
      type = types.ints.positive;
      default = 250;
      description = "Pull-path database read budget before the request degrades to a 404 miss.";
    };

    presignBudgetMs = mkOption {
      type = types.ints.positive;
      default = 250;
      description = "Presign budget before a NAR request degrades to a 404 miss.";
    };

    s3 = {
      # Bucket, endpoint, region and addressing come from the Pusher.
      operationTimeoutSeconds = mkOption { type = types.int; default = 60; description = "Has no effect on the Puller (presigning is local, no S3 request is made). Kept for compatibility."; };
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

      serviceConfig = import ./sandbox.nix // {
        ExecStart = "${cfg.package}/bin/garret-puller ${configFile}";
        EnvironmentFile = cfg.s3.credentialsFile;
        Restart = "on-failure";
        # Server daemons only (the watcher shares the rest of the sandbox):
        # hide other processes and SysV IPC.
        ProtectProc = "invisible";
        ProcSubset = "pid";
        PrivateIPC = true;
        RemoveIPC = true;
        # The binary signals READY=1 once every listener is bound.
        Type = "notify";
        # Its own user, in group `garret` only to share the database
        # (ADR-0009): the signing keys and the admin socket are owner-only, so
        # the public-facing process can reach neither. No StateDirectory: the
        # directory is the Pusher's, and systemd would chown it to this user.
        User = "garret-puller";
        Group = "garret";
        ReadWritePaths = [ (builtins.dirOf pusher.dbPath) ];
      };
    };

    users.users.garret-puller = {
      isSystemUser = true;
      group = "garret";
    };
    users.groups.garret = { };
  };
}
