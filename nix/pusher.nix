# NixOS module for the Pusher (spec 10-packaging).
self: { config, lib, pkgs, ... }:

let
  cfg = config.services.garret.pusher;
  inherit (lib) mkEnableOption mkOption mkIf types;

  common = import ./common.nix { inherit lib pkgs; };

  settings = {
    listen = cfg.listen;
    metrics_listen = cfg.metricsListen;
    db_path = cfg.dbPath;
    store_dir = cfg.storeDir;
    signing_key_files = cfg.signingKeyFiles;
    puller_endpoint = cfg.pullerEndpoint;
    admin_socket = cfg.adminSocketPath;
    s3 = {
      inherit (cfg.s3) bucket region;
      endpoint_url = cfg.s3.endpointUrl;
      path_style = cfg.s3.pathStyle;
      operation_timeout_secs = cfg.s3.operationTimeoutSeconds;
      operation_attempt_timeout_secs = cfg.s3.operationAttemptTimeoutSeconds;
    };
    limits = {
      max_concurrent_uploads = cfg.limits.maxConcurrentUploads;
      max_in_flight_bytes = cfg.limits.maxInFlightBytes;
      part_size = cfg.limits.partSize;
      max_parts_in_flight = cfg.limits.maxPartsInFlight;
      body_idle_timeout_secs = cfg.limits.bodyIdleTimeoutSeconds;
    };
    oidc = cfg.oidc;
  } // lib.optionalAttrs (cfg.quotaBytes != null) {
    gc = {
      quota_bytes = cfg.quotaBytes;
      high_watermark = cfg.watermarks.high;
      low_watermark = cfg.watermarks.low;
      interval_secs = cfg.gcIntervalSeconds;
      orphan_grace_secs = cfg.orphanGraceSeconds;
    };
  };

  configFile = common.configFile "garret-pusher.toml" settings cfg.extraSettings;

  # The Puller runs as its own user in group `garret` (ADR-0009) and must
  # write the database: last-accessed bumps, and every WAL reader writes the
  # -shm file. SQLite gives -wal and -shm the database file's mode but creates
  # the database itself 0644, so it is created group-writable here, before
  # SQLite ever opens it. The loop also repairs the 0644 files of a deployment
  # that predates the split. It runs as `garret`, not root, and only the
  # Pusher can create files in the directory, so there is nothing planted
  # there for it to follow.
  dbPerms = pkgs.writeShellScript "garret-db-perms" ''
    set -eu
    db=${lib.escapeShellArg cfg.dbPath}
    [ -e "$db" ] || ${pkgs.coreutils}/bin/install -m 0660 /dev/null "$db"
    for f in "$db" "$db-wal" "$db-shm"; do
      [ ! -e "$f" ] || ${pkgs.coreutils}/bin/chmod 0660 "$f"
    done
  '';
in
{
  # The Puller module imports this one; the key lets a host import both
  # without declaring every option twice.
  key = toString ./pusher.nix;

  options.services.garret.pusher = {
    enable = mkEnableOption "the garret Pusher";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.garret-pusher;
      description = "The garret-pusher package to run.";
    };

    extraSettings = common.extraSettings;

    listen = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Address for the push API. Front it with TLS. Null: loopback.";
    };

    metricsListen = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Internal metrics listener. Never expose this publicly. Null: loopback.";
    };

    dbPath = mkOption {
      type = types.str;
      default = "/var/lib/garret/garret.db";
      description = ''
        SQLite database, shared with the Puller on this host. Its directory
        must be `garret:garret` mode 0750, so the Puller can open the database
        but never create or replace files beside it. The default location gets
        that from `StateDirectory`; provide it yourself for any other path.
      '';
    };

    storeDir = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Store directory the signed fingerprints refer to. Null: the standard store.";
    };

    s3 = {
      bucket = mkOption { type = types.str; description = "Bucket name."; };
      endpointUrl = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "S3 endpoint, e.g. MEGA S4's regional URL.";
      };
      region = mkOption { type = types.nullOr types.str; default = null; description = "S3 region."; };
      pathStyle = mkOption { type = types.nullOr types.bool; default = null; description = "Path-style addressing. Null: on."; };
      operationTimeoutSeconds = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Per-operation S3 timeout."; };
      operationAttemptTimeoutSeconds = mkOption {
        type = types.nullOr types.ints.positive;
        default = null;
        description = "Per-attempt S3 timeout inside the overall one; must be below operationTimeoutSeconds and above the time to send partSize on the slowest uplink. Null: unset.";
      };
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
        Each must be readable by the `garret` user and nobody else: the Puller
        is in group `garret` for the database, so a group-readable key is one
        the Puller can read.
      '';
    };

    pullerEndpoint = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = ''
        Public URL of the Puller, advertised by `/api/v1/discovery` so
        `garret login <pusher-url>` can write a complete client config. The one
        thing the Pusher cannot infer about the deployment. Null means discovery
        omits it and `garret use`, `list` and `tree` stay unconfigured.
      '';
    };

    quotaBytes = mkOption {
      type = types.nullOr types.ints.positive;
      default = null;
      description = "Storage budget. Null means no quota and no eviction.";
    };

    watermarks = {
      high = mkOption { type = types.nullOr (types.numbers.between 0.0 1.0); default = null; description = "Eviction starts here."; };
      low = mkOption { type = types.nullOr (types.numbers.between 0.0 1.0); default = null; description = "And runs until here."; };
    };

    gcIntervalSeconds = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Seconds between GC ticks."; };

    orphanGraceSeconds = mkOption {
      type = types.nullOr types.ints.unsigned;
      default = null;
      description = "How long an unreferenced Blob is kept before GC may delete it. Applies only when quotaBytes is set: GC, including the orphan sweep, runs only with a quota.";
    };

    limits = {
      maxConcurrentUploads = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Past this, uploads are shed with 429."; };
      maxInFlightBytes = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Cap on buffered upload bytes."; };
      partSize = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Multipart part size; also the single-PutObject threshold."; };
      maxPartsInFlight = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Concurrent parts per upload."; };
      bodyIdleTimeoutSeconds = mkOption { type = types.nullOr types.ints.positive; default = null; description = "Seconds an upload body may send nothing before it is abandoned."; };
    };

    oidc = mkOption {
      type = types.listOf (types.submodule {
        options = {
          issuer = mkOption { type = types.str; description = "Issuer URL."; };
          audience = mkOption { type = types.str; description = "RFC 8707 audience identifying garret."; };
          extra_audiences = mkOption { type = types.listOf types.str; default = [ ]; description = "Further accepted `aud` values (e.g. a machine client's own client id); discovery still advertises only `audience`."; };
          client_id = mkOption {
            type = types.nullOr types.str;
            default = null;
            description = ''
              Public client id for `garret login`'s device flow. Set it on the
              human issuer only — never on the GitHub Actions issuer, which
              needs no client id. Discovery advertises the first issuer that
              sets one, so exactly one should.
            '';
          };
          jwks_url = mkOption { type = types.nullOr types.str; default = null; description = "Skips discovery when set."; };
          github_owner_id = mkOption { type = types.nullOr types.str; default = null; description = "GitHub: immutable owner id."; };
          ref_patterns = mkOption { type = types.listOf types.str; default = [ ]; description = "GitHub: allowed refs."; };
          ref_protected = mkOption { type = types.nullOr types.bool; default = null; description = "GitHub: require protected or unprotected refs."; };
          repository_ids = mkOption { type = types.listOf types.str; default = [ ]; description = "GitHub: allowed immutable repository ids."; };
          event_names = mkOption { type = types.listOf types.str; default = [ ]; description = "GitHub: allowed triggering events, e.g. push."; };
          job_workflow_refs = mkOption { type = types.listOf types.str; default = [ ]; description = "GitHub: allowed job_workflow_ref values (trailing-* globs)."; };
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
      description = ''
        Socket for garret-admin, created mode 0600 and owned by the `garret`
        user: root and the Pusher itself can connect, the Puller (its own user)
        cannot. The file mode is the whole authorization. Null serves no admin
        socket.
      '';
    };

    fsck = {
      calendar = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "weekly";
        description = ''
          systemd `OnCalendar` expression for a scheduled `garret-admin fsck`.
          Null (the default) defines no unit and no timer. The run is a
          read-only dry run: it reports drift, never repairs. Its findings
          reach the `garret_fsck_findings` gauge, and the unit fails (exit 1)
          when it finds dangling rows or size mismatches. Needs
          `adminSocketPath` and `quotaBytes` (fsck reads the orphan grace
          from the `[gc]` section and refuses without it).

          One run lists the whole bucket (one S3 `ListObjectsV2` request per
          1000 blobs, no per-object requests) and reads every Object row into
          memory, so size the schedule to the cache, not to the clock.
        '';
      };
      extraArgs = mkOption {
        type = types.listOf types.str;
        default = [ ];
        example = [ "--verify-sizes" ];
        description = ''
          Extra arguments for `garret-admin fsck`. `--verify-sizes` costs
          nothing more, because blob sizes come from the same listing. Do not
          add `--repair`: repair needs a decision per run, see the restore
          steps in the packaging spec.
        '';
      };
    };
  };

  config = mkIf cfg.enable (lib.mkMerge [ (common.serviceUser "garret") {
    environment.systemPackages = [ self.packages.${pkgs.stdenv.hostPlatform.system}.garret-admin ];

    assertions = [{
      assertion = cfg.oidc != [ ];
      message = "services.garret.pusher.oidc must list at least one issuer.";
    }
    {
      assertion = cfg.fsck.calendar == null || cfg.adminSocketPath != null;
      message = "services.garret.pusher.fsck.calendar needs adminSocketPath: fsck runs on the admin socket.";
    }
    {
      assertion = cfg.fsck.calendar == null || cfg.quotaBytes != null;
      message = "services.garret.pusher.fsck.calendar needs quotaBytes: fsck reads the orphan grace from the [gc] section and refuses without it.";
    }
    {
      assertion = cfg.quotaBytes != null
        || lib.all (v: v == null) [ cfg.watermarks.high cfg.watermarks.low cfg.gcIntervalSeconds cfg.orphanGraceSeconds ];
      message = "services.garret.pusher: watermarks, gcIntervalSeconds and orphanGraceSeconds need quotaBytes: GC runs only with a quota.";
    }];

    systemd.services.garret-pusher = {
      description = "garret Pusher";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      # Upload part buffers are freed on whichever thread finishes the part;
      # glibc's per-thread arenas then keep that memory. Two arenas cut peak
      # RSS by about a third with no measured throughput cost. Pusher only:
      # the same setting lowered the Puller's request rate.
      environment.MALLOC_ARENA_MAX = "2";

      serviceConfig = common.daemonServiceConfig // {
        ExecStart = "${cfg.package}/bin/garret-pusher ${configFile}";
        EnvironmentFile = cfg.s3.credentialsFile;
        StateDirectory = "garret";
        # Group `garret` (the Puller) may reach the database, never create
        # files beside it.
        StateDirectoryMode = "0750";
        RuntimeDirectory = "garret";
        # After any ExecStartPre a deployment adds (a mount guard creating the
        # directory, say): this one needs the directory to exist and be ours.
        ExecStartPre = lib.mkAfter [ "${dbPerms}" ];
        # The Pusher reads signing keys and writes only its own state.
        DynamicUser = false;
        User = "garret";
        Group = "garret";
        ReadWritePaths = [ (builtins.dirOf cfg.dbPath) ];
      };
    };

    systemd.services.garret-fsck = mkIf (cfg.fsck.calendar != null) {
      description = "garret fsck (read-only audit)";
      after = [ "garret-pusher.service" ];
      requires = [ "garret-pusher.service" ];
      serviceConfig = import ./sandbox.nix // {
        Type = "oneshot";
        ExecStart = lib.escapeShellArgs ([
          "${self.packages.${pkgs.stdenv.hostPlatform.system}.garret-admin}/bin/garret-admin"
          "--socket"
          cfg.adminSocketPath
          "fsck"
        ] ++ cfg.fsck.extraArgs);
        User = "garret";
        Group = "garret";
        # Only the admin socket: the Pusher does the S3 and DB work.
        RestrictAddressFamilies = [ "AF_UNIX" ];
      };
    };

    systemd.timers.garret-fsck = mkIf (cfg.fsck.calendar != null) {
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.fsck.calendar;
        Persistent = true;
        RandomizedDelaySec = "15m";
      };
    };
  } ]);
}
