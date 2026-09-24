# The systemd sandbox every garret unit runs in (spec 10-packaging). Each unit
# adds only what is its own: user, writable paths, runtime directory.
#
# The watcher runs as root but needs no capability either: everything it
# touches (the nix database, its credentials, cursor and wake socket, the nix
# daemon's socket) is root-owned or world-accessible.
{
  CapabilityBoundingSet = "";
  NoNewPrivileges = true;
  ProtectSystem = "strict";
  ProtectHome = true;
  PrivateTmp = true;
  PrivateDevices = true;
  ProtectKernelTunables = true;
  ProtectKernelModules = true;
  ProtectKernelLogs = true;
  ProtectControlGroups = true;
  ProtectClock = true;
  ProtectHostname = true;
  LockPersonality = true;
  RestrictNamespaces = true;
  RestrictRealtime = true;
  RestrictSUIDSGID = true;
  # Nothing here generates code at runtime, nor does the nix the watcher runs.
  MemoryDenyWriteExecute = true;
  SystemCallArchitectures = "native";
  SystemCallFilter = [ "@system-service" "~@privileged" ];
  # A filtered call fails rather than killing the process: an error is logged
  # where a SIGSYS would only leave a core.
  SystemCallErrorNumber = "EPERM";
  # TCP for the listeners, S3 and OIDC; unix for the admin, wake, nscd and nix
  # daemon sockets.
  RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
  UMask = "0077";
}
