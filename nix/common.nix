# What the garret modules share (spec 10-packaging): config generation, the
# server daemons' systemd settings, and the service users.
{ lib, pkgs }:

let
  toml = pkgs.formats.toml { };

  # `pkgs.formats.toml` cannot represent null ("unsupported unit type"), and
  # every unset nullable option reaches the settings as null. Dropping the key
  # lets serde apply the Rust default, which is the only place defaults live.
  stripNulls = value:
    if lib.isAttrs value
    then lib.mapAttrs (_: stripNulls) (lib.filterAttrs (_: v: v != null) value)
    else if lib.isList value
    then map stripNulls value
    else value;
in
{
  extraSettings = lib.mkOption {
    type = toml.type;
    default = { };
    description = ''
      Raw TOML settings merged over the generated config (recursively), for
      keys that have no typed option here.
    '';
  };

  # The generated config holds no secrets: those come from files or an
  # EnvironmentFile (agenix/sops-friendly).
  configFile = name: settings: extraSettings:
    toml.generate name (lib.recursiveUpdate (stripNulls settings) extraSettings);

  # The sandbox plus what only the server daemons add.
  daemonServiceConfig = import ./sandbox.nix // {
    Restart = "on-failure";
    ProtectProc = "invisible";
    ProcSubset = "pid";
    PrivateIPC = true;
    RemoveIPC = true;
    # The binary signals READY=1 once every listener is bound.
    Type = "notify";
  };

  # A server daemon's system user, in group `garret` (ADR-0009).
  serviceUser = name: {
    users.users.${name} = {
      isSystemUser = true;
      group = "garret";
    };
    users.groups.garret = { };
  };
}
