# NixOS VM test for the service modules (spec 10-packaging): the Pusher and
# the Puller on one host, as separate users sharing the database (ADR-0009).
# Neither service needs S3 or an OIDC issuer to start, so neither exists here
# and nothing is pushed: what is under test is the host around the binaries.
self:
{ pkgs, ... }:

let
  packages = self.packages.${pkgs.stdenv.hostPlatform.system};

  # Throwaway, trusted nowhere.
  signingKey = pkgs.runCommand "garret-test-signing-key" { } ''
    ${packages.garret-admin}/bin/garret-admin key generate garret-test-1 $out
  '';

  s3 = {
    bucket = "garret";
    endpointUrl = "http://127.0.0.1:1";
    region = "garage";
    credentialsFile = "/etc/garret/s3.env";
  };

  db = "/var/lib/garret/garret.db";
  # A write lock: the Puller's last-accessed bumps take one.
  writeLock = "${pkgs.sqlite}/bin/sqlite3 ${db} 'BEGIN IMMEDIATE; COMMIT;'";
in
{
  name = "garret-module";

  nodes.machine = {
    imports = [ self.nixosModules.pusher self.nixosModules.puller ];

    environment.systemPackages = [ packages.garret-admin pkgs.curl ];
    environment.etc = {
      "garret/signing-key" = {
        source = signingKey;
        mode = "0400";
        user = "garret";
        group = "garret";
      };
      "garret/s3.env".text = ''
        AWS_ACCESS_KEY_ID=test
        AWS_SECRET_ACCESS_KEY=test
      '';
    };

    services.garret = {
      pusher = {
        enable = true;
        inherit s3;
        signingKeyFiles = [ "/etc/garret/signing-key" ];
        oidc = [{ issuer = "https://issuer.invalid"; audience = "garret"; }];
      };
      puller = {
        enable = true;
        inherit s3;
      };
    };
  };

  testScript = ''
    def as_puller(cmd):
        return f"runuser -u garret-puller -- {cmd}"

    machine.wait_for_unit("garret-pusher.service")
    # /ready answers once the Puller has opened the database, which it can
    # only do through the group.
    machine.wait_until_succeeds("curl -fsS http://127.0.0.1:8081/ready")

    with subtest("the Puller runs as its own user and can write the database"):
        machine.succeed('test "$(systemctl show -P User garret-puller)" = garret-puller')
        machine.succeed(as_puller("${writeLock}"))
        machine.fail(as_puller("touch /var/lib/garret/planted"))

    with subtest("the Puller reaches neither the signing key nor the admin socket"):
        machine.fail(as_puller("cat /etc/garret/signing-key"))
        machine.fail(as_puller("garret-admin status"))
        machine.succeed("garret-admin status")

    with subtest("files left by a module without the split are repaired on start"):
        machine.succeed("systemctl stop garret-puller garret-pusher")
        machine.succeed("chmod 0755 /var/lib/garret && chmod 0644 ${db}*")
        machine.succeed("systemctl start garret-pusher garret-puller")
        machine.wait_until_succeeds("curl -fsS http://127.0.0.1:8081/ready")
        machine.succeed(as_puller("${writeLock}"))
        machine.succeed('test "$(stat -c %a /var/lib/garret)" = 750')
  '';
}
