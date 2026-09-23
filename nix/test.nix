# NixOS VM test for the service modules (spec 10-packaging). `cache` runs the
# Pusher and the Puller as separate users sharing the database (ADR-0009),
# over a local Garage; `builder` runs the store watcher. A path goes all the
# way round, under every unit's systemd sandbox: the watcher pushes it through
# the nix daemon, the Pusher stores it, the Puller serves it back.
self:
{ pkgs, ... }:

let
  packages = self.packages.${pkgs.stdenv.hostPlatform.system};

  # Throwaway, trusted nowhere.
  signingKey = pkgs.runCommand "garret-test-signing-key" { } ''
    ${packages.garret-admin}/bin/garret-admin key generate garret-test-1 $out
  '';

  # The stand-in OIDC issuer (on `builder`, where the watcher asks it for a
  # token): a JWKS for the Pusher, and one long-lived token it signed that
  # every token request gets.
  issuerUrl = "http://127.0.0.1:8090";
  oidc = pkgs.runCommand "garret-test-oidc"
    { nativeBuildInputs = [ (pkgs.python3.withPackages (ps: [ ps.pyjwt ps.cryptography ])) ]; }
    ''
      mkdir $out
      python3 - <<'EOF'
      import json, os, jwt
      from cryptography.hazmat.primitives.asymmetric import rsa
      out = os.environ["out"]
      key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
      jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
      jwk.update({"kid": "test-1", "use": "sig", "alg": "RS256"})
      json.dump({"keys": [jwk]}, open(f"{out}/jwks.json", "w"))
      claims = {"iss": "${issuerUrl}", "aud": "garret", "sub": "client-builder", "exp": 4102444800}
      open(f"{out}/token", "w").write(jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test-1"}))
      EOF
    '';
  issuer = pkgs.writeText "garret-test-issuer.py" ''
    import json
    from http.server import BaseHTTPRequestHandler, HTTPServer

    TOKEN = open("${oidc}/token").read()

    class Issuer(BaseHTTPRequestHandler):
        def reply(self, value):
            body = json.dumps(value).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # discovery
            self.reply({
                "token_endpoint": "${issuerUrl}/token",
                "device_authorization_endpoint": "${issuerUrl}/device",
            })

        def do_POST(self):  # the token endpoint, whatever the grant
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            self.reply({"access_token": TOKEN, "token_type": "Bearer"})

    HTTPServer(("127.0.0.1", 8090), Issuer).serve_forever()
  '';

  # A fixed Garage key, imported rather than created, so the credentials file
  # can be static.
  s3KeyId = "GK0123456789abcdef01234567";
  s3Secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  s3 = {
    bucket = "garret";
    endpointUrl = "http://127.0.0.1:3900";
    region = "garage";
    credentialsFile = "/etc/garret/s3.env";
  };

  db = "/var/lib/garret/garret.db";
  # A write lock: the Puller's last-accessed bumps take one.
  writeLock = "${pkgs.sqlite}/bin/sqlite3 ${db} 'BEGIN IMMEDIATE; COMMIT;'";
in
{
  name = "garret-module";

  nodes.cache = {
    imports = [ self.nixosModules.pusher self.nixosModules.puller ];

    virtualisation.memorySize = 2048;
    networking.firewall.allowedTCPPorts = [ 8080 ];
    environment.systemPackages = [ packages.garret-admin pkgs.curl ];
    environment.etc = {
      "garret/signing-key" = {
        source = signingKey;
        mode = "0400";
        user = "garret";
        group = "garret";
      };
      "garret/s3.env".text = ''
        AWS_ACCESS_KEY_ID=${s3KeyId}
        AWS_SECRET_ACCESS_KEY=${s3Secret}
      '';
    };

    services.garage = {
      enable = true;
      package = pkgs.garage;
      settings = {
        replication_factor = 1;
        db_engine = "sqlite";
        rpc_bind_addr = "127.0.0.1:3901";
        rpc_public_addr = "127.0.0.1:3901";
        rpc_secret = "0000000000000000000000000000000000000000000000000000000000000000";
        s3_api = {
          s3_region = "garage";
          api_bind_addr = "127.0.0.1:3900";
        };
      };
    };

    services.garret = {
      pusher = {
        enable = true;
        listen = "0.0.0.0:8080";
        inherit s3;
        signingKeyFiles = [ "/etc/garret/signing-key" ];
        oidc = [{ issuer = issuerUrl; audience = "garret"; jwks_url = "${oidc}/jwks.json"; }];
      };
      puller = {
        enable = true;
        inherit s3;
      };
    };
  };

  nodes.builder = {
    imports = [ self.nixosModules.watcher ];

    environment.etc."garret/watcher-credentials" = {
      text = "builder:secret";
      mode = "0400";
    };
    systemd.services.garret-test-issuer = {
      wantedBy = [ "multi-user.target" ];
      before = [ "garret-watcher.service" ];
      serviceConfig.ExecStart = "${pkgs.python3}/bin/python3 ${issuer}";
    };

    services.garret.watcher = {
      enable = true;
      endpoint = "http://cache:8080";
      oidc = {
        issuer = issuerUrl;
        clientId = "builder";
        audience = "garret";
      };
      credentialsFile = "/etc/garret/watcher-credentials";
      # An hour: only the wake socket can get the push below done in time.
      pollIntervalSeconds = 3600;
    };
  };

  testScript = ''
    def as_puller(cmd):
        return f"runuser -u garret-puller -- {cmd}"

    start_all()

    cache.wait_for_unit("garage.service")
    cache.wait_until_succeeds("garage status")
    node = cache.succeed("garage node id -q | cut -d@ -f1").strip()
    cache.succeed(f"garage layout assign -z dc1 -c 1G {node}")
    cache.succeed("garage layout apply --version 1")
    cache.succeed("garage bucket create garret")
    cache.succeed("garage key import --yes -n garret ${s3KeyId} ${s3Secret}")
    cache.succeed("garage bucket allow --read --write garret --key garret")

    cache.wait_for_unit("garret-pusher.service")
    # /ready answers once the Puller has opened the database, which it can
    # only do through the group.
    cache.wait_until_succeeds("curl -fsS http://127.0.0.1:8081/ready")

    with subtest("the Puller runs as its own user and can write the database"):
        cache.succeed('test "$(systemctl show -P User garret-puller)" = garret-puller')
        cache.succeed(as_puller("${writeLock}"))
        cache.fail(as_puller("touch /var/lib/garret/planted"))

    with subtest("the Puller reaches neither the signing key nor the admin socket"):
        cache.wait_for_file("/run/garret/admin.sock")
        cache.fail(as_puller("cat /etc/garret/signing-key"))
        cache.fail(as_puller("garret-admin status"))
        cache.succeed("garret-admin status")

    with subtest("the watcher pushes a new path and the Puller serves it back"):
        builder.wait_for_unit("garret-watcher.service")
        builder.wait_until_succeeds("test -S /run/garret/watch.sock")
        path = builder.succeed(
            "head -c 65536 /dev/urandom > /tmp/blob && nix-store --add /tmp/blob"
        ).strip()
        builder.succeed(f"${packages.garret}/bin/garret enqueue --socket /run/garret/watch.sock {path}")
        key = cache.succeed("garret-admin key show /etc/garret/signing-key").strip()
        cache.wait_until_succeeds(
            "nix --extra-experimental-features nix-command store verify"
            f" --store http://127.0.0.1:8081 --option trusted-public-keys '{key}' {path}",
            timeout=120,
        )

    with subtest("an online backup lands beside the database, owner-only"):
        cache.succeed("garret-admin backup /var/lib/garret/backup.db")
        cache.succeed('test "$(stat -c %U:%a /var/lib/garret/backup.db)" = garret:600')

    with subtest("the Puller starts while the Pusher cannot"):
        cache.succeed("systemctl stop garret-puller garret-pusher")
        # The Pusher opens the database, then fails on a key it cannot read;
        # its clean close must leave the -wal and -shm the Puller cannot make.
        cache.succeed("chown root /etc/garret/signing-key")
        cache.succeed("systemctl start garret-pusher")
        cache.wait_until_succeeds("systemctl is-failed garret-pusher")
        cache.succeed("systemctl start garret-puller")
        cache.wait_until_succeeds("curl -fsS http://127.0.0.1:8081/ready", timeout=60)
        cache.succeed("chown garret /etc/garret/signing-key")
        cache.succeed("systemctl reset-failed garret-pusher")
        cache.succeed("systemctl start garret-pusher")

    with subtest("files left by a module without the split are repaired on start"):
        cache.succeed("systemctl stop garret-puller garret-pusher")
        cache.succeed("chmod 0755 /var/lib/garret && chmod 0644 ${db}*")
        cache.succeed("systemctl start garret-pusher garret-puller")
        cache.wait_until_succeeds("curl -fsS http://127.0.0.1:8081/ready")
        cache.succeed(as_puller("${writeLock}"))
        cache.succeed('test "$(stat -c %a /var/lib/garret)" = 750')
  '';
}
