# Split Pusher/Puller services colocated on one host over shared SQLite

Garret is split into an OIDC-protected Pusher and a public Puller for
exposure and auth reasons, but both run on the same NixOS host sharing one
SQLite file (WAL) and one S3 bucket — the split is about
attack surface, not scaling. SQLite was chosen over Postgres because
single-tenant, single-host operation doesn't justify running a database
server, and the write patterns are designed around it: the Pusher owns all
writes, the Puller's only writes are >24h-debounced last-accessed bumps,
and upload-in-progress state never touches the DB (row exists ⇒ blob
exists). Moving the services to separate hosts would require revisiting
metadata sharing entirely (LiteFS/replication/S3-resident metadata were
considered and rejected as unneeded).
