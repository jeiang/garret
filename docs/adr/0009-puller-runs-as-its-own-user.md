# The Puller runs as its own user, sharing the database through a group

[ADR-0001](0001-same-host-split-services-sqlite.md) split garret into a
Pusher and a Puller for attack surface, but both units ran as one `garret`
user, so at the OS level the split bought nothing: the internet-facing
Puller could read the signing keys (forging narinfo signatures offline),
connect to the admin socket (whose 0600 mode is its only authorization) and
use the Pusher's bucket-write S3 key. The Puller now runs as
`garret-puller`, in group `garret` only to share the SQLite file. Its
database access cannot be read-only — last-accessed bumps are writes, and a
WAL reader writes the `-shm` file — so the database and its `-wal`/`-shm`
are 0660 while the directory stays 0750: the Puller writes those three files
but cannot create or replace anything beside them, the sidecars included.
So they must always exist once the Pusher has opened the database: every
connection sets SQLite's persistent-WAL mode, and a last close (a Pusher
exiting with an error, say) leaves them in place rather than stranding the
Puller until the Pusher recovers. SQLite creates a database 0644 and gives
`-wal`/`-shm` the database's mode, so the Pusher's unit creates the file
0660 before SQLite first opens it, and re-applies 0660 on every start, which
is also the upgrade path for older deployments. That step runs as `garret`,
not root, so nothing in the directory can turn it against a file the Pusher
does not already own. Signing keys stay owner-only (0400 `garret`) and the
admin socket stays 0600; both are now out of the Puller's reach, and a key
made group-readable would put it back. The Puller takes its own S3
credentials, and needs only GetObject: a presigned URL carries exactly its
key's authority. A static user rather than `DynamicUser`, so the uid the file
modes are reasoned about is stable and nameable. Consequences: the Puller
can still rewrite rows, which only moving the bumps behind the Pusher would
close, and a database replaced or restored in place must lose its `-wal`
and `-shm` too, or the next open replays the stale WAL over it.
