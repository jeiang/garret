//! The HTTP contract between the Pusher/Puller and their clients: routes,
//! response bodies and the substituter's file names (specs 01, 06, 07).

use serde::{Deserialize, Serialize};

/// Pusher: `POST` a JSON array of store-path hashes, get back the missing ones.
pub const MISSING_PATHS: &str = "/api/v1/missing-paths";
/// Pusher: `PUT` one framed preamble plus zstd NAR; answers an [`UploadAck`].
pub const NAR_UPLOAD: &str = "/api/v1/nar/{hash}";
/// Pusher: the anonymous [`Discovery`] document.
pub const DISCOVERY: &str = "/api/v1/discovery";
/// Puller: a [`Page`] of objects.
pub const OBJECTS: &str = "/api/v1/objects";
/// Puller: one object's [`ObjectDetail`].
pub const OBJECT: &str = "/api/v1/objects/{hash}";
/// Puller: one object's dependency [`TreeNode`].
pub const OBJECT_TREE: &str = "/api/v1/objects/{hash}/tree";
/// Puller: the [`Summary`] of every object referring to one.
pub const OBJECT_REFERRERS: &str = "/api/v1/objects/{hash}/referrers";
/// Puller: every [`Pin`].
pub const PINS: &str = "/api/v1/pins";
/// Puller: the Nix binary cache's store description.
pub const NIX_CACHE_INFO: &str = "/nix-cache-info";
/// Puller: a NAR named by [`nar_url`], parsed back by [`nar_file_hash`].
pub const NAR_FILE: &str = "/nar/{file}";

/// Page size when a listing names none (spec 07).
pub const DEFAULT_LIMIT: usize = 50;
/// Largest page a listing returns; larger requests are clamped to it (spec 07).
pub const MAX_LIMIT: usize = 500;

/// A parameterised route with `hash` filled in.
pub fn with_hash(route: &str, hash: &str) -> String {
    route.replace("{hash}", hash)
}

const NARINFO_SUFFIX: &str = ".narinfo";
/// The suffix of a NAR's blob key, and of the narinfo `URL:` that names it.
pub const NAR_SUFFIX: &str = ".nar.zst";

/// `<hash>.narinfo`: the narinfo's file name at the cache root.
pub fn narinfo_file(hash: &str) -> String {
    format!("{hash}{NARINFO_SUFFIX}")
}

/// The hash a [`narinfo_file`] name encodes.
pub fn narinfo_hash(file: &str) -> Option<&str> {
    file.strip_suffix(NARINFO_SUFFIX)
}

/// `nar/<hash>.nar.zst`: the narinfo `URL:`, relative to the cache root.
pub fn nar_url(hash: &str) -> String {
    format!("nar/{hash}{NAR_SUFFIX}")
}

/// The hash in the `{file}` segment of [`NAR_FILE`].
pub fn nar_file_hash(file: &str) -> Option<&str> {
    file.strip_suffix(NAR_SUFFIX)
}

/// The Pusher's answer to a successful upload `PUT` (spec 01).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum UploadAck {
    /// This upload created the object.
    Created,
    /// The cache already held the object; the body was not needed.
    Exists,
    /// Another upload of the same path is running; ask again later.
    InProgress,
}

/// The Pusher's anonymous discovery document (spec 06): everything
/// `garret login` needs to write a whole client config from one URL.
/// Fields are in the order the document has always been serialised.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Discovery {
    /// Absent when no issuer sets a `client_id`, in which case the device flow
    /// has nothing to identify itself as.
    pub oidc: Option<Oidc>,
    /// Public halves of the cache's signing keys — plural, because every object
    /// is signed with every configured key and a rotation runs several at once.
    #[serde(default)]
    pub public_keys: Vec<String>,
    /// Absent when the Pusher has no `puller_endpoint` configured.
    pub puller_endpoint: Option<String>,
}

/// The advertised OIDC settings, destined for the client config's `[oidc]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Oidc {
    /// The `aud` claim the Pusher validates tokens against.
    pub audience: String,
    /// The device flow's public client id.
    pub client_id: Option<String>,
    /// Issuer base URL.
    pub issuer: String,
}

/// One object in a listing: the fields the browse UI shows, nothing heavier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    /// Store path hash — the object key.
    pub hash: String,
    /// Basename after the hash (`hello-1.0`).
    pub name: String,
    /// Full store path.
    pub store_path: String,
    /// Uncompressed NAR size in bytes.
    pub nar_size: i64,
    /// Stored (compressed) blob size in bytes.
    pub file_size: i64,
    /// Unix time the object was pushed.
    pub created_at: i64,
}

/// One object's detail, as [`OBJECT`] reports it: the narinfo fields plus
/// who pushed it. Fields are in the order the document has always been
/// serialised.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectDetail {
    /// Store path hash — the object key.
    pub store_path_hash: String,
    /// Full store path.
    pub store_path: String,
    /// Basename after the hash (`hello-1.0`).
    pub name: String,
    /// Uncompressed NAR hash, `sha256:<nix-base32>`.
    pub nar_hash: String,
    /// Uncompressed NAR size in bytes.
    pub nar_size: i64,
    /// Stored (compressed) blob hash.
    pub file_hash: String,
    /// Stored (compressed) blob size in bytes.
    pub file_size: i64,
    /// Deriving `.drv` store path, if known.
    pub deriver: Option<String>,
    /// Content-address string for CA paths.
    pub ca: Option<String>,
    /// Reference basenames (`<hash>-<name>`).
    pub references: Vec<String>,
    /// Signatures (`<key-name>:<base64>`).
    pub sigs: Vec<String>,
    /// OIDC subject that pushed the object.
    pub pushed_by: Option<String>,
}

/// One page of listing results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    /// The page's objects, newest first.
    pub objects: Vec<Summary>,
    /// Opaque; pass back as `cursor` for the next page. Absent means the end.
    pub next_cursor: Option<String>,
}

/// One node of a dependency tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeNode {
    /// Store path hash of this reference.
    pub hash: String,
    /// Reference basename for children; the object's `name` at the root.
    pub name: String,
    /// Absent from the cache: shown, but marked (spec 07).
    pub missing: bool,
    /// Already expanded elsewhere in this tree; children are omitted.
    pub truncated: bool,
    /// This node's references, empty when missing or truncated.
    pub children: Vec<TreeNode>,
}

/// One GC-exempt root, as [`PINS`] reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pin {
    /// Operator-chosen pin name.
    pub name: String,
    /// Store path hash of the pinned root.
    pub store_path_hash: String,
    /// Full store path of the pinned root.
    pub store_path: String,
    /// Unix time the pin stops protecting; `None` = permanent.
    pub expires_at: Option<i64>,
    /// Unix time the pin was created.
    pub created_at: i64,
}
