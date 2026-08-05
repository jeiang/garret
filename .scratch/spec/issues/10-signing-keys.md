# Narinfo signing and key management

Type: grilling
Status: open

## Question

Design the ed25519 cache-signing story: key generation and storage (file on
host? admin CLI command?), rotation (nix supports multiple
trusted-public-keys — is rotation a supported flow?), whether narinfos are
signed at write time (stored) or at serve time (computed, cacheable — see
`OPTIMIZATIONS.md` item 8), and which admin CLI operations key management
needs.
