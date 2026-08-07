//! `garret use` — pointing the local Nix at the cache (spec 06-client).
//!
//! Writes the *user's* `nix.conf` only. Never `/etc/nix/nix.conf`, never via
//! sudo: a tool that edits system configuration behind a `use` subcommand is a
//! tool that fights NixOS. `--print` covers the declarative case.

use std::path::PathBuf;

use anyhow::{Context, Result};

pub fn path() -> Result<PathBuf> {
    Ok(crate::config::config_home()?.join("nix").join("nix.conf"))
}

/// The two lines Nix needs: where to fetch from, and whose signatures to trust.
///
/// `extra-` prefixes, so this appends to whatever is already configured rather
/// than replacing cache.nixos.org.
pub fn lines(puller: &str, public_keys: &[String]) -> String {
    format!(
        "extra-substituters = {puller}\nextra-trusted-public-keys = {}\n",
        public_keys.join(" ")
    )
}

/// The equivalent for a NixOS host, where editing a user file is the wrong
/// answer — the system nix.conf is generated and would overwrite it.
pub fn nixos_snippet(puller: &str, public_keys: &[String]) -> String {
    let keys: Vec<String> = public_keys.iter().map(|k| format!("\"{k}\"")).collect();
    format!(
        "nix.settings = {{\n  substituters = [ \"{puller}\" ];\n  trusted-public-keys = [ {} ];\n}};\n",
        keys.join(" ")
    )
}

pub enum Outcome {
    Added(PathBuf),
    AlreadyPresent(PathBuf),
}

/// Appends the substituter to the user's nix.conf unless it is already named
/// there.
///
/// Idempotence is a substring match on the URL rather than a nix.conf parse.
/// The cost is that hand-editing the line to a *different* URL and re-running
/// appends a duplicate; the benefit is not carrying a parser for a format with
/// includes, `extra-` merging and per-file precedence.
pub fn apply(puller: &str, public_keys: &[String]) -> Result<Outcome> {
    let path = path()?;
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    if existing.contains(puller) {
        return Ok(Outcome::AlreadyPresent(path));
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut out = existing;
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&lines(puller, public_keys));
    std::fs::write(&path, out).with_context(|| format!("writing {}", path.display()))?;
    Ok(Outcome::Added(path))
}

/// Asks Nix whether it actually honoured the file.
///
/// Substituters in a *user's* nix.conf are silently ignored unless that user is
/// in `trusted-users` — the classic failure here, and the reason comparable
/// tools carry a whole matrix of `--mode` flags. One subprocess replaces all of
/// that: if Nix does not list the URL back, the write did not take effect.
///
/// Returns `None` when nix could not be asked at all, which is not a failure of
/// `use` — the file is still written and a later nix will read it.
pub fn honoured(puller: &str) -> Option<bool> {
    let output = std::process::Command::new("nix")
        .args(["config", "show", "substituters"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).contains(puller))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Vec<String> {
        vec!["garret-1:AAAA".into(), "garret-2:BBBB".into()]
    }

    /// Both keys must land on one space-separated line: nix.conf has no list
    /// syntax, and a second `extra-trusted-public-keys` line would win outright
    /// rather than merging, silently dropping the first key.
    #[test]
    fn every_key_lands_on_one_line() {
        let text = lines("https://cache.example", &keys());
        assert!(text.contains("extra-substituters = https://cache.example\n"));
        assert!(text.contains("extra-trusted-public-keys = garret-1:AAAA garret-2:BBBB\n"));
        assert_eq!(text.lines().count(), 2);
    }

    #[test]
    fn the_nixos_snippet_quotes_every_key() {
        let text = nixos_snippet("https://cache.example", &keys());
        assert!(text.contains("substituters = [ \"https://cache.example\" ];"));
        assert!(text.contains("trusted-public-keys = [ \"garret-1:AAAA\" \"garret-2:BBBB\" ];"));
    }
}
