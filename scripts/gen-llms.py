#!/usr/bin/env python3
"""Build the agent artifacts for the documentation book.

Reads SUMMARY.md for page order, validates every relative link in the book, and
writes llms.txt (an index) and llms-full.txt (the whole book in one file).

Usage: gen-llms.py <src-dir> <out-dir>
"""

import os
import re
import sys

BASE_URL = "https://garret.docs.jeiang.dev"
TAGLINE = (
    "A single-tenant Nix binary cache: OIDC-protected push, standard "
    "substituter pull, SQLite + S3."
)

ENTRY = re.compile(r"^\s*(?:[-*]\s*)?\[(?P<title>[^\]]+)\]\((?P<path>[^)]+\.md)\)\s*$")
LINK = re.compile(r"\]\((?P<target>[^)]+)\)")
SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)


def read_summary(src):
    path = os.path.join(src, "SUMMARY.md")
    if not os.path.exists(path):
        fail(f"missing {path}")
    entries = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            match = ENTRY.match(line)
            if not match:
                continue
            target = match.group("path")
            if not os.path.exists(os.path.join(src, target)):
                fail(f"missing SUMMARY entry: {target}")
            entries.append((match.group("title"), target))
    if not entries:
        fail(f"no entries in {path}")
    return entries


def check_links(src, out):
    out_root = os.path.abspath(out)
    for root, dirs, files in os.walk(src):
        dirs[:] = [
            d for d in dirs if os.path.abspath(os.path.join(root, d)) != out_root
        ]
        for name in sorted(files):
            if not name.endswith(".md"):
                continue
            page = os.path.join(root, name)
            with open(page, encoding="utf-8") as handle:
                lines = handle.readlines()
            fenced = False
            for number, line in enumerate(lines, start=1):
                if line.lstrip().startswith("```"):
                    fenced = not fenced
                    continue
                if fenced:
                    continue
                for match in LINK.finditer(line):
                    target = match.group("target").strip()
                    if not target or target.startswith("#") or SCHEME.match(target):
                        continue
                    target = target.split("#", 1)[0].split("?", 1)[0]
                    if not target:
                        continue
                    resolved = os.path.normpath(os.path.join(root, target))
                    if not os.path.exists(resolved):
                        fail(f"{page}:{number}: broken link {match.group('target')}")


def page_url(target):
    if target == "index.md":
        return BASE_URL + "/"
    return BASE_URL + "/" + target[: -len(".md")] + ".html"


def describe(text):
    fenced = False
    paragraph = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            fenced = not fenced
            continue
        if fenced or stripped.startswith("#"):
            continue
        if not stripped:
            if paragraph:
                break
            continue
        paragraph.append(stripped)
    if not paragraph:
        return ""
    joined = " ".join(paragraph)
    joined = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", joined)
    joined = re.sub(r"[*_`]", "", joined)
    joined = re.sub(r"\s+", " ", joined).strip()
    if len(joined) > 140:
        joined = joined[:137].rstrip() + "..."
    return joined


def strip_heading(text):
    lines = text.splitlines()
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
        while lines and not lines[0].strip():
            lines = lines[1:]
    return "\n".join(lines).strip()


def main(argv):
    if len(argv) != 3:
        print("usage: gen-llms.py <src-dir> <out-dir>", file=sys.stderr)
        return 2
    src, out = argv[1], argv[2]
    if not os.path.isdir(src):
        fail(f"not a directory: {src}")

    entries = read_summary(src)
    check_links(src, out)

    os.makedirs(out, exist_ok=True)
    pages = []
    for title, target in entries:
        with open(os.path.join(src, target), encoding="utf-8") as handle:
            pages.append((title, target, handle.read()))

    with open(os.path.join(out, "llms.txt"), "w", encoding="utf-8") as handle:
        handle.write("# garret\n\n")
        handle.write(f"> {TAGLINE}\n\n")
        handle.write(f"Whole book in one file: {BASE_URL}/llms-full.txt\n\n")
        handle.write("## Summary\n\n")
        for title, target, text in pages:
            description = describe(text)
            line = f"- [{title}]({page_url(target)})"
            if description:
                line += f": {description}"
            handle.write(line + "\n")

    with open(os.path.join(out, "llms-full.txt"), "w", encoding="utf-8") as handle:
        for title, target, text in pages:
            handle.write(f"# {title}\n\n")
            handle.write(f"Source: {page_url(target)}\n\n")
            handle.write(strip_heading(text))
            handle.write("\n\n---\n\n")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
