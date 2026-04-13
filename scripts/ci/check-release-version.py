#!/usr/bin/env python3
"""Validate the workspace release version used by CI workflows."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path


def fail(message: str) -> None:
    print(f"::error::{message}", file=sys.stderr)
    raise SystemExit(1)


def load_workspace_version(text: str, source: str) -> str:
    try:
        return tomllib.loads(text)["workspace"]["package"]["version"]
    except (tomllib.TOMLDecodeError, KeyError, TypeError) as exc:
        fail(f"Could not read [workspace.package].version from {source}: {exc}")


def load_head_version() -> str:
    return load_workspace_version(Path("Cargo.toml").read_text(), "Cargo.toml")


def load_workspace_package_names() -> set[str]:
    try:
        cargo = tomllib.loads(Path("Cargo.toml").read_text())
    except (OSError, tomllib.TOMLDecodeError) as exc:
        fail(f"Could not read workspace members from Cargo.toml: {exc}")

    names: set[str] = set()
    for member in cargo.get("workspace", {}).get("members", []):
        manifest_path = Path(member) / "Cargo.toml"
        try:
            manifest = tomllib.loads(manifest_path.read_text())
            names.add(manifest["package"]["name"])
        except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as exc:
            fail(f"Could not read package name from {manifest_path}: {exc}")

    if not names:
        fail("No workspace package names found in Cargo.toml.")

    return names


def load_base_version(base_ref: str) -> str:
    try:
        text = subprocess.check_output(
            ["git", "show", f"{base_ref}:Cargo.toml"],
            text=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        fail(f"Could not read Cargo.toml at {base_ref}: {exc.stderr.strip()}")
    return load_workspace_version(text, f"{base_ref}:Cargo.toml")


def parse_semver(version: str) -> tuple[int, int, int, str | None]:
    match = re.fullmatch(
        r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
        r"(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?",
        version,
    )
    if not match:
        fail(f"Invalid Cargo SemVer version: {version}")
    major, minor, patch, prerelease = match.groups()
    return int(major), int(minor), int(patch), prerelease


def compare_prerelease(left: str | None, right: str | None) -> int:
    if left == right:
        return 0
    if left is None:
        return 1
    if right is None:
        return -1

    left_parts = left.split(".")
    right_parts = right.split(".")
    for left_part, right_part in zip(left_parts, right_parts):
        if left_part == right_part:
            continue

        left_numeric = left_part.isdigit()
        right_numeric = right_part.isdigit()
        if left_numeric and right_numeric:
            return 1 if int(left_part) > int(right_part) else -1
        if left_numeric:
            return -1
        if right_numeric:
            return 1
        return 1 if left_part > right_part else -1

    if len(left_parts) == len(right_parts):
        return 0
    return 1 if len(left_parts) > len(right_parts) else -1


def compare_semver(left: str, right: str) -> int:
    left_major, left_minor, left_patch, left_pre = parse_semver(left)
    right_major, right_minor, right_patch, right_pre = parse_semver(right)

    left_core = (left_major, left_minor, left_patch)
    right_core = (right_major, right_minor, right_patch)
    if left_core != right_core:
        return 1 if left_core > right_core else -1

    return compare_prerelease(left_pre, right_pre)


def verify_lockfile_version(version: str) -> None:
    try:
        lockfile = tomllib.loads(Path("Cargo.lock").read_text())
    except (OSError, tomllib.TOMLDecodeError) as exc:
        fail(f"Could not read Cargo.lock: {exc}")

    workspace_packages = load_workspace_package_names()
    mismatches: list[str] = []
    for package in lockfile.get("package", []):
        name = package.get("name")
        package_version = package.get("version")
        if name in workspace_packages and package_version != version:
            mismatches.append(f"{name} is {package_version}")

    if mismatches:
        fail(
            "Cargo.lock is not in sync with [workspace.package].version "
            f"{version}: {', '.join(mismatches)}"
        )


def tag_exists(tag: str) -> bool:
    return (
        subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )


def write_env(path: str | None, values: dict[str, str]) -> None:
    if not path:
        return

    with open(path, "a", encoding="utf-8") as env_file:
        for key, value in values.items():
            env_file.write(f"{key}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--mode", choices=("guard", "tagger"), required=True)
    parser.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME", ""))
    parser.add_argument("--github-env", default=os.environ.get("GITHUB_ENV"))
    args = parser.parse_args()

    base_version = load_base_version(args.base_ref)
    head_version = load_head_version()
    version_changed = head_version != base_version
    tag = f"v{head_version}"

    write_env(
        args.github_env,
        {
            "BASE_VERSION": base_version,
            "HEAD_VERSION": head_version,
            "RELEASE_TAG": tag,
            "VERSION_CHANGED": "true" if version_changed else "false",
        },
    )

    if args.mode == "tagger" and not version_changed:
        print(f"Workspace version is still {head_version}; no release tag required.")
        return

    if compare_semver(head_version, base_version) <= 0:
        fail(
            "Release-impacting changes require [workspace.package].version "
            f"to increase (base: {base_version}, head: {head_version})."
        )

    verify_lockfile_version(head_version)

    if args.mode == "guard" and args.event_name == "pull_request" and tag_exists(tag):
        fail(f"Release tag {tag} already exists. Bump Cargo.toml to a fresh version.")

    print(f"Release version check passed: {base_version} -> {head_version} ({tag}).")


if __name__ == "__main__":
    main()
