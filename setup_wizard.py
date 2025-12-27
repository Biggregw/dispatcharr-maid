#!/usr/bin/env python3
"""
setup_wizard.py

Interactive (or flag-driven) setup helper for Dispatcharr-Maid.

What it can do:
  - Create/update .env from .env.example (Dispatcharr connection settings)
  - Create/update config.yaml from config.yaml.example (common settings)
  - Patch docker-compose.yml to mount reverse-proxy logs into /app/npm_logs

Notes:
  - This script edits LOCAL files that are git-ignored (.env, config.yaml).
  - For provider usage from access logs, the app can auto-detect /app/npm_logs,
    but the *mount* is still required for the container to see host logs.
"""

from __future__ import annotations

import argparse
import getpass
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml


REPO_ROOT = Path(__file__).resolve().parent


def _prompt(
    label: str,
    *,
    default: Optional[str] = None,
    required: bool = False,
    secret: bool = False,
) -> str:
    while True:
        suffix = ""
        if default is not None and str(default).strip() != "":
            suffix = f" [{default}]"
        raw = ""
        if secret:
            raw = getpass.getpass(f"{label}{suffix}: ")
        else:
            raw = input(f"{label}{suffix}: ").strip()
        if raw == "" and default is not None:
            raw = str(default)
        if required and str(raw).strip() == "":
            print("  -> This value is required.")
            continue
        return str(raw).strip()


def _prompt_yes_no(label: str, *, default: bool = True) -> bool:
    d = "Y/n" if default else "y/N"
    while True:
        raw = input(f"{label} [{d}]: ").strip().lower()
        if raw == "":
            return bool(default)
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("  -> Please answer y or n.")


def _parse_csv_ints(s: str) -> List[int]:
    out: List[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def ensure_from_example(target: Path, example: Path) -> None:
    if target.exists():
        return
    if not example.exists():
        raise FileNotFoundError(f"Missing example file: {example}")
    shutil.copyfile(example, target)


def update_env_file(env_path: Path, updates: Dict[str, str]) -> Tuple[bool, str]:
    """
    Update or create .env, preserving comments/unknown keys.
    Returns (changed, new_content).
    """
    example_path = env_path.parent / ".env.example"
    if not env_path.exists():
        ensure_from_example(env_path, example_path)

    original = _read_text(env_path)
    lines = original.splitlines(True)

    # Track which keys we updated in-file
    updated_keys = set()
    out: List[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped == "" or stripped.startswith("#") or "=" not in line:
            out.append(line)
            continue

        key, rest = line.split("=", 1)
        key = key.strip()
        if key in updates:
            out.append(f"{key}={updates[key]}\n")
            updated_keys.add(key)
        else:
            out.append(line)

    # Append any missing keys at end (keep file simple)
    missing = [k for k in updates.keys() if k not in updated_keys]
    if missing:
        if out and not out[-1].endswith("\n"):
            out[-1] += "\n"
        out.append("\n# Added by setup_wizard.py\n")
        for k in missing:
            out.append(f"{k}={updates[k]}\n")

    new_content = "".join(out)
    changed = new_content != original
    return changed, new_content


def update_config_yaml(config_path: Path, updates: Dict[str, object]) -> Tuple[bool, str]:
    """
    Update config.yaml while trying to preserve comments/formatting.

    For common keys we patch the file text in-place (so example comments remain).
    If we can't safely patch, we fall back to rewriting via PyYAML.

    Returns (changed, new_content).
    """
    example_path = config_path.parent / "config.yaml.example"
    if not config_path.exists():
        ensure_from_example(config_path, example_path)

    original_text = _read_text(config_path)
    lines = original_text.splitlines(True)

    # Helper: locate a top-level section start line like "filters:".
    def find_section_start(name: str) -> Optional[int]:
        for i, line in enumerate(lines):
            if re.match(rf"^{re.escape(name)}\s*:\s*(#.*)?$", line.strip()):
                # Only accept true top-level keys (no indentation)
                if line.startswith(name):
                    return i
        return None

    # Helper: patch a single key within a named top-level section, with 2-space indentation.
    def patch_section_key(section: str, key: str, new_value_str: str) -> bool:
        start = find_section_start(section)
        if start is None:
            return False
        i = start + 1
        # Walk until next top-level key
        while i < len(lines) and (lines[i].startswith(" ") or lines[i].strip() == ""):
            m = re.match(rf"^(\s{{2}}){re.escape(key)}\s*:\s*(.*)$", lines[i])
            if m:
                # Preserve trailing inline comment if present
                rest = m.group(2)
                comment = ""
                if "#" in rest:
                    comment = "  " + rest[rest.index("#") :].rstrip("\n")
                lines[i] = f"  {key}: {new_value_str}{comment}\n"
                return True
            i += 1
        return False

    # Helper: insert key under section if missing (keeps section, comments intact).
    def insert_section_key(section: str, key: str, new_value_str: str) -> bool:
        start = find_section_start(section)
        if start is None:
            # Add section at end
            if lines and not lines[-1].endswith("\n"):
                lines[-1] += "\n"
            lines.append(f"\n{section}:\n")
            lines.append(f"  {key}: {new_value_str}\n")
            return True

        i = start + 1
        while i < len(lines) and (lines[i].startswith(" ") or lines[i].strip() == ""):
            i += 1
        # Insert before next top-level section
        lines.insert(i, f"  {key}: {new_value_str}\n")
        return True

    changed_any = False

    # Apply known updates (currently filters.channel_group_ids and web.results_retention_days)
    filters = updates.get("filters") if isinstance(updates, dict) else None
    if isinstance(filters, dict) and "channel_group_ids" in filters:
        gids = filters.get("channel_group_ids")
        if isinstance(gids, list):
            new_value = f"[{', '.join(str(int(x)) for x in gids)}]"
        else:
            new_value = "[]"
        if not patch_section_key("filters", "channel_group_ids", new_value):
            insert_section_key("filters", "channel_group_ids", new_value)
        changed_any = True

    web = updates.get("web") if isinstance(updates, dict) else None
    if isinstance(web, dict) and "results_retention_days" in web:
        new_value = str(int(web.get("results_retention_days") or 0))
        if not patch_section_key("web", "results_retention_days", new_value):
            insert_section_key("web", "results_retention_days", new_value)
        changed_any = True

    if changed_any:
        new_text = "".join(lines)
        changed = new_text != original_text
        return changed, new_text

    # Fallback: if we were asked to update unknown keys, do a safe YAML merge.
    try:
        cfg = yaml.safe_load(original_text) or {}
    except Exception:
        raise RuntimeError(f"Could not parse {config_path}. Please fix YAML syntax first.")
    if not isinstance(cfg, dict):
        raise RuntimeError(f"Expected {config_path} to contain a YAML mapping at the top level.")

    def deep_merge(dst: dict, src: dict) -> dict:
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                deep_merge(dst[k], v)
            else:
                dst[k] = v
        return dst

    deep_merge(cfg, updates)
    new_text = yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False)
    return (new_text.strip() != original_text.strip()), new_text


def patch_docker_compose_for_npm_logs(
    compose_path: Path,
    host_logs_path: str,
    *,
    container_path: str = "/app/npm_logs",
) -> Tuple[bool, str]:
    """
    Patch docker-compose.yml to add/update a volume mount:
      <host_logs_path>:/app/npm_logs:ro
    Returns (changed, new_content).
    """
    original = _read_text(compose_path)
    lines = original.splitlines(True)

    mount = f"{host_logs_path}:{container_path}:ro"

    # 1) If there is already any line (commented or not) that mounts to /app/npm_logs:ro, replace it and ENABLE it.
    for i, line in enumerate(lines):
        if f"{container_path}:ro" in line:
            # Preserve indentation, but drop any YAML comment marker.
            # Examples:
            #   "      - /a:/app/npm_logs:ro\n"
            #   "      # - /a:/app/npm_logs:ro\n"
            m = re.match(r"^(?P<indent>\s*)(?:#\s*)?-\s+.*$", line)
            indent = (m.group("indent") if m else "      ")
            lines[i] = f"{indent}- {mount}\n"
            new_content = "".join(lines)
            return (new_content != original), new_content

    # 2) Else, insert beneath the existing "Optional: mount reverse-proxy access logs" comment block
    insert_at = None
    for i, line in enumerate(lines):
        if "Optional: mount reverse-proxy access logs" in line:
            insert_at = i + 1
            # Skip subsequent comment lines in that block
            while insert_at < len(lines) and lines[insert_at].lstrip().startswith("#"):
                insert_at += 1
            break

    # 3) Fallback: insert under the first "volumes:" under the web service.
    if insert_at is None:
        for i, line in enumerate(lines):
            if line.strip() == "volumes:":
                insert_at = i + 1
                break

    if insert_at is None:
        raise RuntimeError(f"Could not find where to insert volume in {compose_path}")

    lines.insert(insert_at, f"      - {mount}\n")
    new_content = "".join(lines)
    return (new_content != original), new_content


def main() -> int:
    p = argparse.ArgumentParser(description="Dispatcharr-Maid setup wizard")
    p.add_argument("--non-interactive", action="store_true", help="Do not prompt; require needed flags")
    p.add_argument("--write", action="store_true", help="Write changes to disk (default: preview only)")
    p.add_argument("--repo", default=str(REPO_ROOT), help="Repo directory (default: this script's folder)")

    # .env
    p.add_argument("--dispatcharr-base-url", default=None)
    p.add_argument("--dispatcharr-user", default=None)
    p.add_argument("--dispatcharr-pass", default=None)
    p.add_argument("--dispatcharr-token", default=None)

    # config.yaml
    p.add_argument("--channel-group-ids", default=None, help="Comma-separated ints (e.g. 1,2,3)")
    p.add_argument("--results-retention-days", default=None, help="0 to disable, or an integer")

    # docker-compose.yml mount for logs
    p.add_argument("--npm-logs-host-path", default=None, help="Host path to proxy logs (e.g. /srv/npm/data/logs)")
    p.add_argument("--skip-compose", action="store_true", help="Do not edit docker-compose.yml")

    args = p.parse_args()
    repo = Path(args.repo).resolve()

    env_path = repo / ".env"
    config_path = repo / "config.yaml"
    compose_path = repo / "docker-compose.yml"

    # Collect updates
    env_updates: Dict[str, str] = {}
    cfg_updates: Dict[str, object] = {}
    wants_compose = not bool(args.skip_compose)

    if args.non_interactive:
        # Require at least base URL/user/pass when non-interactive, unless user is only patching compose/config.
        if args.dispatcharr_base_url is not None:
            env_updates["DISPATCHARR_BASE_URL"] = args.dispatcharr_base_url
        if args.dispatcharr_user is not None:
            env_updates["DISPATCHARR_USER"] = args.dispatcharr_user
        if args.dispatcharr_pass is not None:
            env_updates["DISPATCHARR_PASS"] = args.dispatcharr_pass
        if args.dispatcharr_token is not None:
            env_updates["DISPATCHARR_TOKEN"] = args.dispatcharr_token

        if args.channel_group_ids is not None:
            cfg_updates.setdefault("filters", {})
            cfg_updates["filters"]["channel_group_ids"] = _parse_csv_ints(args.channel_group_ids)
        if args.results_retention_days is not None:
            cfg_updates.setdefault("web", {})
            cfg_updates["web"]["results_retention_days"] = int(args.results_retention_days)
    else:
        print("\n== Dispatcharr-Maid setup wizard ==\n")
        print("This will prepare: .env, config.yaml, and optionally docker-compose.yml.\n")

        if _prompt_yes_no("Configure Dispatcharr connection in .env?", default=True):
            base_url = _prompt("Dispatcharr base URL", default="http://dispatcharr:9191", required=True)
            user = _prompt("Dispatcharr username", required=True)
            passwd = _prompt("Dispatcharr password", required=True, secret=True)
            env_updates["DISPATCHARR_BASE_URL"] = base_url
            env_updates["DISPATCHARR_USER"] = user
            env_updates["DISPATCHARR_PASS"] = passwd
            token = _prompt("Dispatcharr token (optional; usually leave blank)", default="")
            env_updates["DISPATCHARR_TOKEN"] = token

        if _prompt_yes_no("Configure common settings in config.yaml?", default=True):
            gids = _prompt("Channel group IDs (comma-separated, blank to skip)", default="")
            if gids.strip() != "":
                cfg_updates.setdefault("filters", {})
                cfg_updates["filters"]["channel_group_ids"] = _parse_csv_ints(gids)

            rrd = _prompt("Results retention days (0 disables pruning)", default="0")
            try:
                cfg_updates.setdefault("web", {})
                cfg_updates["web"]["results_retention_days"] = int(rrd)
            except Exception:
                print("  -> Invalid integer; skipping results_retention_days.")

        if wants_compose and _prompt_yes_no("Enable provider-usage log mount in docker-compose.yml?", default=True):
            host_path = _prompt(
                "Host path to reverse-proxy access logs (e.g. /srv/npm/data/logs)",
                default="",
                required=True,
            )
            args.npm_logs_host_path = host_path

    # Compute previews
    previews: List[Tuple[str, Path, bool, str]] = []  # (label, path, changed, content)

    if env_updates:
        changed, new_content = update_env_file(env_path, env_updates)
        previews.append((".env", env_path, changed, new_content))

    if cfg_updates:
        changed, new_content = update_config_yaml(config_path, cfg_updates)
        previews.append(("config.yaml", config_path, changed, new_content))

    if wants_compose and args.npm_logs_host_path:
        host_path = str(args.npm_logs_host_path).strip()
        if host_path == "":
            pass
        else:
            changed, new_content = patch_docker_compose_for_npm_logs(compose_path, host_path)
            previews.append(("docker-compose.yml", compose_path, changed, new_content))

    if not previews:
        print("Nothing to do (no updates selected).")
        return 0

    # Preview summary
    print("\n== Preview ==\n")
    for label, path, changed, _ in previews:
        status = "will update" if changed else "no change"
        print(f"- {label}: {status} ({path})")

    if not args.write:
        print("\nRun again with --write to apply changes.")
        return 0

    # Write changes
    for _, path, _, content in previews:
        _write_text(path, content)

    print("\nWrote changes successfully.\n")
    if wants_compose and args.npm_logs_host_path:
        print("Next step: restart the container, e.g.:")
        print("  docker compose restart dispatcharr-maid-web")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

