"""
provider_data.py
Utilities for discovering Dispatcharr provider metadata.
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path


def _log_raw_response(tag, response, config):
    """Persist raw API responses for auditing and debugging."""
    logs_dir = Path(config.resolve_path('logs'))
    logs_dir.mkdir(parents=True, exist_ok=True)
    raw_path = logs_dir / f"{tag}_raw_response.txt"
    raw_path.write_text(response.text, encoding="utf-8")
    logging.info("%s content-type: %s", tag, response.headers.get('Content-Type', ''))
    logging.info("%s raw response saved to %s", tag, raw_path)


def _normalize_provider_payload(payload):
    """Normalize provider payload into map + metadata dicts."""
    if isinstance(payload, dict) and 'results' in payload:
        payload = payload['results']
    if not isinstance(payload, list):
        raise ValueError("M3U accounts payload is not a list or paginated result.")

    provider_map = {}
    provider_metadata = {}
    for entry in payload:
        if not isinstance(entry, dict):
            raise ValueError("M3U account entry is not an object.")
        if 'id' not in entry or 'name' not in entry:
            raise ValueError("M3U account entry missing required 'id' or 'name' fields.")
        provider_id = str(entry['id'])
        provider_name = entry['name']
        provider_map[provider_id] = provider_name

        metadata = {'name': provider_name}
        if 'max_streams' in entry:
            metadata['max_streams'] = entry.get('max_streams')
        provider_metadata[provider_id] = metadata

    if not provider_map:
        raise ValueError("No provider accounts found in M3U accounts response.")

    return provider_map, provider_metadata


def _fetch_provider_payload_via_api(api, config, endpoint):
    response = api.get_raw(endpoint)
    _log_raw_response('m3u_accounts', response, config)

    content_type = response.headers.get('Content-Type', '')
    if 'application/json' not in content_type:
        raise ValueError(
            f"M3U accounts response is not JSON (Content-Type: {content_type})."
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError("Failed to parse M3U accounts JSON response.") from exc

    return payload


def _build_manage_py_query():
    return (
        "from apps.m3u.models import M3UAccount; "
        "import json; "
        "print(json.dumps(["
        "{'id': account.id, 'name': account.name, 'max_streams': account.max_streams} "
        "for account in M3UAccount.objects.all()]))"
    )


# Helper: parse provider payload from manage.py output.
def _parse_provider_payload_from_stdout(stdout):
    stdout = stdout.strip()
    if not stdout:
        raise ValueError("manage.py shell did not return provider data.")

    payload = None
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            break
        except json.JSONDecodeError:
            continue

    if payload is None:
        raise ValueError("Unable to parse provider data from manage.py output.")

    return payload


def _fetch_provider_payload_via_manage_py(manage_py_path):
    manage_py = Path(manage_py_path)
    if not manage_py.exists():
        raise ValueError(f"manage.py not found at {manage_py}.")

    command = [
        sys.executable,
        str(manage_py),
        'shell',
        '-c',
        _build_manage_py_query()
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=str(manage_py.parent)
    )

    if result.returncode != 0:
        raise ValueError(
            "manage.py shell failed: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    return _parse_provider_payload_from_stdout(result.stdout)


# Helper: fetch provider payload via docker exec.
def _fetch_provider_payload_via_docker_exec(container_name):
    command = [
        'docker',
        'exec',
        container_name,
        'python3',
        '/app/manage.py',
        'shell',
        '-c',
        _build_manage_py_query()
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True
        )
    except (FileNotFoundError, OSError) as exc:
        raise ValueError("docker exec is unavailable.") from exc

    if result.returncode != 0:
        raise ValueError(
            "docker exec manage.py shell failed: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    return _parse_provider_payload_from_stdout(result.stdout)


def _write_json(path, payload):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2)


def refresh_provider_data(api, config, force=False):
    """Refresh provider_map.json and provider_metadata.json from Dispatcharr."""
    dispatcharr_cfg = config.get('dispatcharr') or {}
    provider_map_path = config.resolve_path('provider_map.json')
    provider_metadata_path = config.resolve_path('provider_metadata.json')

    write_map = force or not Path(provider_map_path).exists()
    write_metadata = force or not Path(provider_metadata_path).exists()

    if not write_map and not write_metadata:
        logging.info("Provider metadata already present; skipping refresh.")
        return {}, {}

    manage_py_path = dispatcharr_cfg.get('manage_py_path') or os.getenv('DISPATCHARR_MANAGE_PY')
    payload = None

    if manage_py_path:
        try:
            payload = _fetch_provider_payload_via_manage_py(manage_py_path)
        except ValueError as exc:
            logging.warning("Unable to fetch provider data via manage.py: %s", exc)

    if payload is None:
        container_name = dispatcharr_cfg.get('container_name')
        if container_name:
            try:
                payload = _fetch_provider_payload_via_docker_exec(container_name)
            except ValueError as exc:
                logging.warning("Unable to fetch provider data via docker exec: %s", exc)

    if payload is None:
        provider_accounts_endpoint = dispatcharr_cfg.get('m3u_accounts_endpoint')
        if provider_accounts_endpoint and api:
            try:
                payload = _fetch_provider_payload_via_api(api, config, provider_accounts_endpoint)
            except ValueError as exc:
                logging.warning("Unable to fetch provider data via API: %s", exc)
                return {}, {}
        else:
            logging.warning(
                "Provider discovery skipped: configure dispatcharr.manage_py_path, "
                "dispatcharr.container_name, or dispatcharr.m3u_accounts_endpoint."
            )
            return {}, {}

    try:
        provider_map, provider_metadata = _normalize_provider_payload(payload)
    except ValueError as exc:
        logging.warning("Unable to normalize provider data: %s", exc)
        return {}, {}

    if write_map:
        _write_json(provider_map_path, provider_map)
    if write_metadata:
        _write_json(provider_metadata_path, provider_metadata)

    return provider_map, provider_metadata
