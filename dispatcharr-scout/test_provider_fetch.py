"""
Integration test / helper script for fetching provider names from Dispatcharr.

This repository's unit tests should not depend on a live Dispatcharr instance,
so this module is skipped during pytest collection.

To run manually:
  python3 test_provider_fetch.py
"""

import json
import sys

import pytest

from api_utils import DispatcharrAPI


pytest.skip("Integration helper; requires a live Dispatcharr instance.", allow_module_level=True)


def main() -> int:
    print("=" * 70)
    print("TESTING PROVIDER NAME FETCH FROM DISPATCHARR")
    print("=" * 70)
    print()

    print("Step 1: Connecting to Dispatcharr...")
    try:
        api = DispatcharrAPI()
        api.login()
        print("✓ Connected successfully\n")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return 1

    print("Step 2: Fetching M3U accounts from /api/channels/m3u-accounts/ ...")
    try:
        response = api.get_raw("/api/channels/m3u-accounts/")
        print(f"✓ Response status: {response.status_code}")
        print(f"✓ Content-Type: {response.headers.get('Content-Type')}\n")
        data = response.json()
    except Exception as e:
        print(f"✗ Failed to fetch: {e}")
        return 1

    print("Step 3: Extracting provider names...\n")
    if isinstance(data, dict) and "results" in data:
        providers = data["results"]
        print(f"ℹ️ Paginated response with {len(providers)} providers")
    elif isinstance(data, list):
        providers = data
        print(f"ℹ️ Direct list with {len(providers)} providers")
    else:
        print(f"✗ Unexpected response format: {type(data)}")
        print(f"Response: {data}")
        return 1

    print()
    print("=" * 70)
    print("PROVIDER NAMES FOUND IN DISPATCHARR:")
    print("=" * 70)
    print()

    if not providers:
        print("⚠️ No providers found!")
        print("This might mean:")
        print("- No M3U accounts are configured in Dispatcharr")
        print("- The endpoint URL is wrong")
        print("- Permissions issue")
        return 0

    provider_map: dict[str, str] = {}
    for provider in providers:
        if not isinstance(provider, dict):
            continue
        provider_id = provider.get("id")
        provider_name = provider.get("name", "Unknown")
        print(f"ID {str(provider_id):>3}: {provider_name}")
        if provider_id is not None and provider_name:
            provider_map[str(provider_id)] = str(provider_name)

    print()
    print("=" * 70)
    print(f"✓ Successfully fetched {len(provider_map)} provider names!")
    print("=" * 70)
    print()

    print("This would be saved to provider_map.json:\n")
    print(json.dumps(provider_map, indent=2))
    print()

    save = input("Save this to provider_map.json? (y/n): ").strip().lower()
    if save == "y":
        with open("provider_map.json", "w", encoding="utf-8") as f:
            json.dump(provider_map, f, indent=2)
        print("✓ Saved to provider_map.json")
    else:
        print("Not saved")

    print()
    print("=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
