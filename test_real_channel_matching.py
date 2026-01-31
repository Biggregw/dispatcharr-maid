#!/usr/bin/env python3
"""Test channel matching against real Dispatcharr data"""
import os
import pytest
from api_utils import DispatcharrAPI
from channel_matching import matches_channel


def fetch_all_streams(api: DispatcharrAPI, limit: int = 100):
    """Fetch all streams (paginated) from Dispatcharr."""
    all_streams = []
    next_url = f"/api/channels/streams/?limit={int(limit)}"

    while next_url:
        payload = api.get(next_url)
        if not payload or "results" not in payload:
            break

        all_streams.extend(payload["results"])

        if payload.get("next"):
            next_url = payload["next"].split("/api/")[-1]
            next_url = "/api/" + next_url
        else:
            next_url = None

    return all_streams


def _integration_enabled() -> bool:
    """
    Real Dispatcharr integration tests are opt-in to avoid network/credential
    requirements during normal test runs.
    """
    return os.getenv("DISPATCHARR_RUN_INTEGRATION_TESTS", "").lower() in {"1", "true", "yes", "on"}


@pytest.mark.integration
def test_real_dispatcharr_smoke_fetch_streams():
    if not _integration_enabled():
        pytest.skip("Set DISPATCHARR_RUN_INTEGRATION_TESTS=1 to enable real Dispatcharr tests.")

    # DispatcharrAPI itself enforces the required env vars; if they're missing,
    # skip instead of failing collection.
    try:
        api = DispatcharrAPI()
    except ValueError as e:
        pytest.skip(str(e))

    streams = fetch_all_streams(api)
    assert isinstance(streams, list)


if __name__ == "__main__":
    # Interactive utility mode (manual runs only).
    print("Connecting to Dispatcharr...")
    api = DispatcharrAPI()

    print("\n" + "=" * 70)
    channel_input = input("Enter channel name (e.g., 'ITV', 'BBC One'): ").strip()
    regional_input = input("Include filter (e.g., 'york*' or 'york*,lond*', blank for all): ").strip()
    exclude_input = input("Exclude filter (e.g., 'lincoln*', blank for none): ").strip()
    print("=" * 70)

    print("\nFetching all streams from Dispatcharr...")
    all_streams = fetch_all_streams(api)
    print(f"✓ Loaded {len(all_streams)} total streams")

    print(f"\nSearching for matches to: '{channel_input}'")
    if regional_input:
        print(f"Include filter: '{regional_input}'")
    if exclude_input:
        print(f"Exclude filter: '{exclude_input}'")

    print("\n" + "=" * 70)
    print("MATCHING STREAMS:")
    print("=" * 70)

    matches = []
    for stream in all_streams:
        stream_name = stream.get("name", "")
        stream_id = stream.get("id")
        m3u_account = stream.get("m3u_account")

        result, _reason = matches_channel(
            channel_input,
            stream_name,
            regional_input or None,
            exclude_input or None,
        )

        if result:
            matches.append({"id": stream_id, "name": stream_name, "provider": m3u_account})

    if matches:
        by_provider = {}
        for m in matches:
            provider = m["provider"] or "Unknown"
            by_provider.setdefault(provider, []).append(m)

        print(f"\nFound {len(matches)} matching streams across {len(by_provider)} providers:\n")

        for provider_id in sorted(by_provider.keys()):
            streams = by_provider[provider_id]
            print(f"Provider {provider_id}: ({len(streams)} streams)")
            for s in streams[:5]:
                print(f"  • [{s['id']}] {s['name']}")
            if len(streams) > 5:
                print(f"  ... and {len(streams) - 5} more")
            print()
    else:
        print("\n❌ No matching streams found")

    print("=" * 70)
    print(f"Total matches: {len(matches)}")
