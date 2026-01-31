#!/usr/bin/env python3
"""Test channel matching logic"""
import pytest
from channel_matching import matches_channel

TEST_CASES = [
    # (selected_channel, regional_filter, stream_name, expected_match)
    ("BBC One Yorkshire HD", "york*", "BBC One Yorkshire SD", True),
    ("BBC One Yorkshire HD", "york*", "UK: BBC One Yorkshire 4K", True),
    ("BBC One Yorkshire HD", "york*", "BBC One London HD", False),
    ("BBC One Yorkshire HD", "york*", "BBC One HD", False),
    ("BBC One Yorkshire HD", "", "BBC One HD", True),
    ("BBC One Yorkshire HD", "", "BBC One London", True),
    ("Channel 4", "", "Channel 4 HD", True),
    ("Channel 4", "", "Channel 4 +1", False),
    ("Channel 4 +1", "", "Channel 4", False),
    ("Channel 4 +1", "", "Channel 4 +1 HD", True),
    ("Channel 4", "", "Channel4HD", True),
    ("BBC One", "york*,lond*,scot*", "BBC One Yorkshire", True),
    ("BBC One", "york*,lond*,scot*", "BBC One Wales", False),
    ("Sky Sports", "", "Sky Sports Main Event", True),
    ("Sky Sports", "", "SkySportsHD", True),
]


@pytest.mark.parametrize(
    ("selected_channel", "regional_filter", "stream_name", "expected_match"),
    TEST_CASES,
)
def test_matches_channel(selected_channel, regional_filter, stream_name, expected_match):
    result, _reason = matches_channel(selected_channel, stream_name, regional_filter or None)
    assert result == expected_match


if __name__ == "__main__":
    # Keep the old interactive printout behavior for manual runs.
    print("=" * 70)
    print("CHANNEL MATCHING TEST")
    print("=" * 70)

    for selected, regional, stream, expected in TEST_CASES:
        result, reason = matches_channel(selected, stream, regional or None)
        status = "✓" if result == expected else "✗ FAIL"

        print(f"\n{status} Selected: '{selected}'")
        if regional:
            print(f"   Regional: '{regional}'")
        print(f"   Stream: '{stream}'")
        print(f"   Result: {result} ({reason})")
        if result != expected:
            print(f"   EXPECTED: {expected}")

    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
