#!/usr/bin/env python3
"""Test channel matching logic"""
import re
import pytest

def normalize(text):
    """Remove spaces and lowercase"""
    return text.replace(" ", "").lower()

def strip_quality(text):
    """Remove quality indicators"""
    quality_terms = ['hd', 'sd', 'fhd', '4k', 'uhd', 'hevc', 'h264', 'h265']
    result = text
    for term in quality_terms:
        # Remove with word boundaries
        result = re.sub(rf'\b{term}\b', '', result, flags=re.IGNORECASE)
    return result.strip()

def matches_channel(selected_channel, stream_name, regional_filter=None):
    """
    Check if stream matches selected channel
    
    Args:
        selected_channel: The channel name from user selection
        stream_name: Stream name to test
        regional_filter: Optional comma-separated wildcards like "york*,lond*"
    
    Returns:
        (bool, str): (matches, reason)
    """
    # Strip quality from selected channel only
    selected_base = strip_quality(selected_channel)
    selected_normalized = normalize(selected_base)
    
    # Normalize stream name
    stream_normalized = normalize(stream_name)
    
    # Check containment (with a fallback when no regional filter is applied).
    #
    # Rationale: users may select a region-specific channel name (e.g. "BBC One Yorkshire")
    # but still want to match generic or other-region streams when *no* regional filter is set.
    if selected_normalized not in stream_normalized:
        if not regional_filter:
            # Progressively drop trailing words (usually region qualifiers) until we match.
            words = selected_base.split()
            while len(words) > 1:
                words = words[:-1]
                candidate = normalize(" ".join(words))
                if candidate and candidate in stream_normalized:
                    selected_normalized = candidate
                    break
            else:
                return False, f"'{selected_normalized}' not in '{stream_normalized}'"
        else:
            return False, f"'{selected_normalized}' not in '{stream_normalized}'"
    
    # Check for +1/+2 mismatch
    selected_has_timeshift = re.search(r'\+\d', selected_channel)
    stream_has_timeshift = re.search(r'\+\d', stream_name)
    
    if selected_has_timeshift and not stream_has_timeshift:
        return False, "Selected has timeshift but stream doesn't"
    if not selected_has_timeshift and stream_has_timeshift:
        return False, "Stream has timeshift but selected doesn't"
    
    # Apply regional filter if provided
    if regional_filter:
        wildcards = [w.strip() for w in regional_filter.split(',')]
        stream_lower = stream_name.lower()
        
        matched = False
        for wildcard in wildcards:
            # Convert wildcard to regex (e.g., "york*" → "york.*")
            pattern = wildcard.replace('*', '.*')
            if re.search(pattern, stream_lower):
                matched = True
                break
        
        if not matched:
            return False, f"Doesn't match any regional filter: {regional_filter}"
    
    return True, "Match!"

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
