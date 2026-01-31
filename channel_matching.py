#!/usr/bin/env python3
"""Channel matching utilities for stream name comparison."""
import re


def normalize(text):
    """Remove spaces and lowercase."""
    return text.replace(" ", "").lower()


def strip_quality(text):
    """Remove quality indicators."""
    quality_terms = ['hd', 'sd', 'fhd', '4k', 'uhd', 'hevc', 'h264', 'h265']
    result = text
    for term in quality_terms:
        result = re.sub(rf'\b{term}\b', '', result, flags=re.IGNORECASE)
    return result.strip()


def matches_channel(selected_channel, stream_name, regional_filter=None, exclude_filter=None):
    """
    Check if stream matches selected channel.

    Args:
        selected_channel: The channel name from user selection
        stream_name: Stream name to test
        regional_filter: Optional comma-separated wildcards like "york*,lond*"
        exclude_filter: Optional comma-separated wildcards to exclude

    Returns:
        (bool, str): (matches, reason)
    """
    selected_base = strip_quality(selected_channel)
    selected_normalized = normalize(selected_base)
    stream_normalized = normalize(stream_name)

    # Check containment with fallback when no regional filter
    if selected_normalized not in stream_normalized:
        if not regional_filter:
            # Progressively drop trailing words (usually region qualifiers) until we match
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

    # Apply regional INCLUDE filter if provided
    if regional_filter:
        wildcards = [w.strip() for w in regional_filter.split(',')]
        stream_lower = stream_name.lower()

        matched = False
        for wildcard in wildcards:
            pattern = re.escape(wildcard).replace(r'\*', '.*')
            if re.search(pattern, stream_lower):
                matched = True
                break

        if not matched:
            return False, f"Doesn't match any regional filter: {regional_filter}"

    # Apply EXCLUDE filter if provided
    if exclude_filter:
        excludes = [e.strip() for e in exclude_filter.split(',')]
        stream_lower = stream_name.lower()

        for exclude in excludes:
            pattern = re.escape(exclude).replace(r'\*', '.*')
            if re.search(pattern, stream_lower):
                return False, f"Excluded by '{exclude}'"

    return True, "Match!"
