"""
stream_selection_trace.py
Trace and debug how streams are selected for a given group/channel without
requiring both arguments. The script fetches groups/channels from the
Dispatcharr API and prints the streams that pass optional include/exclude
filters.
"""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, List, Optional

from api_utils import DispatcharrAPI


def _normalize_terms(values: Optional[Iterable[str]]) -> List[str]:
    """Normalize a list of include/exclude filters for case-insensitive matching."""
    if not values:
        return []
    return [v.lower() for v in values if v]


def _matches_filters(url: str, include: List[str], exclude: List[str]) -> bool:
    """Return True when the URL matches include/exclude rules."""
    lower_url = url.lower()
    if include and not any(term in lower_url for term in include):
        return False
    if exclude and any(term in lower_url for term in exclude):
        return False
    return True


def _find_group(groups, name: str):
    """Locate a group by name (case-insensitive)."""
    for group in groups:
        if group.get("name", "").lower() == name.lower():
            return group
    return None


def _filter_channels(channels, group_id=None, channel_name=None):
    """Filter channels by group and/or name."""
    selected = channels
    if group_id is not None:
        selected = [ch for ch in selected if ch.get("channel_group_id") == group_id]
    if channel_name:
        needle = channel_name.lower()
        selected = [ch for ch in selected if needle in ch.get("name", "").lower()]
    return selected


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Trace stream selection for groups and channels. "
            "You can provide either or both of --group / --channel; when omitted, "
            "all groups and channels are considered."
        )
    )
    parser.add_argument("--group", help="Channel group name to filter")
    parser.add_argument("--channel", help="Channel name (partial match is allowed)")
    parser.add_argument(
        "--include",
        action="append",
        help="Only show streams whose URL contains this substring (can be repeated)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        help="Skip streams whose URL contains this substring (can be repeated)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()

    include_filters = _normalize_terms(args.include)
    exclude_filters = _normalize_terms(args.exclude)

    api = DispatcharrAPI()
    groups = api.fetch_channel_groups()
    channels = api.fetch_channels()

    if args.group:
        group = _find_group(groups, args.group)
        if not group:
            available = ", ".join(sorted(g.get("name", "") for g in groups))
            raise SystemExit(
                f"Group '{args.group}' not found. Available groups: {available}"
            )
        channels = _filter_channels(channels, group_id=group.get("id"))
        logging.info("Filtering channels in group: %s", group.get("name"))
    else:
        logging.info(
            "No group specified; tracing streams across all %d groups.", len(groups)
        )

    if args.channel:
        before = len(channels)
        channels = _filter_channels(channels, channel_name=args.channel)
        logging.info(
            "Channel filter '%s' reduced selection from %d to %d entries.",
            args.channel,
            before,
            len(channels),
        )

    if not channels:
        raise SystemExit("No channels matched the provided filters.")

    logging.info("Inspecting streams for %d channel(s)...", len(channels))

    for channel in channels:
        channel_name = channel.get("name", "<unknown>")
        channel_id = channel.get("id")
        try:
            streams = api.fetch_channel_streams(channel_id)
        except Exception as exc:  # noqa: BLE001
            logging.error("Failed to fetch streams for %s: %s", channel_name, exc)
            continue

        print(f"\nChannel: {channel_name} (ID: {channel_id})")
        if not streams:
            print("  No streams found")
            continue

        for stream in streams:
            url = stream.get("url", "")
            if not _matches_filters(url, include_filters, exclude_filters):
                continue
            print(f"  - Stream ID {stream.get('id')}: {url}")


if __name__ == "__main__":
    main()
