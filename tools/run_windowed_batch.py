import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from api_utils import DispatcharrAPI
from stream_analysis import Config, analyze_streams, fetch_streams, order_streams_for_channel, score_streams
from windowed_runner import (
    ChannelSelector,
    WindowedRunnerState,
    build_provider_key,
    build_stream_key,
    compute_deadline,
    is_status_failure,
    playable_now_check,
    serialize_summary,
)


DEFAULT_PRIORITY = "stale,failures,favourites"


def parse_args():
    parser = argparse.ArgumentParser(description="Run a windowed nightly channel analysis batch.")
    parser.add_argument("--base-url", required=True, help="Dispatcharr base URL (e.g. http://localhost:8000)")
    parser.add_argument("--groups", help="Comma-separated group IDs to include")
    parser.add_argument("--all-groups", action="store_true", help="Include all channel groups")
    parser.add_argument("--channels", help="Comma-separated channel IDs to process")
    parser.add_argument("--window-from", required=True, help="Local window start (HH:MM)")
    parser.add_argument("--window-to", required=True, help="Local window end (HH:MM)")
    parser.add_argument("--profile", default="deep", help="Analysis profile name (for logging)")
    parser.add_argument("--max-channels-per-run", type=int, default=0, help="Max channels to process (0=unlimited)")
    parser.add_argument("--slot1-check-limit", type=int, default=3, help="Max slot1 candidates to test")
    parser.add_argument("--stream-cooldown-minutes", type=int, default=15, help="Cooldown minutes for stream failures")
    parser.add_argument("--provider-cooldown-minutes", type=int, default=30, help="Cooldown minutes for provider failures")
    parser.add_argument("--provider-fail-threshold", type=int, default=3, help="Failures before provider cooldown")
    parser.add_argument("--provider-fail-window-seconds", type=int, default=120, help="Failure window for provider cooldown")
    parser.add_argument("--priority", default=DEFAULT_PRIORITY, help="Priority boosts: stale,failures,favourites")
    parser.add_argument("--favourite-channels", help="Comma-separated favourite channel IDs")
    parser.add_argument("--data-db", default="data/windowed_runner.sqlite", help="SQLite state database path")
    parser.add_argument("--fast-check-timeout-seconds", type=int, default=4, help="Timeout for playable-now checks")
    return parser.parse_args()


def _parse_csv_int_list(value):
    if not value:
        return []
    items = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            items.append(int(part))
        except ValueError:
            raise ValueError(f"Invalid integer value: {part}")
    return items


def _status_failure_reason(record):
    status = record.get("status") or record.get("validation_result")
    if not status:
        return None
    status_text = str(status).strip().lower()
    if is_status_failure(status_text):
        return f"analysis_status:{status_text}"
    return None


def _prepare_streams_override(streams, channel_id, stream_provider_map):
    prepared = []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        stream = dict(stream)
        stream["channel_id"] = channel_id
        m3u_account = stream.get("m3u_account")
        if m3u_account in ("", None):
            try:
                stream_id = int(stream.get("id"))
            except (TypeError, ValueError):
                stream_id = None
            if stream_id is not None:
                stream["m3u_account"] = stream_provider_map.get(stream_id)
        prepared.append(stream)
    return prepared


def _build_records_for_ordering(current_streams, scored_lookup):
    records_for_ordering = []
    for stream in current_streams:
        sid = stream.get("id")
        if sid is None:
            continue
        try:
            sid_key = int(sid)
        except Exception:
            sid_key = sid
        record = scored_lookup.get(sid_key)
        if record is None:
            record = {
                "stream_id": sid_key,
                "stream_name": stream.get("name"),
                "m3u_account": stream.get("m3u_account"),
                "m3u_account_name": stream.get("m3u_account_name"),
                "service_name": stream.get("service_name"),
                "service_provider": stream.get("service_provider"),
                "resolution": stream.get("resolution"),
                "avg_bitrate_kbps": stream.get("bitrate_kbps") or stream.get("ffmpeg_output_bitrate"),
                "fps": stream.get("source_fps") or stream.get("fps"),
                "video_codec": stream.get("video_codec"),
                "audio_codec": stream.get("audio_codec"),
                "interlaced_status": stream.get("interlaced_status"),
                "status": stream.get("status"),
                "stream_url": stream.get("url"),
            }
        else:
            record = dict(record)
        records_for_ordering.append(record)
    return records_for_ordering


def _order_channel_streams(records_for_ordering, config):
    ordering_cfg = config.get("ordering") or {}
    try:
        fallback_depth = int(ordering_cfg.get("fallback_depth", 3) or 3)
    except (ValueError, TypeError):
        fallback_depth = 3
    try:
        similar_score_delta = float(ordering_cfg.get("similar_score_delta", 5) or 5)
    except (ValueError, TypeError):
        similar_score_delta = 5.0
    ordering_details = order_streams_for_channel(
        records_for_ordering,
        resilience_mode=ordering_cfg.get("resilience_mode"),
        fallback_depth=fallback_depth,
        similar_score_delta=similar_score_delta,
        reliability_sort=ordering_cfg.get("reliability_sort"),
        return_details=True,
    )
    return ordering_details


def run_windowed_batch():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    os.environ["DISPATCHARR_BASE_URL"] = args.base_url

    now = datetime.now()
    deadline = compute_deadline(now, args.window_from, args.window_to)

    requested_groups = _parse_csv_int_list(args.groups)
    requested_channels = _parse_csv_int_list(args.channels)
    favourite_channels = _parse_csv_int_list(args.favourite_channels)

    if not args.all_groups and not requested_groups and not requested_channels:
        logging.error("Provide --groups, --all-groups, or --channels")
        sys.exit(1)

    api = DispatcharrAPI()
    config = Config()

    state_db_path = Path(args.data_db)
    state_db_path.parent.mkdir(parents=True, exist_ok=True)
    state = WindowedRunnerState(str(state_db_path))

    if requested_channels:
        all_channels = [api.fetch_channel(channel_id) for channel_id in requested_channels]
    else:
        all_channels = api.fetch_channels()

    if not all_channels:
        logging.error("No channels available for processing")
        sys.exit(1)

    channels = []
    for channel in all_channels:
        if not isinstance(channel, dict):
            continue
        channel_id = channel.get("id")
        group_id = channel.get("channel_group_id")
        if channel_id is None:
            continue
        if requested_channels:
            if channel_id not in requested_channels:
                continue
        elif not args.all_groups and requested_groups:
            if group_id not in requested_groups:
                continue
        channels.append(channel)

    if not channels:
        logging.error("No channels matched the requested filters")
        sys.exit(1)

    for channel in channels:
        channel_id = channel.get("id")
        group_id = channel.get("channel_group_id")
        flags = "favourite" if channel_id in favourite_channels else None
        state.ensure_channel(channel_id, group_id, priority_boost_flags=flags)

    selector = ChannelSelector(args.priority.split(","))

    stream_provider_map = api.fetch_stream_provider_map()

    logging.info(
        "Windowed run starting at %s with deadline %s (profile=%s)",
        now.isoformat(timespec="seconds"),
        deadline.isoformat(timespec="seconds"),
        args.profile,
    )
    processed_channels = 0
    skipped_due_cooldown = 0
    providers_marked_down = set()

    summary = {
        "start_time": now.isoformat(),
        "deadline": deadline.isoformat(),
        "processed_channels": [],
        "channels_skipped_due_to_cooldown": 0,
        "providers_marked_down": [],
    }

    remaining_ids = [channel.get("id") for channel in channels if channel.get("id") is not None]

    while True:
        if args.max_channels_per_run and processed_channels >= args.max_channels_per_run:
            logging.info("Reached max channels per run (%d)", args.max_channels_per_run)
            break
        if datetime.now() >= deadline:
            logging.info("Deadline reached, stopping")
            break
        channel_states = state.list_channel_states(remaining_ids)
        next_state = selector.select_next(channel_states)
        if not next_state:
            logging.info("No eligible channels remaining")
            break

        channel_id = next_state.channel_id
        channel = next((ch for ch in channels if ch.get("id") == channel_id), None)
        if channel is None:
            remaining_ids = [cid for cid in remaining_ids if cid != channel_id]
            continue

        start_time = time.time()
        result_status = "ok"
        slot1_changed = False
        provider_cooldown_applied = False
        slot1_selected = None
        no_playable = False

        try:
            streams = api.fetch_channel_streams(channel_id)
            if not streams:
                result_status = "no_streams"
            else:
                streams_override = _prepare_streams_override(streams, channel_id, stream_provider_map)
                config.set("filters", "specific_channel_ids", [channel_id])
                config.set("filters", "channel_group_ids", [])

                fetch_streams(
                    api,
                    config,
                    stream_provider_map_override=stream_provider_map,
                    channels_override=[channel],
                    streams_override=streams_override,
                )
                analyze_streams(config, api=api, apply_reorder=False)
                score_streams(api, config, update_stats=False)

                scored_df = pd.read_csv(config.resolve_path("csv/05_iptv_streams_scored_sorted.csv"))
                scored_df["channel_id"] = pd.to_numeric(scored_df["channel_id"], errors="coerce")
                scored_df = scored_df[scored_df["channel_id"] == channel_id]
                if scored_df.empty:
                    result_status = "no_scored_streams"
                else:
                    scored_records = scored_df.to_dict("records")
                    scored_lookup = {}
                    for record in scored_records:
                        sid = record.get("stream_id")
                        if sid in (None, "N/A"):
                            continue
                        try:
                            sid_key = int(sid)
                        except Exception:
                            sid_key = sid
                        scored_lookup[sid_key] = record

                    records_for_ordering = _build_records_for_ordering(streams, scored_lookup)
                    ordering_details = _order_channel_streams(records_for_ordering, config)
                    ordered_ids = ordering_details.get("ordered_ids", [])
                    if not ordered_ids:
                        result_status = "no_ordered_streams"
                    else:
                        current_ids = {stream.get("id") for stream in streams}
                        ordered_ids = [sid for sid in ordered_ids if sid in current_ids]
                        if not ordered_ids:
                            result_status = "no_valid_ordered_streams"
                        else:
                            for record in records_for_ordering:
                                reason = _status_failure_reason(record)
                                if reason:
                                    stream_id = record.get("stream_id")
                                    stream_url = record.get("stream_url")
                                    stream_key = build_stream_key(stream_id, stream_url)
                                    provider_key = build_provider_key(record)
                                    state.record_stream_failure(
                                        stream_key,
                                        reason,
                                        args.stream_cooldown_minutes * 60,
                                    )
                                    if state.record_provider_failure(
                                        provider_key,
                                        reason,
                                        args.provider_cooldown_minutes * 60,
                                        args.provider_fail_threshold,
                                        args.provider_fail_window_seconds,
                                    ):
                                        providers_marked_down.add(provider_key)
                                        provider_cooldown_applied = True

                            slot1_candidates_checked = 0
                            for candidate_id in ordered_ids:
                                if slot1_candidates_checked >= args.slot1_check_limit:
                                    break
                                record = scored_lookup.get(candidate_id) or {}
                                stream_url = record.get("stream_url")
                                if not stream_url:
                                    stream_obj = next((s for s in streams if s.get("id") == candidate_id), {})
                                    stream_url = stream_obj.get("url")
                                stream_key = build_stream_key(candidate_id, stream_url)
                                provider_key = build_provider_key({**record, "stream_url": stream_url})
                                if state.is_stream_in_cooldown(stream_key):
                                    skipped_due_cooldown += 1
                                    continue
                                if state.is_provider_in_cooldown(provider_key):
                                    skipped_due_cooldown += 1
                                    continue
                                slot1_candidates_checked += 1
                                ok, reason = playable_now_check(stream_url, args.fast_check_timeout_seconds)
                                if ok:
                                    slot1_selected = candidate_id
                                    break
                                state.record_stream_failure(
                                    stream_key,
                                    f"playable_now:{reason}",
                                    args.stream_cooldown_minutes * 60,
                                )
                                if state.record_provider_failure(
                                    provider_key,
                                    f"playable_now:{reason}",
                                    args.provider_cooldown_minutes * 60,
                                    args.provider_fail_threshold,
                                    args.provider_fail_window_seconds,
                                ):
                                    providers_marked_down.add(provider_key)
                                    provider_cooldown_applied = True

                            if slot1_selected is None:
                                no_playable = True
                                slot1_selected = ordered_ids[0]
                                result_status = "no_playable_candidates"

                            final_order = [slot1_selected] + [sid for sid in ordered_ids if sid != slot1_selected]
                            slot1_changed = bool(ordered_ids) and ordered_ids[0] != slot1_selected

                            api.update_channel_streams(channel_id, final_order)

        except Exception as exc:
            logging.exception("Channel %s failed: %s", channel_id, exc)
            result_status = "error"

        duration_seconds = int(time.time() - start_time)
        next_eligible_at = int(time.time()) + 60
        state.update_channel_state(
            channel_id,
            result_status,
            duration_seconds,
            next_eligible_at,
            str(slot1_selected) if slot1_selected is not None else None,
        )

        processed_channels += 1
        remaining_ids = [cid for cid in remaining_ids if cid != channel_id]

        logging.info(
            "Channel %s result=%s duration=%ss slot1_changed=%s provider_cooldown=%s",
            channel_id,
            result_status,
            duration_seconds,
            slot1_changed,
            provider_cooldown_applied,
        )

        summary["processed_channels"].append(
            {
                "channel_id": channel_id,
                "status": result_status,
                "duration_seconds": duration_seconds,
                "slot1_changed": slot1_changed,
                "slot1_selected": slot1_selected,
                "provider_cooldown_applied": provider_cooldown_applied,
                "no_playable_candidates": no_playable,
            }
        )

    summary["channels_skipped_due_to_cooldown"] = skipped_due_cooldown
    summary["providers_marked_down"] = sorted(providers_marked_down)

    benchmarks_dir = Path("benchmarks")
    benchmarks_dir.mkdir(parents=True, exist_ok=True)
    summary_path = benchmarks_dir / f"windowed_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.write_text(serialize_summary(summary))

    logging.info(
        "Run complete: processed=%d remaining=%d summary=%s",
        processed_channels,
        len(remaining_ids),
        summary_path,
    )


if __name__ == "__main__":
    run_windowed_batch()
