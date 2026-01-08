from datetime import timezone

from provider_usage import (
    aggregate_provider_usage,
    aggregate_provider_usage_detailed,
    parse_npm_access_line,
)


def test_parse_npm_access_line_and_extract_stream_id():
    line = (
        '[21/Dec/2025:17:02:33 +0000] - 200 200 - GET http tv.example.com '
        '"/live/user/pass/93.ts" [Client 188.28.40.77] [Length 1604072] [Gzip -] '
        '[Sent-to 46.62.199.187] "TiviMate/5.2.0 (Android 11)" "-"'
    )
    ev = parse_npm_access_line(line)
    assert ev is not None
    assert ev.status == 200
    assert ev.method == "GET"
    assert ev.host == "tv.example.com"
    assert ev.path == "/live/user/pass/93.ts"
    assert ev.bytes_sent == 1604072
    assert ev.client_ip == "188.28.40.77"
    assert "TiviMate" in (ev.user_agent or "")
    assert ev.ts.tzinfo is not None
    assert ev.ts.tzinfo.utcoffset(ev.ts) == timezone.utc.utcoffset(ev.ts)
    assert ev.is_playback() is True
    assert ev.extract_stream_id() == 93


def test_aggregate_provider_usage_basic():
    lines = [
        '[21/Dec/2025:17:02:33 +0000] - 200 200 - GET http tv.example.com "/live/u/p/93.ts" [Client 1.1.1.1] [Length 10] "UA" "-"',
        '[21/Dec/2025:17:02:34 +0000] - 200 200 - GET http tv.example.com "/live/u/p/93.ts" [Client 1.1.1.1] [Length 20] "UA" "-"',
        '[21/Dec/2025:17:02:35 +0000] - 502 502 - GET http tv.example.com "/live/u/p/5.ts" [Client 1.1.1.1] [Length 5] "UA" "-"',
        # Non-playback should be ignored by the caller when playback_only=True; here we just
        # ensure it doesn't blow up if included.
        '[21/Dec/2025:17:02:36 +0000] - 200 200 - GET http tv.example.com "/player_api.php?username=u&password=p" [Client 1.1.1.1] [Length 123] "UA" "-"',
    ]

    events = [parse_npm_access_line(l) for l in lines]
    events = [e for e in events if e is not None]

    # Filter to playback like the API does
    events = [e for e in events if e.is_playback()]

    mapping = {93: "1", 5: "2"}
    usage = aggregate_provider_usage(events, mapping)

    assert usage["1"]["requests"] == 2
    assert usage["1"]["bytes_sent"] == 30
    assert usage["1"]["ok_requests"] == 2
    assert usage["1"]["error_requests"] == 0
    assert usage["1"]["unique_streams"] == 1

    assert usage["2"]["requests"] == 1
    assert usage["2"]["bytes_sent"] == 5
    assert usage["2"]["ok_requests"] == 0
    assert usage["2"]["error_requests"] == 1
    assert usage["2"]["unique_streams"] == 1


def test_aggregate_provider_usage_detailed_sessions_and_unknown_diagnostics():
    # Two requests close together => 1 session; a later request => new session.
    lines = [
        '[21/Dec/2025:17:02:00 +0000] - 200 200 - GET http tv.example.com "/live/u/p/93.ts" [Client 1.1.1.1] [Length 10] "UA" "-"',
        '[21/Dec/2025:17:02:10 +0000] - 200 200 - GET http tv.example.com "/live/u/p/93.ts" [Client 1.1.1.1] [Length 20] "UA" "-"',
        '[21/Dec/2025:17:03:00 +0000] - 200 200 - GET http tv.example.com "/live/u/p/93.ts" [Client 1.1.1.1] [Length 30] "UA" "-"',
        # Different client should count as a separate session for the same stream.
        '[21/Dec/2025:17:03:05 +0000] - 200 200 - GET http tv.example.com "/live/u/p/93.ts" [Client 2.2.2.2] [Length 40] "UA" "-"',
        # Unmapped stream id => unknown bucket
        '[21/Dec/2025:17:03:10 +0000] - 200 200 - GET http tv.example.com "/live/u/p/999.ts" [Client 1.1.1.1] [Length 1] "UA" "-"',
    ]

    events = [parse_npm_access_line(l) for l in lines]
    events = [e for e in events if e is not None and e.is_playback()]
    mapping = {93: "1"}  # 999 intentionally missing

    usage, diag = aggregate_provider_usage_detailed(events, mapping, session_gap_seconds=30)

    assert usage["1"]["requests"] == 4
    # sessions: (1.1.1.1,93) => 2 sessions (gap 50s), (2.2.2.2,93) => 1 session
    assert usage["1"]["sessions"] == 3
    assert usage["1"]["unique_clients"] == 2
    assert usage["1"]["bytes_sent"] == 100

    assert usage["unknown"]["requests"] == 1
    assert diag["unmapped_stream_requests"] == 1
    assert any(int(x.get("stream_id")) == 999 for x in diag.get("unmapped_stream_ids_top", []))

