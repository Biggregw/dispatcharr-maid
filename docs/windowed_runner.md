# Windowed Nightly Runner

## Overview
The windowed runner is a manual CLI that analyzes channels within a local-time window (for example 01:00–05:00), persists progress in SQLite, and resumes with remaining channels on subsequent runs. It updates ordering **only** for channels that were analyzed during that window, so ordering converges gradually over time.

## Usage

```bash
python3 tools/run_windowed_batch.py \
  --base-url http://localhost:8000 \
  --groups 1,2,3 \
  --window-from 01:00 \
  --window-to 05:00 \
  --profile deep \
  --max-channels-per-run 0 \
  --slot1-check-limit 3 \
  --stream-cooldown-minutes 15 \
  --provider-cooldown-minutes 30 \
  --provider-fail-threshold 3 \
  --provider-fail-window-seconds 120 \
  --priority "stale,failures,favourites"
```

### Common options
- `--groups` / `--all-groups`: Select which channel groups to analyze.
- `--channels`: Analyze only these channel IDs (ad hoc run).
- `--window-from` / `--window-to`: Local window boundaries (HH:MM).
- `--max-channels-per-run`: Cap the number of channels (0 = unlimited until deadline).
- `--slot1-check-limit`: Maximum candidates to verify for slot 1.
- `--priority`: Priority boost list (default: `stale,failures,favourites`).
- `--favourite-channels`: Comma-separated list of channel IDs to boost when `favourites` is enabled.

## Carry-over behavior
State is persisted to `data/windowed_runner.sqlite` and updated after each channel. The selector prioritizes:
1. Channels never checked.
2. Oldest `last_checked_at` first.
3. Boosts for failure statuses and favourites (if enabled).
4. `next_eligible_at` gates rapid reprocessing within the same run.

## Slot 1 playable-now gate
After ordering a channel’s streams, the runner runs a fast playable-now check on the slot 1 candidate. If it fails, it tries the next candidate (up to `--slot1-check-limit`). The first passing stream becomes slot 1, while the remaining order is preserved. If none pass, the scored order is preserved and the channel run is marked as `no_playable_candidates`.

## Cooldowns
Failures create cooldowns to keep downed providers/streams out of slot 1 for a short period.

- **Stream cooldown:** Failing streams cannot become slot 1 until `--stream-cooldown-minutes` expires.
- **Provider cooldown:** If multiple streams from the same provider fail within `--provider-fail-window-seconds` and exceed `--provider-fail-threshold`, the provider enters cooldown for `--provider-cooldown-minutes`.

### Provider identification
The runner prefers Dispatcharr’s `m3u_account` (provider ID). If it is missing, it falls back to the provider name or the URL host (`host:<hostname>`) as a deterministic key.

## Ad hoc single-channel run
Run a single channel by ID:

```bash
python3 tools/run_windowed_batch.py \
  --base-url http://localhost:8000 \
  --channels 42 \
  --window-from 01:00 \
  --window-to 05:00
```

## Reporting
Each run prints per-channel status (duration, slot-1 change, provider cooldown). A JSON summary is written to `benchmarks/windowed_run_YYYYMMDD_HHMMSS.json` with processed channel details, cooldown skips, and providers marked down.
