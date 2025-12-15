# Dispatcharr Maid

Automated IPTV stream quality analysis and management for Dispatcharr.

## What It Does

Dispatcharr Maid analyzes your IPTV streams using ffmpeg to determine quality metrics, scores them, and automatically reorders them so the best streams appear first in your channel list.

**Key Features:**
- 🔍 Analyzes streams for bitrate, resolution, FPS, codec, and errors
- 📊 Scores streams based on multiple quality factors  
- 🎯 Automatically reorders streams (best first)
- ♻️ Resume capability - interrupted runs continue where they left off
- 📈 Live progress tracking with ETA
- 🧹 Smart cleanup - keeps only the best stream per provider per channel
- 📋 Detailed summary reports

## Architecture

This is a **complete refactoring** of the original Dispatcharr-Utilities project with major improvements:

### Old Architecture (Dispatcharr-Utilities):
- `interactive_stream_sorter.py` → spawns → `dispatcharr-stream-sorter.py` via subprocess
- No progress visibility during long runs
- No resume capability
- Code duplication between scripts

### New Architecture (Dispatcharr-Maid):
```
interactive_maid.py         ← What you run (interactive UI)
        ↓ (imports)
stream_analysis.py          ← Core analysis library
        ↓ (imports)
api_utils.py                ← Dispatcharr API client
```

**Benefits:**
- ✅ Direct function calls (no subprocess overhead)
- ✅ Live progress bars with ETA
- ✅ Resume interrupted runs automatically
- ✅ Better error handling
- ✅ Single source of truth for all logic
- ✅ Can run independently

## Installation

1. **Clone or download this folder**

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Ensure ffmpeg and ffprobe are installed:**
   ```bash
   # Ubuntu/Debian
   sudo apt install ffmpeg
   
   # macOS
   brew install ffmpeg
   
   # Verify installation
   ffmpeg -version
   ffprobe -version
   ```

4. **Create your .env file:**
   ```bash
   cp .env.template .env
   nano .env  # Edit with your Dispatcharr credentials
   ```

   Example `.env`:
   ```ini
   DISPATCHARR_BASE_URL=http://192.168.1.100:9191
   DISPATCHARR_USER=admin
   DISPATCHARR_PASS=your_password
   DISPATCHARR_TOKEN=
   ```

## Usage

Simply run:
```bash
python3 interactive_maid.py
```

### Interactive Menu

```
======================================================================
DISPATCHARR MAID - Interactive Stream Manager
======================================================================

DISPATCHARR CHANNEL GROUPS
======================================================================
 1. UK TV                                     ( 45 channels) [ID: 835]
 2. US TV                                     ( 78 channels) [ID: 842]
 3. Sports                                    ( 23 channels) [ID: 851]
======================================================================

Selection Options:
  Enter numbers (e.g., '1,3,5')
  Enter range (e.g., '1-4')
  Enter 'all' for all groups
  Enter 'q' to quit

Your selection: 1,3

WHAT WOULD YOU LIKE TO DO?
======================================================================
1. Full pipeline + cleanup (fetch → analyze → score → reorder → cleanup)
2. Full pipeline (fetch → analyze → score → reorder)
3. Fetch only
4. Analyze only
5. Score only
6. Reorder only
7. Cleanup only (keep top stream per provider)
8. Generate summary report
9. Change group selection
0. Exit
```

### Typical Workflow

**First time:**
1. Select option `1` or `2` to run the full pipeline

**Subsequent runs:**
- Option `4` to re-analyze only (if you added new channels)
- Option `5` to re-score (if you changed scoring parameters)
- Option `7` to cleanup duplicates

## How It Works

### 1. Fetch
Downloads channel and stream metadata from Dispatcharr API.

**Outputs:**
- `csv/00_channel_groups.csv` - List of all groups
- `csv/01_channels_metadata.csv` - Channel details
- `csv/02_grouped_channel_streams.csv` - All streams with URLs

### 2. Analyze
Uses ffmpeg to test each stream and collect quality metrics:
- **Video codec** (h264, hevc, etc.)
- **Audio codec** (aac, mp3, etc.)
- **Resolution** (1920x1080, 1280x720, etc.)
- **FPS** (frames per second)
- **Bitrate** (kbps)
- **Interlacing** (progressive vs interlaced)
- **Frame drops** (decode errors)
- **Critical errors** (decode errors, discontinuities, timeouts)

**Features:**
- Progress bar with ETA: `[1247/5432] Failed: 23 | Rate: 2.3/s | ETA: 0:30:23`
- Resume capability (checkpoint every 10 streams)
- Skips recently analyzed streams (configurable via `stream_last_measured_days`)
- One connection per provider at a time (prevents hammering)

**Outputs:**
- `csv/03_iptv_stream_measurements.csv` - Full analysis results
- `csv/04_fails.csv` - Failed streams only
- `logs/checkpoint.json` - Resume checkpoint (auto-deleted on completion)

### 3. Score
Calculates a quality score for each stream based on:

```
Score = Bitrate Score + Resolution Score + FPS Bonus 
        - Dropped Frames Penalty - Error Penalty

Where:
  - Bitrate Score: Relative bitrate compared to other streams (0-100+)
  - Resolution Score: 4K=100, 1080p=80, 720p=50, 540p=20
  - FPS Bonus: +100 if FPS ≥ 50
  - HEVC Boost: HEVC bitrate multiplied by 1.5 (more efficient codec)
  - Dropped Frames Penalty: % of dropped frames * 1
  - Error Penalty: 25 points per critical error
```

**Outputs:**
- `csv/05_iptv_streams_scored_sorted.csv` - Scored and sorted streams

**Optional:** Updates stream stats on Dispatcharr server (resolution, bitrate, codecs)

### 4. Reorder
Updates stream order in Dispatcharr so the highest-scoring streams appear first.

**For each channel:**
1. Fetch current streams from API
2. Reorder based on scores (highest first)
3. Preserve any new streams not in CSV
4. Update channel via API

### 5. Cleanup (Optional)
Removes duplicate streams from each channel, keeping only the **top-ranked stream per provider**.

**Example:**
```
Channel: BBC One
  Provider 1: 3 streams → Keep top 1, remove 2
  Provider 2: 2 streams → Keep top 1, remove 1
  Provider 3: 1 stream → Keep it
Result: 3 streams total (1 per provider)
```

This ensures diversity (multiple providers for failover) while eliminating low-quality duplicates.

## Configuration

Edit `config.yaml` to customize behavior:

```yaml
analysis:
  duration: 10           # Seconds to test each stream
  workers: 8             # Concurrent analysis threads
  timeout: 30            # Timeout per operation
  retries: 1             # Retry attempts for failed streams

scoring:
  fps_bonus_points: 100  # Bonus for 50+ FPS streams
  hevc_boost: 1.5        # HEVC bitrate multiplier

filters:
  stream_last_measured_days: 1  # Skip recently analyzed streams
  remove_duplicates: true       # Remove duplicate URLs
```

### Quick Scan vs Thorough Scan

**Quick scan** (for testing):
```yaml
analysis:
  duration: 5
  idet_frames: 100
  workers: 16
  retries: 0
```

**Thorough scan** (for final run):
```yaml
analysis:
  duration: 20
  idet_frames: 1000
  workers: 4
  retries: 2
```

## Resume Capability

If analysis is interrupted (Ctrl+C, SSH disconnect, crash):

1. **Checkpoint saved** every 10 streams to `logs/checkpoint.json`
2. **Re-run** `interactive_maid.py` → choose option `4` (Analyze)
3. **Automatically resumes** from where it left off

Example:
```
Found checkpoint with 1,247 processed streams
Resuming with 4,185 remaining streams
```

## Progress Tracking

During analysis, you'll see:
```
[1247/5432] Failed: 23 | Rate: 2.3/s | ETA: 0:30:23 | 23.0%
```

Where:
- `1247/5432` = Streams processed / Total streams
- `Failed: 23` = Number of failed streams
- `Rate: 2.3/s` = Streams analyzed per second
- `ETA: 0:30:23` = Estimated time remaining

## Summary Reports

After analysis completes:

```
======================================================================
ANALYSIS SUMMARY
======================================================================
Total Streams Analyzed: 5,432
Successful: 5,187 (95.5%)
Failed: 245 (4.5%)

Top 5 Providers by Success Count:
  provider1.example.com: 1,230/1,234 (99.7% success)
  provider2.example.com: 980/987 (99.3% success)
  ...

Problematic Providers (>10% failure rate):
  badprovider.example.com: 123/456 (73.0% failure rate)
  ...
======================================================================
```

## Troubleshooting

### "ffmpeg or ffprobe not found"
Install ffmpeg: `sudo apt install ffmpeg` (Ubuntu) or `brew install ffmpeg` (macOS)

### "Login failed"
Check your `.env` file credentials. Ensure `DISPATCHARR_BASE_URL` includes `http://` and port.

### Analysis is very slow
- Reduce `workers` in `config.yaml` (too many workers can overload providers)
- Increase `duration` for more thorough testing (but slower)
- Check your internet connection

### Many streams failing
- Check `csv/04_fails.csv` to see error patterns
- Some providers may be offline or have poor connectivity
- Increase `timeout` and `retries` in `config.yaml`

### Want to re-analyze everything
Set `stream_last_measured_days: 0` in `config.yaml` to disable skipping recent streams.

## Differences from Original Dispatcharr-Utilities

| Feature | Old (Utilities) | New (Maid) |
|---------|----------------|------------|
| Architecture | Subprocess wrapper | Direct imports |
| Progress visibility | None | Live with ETA |
| Resume capability | No | Yes (checkpoint every 10) |
| Code duplication | Yes (2 API clients) | No (single source) |
| Interactive menu | Basic | Enhanced |
| Summary reports | No | Yes |
| Error handling | Basic | Comprehensive |
| Configuration | INI file | YAML with validation |

## Files and Directories

```
Dispatcharr_Maid/
├── interactive_maid.py         # Main script - run this!
├── stream_analysis.py          # Core analysis library
├── api_utils.py                # Dispatcharr API client
├── config.yaml                 # Configuration
├── requirements.txt            # Python dependencies
├── .env.template               # Template for credentials
├── .env                        # Your credentials (create this)
├── README.md                   # This file
├── logs/                       # Log files and checkpoints
│   ├── checkpoint.json         # Resume checkpoint
│   └── ...
└── csv/                        # Data files
    ├── 00_channel_groups.csv
    ├── 01_channels_metadata.csv
    ├── 02_grouped_channel_streams.csv
    ├── 03_iptv_stream_measurements.csv
    ├── 04_fails.csv
    └── 05_iptv_streams_scored_sorted.csv
```

## Advanced Usage

### Python API

You can import and use the functions directly in your own scripts:

```python
from api_utils import DispatcharrAPI
from stream_analysis import Config, fetch_streams, analyze_streams

api = DispatcharrAPI()
api.login()

config = Config('config.yaml')
config.set('filters', 'channel_group_ids', [835, 842])

fetch_streams(api, config)
analyze_streams(config, progress_callback=my_progress_function)
```

### Custom Progress Callback

```python
def my_progress(stats):
    """Custom progress handler"""
    print(f"Progress: {stats['percent']:.1f}% - {stats['processed']}/{stats['total']}")
    # Send to webhook, update database, etc.

analyze_streams(config, progress_callback=my_progress)
```

## License

Same as original Dispatcharr-Utilities project.

## Credits

Refactored and enhanced version of Dispatcharr-Utilities.

Original concept and core analysis logic from the Dispatcharr-Utilities project.
