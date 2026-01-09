"""
stream_analysis.py
Core stream analysis library for Dispatcharr-Scout
"""

import csv
import json
import logging
import math
import os
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import hashlib

import pandas as pd
import yaml

from api_utils import DispatcharrAPI
from provider_data import refresh_provider_data


# Progress tracking
class ProgressTracker:
    """Track analysis progress with ETA and resumability"""

    def __init__(self, total_streams, checkpoint_file, use_checkpoint=True):
        self.total = total_streams
        # If resuming from a checkpoint, treat already-processed IDs as progress
        # so UIs/CLI progress bars don't look "stuck" at 0%.
        self.processed = 0
        self.failed = 0
        self.start_time = time.time()
        self.checkpoint_file = checkpoint_file
        self.use_checkpoint = use_checkpoint
        self.processed_ids = self.load_checkpoint() if use_checkpoint else set()
        if self.processed_ids:
            self.processed = len(self.processed_ids)
        self.lock = threading.Lock()
    
    def load_checkpoint(self):
        """Load previously processed stream IDs"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and isinstance(data.get('processed_ids'), list):
                        processed_ids = set()
                        for sid in data.get('processed_ids', []):
                            normalized = self._normalize_stream_id(sid)
                            if normalized is not None:
                                processed_ids.add(normalized)
                        return processed_ids
                    logging.warning("Invalid checkpoint structure in %s; ignoring checkpoint", self.checkpoint_file)
            except Exception:
                logging.warning("Failed to load checkpoint from %s; ignoring checkpoint", self.checkpoint_file)
        return set()
    
    def save_checkpoint(self):
        """Save current progress"""
        if not self.use_checkpoint:
            return
        Path(self.checkpoint_file).parent.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_file, 'w') as f:
            json.dump({
                'processed_ids': list(self.processed_ids),
                'timestamp': datetime.now().isoformat(),
                'processed_count': self.processed,
                'failed_count': self.failed
            }, f, indent=2)
    
    def mark_processed(self, stream_id, success=True):
        """Mark a stream as processed"""
        with self.lock:
            self.processed += 1
            if not success:
                self.failed += 1
            normalized_id = self._normalize_stream_id(stream_id)
            if normalized_id is not None:
                self.processed_ids.add(normalized_id)
            
            # Checkpoint every 10 streams
            if self.processed % 10 == 0:
                self.save_checkpoint()
    
    def is_processed(self, stream_id):
        """Check if stream was already processed"""
        normalized_id = self._normalize_stream_id(stream_id)
        return normalized_id in self.processed_ids if normalized_id is not None else False

    def _normalize_stream_id(self, stream_id):
        """Normalize stream IDs for consistent comparison"""
        if stream_id is None:
            return None
        try:
            return str(int(stream_id))
        except (TypeError, ValueError):
            return str(stream_id)
    
    def get_progress(self):
        """Get current progress statistics"""
        with self.lock:
            elapsed = time.time() - self.start_time
            rate = self.processed / elapsed if elapsed > 0 else 0
            remaining = self.total - self.processed
            eta_seconds = remaining / rate if rate > 0 else 0
            
            return {
                'processed': self.processed,
                'total': self.total,
                'failed': self.failed,
                'rate': rate,
                'eta_seconds': int(eta_seconds),
                'percent': (self.processed / self.total * 100) if self.total > 0 else 0
            }
    
    def print_progress(self):
        """Display progress bar"""
        stats = self.get_progress()
        print(f"\r[{stats['processed']}/{stats['total']}] "
              f"Failed: {stats['failed']} | "
              f"Rate: {stats['rate']:.1f}/s | "
              f"ETA: {timedelta(seconds=stats['eta_seconds'])} | "
              f"{stats['percent']:.1f}%", 
              end='', flush=True)
    
    def clear_checkpoint(self):
        """Clear checkpoint file after successful completion"""
        if not self.use_checkpoint:
            return
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)


# Configuration
class Config:
    """Configuration management"""

    def __init__(self, config_file='config.yaml', working_dir=None):
        self.working_dir = Path(working_dir) if working_dir else Path(config_file).parent
        self.config_file = Path(self.working_dir, Path(config_file).name)
        self.config = self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_file):
            # Create default config
            default_config = self._get_default_config()
            logging.warning(
                "Config file %s not found. Auto-created with defaults; please review config.yaml.example.",
                self.config_file,
            )
            with open(self.config_file, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            return default_config
        
        with open(self.config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def _get_default_config(self):
        """Get default configuration"""
        return {
            'analysis': {
                'duration': 10,
                'idet_frames': 500,
                'retries': 1,
                'retry_delay': 10,
                'timeout': 30,
                'workers': 8,
            },
            'scoring': {
                'proxy_first': True,
                'fps_bonus_points': 100,
                'hevc_boost': 1.5,
                'resolution_scores': {
                    '3840x2160': 100,
                    '1920x1080': 80,
                    '1280x720': 50,
                    '960x540': 20,
                },
            },
            'filters': {
                'channel_group_ids': [],
                'remove_duplicates': True,
                'specific_channel_ids': [],
                'channel_name_regex': '',
                'channel_number_regex': '',
                'stream_last_measured_days': 0,
            },
            'ordering': {
                'resilience_mode': False,
                'fallback_depth': 3,
                'similar_score_delta': 5,
            },
            'dispatcharr': {
                'm3u_accounts_endpoint': '/api/channels/m3u-accounts/',
                'manage_py_path': '',
                'container_name': 'dispatcharr',
                'refresh_provider_data': False,
            },
            'web': {
                'results_retention_days': 0,
            },
            'usage': {
                'access_log_dir': '/app/npm_logs',
                'access_log_glob': 'proxy-host-*_access.log*',
                'lookback_days': 7,
                'session_gap_seconds': 30,
            },
        }
    
    def get(self, section, key=None, default=None):
        """Get configuration value"""
        if key is None:
            return self.config.get(section, default if default is not None else {})
        section_data = self.config.get(section, {})
        if not isinstance(section_data, dict):
            return default
        return section_data.get(key, default)
    
    def set(self, section, key, value):
        """Set configuration value"""
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
    
    def save(self):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)

    def resolve_path(self, relative_path):
        """Resolve a path within the working directory"""
        return str(Path(self.working_dir, relative_path))


# Provider rate limiting
provider_semaphores = {}
semaphore_lock = threading.Lock()


def _get_provider_from_url(url):
    """Extract provider identifier from URL"""
    try:
        return urlparse(url).netloc
    except:
        return "unknown_provider"


def _get_provider_semaphore(provider):
    """Get semaphore for provider (1 concurrent connection per provider)"""
    with semaphore_lock:
        if provider not in provider_semaphores:
            provider_semaphores[provider] = threading.Semaphore(1)
        return provider_semaphores[provider]


# FFmpeg utilities
def _check_ffmpeg_installed():
    """Check if ffmpeg and ffprobe are installed"""
    try:
        subprocess.run(['ffmpeg', '-h'], capture_output=True, check=True)
        subprocess.run(['ffprobe', '-h'], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        logging.error("ffmpeg or ffprobe not found in PATH")
        return False


def _get_stream_info(url, timeout):
    """Get stream information using ffprobe"""
    command = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'stream=codec_name,width,height,avg_frame_rate',
        '-of', 'json',
        url
    ]
    try:
        result = subprocess.run(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            timeout=timeout, 
            text=True
        )
        if result.stdout:
            data = json.loads(result.stdout)
            return data.get('streams', [])
        return []
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        logging.debug(f"Stream info check failed: {e}")
        return []


def _check_interlaced_status(url, stream_name, idet_frames, timeout):
    """Check if video stream is interlaced"""
    command = [
        'ffmpeg', '-user_agent', 'VLC/3.0.14',
        '-analyzeduration', '5000000', '-probesize', '5000000',
        '-i', url, '-vf', 'idet', '-frames:v', str(idet_frames), 
        '-an', '-f', 'null', '/dev/null'
    ]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        output = result.stderr
        
        interlaced_frames = 0
        progressive_frames = 0
        
        for line in output.splitlines():
            if "Single frame detection:" in line or "Multi frame detection:" in line:
                tff = re.search(r'TFF:\s*(\d+)', line)
                bff = re.search(r'BFF:\s*(\d+)', line)
                prog = re.search(r'Progressive:\s*(\d+)', line)
                
                if tff: interlaced_frames += int(tff.group(1))
                if bff: interlaced_frames += int(bff.group(1))
                if prog: progressive_frames += int(prog.group(1))
        
        if interlaced_frames > progressive_frames:
            return "INTERLACED"
        elif progressive_frames > interlaced_frames:
            return "PROGRESSIVE"
        else:
            return "UNKNOWN"
            
    except subprocess.TimeoutExpired:
        return "UNKNOWN (Timeout)"
    except Exception as e:
        logging.debug(f"Interlace check failed: {e}")
        return "UNKNOWN (Error)"


def _get_bitrate_and_frame_stats(url, duration, timeout):
    """Get bitrate and frame statistics using ffmpeg"""
    command = [
        'ffmpeg', '-re', '-v', 'debug', '-user_agent', 'VLC/3.0.14',
        '-i', url, '-t', str(duration), '-f', 'null', '-'
    ]
    
    bitrate = "N/A"
    frames_decoded = "N/A"
    frames_dropped = "N/A"
    elapsed = 0
    status = "OK"
    
    try:
        start = time.time()
        result = subprocess.run(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            timeout=timeout, 
            text=True
        )
        elapsed = time.time() - start
        output = result.stderr
        
        # Parse bitrate from statistics
        for line in output.splitlines():
            if "Statistics:" in line and "bytes read" in line:
                try:
                    parts = line.split("bytes read")
                    size_str = parts[0].strip().split()[-1]
                    total_bytes = int(size_str)
                    if total_bytes > 0 and duration > 0:
                        bitrate = (total_bytes * 8) / 1000 / duration
                except ValueError:
                    pass
            
            if "Input stream #" in line and "frames decoded;" in line:
                decoded = re.search(r'(\d+)\s*frames decoded', line)
                errors = re.search(r'(\d+)\s*decode errors', line)
                if decoded: frames_decoded = int(decoded.group(1))
                if errors: frames_dropped = int(errors.group(1))
    
    except subprocess.TimeoutExpired:
        status = "Timeout"
        elapsed = timeout
    except Exception as e:
        logging.debug(f"Bitrate check failed: {e}")
        status = "Error"
    
    return bitrate, frames_decoded, frames_dropped, status, elapsed


def _check_stream_for_critical_errors(url, timeout):
    """Check for critical provider-side errors"""
    command = [
        'ffmpeg',
        '-probesize', '500000', '-analyzeduration', '1000000',
        '-fflags', '+genpts+discardcorrupt', '-flags', 'low_delay',
        '-flush_packets', '1', '-avoid_negative_ts', 'make_zero',
        '-timeout', '5000000', '-rw_timeout', '5000000',
        '-i', url,
        '-t', '20',
        '-map', '0:v:0', '-map', '0:a:0?', '-map', '0:s?',
        '-c:v', 'libx265', '-preset', 'veryfast',
        '-c:a', 'copy',
        '-f', 'null', '-'
    ]
    
    errors = {
        'err_decode': False,
        'err_discontinuity': False,
        'err_timeout': False
    }
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        stderr = result.stderr
        
        if "decode_slice_header error" in stderr:
            errors['err_decode'] = True
        if "timestamp discontinuity" in stderr:
            errors['err_discontinuity'] = True
        if "Connection timed out" in stderr:
            errors['err_timeout'] = True
    
    except subprocess.TimeoutExpired:
        errors['err_timeout'] = True
    except Exception:
        pass
    
    return errors


def _analyze_stream_task(row, config, progress_tracker=None, force_full_analysis=False):
    """Analyze a single stream"""
    url = row.get('stream_url')
    stream_name = row.get('stream_name', 'Unknown')
    stream_id = row.get('stream_id')
    
    if not url:
        return row
    
    # Check if already processed
    if progress_tracker and progress_tracker.is_processed(stream_id) and not force_full_analysis:
        logging.debug(f"Skipping already processed stream: {stream_name}")
        return None
    
    analysis_cfg = config.get('analysis') or {}
    duration = analysis_cfg.get('duration', 10)
    idet_frames = analysis_cfg.get('idet_frames', 500)
    timeout = analysis_cfg.get('timeout', 30)
    retries = analysis_cfg.get('retries', 1)
    retry_delay = analysis_cfg.get('retry_delay', 10)
    
    provider = _get_provider_from_url(url)
    provider_semaphore = _get_provider_semaphore(provider)
    
    with provider_semaphore:
        for attempt in range(retries + 1):
            # Initialize fields
            row['timestamp'] = datetime.now().isoformat()
            row['video_codec'] = 'N/A'
            row['audio_codec'] = 'N/A'
            row['resolution'] = 'N/A'
            row['fps'] = 'N/A'
            row['interlaced_status'] = 'N/A'
            row['bitrate_kbps'] = 'N/A'
            row['frames_decoded'] = 'N/A'
            row['frames_dropped'] = 'N/A'
            row['status'] = 'N/A'
            
            # 1. Get codec info
            streams_info = _get_stream_info(url, timeout)
            video_info = next((s for s in streams_info if 'width' in s), None)
            audio_info = next((s for s in streams_info if 'codec_name' in s and 'width' not in s), None)
            
            if video_info:
                row['video_codec'] = video_info.get('codec_name')
                row['resolution'] = f"{video_info.get('width')}x{video_info.get('height')}"
                fps_str = video_info.get('avg_frame_rate', '0/1')
                try:
                    num, den = map(int, fps_str.split('/'))
                    row['fps'] = round(num / den, 2) if den != 0 else 0
                except (ValueError, ZeroDivisionError):
                    row['fps'] = 0
            
            if audio_info:
                row['audio_codec'] = audio_info.get('codec_name')
            
            # 2. Get bitrate and frame stats
            bitrate, frames_decoded, frames_dropped, status, elapsed = \
                _get_bitrate_and_frame_stats(url, duration, timeout)
            
            row['bitrate_kbps'] = bitrate
            row['frames_decoded'] = frames_decoded
            row['frames_dropped'] = frames_dropped
            row['status'] = status
            
            # 3. Check interlacing if stream is OK
            if status == "OK":
                row['interlaced_status'] = _check_interlaced_status(url, stream_name, idet_frames, timeout)

            # 4. Check for critical errors
            critical_errors = _check_stream_for_critical_errors(url, timeout)
            row.update(critical_errors)
            
            # If OK, break retry loop
            if status == "OK":
                break
            
            # Retry if not last attempt
            if attempt < retries:
                logging.warning(f"Stream '{stream_name}' failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
        
        # Mark as processed
        if progress_tracker:
            progress_tracker.mark_processed(stream_id, status == "OK")
        
        # Respect ffmpeg duration to avoid hammering provider
        if isinstance(elapsed, (int, float)) and elapsed < duration:
            time.sleep(duration - elapsed)

    return row


# Main functions

def _log_raw_response(tag, response, config):
    """Persist raw API responses for auditing and debugging."""
    logs_dir = Path(config.resolve_path('logs'))
    logs_dir.mkdir(parents=True, exist_ok=True)
    raw_path = logs_dir / f"{tag}_raw_response.txt"
    raw_path.write_text(response.text, encoding="utf-8")
    logging.info("%s content-type: %s", tag, response.headers.get('Content-Type', ''))
    logging.info("%s raw response saved to %s", tag, raw_path)


def _fetch_stream_provider_map(api, config):
    """Fetch all streams once and build a stream_id -> m3u_account mapping."""
    stream_provider_map = {}
    next_url = '/api/channels/streams/?limit=100'
    page_index = 0

    while next_url:
        response = api.get_raw(next_url)
        if page_index == 0:
            _log_raw_response('streams_page_1', response, config)

        content_type = response.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            raise ValueError(
                f"Streams response is not JSON (Content-Type: {content_type})."
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError("Failed to parse streams JSON response.") from exc

        results = payload.get('results')
        if results is None:
            raise ValueError("Streams response missing 'results' field.")

        for stream in results:
            if not isinstance(stream, dict):
                continue
            stream_id = stream.get('id')
            m3u_account = stream.get('m3u_account')
            if stream_id is not None and m3u_account is not None:
                stream_provider_map[int(stream_id)] = m3u_account

        next_link = payload.get('next')
        if next_link:
            next_url = next_link.split('/api/')[-1]
            next_url = '/api/' + next_url
        else:
            next_url = None
        page_index += 1

    if not stream_provider_map:
        raise ValueError(
            "Dispatcharr streams response did not expose m3u_account IDs; "
            "cannot populate provider IDs at fetch time."
        )

    return stream_provider_map

def fetch_streams(api, config, output_file=None, progress_callback=None, stream_provider_map_override=None):
    """Fetch streams for channels based on filters.

    Optionally accepts:
      - progress_callback(dict): receives {'processed','total','failed'} updates.
      - stream_provider_map_override(dict[int,int|str]): stream_id -> m3u_account to avoid refetching.
    """
    logging.info("Fetching streams from Dispatcharr...")

    output_file = output_file or config.resolve_path('csv/02_grouped_channel_streams.csv')
    groups_file = config.resolve_path('csv/00_channel_groups.csv')
    metadata_file = config.resolve_path('csv/01_channels_metadata.csv')
    filters = config.get('filters') or {}
    group_ids_list = filters.get('channel_group_ids', [])
    specific_channel_ids = filters.get('specific_channel_ids')  # NEW: specific channels
    channel_name_regex = filters.get('channel_name_regex')
    channel_number_regex = filters.get('channel_number_regex')
    start_range = filters.get('start_channel', 1)
    end_range = filters.get('end_channel', 99999)
    
    dispatcharr_cfg = config.get('dispatcharr') or {}
    if dispatcharr_cfg.get('refresh_provider_data', False):
        refresh_provider_data(
            api,
            config,
            force=bool(dispatcharr_cfg.get('refresh_provider_data', False))
        )

    stream_provider_map = stream_provider_map_override or _fetch_stream_provider_map(api, config)

    # Fetch groups
    groups = api.fetch_channel_groups()
    logging.info(f"Found {len(groups)} channel groups")
    
    # Save groups
    Path(config.resolve_path('csv')).mkdir(parents=True, exist_ok=True)
    with open(groups_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name"])
        for group in groups:
            writer.writerow([group.get("id", ""), group.get("name", "")])
    
    # Fetch all channels
    all_channels = api.fetch_channels()
    logging.info(f"Found {len(all_channels)} total channels")
    
    # Filter channels
    if specific_channel_ids:
        # Use specific channel IDs if provided
        specific_ids_set = set(specific_channel_ids)
        filtered_channels = [
            ch for ch in all_channels 
            if ch.get('id') in specific_ids_set
        ]
        logging.info(f"Using {len(filtered_channels)} specific channels")
    else:
        # Start from group filter (if any), then optionally apply regex filters.
        if group_ids_list:
            target_group_ids = set(group_ids_list)
            filtered_channels = [
                ch for ch in all_channels
                if ch.get('channel_group_id') in target_group_ids
            ]
            logging.info(f"Filtered to {len(filtered_channels)} channels in selected groups")
        else:
            filtered_channels = all_channels

        # Optional: regex-based selection (mainly for persisted UI selections)
        name_re = None
        number_re = None
        try:
            if isinstance(channel_name_regex, str) and channel_name_regex.strip():
                name_re = re.compile(channel_name_regex.strip(), flags=re.IGNORECASE)
        except re.error as exc:
            logging.error(f"Invalid filters.channel_name_regex: {exc}")
        try:
            if isinstance(channel_number_regex, str) and channel_number_regex.strip():
                number_re = re.compile(channel_number_regex.strip())
        except re.error as exc:
            logging.error(f"Invalid filters.channel_number_regex: {exc}")

        if name_re:
            filtered_channels = [
                ch for ch in filtered_channels
                if name_re.search(str(ch.get('name', '') or ''))
            ]
            logging.info(f"Applied channel_name_regex => {len(filtered_channels)} channels")

        if number_re:
            filtered_channels = [
                ch for ch in filtered_channels
                if number_re.search(str(ch.get('channel_number', '') or ''))
            ]
            logging.info(f"Applied channel_number_regex => {len(filtered_channels)} channels")
    
    # Apply channel number range
    final_channels = [
        ch for ch in filtered_channels
        if ch.get("channel_number") and start_range <= int(ch["channel_number"]) <= end_range
    ]
    
    if not final_channels:
        logging.error("No channels match the current filters")
        return
    
    logging.info(f"Processing {len(final_channels)} channels after filters")
    
    # Save channel metadata
    with open(metadata_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        headers = ["id", "channel_number", "name", "channel_group_id", "tvg_id",
                   "tvc_guide_stationid", "epg_data_id", "logo_id"]
        writer.writerow(headers)
        for ch in final_channels:
            writer.writerow([ch.get(h, "") for h in headers])
    
    # Fetch and save streams
    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "channel_number", "channel_id", "channel_group_id",
            "stream_id", "stream_name", "stream_url", "m3u_account"
        ])

        # Fetch per-channel streams concurrently; write results sequentially.
        dispatcharr_cfg = config.get('dispatcharr') or {}
        fetch_workers = int(dispatcharr_cfg.get('fetch_workers', 6) or 6)
        fetch_workers = max(1, min(32, fetch_workers))

        total_channels = len(final_channels)
        processed_channels = 0
        failed_channels = 0

        def _fetch_one(channel):
            channel_id = channel.get("id")
            channel_number = channel.get("channel_number")
            channel_name = channel.get("name", "")
            logging.info(f"Fetching streams for channel {channel_number} - {channel_name}")
            return channel, api.fetch_channel_streams(channel_id)

        with ThreadPoolExecutor(max_workers=fetch_workers) as executor:
            future_to_channel = {executor.submit(_fetch_one, ch): ch for ch in final_channels}
            for future in as_completed(future_to_channel):
                channel = future_to_channel[future]
                channel_id = channel.get("id")
                channel_number = channel.get("channel_number")
                channel_group_id = channel.get("channel_group_id")
                channel_name = channel.get("name", "")
                try:
                    _channel, streams = future.result()
                except Exception as exc:
                    failed_channels += 1
                    logging.warning(
                        "Failed fetching streams for channel %s - %s: %s",
                        channel_number,
                        channel_name,
                        exc,
                    )
                    streams = None

                processed_channels += 1
                if progress_callback:
                    try:
                        progress_callback({
                            'processed': processed_channels,
                            'total': total_channels,
                            'failed': failed_channels,
                        })
                    except Exception:
                        logging.debug("progress_callback failed during fetch_streams", exc_info=True)

                if not streams:
                    logging.warning(f"  No streams for channel {channel_number}")
                    continue

                for stream in streams:
                    stream_id = stream.get("id", "")
                    stream_lookup_id = None
                    try:
                        stream_lookup_id = int(stream_id)
                    except (TypeError, ValueError):
                        stream_lookup_id = None
                    m3u_account = stream.get("m3u_account")
                    if m3u_account in ("", None):
                        m3u_account = stream_provider_map.get(stream_lookup_id)
                    if m3u_account in ("", None):
                        raise ValueError(
                            f"Stream {stream_id} is missing m3u_account from Dispatcharr; "
                            "cannot proceed without provider IDs."
                        )
                    writer.writerow([
                        channel_number,
                        channel_id,
                        channel_group_id,
                        stream_id,
                        stream.get("name", ""),
                        stream.get("url", ""),
                        m3u_account
                    ])

                logging.info(f"  Saved {len(streams)} streams")
    
    logging.info(f"Done! Streams saved to {output_file}")


def analyze_streams(config, input_csv=None,
                   output_csv=None,
                   fails_csv=None, progress_callback=None,
                   force_full_analysis=False):
    """Analyze streams with progress tracking and checkpointing"""

    if not _check_ffmpeg_installed():
        raise Exception("ffmpeg and ffprobe are required but not found")

    logging.info("Loading streams to analyze...")
    analyzed_count = 0

    input_csv = input_csv or config.resolve_path('csv/02_grouped_channel_streams.csv')
    output_csv = output_csv or config.resolve_path('csv/03_iptv_stream_measurements.csv')
    fails_csv = fails_csv or config.resolve_path('csv/04_fails.csv')
    
    # Load streams
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        logging.error(f"Input CSV not found: {input_csv}")
        return analyzed_count

    if 'm3u_account' not in df.columns:
        raise ValueError(
            "Input CSV is missing required 'm3u_account' column; "
            "pipeline contract violation. Refusing to continue."
        )

    missing_provider = df['m3u_account'].isna() | (df['m3u_account'].astype(str).str.strip() == '')
    if missing_provider.any():
        raise ValueError(
            f"Input CSV has {missing_provider.sum()} rows without m3u_account; "
            "Dispatcharr provider IDs are required for analysis."
        )
    
    filters = config.get('filters') or {}
    
    # Apply filters
    group_ids_list = filters.get('channel_group_ids', [])
    specific_channel_ids = filters.get('specific_channel_ids')
    
    if specific_channel_ids:
        specific_ids_set = set(specific_channel_ids)
        df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
        df = df[df['channel_id'].isin(specific_ids_set)]
    elif group_ids_list:
        target_group_ids = set(group_ids_list)
        df['channel_group_id'] = pd.to_numeric(df['channel_group_id'], errors='coerce')
        df = df[df['channel_group_id'].isin(target_group_ids)]
    
    start_range = filters.get('start_channel', 1)
    end_range = filters.get('end_channel', 99999)
    df['channel_number'] = pd.to_numeric(df['channel_number'], errors='coerce')
    df = df[df['channel_number'].between(start_range, end_range)]
    
    if df.empty:
        logging.warning("No streams to analyze after applying filters")
        return analyzed_count
    
    # Prune recently analyzed streams
    try:
        days_to_keep = int(filters.get('stream_last_measured_days', 7))
    except (ValueError, TypeError):
        days_to_keep = 7
    if not force_full_analysis and days_to_keep > 0 and os.path.exists(output_csv):
        try:
            df_processed = pd.read_csv(output_csv)
            df_processed['timestamp'] = pd.to_datetime(df_processed['timestamp'], errors='coerce')
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            recent_urls = df_processed[df_processed['timestamp'] > cutoff_date]['stream_url'].unique()
            df = df[~df['stream_url'].isin(recent_urls)]
            logging.info(f"Skipped {len(recent_urls)} recently analyzed streams")
        except Exception as e:
            logging.warning(f"Could not filter recent streams: {e}")
    
    # Remove duplicates
    if filters.get('remove_duplicates', True):
        before_count = len(df)
        df.drop_duplicates(subset=['channel_id', 'stream_url'], keep='first', inplace=True)
        removed = before_count - len(df)
        if removed > 0:
            logging.info(f"Removed {removed} duplicate streams")
    
    if df.empty:
        logging.info("All streams have been analyzed recently - nothing to do")
        return analyzed_count
    
    streams_to_analyze = df.to_dict('records')
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(
        len(streams_to_analyze),
        config.resolve_path('logs/checkpoint.json'),
        use_checkpoint=not force_full_analysis
    )

    # Emit an initial progress update immediately (0 / total) so UIs can show
    # that analysis is underway before the first stream finishes.
    if progress_callback:
        try:
            progress_callback(progress_tracker.get_progress())
        except Exception:
            # Progress reporting should never fail the analysis.
            logging.debug("progress_callback failed during initial progress emit", exc_info=True)
    
    # Check for existing checkpoint
    if len(progress_tracker.processed_ids) > 0:
        logging.info(f"Resuming from checkpoint - {len(progress_tracker.processed_ids)} streams already processed")
    
    logging.info(f"Analyzing {len(streams_to_analyze)} streams...")
    
    # Prepare output files
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(fails_csv).parent.mkdir(parents=True, exist_ok=True)
    
    final_columns = [
        'channel_number', 'channel_id', 'stream_id', 'stream_name', 'stream_url',
        'channel_group_id', 'm3u_account'
    ]
    final_columns += [
        'timestamp', 'video_codec', 'audio_codec', 'interlaced_status', 'status',
        'bitrate_kbps', 'fps', 'resolution', 'frames_decoded', 'frames_dropped',
        'err_decode', 'err_discontinuity', 'err_timeout'
    ]
    
    output_exists = os.path.exists(output_csv)
    fails_exists = os.path.exists(fails_csv)
    
    analysis_cfg = config.get('analysis') or {}
    workers = analysis_cfg.get('workers', 8)
    
    try:
        with open(output_csv, 'a', newline='', encoding='utf-8') as f_out, \
             open(fails_csv, 'a', newline='', encoding='utf-8') as f_fails:

            writer_out = csv.DictWriter(f_out, fieldnames=final_columns, extrasaction='ignore')
            writer_fails = csv.DictWriter(f_fails, fieldnames=final_columns, extrasaction='ignore')

            if not output_exists or os.path.getsize(output_csv) == 0:
                writer_out.writeheader()
            if not fails_exists or os.path.getsize(fails_csv) == 0:
                writer_fails.writeheader()

            flush_every = int(analysis_cfg.get('flush_every', 25) or 25)
            flush_every = max(1, min(5000, flush_every))

            cancelled = False
            it = iter(streams_to_analyze)
            max_in_flight = max(1, workers * 4)

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = set()
                future_to_row = {}

                def _submit_next():
                    row = next(it, None)
                    if row is None:
                        return False
                    fut = executor.submit(_analyze_stream_task, row, config, progress_tracker, force_full_analysis)
                    futures.add(fut)
                    future_to_row[fut] = row
                    return True

                # Prime the queue
                for _ in range(min(max_in_flight, len(streams_to_analyze))):
                    if not _submit_next():
                        break

                while futures:
                    done, _pending = wait(futures, return_when=FIRST_COMPLETED)
                    for future in done:
                        futures.discard(future)
                        original_row = future_to_row.pop(future, None) or {}
                        try:
                            result_row = future.result()

                            if result_row is None:  # Already processed
                                pass
                            else:
                                analyzed_count += 1
                                writer_out.writerow(result_row)

                                if result_row.get('status') != 'OK':
                                    writer_fails.writerow(result_row)

                                if analyzed_count % flush_every == 0:
                                    f_out.flush()
                                    f_fails.flush()

                            # Update progress (and allow cancellation if callback returns False)
                            if progress_callback:
                                try:
                                    keep_going = progress_callback(progress_tracker.get_progress())
                                    if keep_going is False:
                                        cancelled = True
                                except Exception:
                                    logging.debug("progress_callback failed during analysis", exc_info=True)
                            else:
                                progress_tracker.print_progress()

                        except Exception as exc:
                            logging.error(
                                "Stream %s generated exception: %s",
                                original_row.get('stream_name'),
                                exc,
                            )

                        if cancelled:
                            break

                        # Backfill to keep the queue full
                        while not cancelled and len(futures) < max_in_flight:
                            if not _submit_next():
                                break

                    if cancelled:
                        # Best-effort cancel of remaining tasks
                        for fut in list(futures):
                            fut.cancel()
                        break

            # Final flush
            f_out.flush()
            f_fails.flush()
        
        print()  # New line after progress
        logging.info("Analysis complete!")
        
        # Clear checkpoint
        progress_tracker.clear_checkpoint()
        
        # Deduplicate final results
        logging.info("Deduplicating results...")
        try:
            df_final = pd.read_csv(output_csv)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            # File doesn't exist or is empty - nothing to deduplicate
            logging.warning(f"Output CSV is empty or doesn't exist: {output_csv}")
            return analyzed_count
        
        if df_final.empty:
            logging.warning("Output CSV contains no data")
            return analyzed_count
            
        df_final['stream_id'] = pd.to_numeric(df_final['stream_id'], errors='coerce')
        df_final.dropna(subset=['stream_id'], inplace=True)
        
        if df_final.empty:
            # All rows were dropped - write empty CSV with headers
            df_empty = pd.DataFrame(columns=final_columns)
            df_empty.to_csv(output_csv, index=False, na_rep='N/A')
            logging.warning("All rows dropped during deduplication")
            return analyzed_count
            
        df_final['stream_id'] = df_final['stream_id'].astype(int)
        df_final.sort_values(by='timestamp', ascending=True, inplace=True)
        df_final.drop_duplicates(subset=['stream_id'], keep='last', inplace=True)
        df_final = df_final.reindex(columns=final_columns)
        df_final.to_csv(output_csv, index=False, na_rep='N/A')
        logging.info(f"Results saved to {output_csv}")
        return analyzed_count
    
    except Exception as e:
        logging.error(f"Error during analysis: {e}")
        raise


def _calculate_proxy_startup_score(bitrate_kbps):
    """Favor streams that buffer quickly under proxy timeouts."""
    if bitrate_kbps is None or pd.isna(bitrate_kbps):
        return 0

    bitrate = float(bitrate_kbps)

    # Lower bitrate generally buffers faster through the proxy. Higher values
    # are still allowed but are penalized because they are more likely to stall
    # during the aggressive startup window.
    if bitrate <= 3000:
        return 100
    if bitrate <= 6000:
        return 85
    if bitrate <= 10000:
        return 70
    if bitrate <= 15000:
        return 55
    if bitrate <= 20000:
        return 40
    return 25


def _calculate_proxy_stability_score(dropped_frame_pct, errors):
    """Penalize early instability that risks rebuffering under strict thresholds."""
    try:
        dropped_pct = float(dropped_frame_pct) if not pd.isna(dropped_frame_pct) else 0
    except (ValueError, TypeError):
        dropped_pct = 0

    try:
        error_count = int(errors) if not pd.isna(errors) else 0
    except (ValueError, TypeError):
        error_count = 0

    stability = 100

    # Decode/discontinuity/timeout errors are the strongest signal of proxy
    # survival risk. Penalize quadratically to push unstable streams down.
    if error_count > 0:
        stability -= min(error_count * error_count * 25, 90)

    # Dropped frames during the sample window often precede buffering.
    if dropped_pct > 0:
        stability -= min(dropped_pct * 2, 40)

    return max(stability, 0)


def _resolution_category(resolution_value):
    """Return a coarse resolution bucket used for validation strictness.

    - sd:    resolutions up to PAL/NTSC (<= 720x576)
    - hd:    720p/1080p class streams that can tolerate a brief hiccup
    - uhd:   1440p/4K and above remain strict
    """
    if not resolution_value:
        return "unknown"

    resolution = str(resolution_value).lower()
    numbers = [p for p in re.split(r"[^0-9]+", resolution) if p]
    if not numbers:
        return "unknown"

    try:
        if len(numbers) >= 2:
            width, height = int(numbers[0]), int(numbers[1])
        else:
            width, height = 0, int(numbers[0])
    except ValueError:
        return "unknown"

    height_dim = height or max(width, height)
    if width and height and width <= 720 and height <= 576:
        return "sd"
    if height_dim >= 1440:
        return "uhd"
    if height_dim >= 720:
        return "hd"
    return "unknown"


def _determine_validation(row):
    """Classify validation outcome before scoring.

    Streams that failed to start, hit early decode/discontinuity/timeout errors,
    or produced no decoded frames are considered failed for ordering purposes.
    """
    reasons = []

    status = str(row.get('status', '')).strip().lower()
    status_reason = f"status:{status}" if status and status != 'ok' else None

    for err_key in ('err_decode', 'err_discontinuity'):
        err_val = row.get(err_key)
        try:
            has_error = bool(int(err_val))
        except Exception:
            has_error = bool(err_val)
        if has_error:
            reasons.append(err_key)

    try:
        frames_decoded = float(row.get('avg_frames_decoded', 0))
    except (ValueError, TypeError):
        frames_decoded = 0
    if frames_decoded == 0:
        reasons.append('no_frames_decoded')

    # Resolution-aware timeout handling: HD can tolerate a single transient
    # timeout when paired with a sane bitrate. This keeps Tier-1 available for
    # realistically watchable HD while preserving strictness elsewhere.
    category = _resolution_category(row.get('resolution'))

    bitrate = row.get('avg_bitrate_kbps', row.get('bitrate_kbps'))
    try:
        bitrate = float(bitrate)
    except (ValueError, TypeError):
        bitrate = None

    # Guard against "fake" high-resolution streams that advertise HD/UHD but
    # ship sub-SD bitrates. Bitrate below the minimum for its resolution class
    # is treated as a hard validation failure to keep Tier-1 honest.
    min_bitrate_thresholds = {
        'sd': 800,
        'hd': 3500,
        'uhd': 25000,
    }
    if bitrate is not None:
        min_required = min_bitrate_thresholds.get(category)
        if min_required is not None and bitrate < min_required:
            reasons.append(f"err_bitrate_too_low_{category}")

    try:
        timeout_count = int(row.get('err_timeout', 0) or 0)
    except Exception:
        timeout_count = 0
    if status == 'timeout' and timeout_count == 0:
        timeout_count = 1

    timeout_allowance = 1 if category == 'hd' else 0
    bitrate_upper_bounds = {
        'sd': 6000,
        'hd': 15000,
        'uhd': 20000,
    }
    bitrate_upper = bitrate_upper_bounds.get(category, 12000)

    timeout_reason = None
    if timeout_count > timeout_allowance:
        timeout_reason = 'err_timeout_excess'
    elif timeout_count > 0 and bitrate is not None and bitrate > bitrate_upper:
        timeout_reason = 'err_timeout_high_bitrate'

    if timeout_reason:
        reasons.append(timeout_reason)
    elif status_reason and status != 'timeout':
        # Non-timeout status failures stay strict.
        reasons.append(status_reason)

    if reasons:
        return 'fail', ';'.join(sorted(set(reasons)))
    return 'pass', 'clean'


def _calculate_legacy_score(row, scoring_cfg):
    resolution_scores = scoring_cfg.get('resolution_scores', {}) or {}
    fps_bonus_points = scoring_cfg.get('fps_bonus_points', 0) or 0
    hevc_boost = scoring_cfg.get('hevc_boost', 1.0) or 1.0

    resolution = str(row.get('resolution', '')).lower()
    resolution_score = resolution_scores.get(resolution, 0)

    try:
        bitrate = float(row.get('avg_bitrate_kbps'))
    except Exception:
        bitrate = 0.0

    codec = str(row.get('video_codec', '')).lower()
    if 'hevc' in codec or '265' in codec:
        bitrate *= hevc_boost

    bitrate_score = bitrate / 100

    try:
        fps = float(row.get('fps'))
    except Exception:
        fps = 0.0

    fps_bonus = fps_bonus_points if fps >= 50 else 0

    dropped_pct = _normalize_numeric(row.get('dropped_frame_percentage'))

    error_penalty = 0
    for err_key in ('err_decode', 'err_discontinuity', 'err_timeout'):
        try:
            error_penalty += 200 * int(row.get(err_key, 0))
        except Exception:
            continue

    score = resolution_score + bitrate_score + fps_bonus - dropped_pct - error_penalty
    return score


def _score_streams_proxy_first(df, include_provider_name, output_csv, update_stats, api):
    # Convert types
    df['bitrate_kbps'] = pd.to_numeric(df['bitrate_kbps'], errors='coerce')
    df['frames_decoded'] = pd.to_numeric(df['frames_decoded'], errors='coerce')
    df['frames_dropped'] = pd.to_numeric(df['frames_dropped'], errors='coerce')
    # Group by stream and calculate averages
    summary = df.groupby('stream_id').agg(
        avg_bitrate_kbps=('bitrate_kbps', 'mean'),
        avg_frames_decoded=('frames_decoded', 'mean'),
        avg_frames_dropped=('frames_dropped', 'mean')
    ).reset_index()

    # Merge with latest metadata
    latest_meta = df.drop_duplicates(subset='stream_id', keep='last')
    # Only drop columns that exist to avoid KeyError
    cols_to_drop = ['bitrate_kbps', 'frames_decoded', 'frames_dropped']
    cols_to_drop = [col for col in cols_to_drop if col in latest_meta.columns]
    if cols_to_drop:
        latest_meta = latest_meta.drop(columns=cols_to_drop)
    summary = pd.merge(
        summary,
        latest_meta,
        on='stream_id'
    )

    def _compute_dropped_frame_percentage(row):
        decoded = row.get('avg_frames_decoded')
        dropped = row.get('avg_frames_dropped')

        try:
            decoded = float(decoded)
        except (TypeError, ValueError):
            decoded = float('nan')

        try:
            dropped = float(dropped)
        except (TypeError, ValueError):
            dropped = float('nan')

        if pd.isna(decoded) or decoded <= 0:
            return 100.0
        if pd.isna(dropped):
            return 100.0

        pct = (dropped / decoded) * 100
        if pd.isna(pct) or pct in (float('inf'), float('-inf')):
            return 100.0

        return pct

    summary['dropped_frame_percentage'] = summary.apply(
        _compute_dropped_frame_percentage, axis=1
    )

    # Proxy-first scoring: prioritize fast startup and early stability.
    logging.info("Using proxy-optimized scoring (validation-first)")

    error_columns = ['err_decode', 'err_discontinuity', 'err_timeout']
    for col in error_columns:
        summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)
    summary['total_errors'] = summary[error_columns].sum(axis=1)

    validation_results = summary.apply(_determine_validation, axis=1)
    summary['validation_result'] = validation_results.apply(lambda x: x[0])
    summary['validation_reason'] = validation_results.apply(lambda x: x[1])

    summary['startup_score'] = summary['avg_bitrate_kbps'].apply(
        _calculate_proxy_startup_score
    )

    summary['stability_score'] = summary.apply(
        lambda row: _calculate_proxy_stability_score(
            row.get('dropped_frame_percentage', 0),
            row.get('total_errors', 0)
        ), axis=1
    )

    summary['score'] = summary['startup_score'] + summary['stability_score']
    # Continuous ordering score uses all available metadata and a soft validation penalty.
    summary['ordering_score'] = summary.apply(_continuous_ordering_score, axis=1)
    summary['ordering_score'] = summary['ordering_score'] + summary['stream_id'].apply(_stable_tiebreaker)

    df_sorted = summary.sort_values(
        by=['channel_number', 'ordering_score'],
        ascending=[True, False]
    )

    final_columns = [
        'stream_id', 'channel_number', 'channel_id', 'channel_group_id', 'stream_name',
        'stream_url', 'm3u_account',
    ]
    if include_provider_name:
        final_columns.append('m3u_account_name')
    final_columns += [
        'avg_bitrate_kbps', 'avg_frames_decoded',
        'avg_frames_dropped', 'dropped_frame_percentage', 'fps', 'resolution',
        'video_codec', 'audio_codec', 'interlaced_status', 'status', 'score',
        'validation_result', 'validation_reason',
        'startup_score', 'stability_score', 'ordering_score'
    ]
    for col in final_columns:
        if col not in df_sorted.columns:
            df_sorted[col] = 'N/A'

    df_sorted = df_sorted[final_columns]
    df_sorted.to_csv(output_csv, index=False, na_rep='N/A')
    logging.info(f"Scored streams saved to {output_csv}")

    if update_stats:
        logging.info("Updating stream stats on server...")
        for _, row in df_sorted.iterrows():
            stream_id = row.get("stream_id")
            if not stream_id or pd.isna(stream_id):
                continue

            stats = {
                "resolution": row.get("resolution"),
                "source_fps": row.get("fps"),
                "video_codec": row.get("video_codec"),
                "audio_codec": row.get("audio_codec"),
                "ffmpeg_output_bitrate": int(row.get("avg_bitrate_kbps"))
                    if pd.notna(row.get("avg_bitrate_kbps")) else None,
            }

            stats = {k: v for k, v in stats.items() if pd.notna(v)}

            if stats:
                try:
                    api.update_stream_stats(int(stream_id), stats)
                except Exception as e:
                    logging.warning(f"Could not update stats for stream {stream_id}: {e}")


def _score_streams_legacy(df, scoring_cfg, include_provider_name, output_csv, update_stats, api):
    logging.info("Using legacy scoring (resolution/bitrate driven)")

    df['bitrate_kbps'] = pd.to_numeric(df['bitrate_kbps'], errors='coerce')
    df['frames_decoded'] = pd.to_numeric(df['frames_decoded'], errors='coerce')
    df['frames_dropped'] = pd.to_numeric(df['frames_dropped'], errors='coerce')

    summary = df.groupby('stream_id').agg(
        avg_bitrate_kbps=('bitrate_kbps', 'mean'),
        avg_frames_decoded=('frames_decoded', 'mean'),
        avg_frames_dropped=('frames_dropped', 'mean')
    ).reset_index()

    latest_meta = df.drop_duplicates(subset='stream_id', keep='last')
    # Only drop columns that exist to avoid KeyError
    cols_to_drop = ['bitrate_kbps', 'frames_decoded', 'frames_dropped']
    cols_to_drop = [col for col in cols_to_drop if col in latest_meta.columns]
    if cols_to_drop:
        latest_meta = latest_meta.drop(columns=cols_to_drop)
    summary = pd.merge(
        summary,
        latest_meta,
        on='stream_id'
    )

    # Avoid division by zero: if no frames decoded, percentage is 0
    summary['dropped_frame_percentage'] = (
        summary['avg_frames_dropped'] / summary['avg_frames_decoded'].replace(0, float('nan')) * 100
    ).fillna(0).replace([float('inf'), float('-inf')], 0)

    for col in ('err_decode', 'err_discontinuity', 'err_timeout'):
        summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)

    summary['score'] = summary.apply(
        lambda row: _calculate_legacy_score(row, scoring_cfg), axis=1
    )
    summary['ordering_score'] = summary.apply(_continuous_ordering_score, axis=1)
    summary['ordering_score'] = summary['ordering_score'] + summary['stream_id'].apply(_stable_tiebreaker)

    df_sorted = summary.sort_values(
        by=['channel_number', 'ordering_score'],
        ascending=[True, False]
    )

    final_columns = [
        'stream_id', 'channel_number', 'channel_id', 'channel_group_id', 'stream_name',
        'stream_url', 'm3u_account',
    ]
    if include_provider_name:
        final_columns.append('m3u_account_name')
    final_columns += [
        'avg_bitrate_kbps', 'avg_frames_decoded', 'avg_frames_dropped',
        'dropped_frame_percentage', 'fps', 'resolution', 'video_codec', 'audio_codec',
        'interlaced_status', 'status', 'score', 'ordering_score'
    ]
    for col in final_columns:
        if col not in df_sorted.columns:
            df_sorted[col] = 'N/A'

    df_sorted = df_sorted[final_columns]
    df_sorted.to_csv(output_csv, index=False, na_rep='N/A')
    logging.info(f"Scored streams saved to {output_csv}")

    if update_stats:
        logging.info("Updating stream stats on server...")
        for _, row in df_sorted.iterrows():
            stream_id = row.get("stream_id")
            if not stream_id or pd.isna(stream_id):
                continue

            stats = {
                "resolution": row.get("resolution"),
                "source_fps": row.get("fps"),
                "video_codec": row.get("video_codec"),
                "audio_codec": row.get("audio_codec"),
                "ffmpeg_output_bitrate": int(row.get("avg_bitrate_kbps"))
                    if pd.notna(row.get("avg_bitrate_kbps")) else None,
            }

            stats = {k: v for k, v in stats.items() if pd.notna(v)}

            if stats:
                try:
                    api.update_stream_stats(int(stream_id), stats)
                except Exception as e:
                    logging.warning(f"Could not update stats for stream {stream_id}: {e}")


def score_streams(api, config, input_csv=None,
                 output_csv=None,
                 update_stats=False):
    """Calculate scores and sort streams"""

    logging.info("Scoring streams...")

    input_csv = input_csv or config.resolve_path('csv/03_iptv_stream_measurements.csv')
    output_csv = output_csv or config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')

    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        logging.error(f"Input CSV not found: {input_csv}")
        return

    filters = config.get('filters') or {}
    scoring_cfg = config.get('scoring') or {}
    # Apply filters
    group_ids_list = filters.get('channel_group_ids', [])
    specific_channel_ids = filters.get('specific_channel_ids')

    if specific_channel_ids:
        specific_ids_set = set(specific_channel_ids)
        df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
        df = df[df['channel_id'].isin(specific_ids_set)]
    elif group_ids_list:
        target_group_ids = set(group_ids_list)
        df['channel_group_id'] = pd.to_numeric(df['channel_group_id'], errors='coerce')
        df = df[df['channel_group_id'].isin(target_group_ids)]

    start_range = filters.get('start_channel', 1)
    end_range = filters.get('end_channel', 99999)
    df['channel_number'] = pd.to_numeric(df['channel_number'], errors='coerce')
    df = df[df['channel_number'].between(start_range, end_range)]

    if df.empty:
        logging.warning("No streams to score")
        return

    include_provider_name = 'm3u_account_name' in df.columns
    proxy_first = bool(scoring_cfg.get('proxy_first', scoring_cfg.get('proxy_startup_bias', False)))

    if proxy_first:
        _score_streams_proxy_first(df, include_provider_name, output_csv, update_stats, api)
    else:
        _score_streams_legacy(df, scoring_cfg, include_provider_name, output_csv, update_stats, api)


def _parse_resolution(resolution):
    try:
        width_str, height_str = str(resolution).lower().split('x')
        return int(width_str), int(height_str)
    except Exception:
        return None


def _normalize_numeric(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def _stable_tiebreaker(value):
    """
    Generate a tiny deterministic value for tie-breaking.

    This ensures ordering stability without relying on input list order.
    """
    digest = hashlib.blake2b(str(value).encode('utf-8'), digest_size=8).hexdigest()
    max_int = float(2**64 - 1)
    return int(digest, 16) / max_int * 1e-6


def _codec_quality_score(codec_value):
    codec = str(codec_value or '').lower()
    if 'av1' in codec:
        return 1.0
    if 'hevc' in codec or '265' in codec:
        return 0.9
    if 'h264' in codec or 'avc' in codec:
        return 0.8
    if 'mpeg2' in codec or 'mpeg-2' in codec:
        return 0.6
    return 0.7


def _audio_quality_score(codec_value):
    codec = str(codec_value or '').lower()
    if 'eac3' in codec or 'e-ac-3' in codec:
        return 0.9
    if 'ac3' in codec:
        return 0.85
    if 'aac' in codec:
        return 0.8
    if 'mp3' in codec:
        return 0.7
    return 0.75


def _validation_penalty(validation_value):
    if validation_value is None:
        return 0.9
    value = str(validation_value).strip().lower()
    if value == 'pass':
        return 1.0
    if value == 'fail':
        return 0.75
    return 0.85


def _continuous_ordering_score(record):
    """
    Compute a continuous, deterministic score from all available metadata.
    Uses diminishing returns so extreme values don't dominate.
    """
    base_score = _normalize_numeric(record.get('score'))
    parsed = _parse_resolution(record.get('resolution'))
    if parsed:
        width, height = parsed
        total_pixels = max(int(width) * int(height), 0)
    else:
        total_pixels = 0

    fps = max(_normalize_numeric(record.get('fps')), 0.0)
    avg_bitrate_kbps = max(_normalize_numeric(record.get('avg_bitrate_kbps')), 0.0)

    # Diminishing returns via log1p normalization against practical ceilings.
    resolution_score = math.log1p(total_pixels) / math.log1p(3840 * 2160)
    fps_score = math.log1p(fps) / math.log1p(120)
    bitrate_score = math.log1p(avg_bitrate_kbps) / math.log1p(50000)
    base_component = math.log1p(max(base_score, 0.0))

    codec_score = _codec_quality_score(record.get('video_codec'))
    audio_score = _audio_quality_score(record.get('audio_codec'))

    interlaced_status = str(record.get('interlaced_status') or '').lower()
    interlace_penalty = 0.95 if 'interlaced' in interlaced_status else 1.0
    validation_penalty = _validation_penalty(record.get('validation_result') or record.get('status'))

    score = (
        base_component * 1.5
        + resolution_score * 2.0
        + fps_score * 0.6
        + bitrate_score * 1.0
        + codec_score * 0.3
        + audio_score * 0.1
    )
    score *= interlace_penalty * validation_penalty
    return score


def order_streams_for_channel(
    records,
    resilience_mode=False,
    fallback_depth=3,
    similar_score_delta=5,
    provider_fn=None,
    validation_dominant=True,
):
    """Order streams using a continuous, deterministic score.

    All streams remain present; validation only reduces confidence via a penalty.
    """

    if not records:
        return []

    def _score(record):
        ordering = record.get('ordering_score')
        if ordering in (None, 'N/A'):
            ordering = _continuous_ordering_score(record)
        score = _normalize_numeric(ordering)
        tie_value = record.get('stream_id') or record.get('stream_url') or record.get('stream_name') or ''
        return score + _stable_tiebreaker(tie_value)

    ordered = sorted(list(records), key=_score, reverse=True)
    return [r.get('stream_id') for r in ordered]


def reorder_streams(api, config, input_csv=None, collect_summary=False, apply_changes=True):
    """Reorder streams in Dispatcharr based on scores"""

    logging.info("Reordering streams in Dispatcharr...")

    input_csv = input_csv or config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')

    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        logging.error(f"Input CSV not found: {input_csv}")
        return None

    filters = config.get('filters') or {}
    ordering_cfg = config.get('ordering') or {}
    resilience_mode = bool(ordering_cfg.get('resilience_mode', False))
    try:
        fallback_depth = int(ordering_cfg.get('fallback_depth', 3) or 3)
    except (ValueError, TypeError):
        fallback_depth = 3
    try:
        similar_score_delta = float(ordering_cfg.get('similar_score_delta', 5) or 5)
    except (ValueError, TypeError):
        similar_score_delta = 5.0

    # Apply filters
    group_ids_list = filters.get('channel_group_ids', [])
    specific_channel_ids = filters.get('specific_channel_ids')
    
    if specific_channel_ids:
        specific_ids_set = set(specific_channel_ids)
        df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
        df = df[df['channel_id'].isin(specific_ids_set)]
    elif group_ids_list:
        target_group_ids = set(group_ids_list)
        df['channel_group_id'] = pd.to_numeric(df['channel_group_id'], errors='coerce')
        df = df[df['channel_group_id'].isin(target_group_ids)]
    
    start_range = filters.get('start_channel', 1)
    end_range = filters.get('end_channel', 99999)
    df['channel_number'] = pd.to_numeric(df['channel_number'], errors='coerce')
    df = df[df['channel_number'].between(start_range, end_range)]

    if df.empty:
        logging.warning("No streams to reorder")
        return
    
    df['stream_id'] = pd.to_numeric(df['stream_id'], errors='coerce')
    df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
    df.dropna(subset=['stream_id', 'channel_id'], inplace=True)
    df['stream_id'] = df['stream_id'].astype(int)
    df['channel_id'] = df['channel_id'].astype(int)

    grouped = df.groupby("channel_id")

    summary = {'channels': []} if collect_summary else None
    any_final_streams = False if collect_summary else None

    channel_lookup = {}
    if collect_summary:
        try:
            channel_lookup = {
                int(ch.get('id')): ch
                for ch in (api.fetch_channels() or [])
                if isinstance(ch, dict) and ch.get('id') is not None
            }
        except Exception:
            logging.debug("Could not fetch channel metadata for ordering summary", exc_info=True)

    def _safe_int(value):
        try:
            return int(value)
        except Exception:
            return value

    def _provider(row):
        return row.get('m3u_account_name') or row.get('m3u_account') or 'unknown_provider'

    for channel_id, group in grouped:
        group_records = group.to_dict('records')
        record_lookup = {}
        for record in group_records:
            sid = record.get('stream_id')
            if sid in (None, 'N/A'):
                continue
            try:
                sid_int = int(sid)
            except Exception:
                sid_int = sid
            record_lookup[sid_int] = record

        # Get current streams from API
        current_streams = api.fetch_channel_streams(channel_id)
        if not current_streams:
            logging.warning(f"Could not fetch streams for channel {channel_id}")
            continue
        
        current_ids_set = {s['id'] for s in current_streams}

        records_for_ordering = []
        for stream in current_streams:
            sid = stream.get('id')
            if sid is None:
                continue
            try:
                sid_key = int(sid)
            except Exception:
                sid_key = sid
            record = record_lookup.get(sid_key)
            if record is None:
                record = {
                    'stream_id': sid_key,
                    'stream_name': stream.get('name'),
                    'm3u_account': stream.get('m3u_account'),
                    'm3u_account_name': stream.get('m3u_account_name'),
                    'resolution': stream.get('resolution'),
                    'avg_bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate'),
                    'fps': stream.get('source_fps') or stream.get('fps'),
                    'video_codec': stream.get('video_codec'),
                    'audio_codec': stream.get('audio_codec'),
                    'interlaced_status': stream.get('interlaced_status'),
                    'status': stream.get('status'),
                }
            records_for_ordering.append(record)

        ordered_stream_ids = order_streams_for_channel(
            records_for_ordering,
            resilience_mode=resilience_mode,
            fallback_depth=fallback_depth,
            similar_score_delta=similar_score_delta,
            provider_fn=_provider,
        )
        final_ids = [sid for sid in ordered_stream_ids if sid in current_ids_set]

        channel_entry = None
        if collect_summary:
            current_lookup = {}
            for stream in current_streams:
                if not isinstance(stream, dict):
                    continue
                sid = stream.get('id')
                if sid is None:
                    continue
                try:
                    sid_key = int(sid)
                except Exception:
                    sid_key = sid
                current_lookup[sid_key] = {
                    'stream_name': stream.get('name'),
                    'provider_id': stream.get('m3u_account'),
                    'provider_name': stream.get('m3u_account_name'),
                    'resolution': stream.get('resolution'),
                    'bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate')
                }

            channel_meta = channel_lookup.get(_safe_int(channel_id), {}) if channel_lookup else {}
            channel_entry = {
                'channel_id': _safe_int(channel_id),
                'channel_number': channel_meta.get('channel_number'),
                'channel_name': channel_meta.get('name'),
                'final_order': [],
                'excluded_streams': []
            }

            def _merge_entry(sid, source_record=None):
                source_record = source_record or {}
                fallback = current_lookup.get(sid) or {}
                entry = {
                    'stream_id': sid,
                    'stream_name': source_record.get('stream_name') or fallback.get('stream_name'),
                    'provider_id': source_record.get('m3u_account') if isinstance(source_record, dict) else None,
                    'provider_name': source_record.get('m3u_account_name') if isinstance(source_record, dict) else None,
                    'resolution': source_record.get('resolution'),
                    'video_codec': source_record.get('video_codec'),
                    'bitrate_kbps': source_record.get('avg_bitrate_kbps'),
                    'validation_result': source_record.get('validation_result') or source_record.get('status'),
                    'validation_reason': source_record.get('validation_reason'),
                    'final_score': source_record.get('ordering_score') if source_record.get('ordering_score') not in (None, 'N/A') else source_record.get('score')
                }

                if entry.get('provider_id') is None and isinstance(fallback, dict):
                    entry['provider_id'] = fallback.get('provider_id')
                if entry.get('provider_name') is None and isinstance(fallback, dict):
                    entry['provider_name'] = fallback.get('provider_name')
                if entry.get('resolution') is None and isinstance(fallback, dict):
                    entry['resolution'] = fallback.get('resolution')
                if entry.get('bitrate_kbps') is None and isinstance(fallback, dict):
                    entry['bitrate_kbps'] = fallback.get('bitrate_kbps')

                return entry

            final_set = set(final_ids)

            for idx, sid in enumerate(final_ids, start=1):
                record = record_lookup.get(sid, {})
                merged = _merge_entry(sid, record)
                merged.update({
                    'order': idx,
                    'quality_tier': 'ordered',
                })
                channel_entry['final_order'].append(merged)

            # Persist final stream metadata for history/UI rendering.
            final_ids_ordered = list(final_ids)
            final_streams = []
            if final_ids_ordered:
                any_final_streams = True
                for sid in final_ids_ordered:
                    record = record_lookup.get(sid)
                    merged = _merge_entry(sid, record)
                    validation_result = merged.get('validation_result')
                    validation_reason = merged.get('validation_reason')
                    final_streams.append({
                        'id': sid,
                        'name': merged.get('stream_name'),
                        'provider_id': merged.get('provider_id'),
                        'provider_name': merged.get('provider_name'),
                        'resolution': merged.get('resolution'),
                        'bitrate_kbps': merged.get('bitrate_kbps'),
                        'validation': validation_result,
                        'validation_reason': validation_reason,
                        'quality_tier': 'ordered',
                    })
            else:
                final_streams = []

            channel_entry['final_streams'] = final_streams
            channel_entry['final_stream_ids'] = final_ids_ordered

        if not final_ids:
            if collect_summary and channel_entry is not None:
                summary['channels'].append(channel_entry)
            logging.warning(f"No valid streams for channel {channel_id}")
            continue
        
        if apply_changes:
            try:
                api.update_channel_streams(channel_id, final_ids)
                logging.info(f"Reordered channel {channel_id}")
            except Exception as e:
                logging.error(f"Failed to reorder channel {channel_id}: {e}")

        if collect_summary and channel_entry is not None:
            summary['channels'].append(channel_entry)

    if collect_summary:
        if any_final_streams is False:
            summary['tier1_empty'] = True
        return summary
    logging.info("Reordering complete!")

def refresh_channel_streams(api, config, channel_id, base_search_text=None, include_filter=None, exclude_filter=None, allowed_stream_ids=None, preview=False, stream_name_regex=None, stream_name_regex_override=None, all_streams_override=None, all_channels_override=None, provider_names=None):
    """
    Find and add all matching streams from all providers for a specific channel

    Args:
        api: DispatcharrAPI instance
        config: Config instance
        channel_id: ID of the channel to refresh
        base_search_text: Optional override for the channel name used when matching streams
        include_filter: Optional comma-separated wildcards (e.g., "york*,lond*")
        exclude_filter: Optional comma-separated exclusions (e.g., "lincoln*")
        allowed_stream_ids: Optional explicit selection from a prior preview. A full scan still
            happens to enforce channel filters (regex, include/exclude, timeshift parity) and to
            regenerate counts/preview data after re-applying those rules.
    Returns:
        dict with stats about streams found/added
    """
    import re

    # Performance: precompile regexes used in matching loops
    _QUALITY_RE = re.compile(r'\b(?:hd|sd|fhd|4k|uhd|hevc|h264|h265)\b', flags=re.IGNORECASE)
    _TIMESHIFT_RE = re.compile(r'\+\d')
    
    def normalize(text):
        """Remove spaces and lowercase"""
        return text.replace(" ", "").lower()
    
    def strip_quality(text):
        """Remove quality indicators"""
        return _QUALITY_RE.sub('', text).strip()

    def compile_wildcard_filter(filter_text):
        """
        Compile a comma-separated glob filter (supports '*' wildcard) into regex objects.
        Uses a safe glob-to-regex conversion (escapes all non-wildcard characters).
        """
        if not filter_text:
            return []
        parts = [p.strip().lower() for p in filter_text.split(',') if p.strip()]
        regexes = []
        for part in parts:
            # Convert glob (*) to regex (.*), escape everything else
            pat = re.escape(part).replace(r'\*', '.*')
            regexes.append(re.compile(pat))
        return regexes

    # Optional regex for stream name filtering (applied after base match + wildcard include/exclude).
    _stream_name_re = None
    if isinstance(stream_name_regex, str) and stream_name_regex.strip():
        try:
            _stream_name_re = re.compile(stream_name_regex.strip(), flags=re.IGNORECASE)
        except re.error as exc:
            logging.error(f"Invalid stream_name_regex ignored: {exc}")
            _stream_name_re = None

    # Optional regex OVERRIDE for stream name matching.
    # When set, this replaces the base match + include/exclude (+ timeshift) rules.
    _stream_name_override_re = None
    if isinstance(stream_name_regex_override, str) and stream_name_regex_override.strip():
        try:
            _stream_name_override_re = re.compile(stream_name_regex_override.strip(), flags=re.IGNORECASE)
            logging.info("Using stream-name REGEX OVERRIDE (base/include/exclude/+1 rules ignored).")
        except re.error as exc:
            logging.error(f"Invalid stream_name_regex_override: {exc}")
            return {'error': f'Invalid regex override: {exc}'}

    # Precompute constants used for all stream comparisons (initialized later,
    # once `search_name` is known).
    _selected_normalized = None
    _selected_has_timeshift = None
    _include_regexes = None
    _exclude_regexes = None
    
    def matches_stream(stream_name):
        """Check if stream matches the selected channel (fast path; precomputed filters)."""
        if _stream_name_override_re is not None:
            return bool(_stream_name_override_re.search(stream_name))

        stream_normalized = normalize(stream_name)
        
        if _selected_normalized not in stream_normalized:
            return False
        
        stream_has_timeshift = bool(_TIMESHIFT_RE.search(stream_name))
        
        if _selected_has_timeshift and not stream_has_timeshift:
            return False
        if not _selected_has_timeshift and stream_has_timeshift:
            return False
        
        if _include_regexes:
            stream_lower = stream_name.lower()
            if not any(r.search(stream_lower) for r in _include_regexes):
                return False
        
        if _exclude_regexes:
            stream_lower = stream_name.lower()
            if any(r.search(stream_lower) for r in _exclude_regexes):
                return False

        if _stream_name_re and not _stream_name_re.search(stream_name):
            return False
        
        return True
    
    logging.info(f"Refreshing channel {channel_id} from all providers...")
    
    # Get channel details (allow caller-provided cache to avoid N API calls).
    if isinstance(all_channels_override, list):
        all_channels = all_channels_override
    else:
        all_channels = api.fetch_channels()

    target_channel = None
    for ch in all_channels:
        if not isinstance(ch, dict) or ch.get('id') is None:
            continue
        try:
            if int(ch.get('id')) == int(channel_id):
                target_channel = ch
                break
        except Exception:
            continue
    
    if not target_channel:
        logging.error(f"Channel {channel_id} not found")
        return {'error': 'Channel not found'}
    
    channel_name = target_channel.get('name', '')
    search_name = channel_name if base_search_text is None else base_search_text
    logging.info(f"Channel: {channel_name}")
    if base_search_text is not None:
        logging.info(f"Using custom base search text: {search_name}")
    
    if _stream_name_override_re is None:
        if include_filter:
            logging.info(f"Include filter: {include_filter}")
        if exclude_filter:
            logging.info(f"Exclude filter: {exclude_filter}")

    # Precompute match constants once (used inside the stream loop).
    # In override mode these values are unused, but we still set safe defaults.
    _selected_normalized = normalize(strip_quality(search_name))
    _selected_has_timeshift = bool(_TIMESHIFT_RE.search(search_name))
    _include_regexes = [] if _stream_name_override_re is not None else compile_wildcard_filter(include_filter)
    _exclude_regexes = [] if _stream_name_override_re is not None else compile_wildcard_filter(exclude_filter)
    
    # Get current streams for this channel
    current_streams = api.fetch_channel_streams(channel_id)
    current_stream_ids = {s['id'] for s in current_streams} if current_streams else set()

    def _stream_snapshot(stream):
        if not isinstance(stream, dict):
            return {}
        return {
            'id': stream.get('id'),
            'name': stream.get('name'),
            'provider_id': stream.get('m3u_account'),
            'provider_name': stream.get('m3u_account_name'),
            'resolution': stream.get('resolution'),
            'bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate')
        }
    logging.info(f"Channel currently has {len(current_stream_ids)} streams")
    
    # Fetch ALL streams from ALL providers (paginated) unless a shared cache is provided.
    if isinstance(all_streams_override, list):
        all_streams = all_streams_override
    else:
        logging.info("Searching all providers...")
        all_streams = []
        next_url = '/api/channels/streams/?limit=100'

        while next_url:
            result = api.get(next_url)
            if not result or 'results' not in result:
                break

            all_streams.extend(result['results'])

            if result.get('next'):
                next_url = result['next'].split('/api/')[-1]
                next_url = '/api/' + next_url
            else:
                next_url = None
    
    logging.info(f"Checking {len(all_streams)} streams from all providers...")
    
    # Preview responses can become very large when regex override is broad.
    # Cap the returned stream list while still computing accurate counts.
    _PREVIEW_STREAM_LIMIT = 250

    total_matching = 0
    preview_truncated = False

    allowed_set = None
    if allowed_stream_ids is not None:
        # Even when the caller supplies a hand-picked list, we still iterate all streams so the
        # selection is revalidated against the current channel filters and the preview counts are
        # recomputed from the filtered universe.
        allowed_set = {int(sid) for sid in allowed_stream_ids}

    # Build filtered streams (used for actual update) and a capped list of
    # detailed streams for preview/UI.
    filtered_streams = []   # used when preview=False (full set)
    preview_streams = []    # capped list returned to UI when preview=True
    filtered_count = 0      # accurate count after all filtering (and allowed_set)

    provider_lookup = provider_names if isinstance(provider_names, dict) else None

    for stream in all_streams:
        stream_name = stream.get('name', '')
        stream_id = stream.get('id')
        provider_id = stream.get('m3u_account')
        provider_name = stream.get('m3u_account_name')

        if provider_name is None and provider_lookup and provider_id is not None:
            provider_name = provider_lookup.get(str(provider_id))

        if not matches_stream(stream_name):
            continue

        total_matching += 1

        if allowed_set is not None and (stream_id is None or int(stream_id) not in allowed_set):
            continue

        filtered_count += 1

        detailed_stream = {
            'id': stream_id,
            'name': stream_name,
            'provider_id': provider_id,
            'provider_name': provider_name,
            # URL is not used by the UI and can be large/sensitive; omit.
            'resolution': stream.get('resolution'),
            'bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate')
        }

        if preview:
            if len(preview_streams) < _PREVIEW_STREAM_LIMIT:
                preview_streams.append(detailed_stream)
            else:
                preview_truncated = True
        else:
            filtered_streams.append(detailed_stream)

    logging.info(f"Found {total_matching} matching streams")

    final_stream_ids = [s['id'] for s in filtered_streams] if not preview else []
    previous_streams = [_stream_snapshot(s) for s in (current_streams or [])]
    final_streams = filtered_streams if not preview else preview_streams
    
    if (not preview and not final_stream_ids) or (preview and filtered_count == 0):
        logging.info("No streams remaining after filtering - channel will be emptied")
    else:
        logging.info(f"Replacing channel streams with {filtered_count if preview else len(final_stream_ids)} matching streams...")
    
    result = {
        'total_matching': total_matching,
        'previous_count': len(current_stream_ids),
        'new_count': filtered_count if preview else len(final_stream_ids),
        'removed': len(current_stream_ids),
        'added': filtered_count if preview else len(final_stream_ids),
        'streams': preview_streams if preview else filtered_streams,
        'channel_name': channel_name,
        'base_search_text': search_name,
        'preview_limit': _PREVIEW_STREAM_LIMIT if preview else None,
        'preview_truncated': bool(preview_truncated) if preview else False,
        'previous_streams': previous_streams,
        'final_streams': final_streams,
        'final_stream_ids': final_stream_ids
    }

    if preview:
        return result

    # Update channel with new stream list
    try:
        api.update_channel_streams(channel_id, final_stream_ids)
        logging.info(f"✓ Successfully replaced channel streams")
        return result
    except Exception as e:
        logging.error(f"Failed to update channel: {e}")
        return {'error': str(e)}
