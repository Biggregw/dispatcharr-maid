"""
stream_analysis.py
Core stream analysis library for Dispatcharr-Maid
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from urllib.parse import urlparse

import hashlib

import pandas as pd
import yaml

from api_utils import DispatcharrAPI
from playback_sessions import EARLY_ABANDONMENT_SECONDS
from provider_data import refresh_provider_data


# Progress tracking
class ProgressTracker:
    """Track analysis progress with ETA and resumability"""

    def __init__(self, total_streams, checkpoint_file, use_checkpoint=True, total_channels=0):
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
        self.total_channels = total_channels
        self.current_channel_index = 0
        self.current_channel_id = None
        self.current_channel_label = None
        self.last_sorted_channel_label = None
        self.streams_completed_in_channel = 0
        self.streams_total_in_channel = 0
    
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
    
    def mark_processed(self, stream_id, success=True, channel_id=None):
        """Mark a stream as processed"""
        with self.lock:
            self.processed += 1
            if not success:
                self.failed += 1
            if channel_id is not None and channel_id == self.current_channel_id:
                self.streams_completed_in_channel += 1
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
    
    def set_channel_context(
        self,
        channel_index,
        channel_id,
        streams_total,
        streams_completed=0,
        channel_label=None,
    ):
        """Set channel-scoped progress details."""
        with self.lock:
            self.current_channel_index = channel_index
            self.current_channel_id = channel_id
            self.current_channel_label = channel_label
            self.streams_total_in_channel = streams_total
            self.streams_completed_in_channel = streams_completed

    def mark_channel_sorted(self, channel_label):
        """Record the most recently sorted channel label."""
        with self.lock:
            self.last_sorted_channel_label = channel_label

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
                'percent': (self.processed / self.total * 100) if self.total > 0 else 0,
                'total_channels': self.total_channels,
                'current_channel_index': self.current_channel_index,
                'current_channel_id': self.current_channel_id,
                'current_channel_label': self.current_channel_label,
                'last_sorted_channel_label': self.last_sorted_channel_label,
                'streams_completed_in_channel': self.streams_completed_in_channel,
                'streams_total_in_channel': self.streams_total_in_channel,
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

    def __init__(self, config_file='config.yaml', working_dir=None, csv_root=None):
        self.working_dir = Path(working_dir) if working_dir else Path(config_file).parent
        self.config_file = Path(self.working_dir, Path(config_file).name)
        self.csv_root = Path(csv_root) if csv_root else None
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

        default_config = self._get_default_config()
        try:
            with open(self.config_file, 'r') as f:
                loaded = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            logging.warning(
                "Config file %s is invalid YAML; using defaults. Error: %s",
                self.config_file,
                exc,
            )
            return default_config

        if loaded is None:
            logging.warning(
                "Config file %s is empty; using defaults.",
                self.config_file,
            )
            return default_config
        if not isinstance(loaded, dict):
            logging.warning(
                "Config file %s must be a YAML mapping; using defaults.",
                self.config_file,
            )
            return default_config
        return loaded
    
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

    def set_csv_root(self, csv_root):
        """Override the CSV root directory (optional)."""
        self.csv_root = Path(csv_root) if csv_root else None

    def resolve_path(self, relative_path):
        """Resolve a path within the working directory"""
        if self.csv_root and (relative_path == "csv" or relative_path.startswith("csv/")):
            suffix = relative_path[4:] if relative_path != "csv" else ""
            return str(self.csv_root / suffix) if suffix else str(self.csv_root)
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


def _normalize_csv_value(value):
    if isinstance(value, str):
        return value.replace("\r\n", "\n").replace("\r", "\n")
    return value


def _normalize_csv_row(row):
    if not isinstance(row, dict):
        return row
    return {key: _normalize_csv_value(value) for key, value in row.items()}


def _build_csv_writer(file_handle, fieldnames):
    return csv.DictWriter(
        file_handle,
        fieldnames=fieldnames,
        extrasaction="ignore",
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
        lineterminator="\n",
    )


def _read_analysis_csv(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


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
        '-show_entries',
        'stream=codec_type,codec_name,width,height,avg_frame_rate,r_frame_rate,profile,level,pix_fmt,bit_rate'
        ':format=format_name,bit_rate',
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
            return {
                'streams': data.get('streams', []),
                'format': data.get('format', {}) or {}
            }
        return {'streams': [], 'format': {}}
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        logging.debug(f"Stream info check failed: {e}")
        return {'streams': [], 'format': {}}


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
            row['video_stream_count'] = 'N/A'
            row['audio_stream_count'] = 'N/A'
            row['format_name'] = 'N/A'
            row['video_profile'] = 'N/A'
            row['video_level'] = 'N/A'
            row['pixel_format'] = 'N/A'
            row['r_frame_rate'] = 'N/A'
            row['declared_bitrate_kbps'] = 'N/A'
            row['status'] = 'N/A'
            
            # 1. Get codec info
            probe_info = _get_stream_info(url, timeout)
            streams_info = probe_info.get('streams', []) if isinstance(probe_info, dict) else []
            format_info = probe_info.get('format', {}) if isinstance(probe_info, dict) else {}
            video_streams = [s for s in streams_info if 'width' in s]
            audio_streams = [s for s in streams_info if 'codec_name' in s and 'width' not in s]
            video_info = next(iter(video_streams), None)
            audio_info = next(iter(audio_streams), None)
            row['video_stream_count'] = len(video_streams) if streams_info else 'N/A'
            row['audio_stream_count'] = len(audio_streams) if streams_info else 'N/A'

            if format_info:
                row['format_name'] = format_info.get('format_name') or row['format_name']
            
            if video_info:
                row['video_codec'] = video_info.get('codec_name')
                row['resolution'] = f"{video_info.get('width')}x{video_info.get('height')}"
                fps_str = video_info.get('avg_frame_rate', '0/1')
                try:
                    num, den = map(int, fps_str.split('/'))
                    row['fps'] = round(num / den, 2) if den != 0 else 0
                except (ValueError, ZeroDivisionError):
                    row['fps'] = 0
                row['video_profile'] = video_info.get('profile') or row['video_profile']
                row['video_level'] = video_info.get('level') or row['video_level']
                row['pixel_format'] = video_info.get('pix_fmt') or row['pixel_format']
                row['r_frame_rate'] = video_info.get('r_frame_rate') or row['r_frame_rate']
            
            if audio_info:
                row['audio_codec'] = audio_info.get('codec_name')

            declared_bitrate = None
            for candidate in (
                (video_info or {}).get('bit_rate'),
                (format_info or {}).get('bit_rate')
            ):
                try:
                    value = float(candidate)
                except (TypeError, ValueError):
                    continue
                if value > 0:
                    declared_bitrate = value
                    break
            if declared_bitrate:
                row['declared_bitrate_kbps'] = round(declared_bitrate / 1000, 2)
            
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
            progress_tracker.mark_processed(
                stream_id,
                status == "OK",
                channel_id=row.get('channel_id'),
            )
        
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

def fetch_streams(
    api,
    config,
    output_file=None,
    progress_callback=None,
    stream_provider_map_override=None,
    channels_override=None,
    streams_override=None,
):
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
    
    # Fetch all channels (unless an explicit override is provided)
    if channels_override is not None:
        all_channels = channels_override
        logging.info(f"Using {len(all_channels)} pre-resolved channels")
    else:
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
    
    streams_by_channel = None
    if streams_override is not None:
        streams_by_channel = defaultdict(list)
        for stream in streams_override:
            if not isinstance(stream, dict):
                continue
            channel_id = stream.get("channel_id")
            if channel_id is None:
                continue
            streams_by_channel[channel_id].append(stream)

    # Fetch and save streams
    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "channel_number", "channel_id", "channel_group_id",
            "stream_id", "stream_name", "stream_url", "m3u_account"
        ])

        total_channels = len(final_channels)
        processed_channels = 0
        failed_channels = 0

        if streams_by_channel is not None:
            logging.info("Using pre-fetched stream cache for channel streams")
            for channel in final_channels:
                channel_id = channel.get("id")
                channel_number = channel.get("channel_number")
                channel_group_id = channel.get("channel_group_id")
                streams = streams_by_channel.get(channel_id, [])
                processed_channels += 1
                if progress_callback:
                    progress_callback({
                        'processed': processed_channels,
                        'total': total_channels,
                        'failed': failed_channels,
                    })

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
        else:
            # Fetch per-channel streams concurrently; write results sequentially.
            dispatcharr_cfg = config.get('dispatcharr') or {}
            fetch_workers = int(dispatcharr_cfg.get('fetch_workers', 6) or 6)
            fetch_workers = max(1, min(32, fetch_workers))

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


def _apply_channel_reorder(api, channel_id, channel_label, ordered_ids):
    if not api:
        logging.warning(
            "Apply reorder requested for channel %s (%s) but no Dispatcharr API client available",
            channel_label,
            channel_id,
        )
        return False
    if channel_id in (None, '', 'N/A'):
        logging.warning(
            "Apply reorder requested for channel %s (%s) but no channel ID was available",
            channel_label,
            channel_id,
        )
        return False
    if not ordered_ids:
        logging.warning(
            "Apply reorder requested for channel %s (%s) but no ordered stream IDs were computed",
            channel_label,
            channel_id,
        )
        return False

    logging.info(
        "Applying reorder to Dispatcharr for channel %s (%s) with %d streams",
        channel_label,
        channel_id,
        len(ordered_ids),
    )
    try:
        api.update_channel_streams(channel_id, ordered_ids)
    except Exception as exc:
        logging.error(
            "Failed to apply reorder to Dispatcharr for channel %s (%s): %s",
            channel_label,
            channel_id,
            exc,
        )
        return False

    logging.info(
        "Successfully applied reorder to Dispatcharr for channel %s (%s)",
        channel_label,
        channel_id,
    )
    return True


def analyze_streams(config, input_csv=None,
                   output_csv=None,
                   fails_csv=None, progress_callback=None,
                   force_full_analysis=False,
                   streams_callback=None,
                   api=None,
                   apply_reorder=False):
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
            df_processed = _read_analysis_csv(output_csv)
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
    if streams_callback:
        try:
            streams_callback(streams_to_analyze)
        except Exception:
            logging.debug("streams_callback failed during analysis setup", exc_info=True)

    streams_by_channel = defaultdict(list)
    channel_lookup = {}
    missing_channel_rows = []
    for row in streams_to_analyze:
        channel_id = row.get('channel_id')
        if channel_id is None or (isinstance(channel_id, float) and pd.isna(channel_id)):
            # Explicitly track missing channel IDs so they do not get silently grouped.
            missing_channel_rows.append(row)
            continue
        if channel_id not in channel_lookup:
            channel_lookup[channel_id] = {
                'channel_number': row.get('channel_number'),
                'channel_group_id': row.get('channel_group_id'),
            }
        streams_by_channel[channel_id].append(row)

    def _channel_sort_key(channel_id):
        channel_number = channel_lookup.get(channel_id, {}).get('channel_number')
        try:
            numeric = int(channel_number)
            missing = False
        except (TypeError, ValueError):
            numeric = 0
            missing = True
        return (1 if missing else 0, numeric, str(channel_id))

    def _channel_label(channel_number, channel_id):
        if channel_number not in (None, ''):
            return str(channel_number)
        if channel_id not in (None, ''):
            return str(channel_id)
        return "Unknown"

    channel_order = sorted(channel_lookup.keys(), key=_channel_sort_key)
    total_channels = len(channel_order) + (1 if missing_channel_rows else 0)
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(
        len(streams_to_analyze),
        config.resolve_path('logs/checkpoint.json'),
        use_checkpoint=not force_full_analysis,
        total_channels=total_channels,
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
        'video_stream_count', 'audio_stream_count', 'format_name', 'video_profile',
        'video_level', 'pixel_format', 'r_frame_rate', 'declared_bitrate_kbps',
        'err_decode', 'err_discontinuity', 'err_timeout'
    ]
    
    output_exists = os.path.exists(output_csv)
    fails_exists = os.path.exists(fails_csv)
    
    analysis_cfg = config.get('analysis') or {}
    workers = analysis_cfg.get('workers', 8)
    
    try:
        with open(output_csv, 'a', newline='', encoding='utf-8') as f_out, \
             open(fails_csv, 'a', newline='', encoding='utf-8') as f_fails:

            writer_out = _build_csv_writer(f_out, final_columns)
            writer_fails = _build_csv_writer(f_fails, final_columns)

            if not output_exists or os.path.getsize(output_csv) == 0:
                writer_out.writeheader()
            if not fails_exists or os.path.getsize(fails_csv) == 0:
                writer_fails.writeheader()

            flush_every = int(analysis_cfg.get('flush_every', 25) or 25)
            flush_every = max(1, min(5000, flush_every))

            cancelled = False

            if missing_channel_rows:
                logging.warning(
                    "Encountered %d streams without channel_id; processing them last.",
                    len(missing_channel_rows),
                )

            channel_batches = []
            for channel_id in channel_order:
                channel_batches.append({
                    'channel_id': channel_id,
                    'channel_number': channel_lookup.get(channel_id, {}).get('channel_number'),
                    'streams': streams_by_channel.get(channel_id, []),
                })
            if missing_channel_rows:
                channel_batches.append({
                    'channel_id': None,
                    'channel_number': None,
                    'streams': missing_channel_rows,
                })

            for channel_index, batch in enumerate(channel_batches, start=1):
                channel_id = batch['channel_id']
                channel_streams = batch['streams']
                channel_number = batch['channel_number']
                channel_label = _channel_label(channel_number, channel_id)
                processed_in_channel = sum(
                    1 for row in channel_streams
                    if progress_tracker.is_processed(row.get('stream_id'))
                )

                progress_tracker.set_channel_context(
                    channel_index,
                    channel_id,
                    len(channel_streams),
                    processed_in_channel,
                    channel_label=channel_label,
                )
                logging.info(
                    "Channel %s starting analysis (%d streams)",
                    channel_label,
                    len(channel_streams),
                )
                if progress_callback:
                    try:
                        progress_callback(progress_tracker.get_progress())
                    except Exception:
                        logging.debug("progress_callback failed during channel start", exc_info=True)

                if not channel_streams:
                    logging.warning(f"  No streams for channel {channel_label}")
                    logging.info("Channel %s analysis complete", channel_label)
                    logging.info("Channel %s sorted", channel_label)
                    progress_tracker.mark_channel_sorted(channel_label)
                    if apply_reorder:
                        _apply_channel_reorder(api, channel_id, channel_label, [])
                    if progress_callback:
                        try:
                            progress_callback(progress_tracker.get_progress())
                        except Exception:
                            logging.debug("progress_callback failed after empty channel", exc_info=True)
                    continue

                channel_results = []
                channel_failures = []
                it = iter(channel_streams)
                max_in_flight = max(1, workers * 4)

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = set()
                    future_to_row = {}

                    def _submit_next():
                        row = next(it, None)
                        if row is None:
                            return False
                        fut = executor.submit(
                            _analyze_stream_task,
                            row,
                            config,
                            progress_tracker,
                            force_full_analysis,
                        )
                        futures.add(fut)
                        future_to_row[fut] = row
                        return True

                    for _ in range(min(max_in_flight, len(channel_streams))):
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
                                    channel_results.append(result_row)

                                    if result_row.get('status') != 'OK':
                                        channel_failures.append(result_row)

                                if analyzed_count % flush_every == 0:
                                    f_out.flush()
                                    f_fails.flush()

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

                            while not cancelled and len(futures) < max_in_flight:
                                if not _submit_next():
                                    break

                        if cancelled:
                            for fut in list(futures):
                                fut.cancel()
                            break

                if cancelled:
                    break

                ordered_ids = order_streams_for_channel(channel_results)
                rows_by_id = {}
                unordered_rows = []
                for row in channel_results:
                    stream_id = row.get('stream_id')
                    if stream_id in (None, '', 'N/A'):
                        unordered_rows.append(row)
                        continue
                    rows_by_id[str(stream_id)] = row
                for stream_id in ordered_ids:
                    key = str(stream_id)
                    row = rows_by_id.pop(key, None)
                    if row:
                        writer_out.writerow(_normalize_csv_row(row))
                for row in unordered_rows:
                    writer_out.writerow(_normalize_csv_row(row))
                for row in rows_by_id.values():
                    writer_out.writerow(_normalize_csv_row(row))

                failures_by_id = {}
                unordered_failures = []
                for row in channel_failures:
                    stream_id = row.get('stream_id')
                    if stream_id in (None, '', 'N/A'):
                        unordered_failures.append(row)
                        continue
                    failures_by_id[str(stream_id)] = row
                for stream_id in ordered_ids:
                    key = str(stream_id)
                    row = failures_by_id.pop(key, None)
                    if row:
                        writer_fails.writerow(_normalize_csv_row(row))
                for row in unordered_failures:
                    writer_fails.writerow(_normalize_csv_row(row))
                for row in failures_by_id.values():
                    writer_fails.writerow(_normalize_csv_row(row))

                f_out.flush()
                f_fails.flush()
                logging.info("Channel %s analysis complete", channel_label)
                logging.info("Channel %s sorted", channel_label)
                progress_tracker.mark_channel_sorted(channel_label)
                if apply_reorder:
                    _apply_channel_reorder(api, channel_id, channel_label, ordered_ids)
                if progress_callback:
                    try:
                        progress_callback(progress_tracker.get_progress())
                    except Exception:
                        logging.debug("progress_callback failed after channel completion", exc_info=True)

            if cancelled:
                logging.info("Analysis cancelled by request")

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
            df_final = _read_analysis_csv(output_csv)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            # File doesn't exist or is empty - nothing to deduplicate
            logging.warning(f"Output CSV is empty or doesn't exist: {output_csv}")
            return analyzed_count
        
        if df_final.empty:
            logging.warning("Output CSV contains no data")
            return analyzed_count
            
        df_final['stream_id'] = pd.to_numeric(df_final['stream_id'], errors='coerce')
        if df_final['stream_id'].notna().any():
            mask = df_final['stream_id'].isna() | ~df_final['stream_id'].duplicated(keep='last')
            df_final = df_final[mask]

        df_final = df_final.reindex(columns=final_columns)
        df_final.to_csv(
            output_csv,
            index=False,
            na_rep='N/A',
            quoting=csv.QUOTE_ALL,
            escapechar="\\",
            lineterminator="\n",
        )
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


def _parse_history_timestamp(value):
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OSError, ValueError, TypeError):
            return None
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _read_ndjson(path):
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except Exception as exc:
        logging.warning("Failed to read historical log %s: %s", path, exc)
    return []


def _historical_stream_penalties(window_hours=24):
    """Compute capped, time-limited penalties from prior failover evidence."""
    logs_dir = Path("logs")
    quality_checks_path = logs_dir / "quality_checks.ndjson"
    suggestions_path = logs_dir / "quality_check_suggestions.ndjson"

    if not quality_checks_path.exists() and not suggestions_path.exists():
        return {}

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=window_hours)
    top_streams_by_channel = defaultdict(list)
    penalties = defaultdict(float)
    suggestion_usage_by_channel = {}

    for record in _read_ndjson(quality_checks_path):
        timestamp = _parse_history_timestamp(record.get("timestamp"))
        if not timestamp or timestamp < cutoff:
            continue
        try:
            order_index = int(record.get("order_index") or 0)
        except (TypeError, ValueError):
            order_index = 0
        if order_index != 0:
            continue
        channel_id = record.get("channel_id")
        stream_id = record.get("stream_id") or record.get("id")
        if channel_id is None or stream_id is None:
            continue
        top_streams_by_channel[str(channel_id)].append(
            (timestamp, str(stream_id))
        )

    for channel_key, timeline in top_streams_by_channel.items():
        if not timeline:
            continue
        timeline.sort(key=lambda item: item[0])
        last_stream = None
        for timestamp, stream_id in timeline:
            if last_stream and stream_id != last_stream:
                # Playback swap/failover evidence: penalize the stream that was replaced.
                penalties[last_stream] -= 10.0
            last_stream = stream_id

    for record in _read_ndjson(suggestions_path):
        timestamp = _parse_history_timestamp(
            record.get("timestamp")
            or record.get("observed_at")
            or record.get("created_at")
        )
        if not timestamp or timestamp < cutoff:
            continue
        channel_id = record.get("channel_id") or record.get("id")
        if channel_id is None:
            continue
        channel_key = str(channel_id)
        usage = suggestion_usage_by_channel.setdefault(
            channel_key,
            {"channel_id": channel_id, "suggestion_present": False, "ordering_used": False},
        )
        usage["suggestion_present"] = True
        reason = str(
            record.get("reason")
            or record.get("suggestion_reason")
            or record.get("message")
            or ""
        ).lower()
        if not reason:
            continue
        timeline = top_streams_by_channel.get(channel_key, [])
        if not timeline:
            continue
        last_stream = None
        for entry_time, stream_id in timeline:
            if entry_time <= timestamp:
                last_stream = stream_id
            else:
                break
        if not last_stream:
            continue
        if "unstable" in reason or "switch" in reason:
            # Probe-level instability signal from suggestions: small deterministic penalty.
            penalties[last_stream] -= 5.0
            usage["ordering_used"] = True

    for usage in suggestion_usage_by_channel.values():
        logging.info(
            "ordering_input channel=%s suggestion_present=%s ordering_used=%s",
            usage["channel_id"],
            usage["suggestion_present"],
            usage["ordering_used"],
        )

    capped = {}
    for stream_id, penalty in penalties.items():
        capped[stream_id] = max(penalty, -15.0)
    return capped


def _apply_historical_penalties(summary, penalties):
    if not penalties:
        summary['historical_penalty'] = 0.0
        return summary
    summary['historical_penalty'] = summary['stream_id'].apply(
        lambda stream_id: penalties.get(str(stream_id), 0.0)
    )
    return summary


PLAYBACK_HISTORY_WINDOW_HOURS = 168
PLAYBACK_RECENT_WINDOW_HOURS = 24
HISTORY_ADJUSTMENT_CAP_RATIO = 0.25
HISTORY_EARLY_ABANDON_WEIGHT = 0.12
HISTORY_DURATION_WEIGHT = 0.08
HISTORY_TREND_WEIGHT = 0.05


def _summarize_playback_history(window_hours=PLAYBACK_HISTORY_WINDOW_HOURS):
    playback_path = Path("logs") / "playback_sessions.ndjson"
    if not playback_path.exists():
        return {}

    sessions = []
    for record in _read_ndjson(playback_path):
        stream_id = record.get("stream_id")
        if stream_id is None:
            continue
        try:
            stream_id = int(stream_id)
        except (TypeError, ValueError):
            continue
        start_ts = _parse_history_timestamp(record.get("session_start"))
        end_ts = _parse_history_timestamp(record.get("session_end"))
        if not start_ts or not end_ts:
            continue
        duration_seconds = record.get("duration_seconds")
        if duration_seconds is None:
            duration_seconds = max(0, int((end_ts - start_ts).total_seconds()))
        try:
            duration_seconds = int(duration_seconds)
        except (TypeError, ValueError):
            continue
        sessions.append(
            {
                "stream_id": stream_id,
                "session_start": start_ts,
                "session_end": end_ts,
                "duration_seconds": duration_seconds,
                "early_abandonment": bool(record.get("early_abandonment")),
            }
        )

    if not sessions:
        return {}

    latest_ts = max(session["session_end"] for session in sessions)
    window_cutoff = latest_ts - timedelta(hours=window_hours)
    recent_hours = min(PLAYBACK_RECENT_WINDOW_HOURS, window_hours)
    recent_cutoff = latest_ts - timedelta(hours=recent_hours)
    prior_cutoff = recent_cutoff - timedelta(hours=recent_hours)

    buckets = defaultdict(lambda: {"window": [], "recent": [], "prior": []})
    for session in sessions:
        if session["session_start"] < window_cutoff:
            continue
        bucket = buckets[str(session["stream_id"])]
        bucket["window"].append(session)
        if session["session_start"] >= recent_cutoff:
            bucket["recent"].append(session)
        elif session["session_start"] >= prior_cutoff:
            bucket["prior"].append(session)

    def _bucket_stats(items):
        if not items:
            return {
                "total_sessions": 0,
                "median_session_duration_seconds": None,
                "early_abandonment_rate": None,
            }
        durations = [item["duration_seconds"] for item in items]
        total = len(items)
        early_count = sum(1 for item in items if item["early_abandonment"])
        return {
            "total_sessions": total,
            "median_session_duration_seconds": median(durations) if durations else None,
            "early_abandonment_rate": early_count / total if total else None,
        }

    summary = {}
    for stream_id, bucket in buckets.items():
        window_stats = _bucket_stats(bucket["window"])
        recent_stats = _bucket_stats(bucket["recent"])
        prior_stats = _bucket_stats(bucket["prior"])
        summary[stream_id] = {
            **window_stats,
            "recent_early_abandonment_rate": recent_stats["early_abandonment_rate"],
            "prior_early_abandonment_rate": prior_stats["early_abandonment_rate"],
        }
    return summary


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))


def _compute_history_adjustment(record, playback_summary):
    if not playback_summary:
        return 0.0
    stream_id = record.get("stream_id")
    if stream_id is None:
        return 0.0
    summary = playback_summary.get(str(stream_id))
    if not summary:
        return 0.0
    base_score = _safe_float(record.get("base_score"))
    if base_score is None or base_score <= 0:
        return 0.0

    early_rate = summary.get("early_abandonment_rate")
    median_duration = summary.get("median_session_duration_seconds")
    recent_rate = summary.get("recent_early_abandonment_rate")
    prior_rate = summary.get("prior_early_abandonment_rate")

    early_factor = 0.0
    if early_rate is not None:
        early_factor = _clamp(1 - (2 * float(early_rate)), -1.0, 1.0)

    duration_factor = 0.0
    if median_duration is not None:
        target_span = max(1.0, float(EARLY_ABANDONMENT_SECONDS) * 4.0)
        duration_factor = _clamp(
            (float(median_duration) - float(EARLY_ABANDONMENT_SECONDS)) / target_span,
            -1.0,
            1.0,
        )

    trend_factor = 0.0
    if recent_rate is not None and prior_rate is not None:
        trend_delta = float(prior_rate) - float(recent_rate)
        trend_factor = _clamp(trend_delta / 0.5, -1.0, 1.0)

    adjustment_ratio = (
        early_factor * HISTORY_EARLY_ABANDON_WEIGHT
        + duration_factor * HISTORY_DURATION_WEIGHT
        + trend_factor * HISTORY_TREND_WEIGHT
    )
    adjustment_ratio = _clamp(
        adjustment_ratio,
        -HISTORY_ADJUSTMENT_CAP_RATIO,
        HISTORY_ADJUSTMENT_CAP_RATIO,
    )
    adjustment = base_score * adjustment_ratio

    validation_value = str(
        record.get("validation_result") or record.get("status") or ""
    ).strip().lower()
    if validation_value == "fail" and adjustment > 0:
        adjustment = 0.0

    return adjustment


def _apply_playback_history_adjustments(summary, playback_summary):
    if not playback_summary:
        summary['history_adjustment'] = 0.0
        return summary
    summary['history_adjustment'] = summary.apply(
        lambda row: _compute_history_adjustment(row, playback_summary), axis=1
    )
    return summary


RELIABILITY_WINDOW_HOURS = 168  # 7 days: long enough to capture stability trends.
RELIABILITY_RECENT_HOURS = 24  # Recent window to ensure fresh failures dominate.
RELIABILITY_SNAPSHOT_PATH = Path("logs") / "reliability_snapshot.json"
# Weighting constants are deterministic and documented to keep scoring auditable.
# Base band separation keeps recent-failure streams strictly below no-recent-failure streams
# while avoiding an oversized "cliff" in the score itself.
RELIABILITY_NO_RECENT_FAILURE_BONUS = 1.0
RELIABILITY_ADJUSTMENT_CAP = 0.4  # Adjustment stays below the base-band gap.
RELIABILITY_SUCCESS_WEIGHT = 0.01  # Each observed top-stream snapshot increases reliability.
RELIABILITY_RECENT_SUCCESS_WEIGHT = 0.03  # Recent successes matter more than older ones.
RELIABILITY_FAILURE_WEIGHT = 0.05  # Each observed switch-away reduces reliability.
RELIABILITY_RECENT_FAILURE_WEIGHT = 0.12  # Recent failures carry extra penalty.
RELIABILITY_INSTABILITY_FAILURE_UNITS = 2  # Instability indicators count as multiple failures.


def _reliability_input_signature(logs_dir: Path):
    """Capture inputs for reliability snapshots (append-only logs only)."""
    # Keep this list aligned with every file read in _compute_reliability_snapshot.
    # If new outcome logs are introduced, they must be added here to avoid stale snapshots.
    inputs = {}
    for name in ("quality_checks.ndjson", "quality_check_suggestions.ndjson"):
        path = logs_dir / name
        if path.exists():
            stat = path.stat()
            inputs[name] = {
                "mtime": stat.st_mtime,
                "size": stat.st_size,
            }
        else:
            inputs[name] = None
    return inputs


def _load_reliability_snapshot(logs_dir: Path):
    """Read cached reliability snapshot when inputs match."""
    snapshot_path = RELIABILITY_SNAPSHOT_PATH
    if not snapshot_path.exists():
        return None
    try:
        with snapshot_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        logging.warning("Failed to read reliability snapshot: %s", exc)
        return None

    expected_inputs = _reliability_input_signature(logs_dir)
    if payload.get("inputs") != expected_inputs:
        return None
    if payload.get("window_hours") != RELIABILITY_WINDOW_HOURS:
        return None
    if payload.get("recent_hours") != RELIABILITY_RECENT_HOURS:
        return None
    return payload


def _write_reliability_snapshot(logs_dir: Path, payload: dict):
    """Persist reliability snapshot to an explicit cache file."""
    snapshot_path = RELIABILITY_SNAPSHOT_PATH
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _compute_reliability_snapshot(logs_dir: Path):
    """Derive reliability metrics from historical logs and cache the results."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=RELIABILITY_WINDOW_HOURS)
    recent_cutoff = now - timedelta(hours=RELIABILITY_RECENT_HOURS)
    quality_checks_path = logs_dir / "quality_checks.ndjson"
    suggestions_path = logs_dir / "quality_check_suggestions.ndjson"
    # Reliability currently derives only from quality checks and suggestions logs.
    # No explicit stream selection outcome log is available in this repository.

    top_streams_by_channel = defaultdict(list)
    for record in _read_ndjson(quality_checks_path):
        timestamp = _parse_history_timestamp(record.get("timestamp"))
        if not timestamp or timestamp < cutoff:
            continue
        try:
            order_index = int(record.get("order_index") or 0)
        except (TypeError, ValueError):
            order_index = 0
        if order_index != 0:
            continue
        channel_id = record.get("channel_id")
        stream_id = record.get("stream_id") or record.get("id")
        if channel_id is None or stream_id is None:
            continue
        top_streams_by_channel[str(channel_id)].append(
            (timestamp, str(stream_id))
        )

    for timeline in top_streams_by_channel.values():
        timeline.sort(key=lambda item: item[0])

    success_counts = defaultdict(int)
    recent_success_counts = defaultdict(int)
    failure_counts = defaultdict(int)
    recent_failure_counts = defaultdict(int)

    for timeline in top_streams_by_channel.values():
        last_stream = None
        for timestamp, stream_id in timeline:
            success_counts[stream_id] += 1
            if timestamp >= recent_cutoff:
                recent_success_counts[stream_id] += 1
            if last_stream and stream_id != last_stream:
                failure_counts[last_stream] += 1
                if timestamp >= recent_cutoff:
                    recent_failure_counts[last_stream] += 1
            last_stream = stream_id

    instability_counts = defaultdict(int)
    recent_instability_counts = defaultdict(int)
    for record in _read_ndjson(suggestions_path):
        timestamp = _parse_history_timestamp(
            record.get("timestamp")
            or record.get("observed_at")
            or record.get("created_at")
        )
        if not timestamp or timestamp < cutoff:
            continue
        channel_id = record.get("channel_id") or record.get("id")
        if channel_id is None:
            continue
        reason = str(
            record.get("reason")
            or record.get("suggestion_reason")
            or record.get("message")
            or ""
        ).lower()
        if not reason:
            continue
        # Map explicit instability reasons to deterministic failure units.
        if "repeated_switching" in reason:
            failure_units = RELIABILITY_INSTABILITY_FAILURE_UNITS
        elif "unstable" in reason or "switch" in reason:
            failure_units = 1
        else:
            continue

        timeline = top_streams_by_channel.get(str(channel_id), [])
        if not timeline:
            continue
        last_stream = None
        for entry_time, stream_id in timeline:
            if entry_time <= timestamp:
                last_stream = stream_id
            else:
                break
        if not last_stream:
            continue
        instability_counts[last_stream] += failure_units
        if timestamp >= recent_cutoff:
            recent_instability_counts[last_stream] += failure_units

    streams = {}
    all_stream_ids = set(success_counts) | set(failure_counts) | set(instability_counts)
    for stream_id in all_stream_ids:
        total_successes = success_counts.get(stream_id, 0)
        recent_successes = recent_success_counts.get(stream_id, 0)
        total_failures = (
            failure_counts.get(stream_id, 0)
            + instability_counts.get(stream_id, 0)
        )
        recent_failures = (
            recent_failure_counts.get(stream_id, 0)
            + recent_instability_counts.get(stream_id, 0)
        )

        # Recent failures gate overall reliability so they never outrank streams without them.
        # The base-band separation is small (not a giant cliff) but remains larger than any
        # adjustment so recovery happens once failures age out of the recent window.
        base_score = RELIABILITY_NO_RECENT_FAILURE_BONUS if recent_failures == 0 else 0.0
        adjustment = (
            (recent_successes * RELIABILITY_RECENT_SUCCESS_WEIGHT)
            + (total_successes * RELIABILITY_SUCCESS_WEIGHT)
            - (recent_failures * RELIABILITY_RECENT_FAILURE_WEIGHT)
            - (total_failures * RELIABILITY_FAILURE_WEIGHT)
        )
        adjustment = max(-RELIABILITY_ADJUSTMENT_CAP, min(RELIABILITY_ADJUSTMENT_CAP, adjustment))
        reliability_score = base_score + adjustment

        streams[stream_id] = {
            "reliability_score": reliability_score,
            "recent_failures": recent_failures,
            "recent_successes": recent_successes,
            "total_failures": total_failures,
            "total_successes": total_successes,
            "instability_events": instability_counts.get(stream_id, 0),
        }

    return {
        "computed_at": now.isoformat().replace("+00:00", "Z"),
        "window_hours": RELIABILITY_WINDOW_HOURS,
        "recent_hours": RELIABILITY_RECENT_HOURS,
        "inputs": _reliability_input_signature(logs_dir),
        "streams": streams,
    }


def _load_or_compute_reliability_snapshot(logs_dir: Path):
    """Return cached reliability snapshot, recomputing only when inputs change."""
    cached = _load_reliability_snapshot(logs_dir)
    if cached is not None:
        return cached
    snapshot = _compute_reliability_snapshot(logs_dir)
    _write_reliability_snapshot(logs_dir, snapshot)
    return snapshot


def _score_streams_proxy_first(
    df,
    include_provider_name,
    output_csv,
    update_stats,
    api,
    historical_penalties,
    provider_capacity_biases,
    playback_summary,
):
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
    summary['base_score'] = summary.apply(_continuous_ordering_score, axis=1)
    summary = _apply_playback_history_adjustments(summary, playback_summary)
    summary = _apply_historical_penalties(summary, historical_penalties)
    summary = _apply_provider_capacity_bias(summary, provider_capacity_biases)
    summary['provider_capacity_bias'] = summary.apply(
        lambda row: min(
            float(row.get('provider_capacity_bias') or 0.0),
            float(row.get('base_score') or 0.0) * 0.05,
        ),
        axis=1,
    )
    summary['final_score'] = (
        summary['base_score']
        + summary['history_adjustment']
        + summary['historical_penalty']
        + summary['provider_capacity_bias']
    )
    summary['ordering_score'] = summary['final_score']

    summary['provider_sort_key'] = summary.apply(
        lambda row: str(
            row.get('m3u_account_name')
            or row.get('m3u_account')
            or ''
        ).lower(),
        axis=1,
    )
    df_sorted = summary.sort_values(
        by=['channel_number', 'final_score', 'base_score', 'provider_sort_key', 'stream_id'],
        ascending=[True, False, False, True, True],
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
        'startup_score', 'stability_score', 'base_score', 'history_adjustment',
        'historical_penalty', 'final_score',
        'provider_capacity_bias', 'provider_capacity_factor', 'provider_max_streams',
        'ordering_score'
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


def _score_streams_legacy(
    df,
    scoring_cfg,
    include_provider_name,
    output_csv,
    update_stats,
    api,
    historical_penalties,
    provider_capacity_biases,
    playback_summary,
):
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
    summary['base_score'] = summary.apply(_continuous_ordering_score, axis=1)
    summary = _apply_playback_history_adjustments(summary, playback_summary)
    summary = _apply_historical_penalties(summary, historical_penalties)
    summary = _apply_provider_capacity_bias(summary, provider_capacity_biases)
    summary['provider_capacity_bias'] = summary.apply(
        lambda row: min(
            float(row.get('provider_capacity_bias') or 0.0),
            float(row.get('base_score') or 0.0) * 0.05,
        ),
        axis=1,
    )
    summary['final_score'] = (
        summary['base_score']
        + summary['history_adjustment']
        + summary['historical_penalty']
        + summary['provider_capacity_bias']
    )
    summary['ordering_score'] = summary['final_score']

    summary['provider_sort_key'] = summary.apply(
        lambda row: str(
            row.get('m3u_account_name')
            or row.get('m3u_account')
            or ''
        ).lower(),
        axis=1,
    )
    df_sorted = summary.sort_values(
        by=['channel_number', 'final_score', 'base_score', 'provider_sort_key', 'stream_id'],
        ascending=[True, False, False, True, True],
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
        'interlaced_status', 'status', 'score', 'base_score', 'history_adjustment',
        'historical_penalty', 'final_score',
        'provider_capacity_bias', 'provider_capacity_factor', 'provider_max_streams',
        'ordering_score'
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
    historical_penalties = _historical_stream_penalties()
    provider_metadata = _load_provider_metadata(config)
    provider_capacity_biases = _build_provider_capacity_biases(df, provider_metadata)
    playback_summary = _summarize_playback_history()

    if proxy_first:
        _score_streams_proxy_first(
            df,
            include_provider_name,
            output_csv,
            update_stats,
            api,
            historical_penalties,
            provider_capacity_biases,
            playback_summary,
        )
    else:
        _score_streams_legacy(
            df,
            scoring_cfg,
            include_provider_name,
            output_csv,
            update_stats,
            api,
            historical_penalties,
            provider_capacity_biases,
            playback_summary,
        )


def _parse_resolution(resolution):
    try:
        width_str, height_str = str(resolution).lower().split('x')
        return int(width_str), int(height_str)
    except Exception:
        return None


def _is_hd_or_better_resolution(parsed_resolution):
    if not parsed_resolution:
        return False
    _, height = parsed_resolution
    return height >= 720


def _normalize_numeric(value):
    if value in (None, '', 'N/A'):
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _safe_float(value):
    try:
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _parse_rate(rate_value):
    if rate_value in (None, '', 'N/A'):
        return None
    text = str(rate_value)
    if '/' in text:
        try:
            numerator, denominator = text.split('/', 1)
            denominator = float(denominator)
            if denominator == 0:
                return None
            return float(numerator) / denominator
        except (ValueError, TypeError):
            return None
    return _safe_float(text)


def _stable_tiebreaker(value):
    """
    Generate a tiny deterministic value for tie-breaking.

    This ensures ordering stability without relying on input list order.
    """
    digest = hashlib.blake2b(str(value).encode('utf-8'), digest_size=8).hexdigest()
    max_int = float(2**64 - 1)
    return int(digest, 16) / max_int * 1e-6


PROVIDER_CAPACITY_WEIGHT = 0.35


def _load_provider_metadata(config):
    provider_metadata_path = config.resolve_path('provider_metadata.json')
    try:
        with open(provider_metadata_path, 'r', encoding='utf-8') as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
        logging.warning("provider_metadata.json must be a JSON object mapping provider_id to metadata.")
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logging.warning("Could not load provider_metadata.json: %s", exc)
    return {}


def _build_provider_capacity_biases(df, provider_metadata):
    """Build normalized capacity bias per provider using known max stream limits."""
    if df is None or df.empty or not provider_metadata:
        return {}
    if 'm3u_account' not in df.columns:
        return {}

    capacity_by_provider = {}
    for provider_id in df['m3u_account'].dropna().astype(str).unique():
        meta = provider_metadata.get(provider_id)
        if not isinstance(meta, dict):
            continue
        max_streams = meta.get('max_streams')
        if isinstance(max_streams, (int, float)) and max_streams > 0:
            capacity_by_provider[provider_id] = int(max_streams)

    if not capacity_by_provider:
        return {}

    max_seen = max(capacity_by_provider.values())
    if max_seen <= 0:
        return {}

    biases = {}
    for provider_id, max_streams in capacity_by_provider.items():
        capacity_factor = max_streams / max_seen
        biases[provider_id] = {
            'max_streams': max_streams,
            'capacity_factor': capacity_factor,
            # Small, capped tie-breaker so quality wins unless scores are already close.
            'capacity_bias': capacity_factor * PROVIDER_CAPACITY_WEIGHT,
        }
    return biases


def _apply_provider_capacity_bias(summary, provider_capacity_biases):
    if not provider_capacity_biases or 'm3u_account' not in summary.columns:
        summary['provider_capacity_bias'] = 0.0
        summary['provider_capacity_factor'] = 0.0
        summary['provider_max_streams'] = None
        return summary

    def _bias_for_provider(value):
        payload = provider_capacity_biases.get(str(value), {})
        return payload.get('capacity_bias', 0.0)

    def _factor_for_provider(value):
        payload = provider_capacity_biases.get(str(value), {})
        return payload.get('capacity_factor', 0.0)

    def _max_for_provider(value):
        payload = provider_capacity_biases.get(str(value), {})
        return payload.get('max_streams')

    summary['provider_capacity_bias'] = summary['m3u_account'].apply(_bias_for_provider)
    summary['provider_capacity_factor'] = summary['m3u_account'].apply(_factor_for_provider)
    summary['provider_max_streams'] = summary['m3u_account'].apply(_max_for_provider)
    return summary


def _codec_quality_score(codec_value):
    codec = str(codec_value or '').lower()
    if 'h264' in codec or 'avc' in codec:
        return 0.95
    if 'hevc' in codec or '265' in codec:
        return 0.95
    if 'av1' in codec:
        return 0.85
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


def _codec_confidence_multiplier(codec_value):
    codec = str(codec_value or '').lower()
    if not codec or codec in ('n/a', 'unknown'):
        return 0.95
    if 'h264' in codec or 'avc' in codec:
        return 1.03
    if 'hevc' in codec or '265' in codec:
        return 1.03
    return 0.98


def _firestick_bitrate_penalty(codec_value, avg_bitrate_kbps):
    if avg_bitrate_kbps is None or avg_bitrate_kbps <= 0:
        return 1.0
    codec = str(codec_value or '').lower()
    if 'h264' in codec or 'avc' in codec:
        if avg_bitrate_kbps >= 25000:
            return 0.94
        if avg_bitrate_kbps >= 19000:
            return 0.97
        return 1.0
    if 'hevc' in codec or '265' in codec:
        if avg_bitrate_kbps >= 35000:
            return 0.94
        if avg_bitrate_kbps >= 30000:
            return 0.97
        return 1.0
    return 1.0


def _decoded_frames_from_record(record):
    avg_frames = record.get('avg_frames_decoded')
    if avg_frames is None:
        return record.get('frames_decoded')
    return avg_frames


def _decoded_frames_value(record):
    return _safe_float(_decoded_frames_from_record(record))


def _validation_tokens(record):
    reason = record.get('validation_reason')
    tokens = []
    if isinstance(reason, str):
        for token in re.split(r'[;,]', reason):
            token = token.strip()
            if token:
                tokens.append(token)
    elif isinstance(reason, (list, tuple, set)):
        for token in reason:
            token = str(token).strip()
            if token:
                tokens.append(token)

    for err_key in ('err_decode', 'err_discontinuity'):
        value = record.get(err_key)
        try:
            has_error = bool(int(value))
        except Exception:
            has_error = bool(value)
        if has_error and err_key not in tokens:
            tokens.append(err_key)

    if 'avg_frames_decoded' in record:
        frames = _safe_float(record.get('avg_frames_decoded'))
        if frames == 0 and 'no_frames_decoded' not in tokens:
            tokens.append('no_frames_decoded')

    return tokens


def _decoded_frames_multiplier(decoded_frames):
    if decoded_frames is None:
        return 1.0
    if decoded_frames == 0:
        return 0.7
    if decoded_frames < 30:
        return 0.9
    if decoded_frames > 200:
        return 1.02
    return 1.0


def _bitrate_safe_for_fps(codec_value, avg_bitrate_kbps):
    if avg_bitrate_kbps is None or avg_bitrate_kbps <= 0:
        return False
    codec = str(codec_value or '').lower()
    if 'h264' in codec or 'avc' in codec:
        return avg_bitrate_kbps <= 19000
    if 'hevc' in codec or '265' in codec:
        return avg_bitrate_kbps <= 30000
    return False


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
    # Resolution is slightly more influential to better separate 1080p from 720p.
    resolution_score = math.log1p(total_pixels) / math.log1p(3840 * 2160)
    resolution_detail = math.sqrt(total_pixels) / math.sqrt(3840 * 2160) if total_pixels > 0 else 0.0
    fps_score = math.log1p(fps) / math.log1p(120)
    bitrate_score = math.log1p(avg_bitrate_kbps) / math.log1p(20000)
    base_component = math.log1p(max(base_score, 0.0))

    codec_score = _codec_quality_score(record.get('video_codec'))
    audio_score = _audio_quality_score(record.get('audio_codec'))

    interlaced_status = str(record.get('interlaced_status') or '').lower()
    interlace_penalty = 0.95 if 'interlaced' in interlaced_status else 1.0
    validation_penalty = _validation_penalty(record.get('validation_result') or record.get('status'))
    codec_confidence = _codec_confidence_multiplier(record.get('video_codec'))
    firestick_bitrate_penalty = _firestick_bitrate_penalty(record.get('video_codec'), avg_bitrate_kbps)

    # Probe-derived fields used for weak confidence tweaks (never dominant):
    # format_name, r_frame_rate, declared_bitrate_kbps, video_profile, video_level,
    # pixel_format, avg_frames_decoded, avg_frames_dropped, video_stream_count, audio_stream_count.
    format_name = record.get('format_name')
    r_frame_rate_raw = record.get('r_frame_rate')
    r_frame_rate = _parse_rate(r_frame_rate_raw)
    declared_bitrate_kbps = _safe_float(record.get('declared_bitrate_kbps'))
    video_profile = record.get('video_profile')
    video_level = record.get('video_level')
    pixel_format = record.get('pixel_format')
    declared_bitrate_present = declared_bitrate_kbps if declared_bitrate_kbps and declared_bitrate_kbps > 0 else None
    r_frame_rate_present = r_frame_rate if r_frame_rate and r_frame_rate > 0 else None

    decoded_frames = _safe_float(_decoded_frames_from_record(record))
    decoded_frames_multiplier = _decoded_frames_multiplier(decoded_frames)

    # Metadata completeness and gentle probe-structure penalties should be very weak.
    metadata_penalty = 1.0
    core_fps = _safe_float(record.get('fps'))
    core_bitrate = _safe_float(record.get('avg_bitrate_kbps'))
    core_incomplete = (
        not parsed
        or str(record.get('video_codec') or '').strip().upper() in ('', 'N/A')
        or core_fps is None
        or core_bitrate is None
    )
    confidence_weak = validation_penalty < 0.95 or (decoded_frames is not None and decoded_frames < 30)
    if core_incomplete and confidence_weak:
        # Treat missing core metadata as unknown quality; apply a mild, capped penalty only if confidence is weak.
        metadata_penalty *= 0.97
    if total_pixels == 0 and confidence_weak:
        metadata_penalty *= 0.98
    if str(record.get('video_codec') or '').upper() in ('', 'N/A') and confidence_weak:
        metadata_penalty *= 0.99
    if str(record.get('audio_codec') or '').upper() in ('', 'N/A') and confidence_weak:
        metadata_penalty *= 0.995

    completeness_checks = [
        format_name,
        video_profile,
        video_level,
        pixel_format,
        r_frame_rate_present,
        declared_bitrate_present
    ]
    present_count = sum(
        1
        for value in completeness_checks
        if value not in (None, '', 'N/A')
    )
    if present_count:
        completeness_ratio = present_count / len(completeness_checks)
        # Tiny bonus for richer, stable probe metadata (capped).
        metadata_penalty *= min(1.01, 1.0 + (completeness_ratio * 0.01))

    if fps > 0 and r_frame_rate:
        # Penalize only extreme average vs. real frame rate mismatches.
        mismatch = abs(fps - r_frame_rate) / max(fps, r_frame_rate)
        if mismatch > 0.5:
            metadata_penalty *= 0.99
        elif mismatch > 0.2:
            metadata_penalty *= 0.995

    if declared_bitrate_kbps and avg_bitrate_kbps > 0:
        # Penalize only large declared vs measured bitrate mismatches.
        ratio = max(declared_bitrate_kbps, avg_bitrate_kbps) / min(declared_bitrate_kbps, avg_bitrate_kbps)
        if ratio > 2.0:
            metadata_penalty *= 0.99
        elif ratio > 1.5:
            metadata_penalty *= 0.995

    structure_penalty = 1.0
    video_stream_count = record.get('video_stream_count')
    if video_stream_count not in (None, '', 'N/A'):
        try:
            if int(video_stream_count) > 1:
                structure_penalty *= 0.985
        except (TypeError, ValueError):
            pass
    audio_stream_count = record.get('audio_stream_count')
    if audio_stream_count not in (None, '', 'N/A'):
        try:
            extra_audio = max(int(audio_stream_count) - 2, 0)
            if extra_audio:
                structure_penalty *= max(1.0 - (extra_audio * 0.003), 0.985)
        except (TypeError, ValueError):
            pass

    dropped_frames = _safe_float(record.get('avg_frames_dropped') or record.get('frames_dropped'))
    if decoded_frames and dropped_frames is not None and decoded_frames > 0 and dropped_frames >= 0:
        # Use drop ratio only when measured data exists; keep penalties tiny.
        drop_ratio = dropped_frames / decoded_frames
        if drop_ratio > 0.2:
            structure_penalty *= 0.99
        elif drop_ratio > 0.05:
            structure_penalty *= 0.995

    fps_bonus = 1.0
    if (
        _is_hd_or_better_resolution(parsed)
        and _bitrate_safe_for_fps(record.get('video_codec'), avg_bitrate_kbps)
        and validation_penalty >= 0.95
        and (decoded_frames is None or decoded_frames >= 30)
    ):
        fps_bonus = 1.0 + min(fps, 60.0) / 60.0 * 0.02

    score = (
        base_component * 1.55
        + resolution_score * 2.2
        + resolution_detail * 0.4
        + fps_score * 0.55
        + bitrate_score * 0.9
        + codec_score * 0.32
        + audio_score * 0.12
    )
    score *= (
        interlace_penalty
        * validation_penalty
        * metadata_penalty
        * structure_penalty
        * codec_confidence
        * firestick_bitrate_penalty
        * decoded_frames_multiplier
        * fps_bonus
    )
    return score


def order_streams_for_channel(
    records,
    resilience_mode=False,
    fallback_depth=3,
    similar_score_delta=5,
    reliability_sort=False,
    return_details=False,
):
    """Order streams using a continuous, deterministic score.

    All streams remain present; deterministic tiering enforces health boundaries
    before ordering within each tier using the continuous score.
    Slot 1 is chosen only from the top safety tiers.

    Deprecated ordering toggles (resilience_mode, fallback_depth, similar_score_delta,
    reliability_sort) are accepted for compatibility but ignored.
    """

    if not records:
        return [] if not return_details else {
            'ordered_ids': [],
            'slot1_id': None,
            'slot1_rule': None,
            'slot1_reason': None,
            'slot1_overrode': False,
        }

    def _is_buffering(record):
        # Buffering detection is conservative and based on timeout signals only;
        # we intentionally assume buffering when evidence is ambiguous to protect
        # slot-1 UX without adding new probes or runtime hooks.
        status = str(record.get('status') or '').strip().lower()
        if status == 'timeout':
            return True
        try:
            if int(record.get('err_timeout', 0) or 0) > 0:
                return True
        except Exception:
            pass
        return False

    def _is_probe_failure(record):
        try:
            frames_decoded = float(_decoded_frames_from_record(record))
        except Exception:
            frames_decoded = None
        if frames_decoded == 0:
            return True
        validation_result = str(record.get('validation_result') or record.get('status') or '').strip().lower()
        if validation_result == 'fail' and not _is_buffering(record):
            return True
        status = str(record.get('status') or '').strip().lower()
        if status and status not in ('ok', 'timeout'):
            return True
        return any(
            token in ('err_decode', 'err_discontinuity', 'no_frames_decoded')
            or token.startswith('status:')
            for token in _validation_tokens(record)
        )

    def _is_validation_failed(record):
        validation_result = str(record.get('validation_result') or '').strip().lower()
        if validation_result == 'fail':
            return True
        if _is_probe_failure(record):
            return True
        status = str(record.get('status') or '').strip().lower()
        if status in ('timeout', 'error') or status.startswith('error') or status.startswith('err'):
            return True
        if 'avg_frames_decoded' in record and _safe_float(record.get('avg_frames_decoded')) == 0:
            return True
        return False

    def _near_ceiling_bitrate(record):
        avg_bitrate = _safe_float(record.get('avg_bitrate_kbps'))
        if avg_bitrate is None or avg_bitrate <= 0:
            return False
        codec = str(record.get('video_codec') or '').lower()
        if 'h264' in codec or 'avc' in codec:
            return avg_bitrate >= 25000
        if 'hevc' in codec or '265' in codec:
            return avg_bitrate >= 35000
        return False

    def _is_hd_or_better(record):
        resolution = record.get('resolution')
        parsed = _parse_resolution(resolution)
        if parsed:
            _, height = parsed
            return height >= 720
        # Fall back to coarse categories only when numeric parsing is impossible.
        category = _resolution_category(resolution)
        return category in ('hd', 'uhd')

    def _score_value(record):
        ordering = record.get('final_score')
        if ordering in (None, '', 'N/A'):
            ordering = record.get('ordering_score')
        if ordering in (None, '', 'N/A'):
            ordering = record.get('score')
        return _safe_float(ordering)

    for record in records:
        ordering_score = _safe_float(record.get('ordering_score'))
        score = _safe_float(record.get('score'))
        final_score = _safe_float(record.get('final_score'))
        record['ordering_score'] = ordering_score
        record['score'] = score
        if final_score is None:
            final_score = ordering_score
        if final_score is None:
            final_score = score
        # final_score is recomputed on each analysis run; reuse the current run's value for ordering.
        record['final_score'] = final_score

    def _status_value(record):
        return str(record.get('status') or '').strip().lower()

    def _has_decode_errors(record):
        for key in ('err_decode', 'err_discontinuity'):
            value = record.get(key)
            try:
                if int(value):
                    return True
            except Exception:
                if value:
                    return True
        return False

    def _timeout_count(record):
        try:
            return int(record.get('err_timeout', 0) or 0)
        except Exception:
            return 0

    def _missing_core_probe_metadata(record):
        fps = _safe_float(record.get('fps'))
        bitrate = _safe_float(record.get('avg_bitrate_kbps'))
        codec = str(record.get('video_codec') or '').strip().upper()
        return fps is None or bitrate is None or codec in ('', 'N/A')

    def _is_status_failure_non_timeout(record):
        status = _status_value(record)
        return bool(status) and status not in ('ok', 'timeout')

    def _basic_validation_pass(record):
        decoded_frames = _decoded_frames_value(record)
        decoded_frames_ok = decoded_frames is None or decoded_frames > 0
        return (
            _status_value(record) in ('', 'ok')
            and _timeout_count(record) == 0
            and not _has_decode_errors(record)
            and not _is_buffering(record)
            and decoded_frames_ok
        )

    def _tier_for_record(record):
        validation_result = str(record.get('validation_result') or '').strip().lower()
        if validation_result == 'fail':
            return 5
        decoded_frames = _decoded_frames_value(record)
        if decoded_frames == 0 or _has_decode_errors(record) or _is_status_failure_non_timeout(record):
            return 5
        if _status_value(record) == 'timeout' or _timeout_count(record) > 0 or _is_buffering(record):
            return 4
        if _basic_validation_pass(record):
            if decoded_frames is not None and decoded_frames > 30 and _is_hd_or_better(record):
                return 1
            if decoded_frames is not None and decoded_frames > 30 and not _is_hd_or_better(record):
                return 2
            weak_confidence = (
                decoded_frames is None
                or 0 < decoded_frames <= 30
                or _missing_core_probe_metadata(record)
                or _status_value(record) == ''
            )
            if weak_confidence:
                return 3
        return 3

    def _score_key(record):
        score_value = _score_value(record)
        if score_value is None:
            score_value = 0.0
        frames = _decoded_frames_value(record)
        if frames is None:
            frames = 0.0
        return (-frames, -score_value)

    tier1_records = [record for record in records if not _is_validation_failed(record)]
    tier2_records = [record for record in records if _is_validation_failed(record)]
    ordered = sorted(tier1_records, key=_score_key) + sorted(tier2_records, key=_score_key)

    strict_score_ordering = False
    if strict_score_ordering:
        # Enforce the invariant: when all ordering toggles are off, do not reshuffle
        # the score-sorted list (slot-1 safety overrides would violate strict ranking).
        ordered_ids = [r.get('stream_id') for r in ordered]
        if return_details:
            return {
                'ordered_ids': ordered_ids,
                'slot1_id': ordered_ids[0] if ordered_ids else None,
                'slot1_rule': None,
                'slot1_reason': None,
                'slot1_overrode': False,
            }
        return ordered_ids

    tiered_records = {tier: [] for tier in range(1, 6)}
    for record in records:
        tiered_records[_tier_for_record(record)].append(record)

    ordered = []
    for tier in range(1, 6):
        ordered.extend(sorted(tiered_records[tier], key=_score_key))

    tier1_sorted = sorted(tiered_records[1], key=_score_key)
    tier2_sorted = sorted(tiered_records[2], key=_score_key)
    tier3_sorted = sorted(tiered_records[3], key=_score_key)

    slot1_rule = None
    slot1_reason = None
    slot1 = None
    if tier1_sorted:
        slot1 = tier1_sorted[0].get('stream_id')
        slot1_rule = 'tier1_safe'
        slot1_reason = 'Selected from Tier 1 (Safe Live Now).'
    elif tier2_sorted:
        slot1 = tier2_sorted[0].get('stream_id')
        slot1_rule = 'tier2_sd_fallback'
        slot1_reason = 'Selected from Tier 2 (Safe SD Fallback).'
    elif tier3_sorted:
        slot1 = tier3_sorted[0].get('stream_id')
        slot1_rule = 'tier3_weak_signal'
        slot1_reason = 'Selected from Tier 3 (Weak Signal).'

    ordered_ids = [r.get('stream_id') for r in ordered]
    final_order = ordered_ids if slot1 is None else [slot1] + [sid for sid in ordered_ids if sid != slot1]
    if return_details:
        return {
            'ordered_ids': final_order,
            'slot1_id': slot1,
            'slot1_rule': slot1_rule,
            'slot1_reason': slot1_reason,
            'slot1_overrode': slot1 is not None and bool(ordered_ids) and ordered_ids[0] != slot1,
        }
    return final_order


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
    scoring_cfg = config.get('scoring') or {}
    _ignored_resilience_mode = ordering_cfg.get('resilience_mode')
    _ignored_reliability_sort = ordering_cfg.get('reliability_sort')
    try:
        fallback_depth = int(ordering_cfg.get('fallback_depth', 3) or 3)
    except (ValueError, TypeError):
        fallback_depth = 3
    try:
        similar_score_delta = float(ordering_cfg.get('similar_score_delta', 5) or 5)
    except (ValueError, TypeError):
        similar_score_delta = 5.0
    logging.info("Pure Now ordering with Firestick-optimised weighting active")

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

    grouped = {channel_id: group for channel_id, group in df.groupby("channel_id")}
    channel_order_df = df[['channel_id', 'channel_number']].dropna(subset=['channel_id']).drop_duplicates()
    channel_order_df['channel_number'] = pd.to_numeric(channel_order_df['channel_number'], errors='coerce')
    channel_order_df = channel_order_df.sort_values(['channel_number', 'channel_id'], na_position='last')
    channel_order = channel_order_df['channel_id'].tolist()

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

    for channel_id in channel_order:
        group = grouped.get(channel_id)
        if group is None:
            continue
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
                    # Pass through service metadata if the source stream already includes it.
                    'service_name': stream.get('service_name'),
                    'service_provider': stream.get('service_provider'),
                    'resolution': stream.get('resolution'),
                    'avg_bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate'),
                    'fps': stream.get('source_fps') or stream.get('fps'),
                    'video_codec': stream.get('video_codec'),
                    'audio_codec': stream.get('audio_codec'),
                    'interlaced_status': stream.get('interlaced_status'),
                    'status': stream.get('status'),
                }
            records_for_ordering.append(record)

        # Pure Now ordering is validation-dominant and ignores reliability/resilience toggles.
        ordering_details = order_streams_for_channel(
            records_for_ordering,
            resilience_mode=_ignored_resilience_mode,
            fallback_depth=fallback_depth,
            similar_score_delta=similar_score_delta,
            reliability_sort=_ignored_reliability_sort,
            return_details=collect_summary,
        )
        if collect_summary:
            ordered_stream_ids = ordering_details.get('ordered_ids', [])
            slot1_stream_id = ordering_details.get('slot1_id')
            slot1_rule = ordering_details.get('slot1_rule')
            slot1_reason = ordering_details.get('slot1_reason')
            slot1_overrode = ordering_details.get('slot1_overrode')
        else:
            ordered_stream_ids = ordering_details
            slot1_stream_id = None
            slot1_rule = None
            slot1_reason = None
            slot1_overrode = False

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
                    # Pass through service metadata if the source stream already includes it.
                    'service_name': stream.get('service_name'),
                    'service_provider': stream.get('service_provider'),
                    'resolution': stream.get('resolution'),
                    'bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate')
                }

            channel_meta = channel_lookup.get(_safe_int(channel_id), {}) if channel_lookup else {}
            channel_entry = {
                'channel_id': _safe_int(channel_id),
                'channel_number': channel_meta.get('channel_number'),
                'channel_name': channel_meta.get('name'),
                'final_order': [],
                'excluded_streams': [],
                'slot1_stream_id': slot1_stream_id,
                'slot1_rule': slot1_rule,
                'slot1_reason': slot1_reason,
                'slot1_overrode': slot1_overrode,
            }

            def _merge_entry(sid, source_record=None):
                source_record = source_record or {}
                fallback = current_lookup.get(sid) or {}
                entry = {
                    'stream_id': sid,
                    'stream_name': source_record.get('stream_name') or fallback.get('stream_name'),
                    'provider_id': source_record.get('m3u_account') if isinstance(source_record, dict) else None,
                    'provider_name': source_record.get('m3u_account_name') if isinstance(source_record, dict) else None,
                    # Pass through service metadata if the source stream already includes it.
                    'service_name': source_record.get('service_name') if isinstance(source_record, dict) else None,
                    'service_provider': source_record.get('service_provider') if isinstance(source_record, dict) else None,
                    'resolution': source_record.get('resolution'),
                    'video_codec': source_record.get('video_codec'),
                    'bitrate_kbps': source_record.get('avg_bitrate_kbps'),
                    'validation_result': source_record.get('validation_result') or source_record.get('status'),
                    'validation_reason': source_record.get('validation_reason'),
                    'provider_capacity_bias': source_record.get('provider_capacity_bias'),
                    'provider_capacity_factor': source_record.get('provider_capacity_factor'),
                    'provider_max_streams': source_record.get('provider_max_streams'),
                    'final_score': (
                        source_record.get('final_score')
                        if source_record.get('final_score') not in (None, 'N/A')
                        else (
                            source_record.get('ordering_score')
                            if source_record.get('ordering_score') not in (None, 'N/A')
                            else source_record.get('score')
                        )
                    )
                }

                if entry.get('provider_id') is None and isinstance(fallback, dict):
                    entry['provider_id'] = fallback.get('provider_id')
                if entry.get('provider_name') is None and isinstance(fallback, dict):
                    entry['provider_name'] = fallback.get('provider_name')
                if entry.get('service_name') is None and isinstance(fallback, dict):
                    entry['service_name'] = fallback.get('service_name')
                if entry.get('service_provider') is None and isinstance(fallback, dict):
                    entry['service_provider'] = fallback.get('service_provider')
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
                if idx == 1 and sid == slot1_stream_id:
                    merged['slot1_rule'] = slot1_rule
                    merged['slot1_reason'] = slot1_reason
                    merged['slot1_overrode'] = slot1_overrode
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
                        'service_name': merged.get('service_name'),
                        'service_provider': merged.get('service_provider'),
                        'resolution': merged.get('resolution'),
                        'bitrate_kbps': merged.get('bitrate_kbps'),
                        'provider_capacity_bias': merged.get('provider_capacity_bias'),
                        'provider_capacity_factor': merged.get('provider_capacity_factor'),
                        'provider_max_streams': merged.get('provider_max_streams'),
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

def refresh_channel_streams(api, config, channel_id, base_search_text=None, include_filter=None, exclude_filter=None, exclude_plus_one=False, allowed_stream_ids=None, preview=False, stream_name_regex=None, stream_name_regex_override=None, all_streams_override=None, all_channels_override=None, provider_names=None, clear_learned_rules=False):
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
    import json
    import os
    import re
    from datetime import datetime, timezone

    # Performance: precompile regexes used in matching loops
    _QUALITY_RE = re.compile(r'\b(?:hd|sd|fhd|4k|uhd|hevc|h264|h265)\b', flags=re.IGNORECASE)
    _TIMESHIFT_SEMANTIC_RE = re.compile(r'(?:\+\s*\d+|\bplus\s*\d+\b)', flags=re.IGNORECASE)
    _QUALITY_NEUTRAL_TOKENS = {
        'sd', 'hd', 'fhd', 'uhd', '4k', '8k',
        'hevc', 'h264', 'h265', 'avc', 'x264', 'x265',
        '720p', '1080p', '1440p', '2160p', '4320p',
        'hdr', 'hdr10', 'dolby', 'vision',
    }

    _REGION_NEUTRAL_TOKENS = {
        'england', 'wales', 'scotland', 'ni', 'northern',
        'london', 'midlands', 'yorkshire',
        'north', 'south', 'east', 'west'
    }

    _PREFIX_NEUTRAL_TOKENS = {
        'uk', 'us', 'usa', 'ca', 'au', 'ie', 'de', 'fr', 'es', 'it'
    }
    
    _NUMERIC_WORD_ALIASES = {
        'one': '1',
        'two': '2',
        'three': '3',
        'four': '4',
        'five': '5',
        'six': '6',
        'seven': '7',
        'eight': '8',
        'nine': '9',
        'ten': '10',
    }

    def canonicalize_name(text):
        """Lowercase and normalize punctuation/whitespace for name comparisons."""
        lowered = text.lower()
        cleaned = re.sub(r'[^0-9a-z]', ' ', lowered)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if not cleaned:
            return cleaned
        tokens = cleaned.split(' ')
        normalized_tokens = [
            _NUMERIC_WORD_ALIASES.get(token, token)
            for token in tokens
            if token
        ]
        return ' '.join(normalized_tokens)
    
    def strip_quality(text):
        """Remove quality indicators"""
        return _QUALITY_RE.sub('', text).strip()

    def tokenize_canonical(text):
        return [token for token in canonicalize_name(text).split(' ') if token]

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

    def _deny_log_path():
        return os.path.join('logs', 'channel_stream_denies.ndjson')

    def _load_channel_denies(target_channel_id):
        denied_ids = set()
        deny_path = _deny_log_path()
        if not os.path.exists(deny_path):
            return denied_ids
        try:
            with open(deny_path, 'r', encoding='utf-8') as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if str(record.get('channel_id')) != str(target_channel_id):
                        continue
                    stream_id = record.get('stream_id')
                    if stream_id is None:
                        continue
                    try:
                        denied_ids.add(int(stream_id))
                    except (TypeError, ValueError):
                        continue
        except OSError as exc:
            logging.error(f"Failed reading deny log: {exc}")
        return denied_ids

    def _append_channel_denies(records):
        if not records:
            return
        deny_path = _deny_log_path()
        os.makedirs(os.path.dirname(deny_path), exist_ok=True)
        with open(deny_path, 'a', encoding='utf-8') as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False) + '\n')

    def _clear_channel_denies(target_channel_id):
        deny_path = _deny_log_path()
        if not os.path.exists(deny_path):
            return
        try:
            retained_lines = []
            removed = False
            with open(deny_path, 'r', encoding='utf-8') as handle:
                for line in handle:
                    raw_line = line.strip()
                    if not raw_line:
                        continue
                    try:
                        record = json.loads(raw_line)
                    except json.JSONDecodeError:
                        retained_lines.append(line)
                        continue
                    if str(record.get('channel_id')) == str(target_channel_id):
                        removed = True
                        continue
                    retained_lines.append(line)
            if not removed:
                return
            if retained_lines:
                tmp_path = f"{deny_path}.tmp"
                with open(tmp_path, 'w', encoding='utf-8') as handle:
                    handle.writelines(retained_lines)
                os.replace(tmp_path, deny_path)
            else:
                os.remove(deny_path)
        except OSError as exc:
            logging.error(f"Failed clearing deny log: {exc}")

    if clear_learned_rules:
        _clear_channel_denies(channel_id)

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
            logging.info("Using stream-name REGEX OVERRIDE (base/include/exclude rules ignored).")
        except re.error as exc:
            logging.error(f"Invalid stream_name_regex_override: {exc}")
            return {'error': f'Invalid regex override: {exc}'}

    # Precompute constants used for all stream comparisons (initialized later,
    # once `search_name` is known).
    _selected_normalized = None
    _selected_has_timeshift = None
    _selected_numeric_re = None
    _selected_tokens = None
    _selected_token_set = None
    _include_regexes = None
    _exclude_regexes = None
    _alias_neutral_tokens = set()
    _derived_negative_tokens = set()
    
    def _find_match_window(stream_tokens, selected_tokens):
        if not selected_tokens:
            return None
        selected_len = len(selected_tokens)
        for start in range(len(stream_tokens) - selected_len + 1):
            if stream_tokens[start:start + selected_len] == selected_tokens:
                return start + selected_len
        selected_index = 0
        end_index = None
        for idx, token in enumerate(stream_tokens):
            if token == selected_tokens[selected_index]:
                selected_index += 1
                end_index = idx + 1
                if selected_index == selected_len:
                    return end_index
        return None

    def _passes_semantic_suffix(selected_tokens, stream_tokens):
        match_end = _find_match_window(stream_tokens, selected_tokens)
        if match_end is None:
            return False
        # Prefix tokens (like ones in _PREFIX_NEUTRAL_TOKENS) must not be treated as suffixes.
        suffix_tokens = stream_tokens[match_end:]
        for token in suffix_tokens:
            if (
                token not in _QUALITY_NEUTRAL_TOKENS
                and token not in _alias_neutral_tokens
                and token not in _REGION_NEUTRAL_TOKENS
            ):
                return False
        return True

    def _is_timeshift_token(token):
        if token == 'plus':
            return True
        return bool(_TIMESHIFT_SEMANTIC_RE.search(token))

    def _extract_suffix_tokens(stream_name):
        stream_tokens = tokenize_canonical(stream_name)
        match_end = _find_match_window(stream_tokens, _selected_tokens)
        if match_end is None:
            return []
        suffix_tokens = stream_tokens[match_end:]
        filtered = []
        for token in suffix_tokens:
            if token in _QUALITY_NEUTRAL_TOKENS:
                continue
            if token in _REGION_NEUTRAL_TOKENS:
                continue
            if token.isdigit():
                continue
            if _is_timeshift_token(token):
                continue
            filtered.append(token)
        return filtered

    def _allows_numeric_one_equivalence(selected_name, stream_name):
        selected_tokens = tokenize_canonical(selected_name)
        stream_tokens = tokenize_canonical(stream_name)
        if not selected_tokens or not stream_tokens:
            return False
        if selected_tokens[-1] != '1':
            return False
        base_tokens = selected_tokens[:-1]
        if not base_tokens:
            return False
        if any(token.isdigit() for token in base_tokens):
            return False
        for token in stream_tokens:
            for digits in re.findall(r'\d+', token):
                if digits != '1':
                    return False
        normalized_stream_tokens = [
            token[:-1] if (token.endswith('1') and not token.isdigit()) else token
            for token in stream_tokens
        ]
        return normalized_stream_tokens == base_tokens

    def _allows_trailing_one_equivalence(selected_name, stream_name):
        selected_tokens = tokenize_canonical(selected_name)
        stream_tokens = tokenize_canonical(stream_name)
        if not selected_tokens or not stream_tokens:
            return False

        def _strip_trailing_one(tokens):
            last_token = tokens[-1]
            if last_token.isdigit() or not last_token.endswith('1'):
                return None
            base_token = last_token[:-1]
            if not base_token or base_token[-1].isdigit():
                return None
            return tokens[:-1] + [base_token]

        selected_base = _strip_trailing_one(selected_tokens)
        if selected_base is not None and selected_base == stream_tokens:
            return True
        stream_base = _strip_trailing_one(stream_tokens)
        if stream_base is not None and stream_base == selected_tokens:
            return True
        return False

    def matches_stream(stream_name):
        """Check if stream matches the selected channel (fast path; precomputed filters)."""
        if exclude_plus_one and _TIMESHIFT_SEMANTIC_RE.search(stream_name):
            return False

        if _stream_name_override_re is not None:
            # Regex-only override: bypass grammar and other channel filters.
            return bool(_stream_name_override_re.search(stream_name))

        stream_normalized = canonicalize_name(stream_name)
        
        if _selected_normalized not in stream_normalized:
            if not _allows_trailing_one_equivalence(selected_cleaned, stream_name):
                return False
        if _selected_numeric_re and not _selected_numeric_re.search(stream_normalized):  # use canonical form so word-number aliases (e.g., "one"->"1") pass
            if not (
                _allows_numeric_one_equivalence(selected_cleaned, stream_name)
                or _allows_trailing_one_equivalence(selected_cleaned, stream_name)
            ):
                return False
        
        stream_has_timeshift = bool(_TIMESHIFT_SEMANTIC_RE.search(stream_name))
        
        if _selected_has_timeshift and not stream_has_timeshift:
            return False
        if not _selected_has_timeshift and stream_has_timeshift:
            return False

        if _selected_tokens is not None:
            if not _passes_semantic_suffix(_selected_tokens, tokenize_canonical(stream_normalized)):
                if not _allows_trailing_one_equivalence(selected_cleaned, stream_name):
                    return False

        if _derived_negative_tokens:
            suffix_tokens = _extract_suffix_tokens(stream_name)
            for token in suffix_tokens:
                if token in _derived_negative_tokens:
                    logging.info(
                        f"Rejected stream due to derived negative token '{token}': {stream_name}"
                    )
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
    selected_cleaned = strip_quality(search_name)
    _selected_normalized = canonicalize_name(selected_cleaned)
    _selected_has_timeshift = bool(_TIMESHIFT_SEMANTIC_RE.search(search_name))
    _selected_tokens = tokenize_canonical(selected_cleaned)
    _selected_token_set = set(_selected_tokens)
    numeric_match = re.search(r'(?:^|\s)(\d+)\s*$', _selected_normalized)
    if numeric_match:
        numeric_token = numeric_match.group(1)
        _selected_numeric_re = re.compile(rf'\b{re.escape(numeric_token)}\b', flags=re.IGNORECASE)
    _include_regexes = [] if _stream_name_override_re is not None else compile_wildcard_filter(include_filter)
    _exclude_regexes = [] if _stream_name_override_re is not None else compile_wildcard_filter(exclude_filter)
    
    # Get current streams for this channel
    current_streams = api.fetch_channel_streams(channel_id)
    current_stream_ids = {s['id'] for s in current_streams} if current_streams else set()

    def _infer_alias_neutral_tokens(streams):
        if not streams:
            return set()
        token_counts = defaultdict(int)
        stream_count = 0
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            stream_name = stream.get('name')
            if not stream_name:
                continue
            stream_tokens = tokenize_canonical(stream_name)
            if not _passes_semantic_suffix(_selected_tokens, stream_tokens):
                continue
            tokens = {
                token for token in stream_tokens
                if token not in _selected_token_set and token not in _QUALITY_NEUTRAL_TOKENS
            }
            if not tokens:
                continue
            stream_count += 1
            for token in tokens:
                token_counts[token] += 1
        if stream_count == 0:
            return set()
        return {
            token for token, count in token_counts.items()
            if 0 < count < stream_count
            and token not in _selected_token_set
            and not token.isdigit()
            and token not in _QUALITY_NEUTRAL_TOKENS
        }

    _alias_neutral_tokens = _infer_alias_neutral_tokens(current_streams)

    def _stream_snapshot(stream):
        if not isinstance(stream, dict):
            return {}
        return {
            'id': stream.get('id'),
            'name': stream.get('name'),
            'provider_id': stream.get('m3u_account'),
            'provider_name': stream.get('m3u_account_name'),
            # Pass through service metadata if the source stream already includes it.
            'service_name': stream.get('service_name'),
            'service_provider': stream.get('service_provider'),
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

    if _stream_name_override_re is None:
        preview_only_streams = []
        for stream in all_streams:
            stream_name = stream.get('name', '')
            stream_id = stream.get('id')
            if not matches_stream(stream_name):
                continue
            if stream_id in current_stream_ids:
                continue
            preview_only_streams.append(stream)

        current_suffix_tokens = set()
        for stream in current_streams or []:
            stream_name = stream.get('name')
            if not stream_name:
                continue
            current_suffix_tokens.update(_extract_suffix_tokens(stream_name))

        preview_token_counts = defaultdict(int)
        for stream in preview_only_streams:
            stream_name = stream.get('name')
            if not stream_name:
                continue
            stream_tokens = set(_extract_suffix_tokens(stream_name))
            for token in stream_tokens:
                preview_token_counts[token] += 1

        _derived_negative_tokens = {
            token for token, count in preview_token_counts.items()
            if count >= 2 and token not in current_suffix_tokens
        }
        if _derived_negative_tokens:
            sorted_tokens = ', '.join(sorted(_derived_negative_tokens))
            logging.info(f"Derived negative tokens learned: {sorted_tokens}")
    
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

    deny_overrides = _stream_name_override_re is not None
    denied_stream_ids = set() if (deny_overrides or clear_learned_rules) else _load_channel_denies(channel_id)
    deny_records = []
    record_denies = allowed_stream_ids is not None and not preview

    for stream in all_streams:
        stream_name = stream.get('name', '')
        stream_id = stream.get('id')
        provider_id = stream.get('m3u_account')
        provider_name = stream.get('m3u_account_name')

        if provider_name is None and provider_lookup and provider_id is not None:
            provider_name = provider_lookup.get(str(provider_id))

        if (
            denied_stream_ids
            and stream_id is not None
            and int(stream_id) in denied_stream_ids
            and not (allowed_set is not None and int(stream_id) in allowed_set)
        ):
            logging.info(
                f"Rejected stream {stream_id} for channel {channel_id} due to persisted deny"
            )
            continue

        if not matches_stream(stream_name):
            continue

        total_matching += 1

        if allowed_set is not None and (stream_id is None or int(stream_id) not in allowed_set):
            if record_denies and stream_id is not None:
                deny_records.append({
                    'channel_id': int(channel_id),
                    'stream_id': int(stream_id),
                    'stream_name': stream_name,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            continue

        filtered_count += 1

        detailed_stream = {
            'id': stream_id,
            'name': stream_name,
            'provider_id': provider_id,
            'provider_name': provider_name,
            # Pass through service metadata if the source stream already includes it.
            'service_name': stream.get('service_name'),
            'service_provider': stream.get('service_provider'),
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
        if record_denies:
            _append_channel_denies(deny_records)
        logging.info(f"✓ Successfully replaced channel streams")
        return result
    except Exception as e:
        logging.error(f"Failed to update channel: {e}")
        return {'error': str(e)}
