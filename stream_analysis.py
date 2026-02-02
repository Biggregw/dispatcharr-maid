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
import sqlite3
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
    except Exception:
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
    """Get bitrate, frame statistics, and TTFF using ffmpeg"""
    command = [
        'ffmpeg', '-re', '-v', 'debug', '-user_agent', 'VLC/3.0.14',
        '-i', url, '-t', str(duration), '-f', 'null', '-'
    ]

    bitrate = "N/A"
    frames_decoded = "N/A"
    frames_dropped = "N/A"
    ttff_ms = None
    elapsed = 0
    status = "OK"

    try:
        start = time.time()

        # Use Popen to read stderr in real-time for TTFF measurement
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stderr_lines = []
        first_frame_detected = False

        # Read stderr line by line to detect first frame
        try:
            while True:
                line = proc.stderr.readline()
                if not line:
                    break
                stderr_lines.append(line)

                # Detect first frame - look for frame progress or decoded frame indicators
                if not first_frame_detected:
                    # Pattern 1: frame=N in progress output
                    if re.search(r'frame=\s*[1-9]', line):
                        ttff_ms = int((time.time() - start) * 1000)
                        first_frame_detected = True
                    # Pattern 2: "size=" with non-zero value indicates data flowing
                    elif 'size=' in line and not 'size=       0' in line and not 'size=0' in line:
                        if re.search(r'size=\s*[1-9]', line):
                            ttff_ms = int((time.time() - start) * 1000)
                            first_frame_detected = True

                # Check timeout
                if time.time() - start > timeout:
                    proc.kill()
                    status = "Timeout"
                    break

            proc.wait(timeout=5)  # Wait for process to finish
        except Exception:
            proc.kill()
            proc.wait()

        elapsed = time.time() - start
        output = ''.join(stderr_lines)

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

    return bitrate, frames_decoded, frames_dropped, ttff_ms, status, elapsed


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
            row['ttff_ms'] = 'N/A'
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
            bitrate, frames_decoded, frames_dropped, ttff_ms, status, elapsed = \
                _get_bitrate_and_frame_stats(url, duration, timeout)

            row['bitrate_kbps'] = bitrate
            row['frames_decoded'] = frames_decoded
            row['frames_dropped'] = frames_dropped
            row['ttff_ms'] = ttff_ms
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

def fetch_streams(api, config, output_file=None, progress_callback=None, stream_provider_map_override=None, channels_override=None):
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
        'ttff_ms', 'video_stream_count', 'audio_stream_count', 'format_name',
        'video_profile', 'video_level', 'pixel_format', 'r_frame_rate',
        'declared_bitrate_kbps', 'err_decode', 'err_discontinuity', 'err_timeout'
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
            width, height = int(numbers[0]), 0
    except ValueError:
        return "unknown"

    height_dim = height or width
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
    df['ttff_ms'] = pd.to_numeric(df['ttff_ms'], errors='coerce')
    # Group by stream and calculate averages
    summary = df.groupby('stream_id').agg(
        avg_bitrate_kbps=('bitrate_kbps', 'mean'),
        avg_frames_decoded=('frames_decoded', 'mean'),
        avg_frames_dropped=('frames_dropped', 'mean'),
        avg_ttff_ms=('ttff_ms', 'mean')
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
        'avg_frames_dropped', 'avg_ttff_ms', 'dropped_frame_percentage', 'fps',
        'resolution', 'video_codec', 'audio_codec', 'interlaced_status', 'status',
        'score', 'validation_result', 'validation_reason',
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
    df['ttff_ms'] = pd.to_numeric(df['ttff_ms'], errors='coerce')

    summary = df.groupby('stream_id').agg(
        avg_bitrate_kbps=('bitrate_kbps', 'mean'),
        avg_frames_decoded=('frames_decoded', 'mean'),
        avg_frames_dropped=('frames_dropped', 'mean'),
        avg_ttff_ms=('ttff_ms', 'mean')
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
        'avg_bitrate_kbps', 'avg_frames_decoded', 'avg_frames_dropped', 'avg_ttff_ms',
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
        return 0.7
    value = str(validation_value).strip().lower()
    if value == 'pass':
        return 1.0
    if value == 'fail':
        return 0.4  # Aggressive penalty - reliability over quality
    return 0.6


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
    # Resolution weight reduced - reliability matters more than pixels.
    resolution_score = math.log1p(total_pixels) / math.log1p(3840 * 2160)
    resolution_detail = math.sqrt(total_pixels) / math.sqrt(3840 * 2160) if total_pixels > 0 else 0.0
    fps_score = math.log1p(fps) / math.log1p(120)
    # Invert bitrate scoring: lower bitrate = less buffering risk = higher score
    # Streams under 5Mbps get full credit, higher bitrates get progressively less
    if avg_bitrate_kbps <= 5000:
        bitrate_score = 1.0
    else:
        bitrate_score = max(0.3, 1.0 - (avg_bitrate_kbps - 5000) / 15000)
    base_component = math.log1p(max(base_score, 0.0))

    # TTFF (time-to-first-frame) dominates scoring - fast startup is critical
    ttff_ms = _normalize_numeric(record.get('ttff_ms') or record.get('avg_ttff_ms'))
    if ttff_ms is None or ttff_ms <= 0:
        ttff_score = 0.5  # Unknown = assume medium
    elif ttff_ms <= 1500:
        ttff_score = 1.0  # Fast - full credit (slot-1 eligible)
    elif ttff_ms <= 2500:
        ttff_score = 0.6  # Medium - reduced
    elif ttff_ms <= 4000:
        ttff_score = 0.3  # Slow - penalty
    else:
        ttff_score = 0.15  # Very slow - heavy penalty

    codec_score = _codec_quality_score(record.get('video_codec'))
    audio_score = _audio_quality_score(record.get('audio_codec'))

    interlaced_status = str(record.get('interlaced_status') or '').lower()
    interlace_penalty = 0.95 if 'interlaced' in interlaced_status else 1.0
    validation_penalty = _validation_penalty(record.get('validation_result') or record.get('status'))

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
    if core_incomplete:
        # Treat missing core metadata as unknown quality; apply a mild, capped penalty.
        metadata_penalty *= 0.96
    if total_pixels == 0:
        metadata_penalty *= 0.98
    if str(record.get('video_codec') or '').upper() in ('', 'N/A'):
        metadata_penalty *= 0.99
    if str(record.get('audio_codec') or '').upper() in ('', 'N/A'):
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

    decoded_frames = _safe_float(record.get('avg_frames_decoded') or record.get('frames_decoded'))
    dropped_frames = _safe_float(record.get('avg_frames_dropped') or record.get('frames_dropped'))
    if decoded_frames and dropped_frames is not None and decoded_frames > 0 and dropped_frames >= 0:
        # Use drop ratio only when measured data exists; keep penalties tiny.
        drop_ratio = dropped_frames / decoded_frames
        if drop_ratio > 0.2:
            structure_penalty *= 0.99
        elif drop_ratio > 0.05:
            structure_penalty *= 0.995

    score = (
        ttff_score * 4.0            # TTFF dominates - fast startup is critical
        + base_component * 2.0      # Stability/startup score
        + resolution_score * 0.8    # Resolution matters less than startup speed
        + resolution_detail * 0.15
        + fps_score * 0.2
        + bitrate_score * 1.0       # Lower bitrate = higher score
        + codec_score * 0.3
        + audio_score * 0.1
    )
    score *= interlace_penalty * validation_penalty * metadata_penalty * structure_penalty
    return score


def _continuous_ordering_score_breakdown(record):
    """
    Compute score breakdown with all component scores for logging.
    Returns (total_score, breakdown_dict).
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

    resolution_score = math.log1p(total_pixels) / math.log1p(3840 * 2160)
    resolution_detail = math.sqrt(total_pixels) / math.sqrt(3840 * 2160) if total_pixels > 0 else 0.0
    fps_score = math.log1p(fps) / math.log1p(120)

    if avg_bitrate_kbps <= 5000:
        bitrate_score = 1.0
    else:
        bitrate_score = max(0.3, 1.0 - (avg_bitrate_kbps - 5000) / 15000)
    base_component = math.log1p(max(base_score or 0.0, 0.0))

    ttff_ms = _normalize_numeric(record.get('ttff_ms') or record.get('avg_ttff_ms'))
    if ttff_ms is None or ttff_ms <= 0:
        ttff_score = 0.5
    elif ttff_ms <= 1500:
        ttff_score = 1.0
    elif ttff_ms <= 2500:
        ttff_score = 0.6
    elif ttff_ms <= 4000:
        ttff_score = 0.3
    else:
        ttff_score = 0.15

    codec_score = _codec_quality_score(record.get('video_codec'))
    audio_score = _audio_quality_score(record.get('audio_codec'))

    interlaced_status = str(record.get('interlaced_status') or '').lower()
    interlace_penalty = 0.95 if 'interlaced' in interlaced_status else 1.0
    validation_penalty = _validation_penalty(record.get('validation_result') or record.get('status'))

    raw_score = (
        ttff_score * 4.0
        + base_component * 2.0
        + resolution_score * 0.8
        + resolution_detail * 0.15
        + fps_score * 0.2
        + bitrate_score * 1.0
        + codec_score * 0.3
        + audio_score * 0.1
    )
    final_score = raw_score * interlace_penalty * validation_penalty

    breakdown = {
        'ttff_ms': ttff_ms,
        'ttff_score': round(ttff_score, 3),
        'bitrate_kbps': round(avg_bitrate_kbps, 1) if avg_bitrate_kbps else None,
        'bitrate_score': round(bitrate_score, 3),
        'resolution': record.get('resolution'),
        'resolution_score': round(resolution_score, 3),
        'fps': round(fps, 1) if fps else None,
        'fps_score': round(fps_score, 3),
        'video_codec': record.get('video_codec'),
        'codec_score': round(codec_score, 3),
        'audio_codec': record.get('audio_codec'),
        'audio_score': round(audio_score, 3),
        'base_score': round(base_score, 3) if base_score else None,
        'base_component': round(base_component, 3),
        'interlace_penalty': round(interlace_penalty, 3),
        'validation_penalty': round(validation_penalty, 3),
        'validation_result': record.get('validation_result') or record.get('status'),
        'raw_score': round(raw_score, 3),
        'final_score': round(final_score, 3),
    }
    return final_score, breakdown


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

    similar_delta = _safe_float(similar_score_delta)
    if similar_delta is None:
        similar_delta = 0.0

    def _normalize_codec(codec_value):
        codec = str(codec_value or '').lower()
        if 'hevc' in codec or '265' in codec:
            return 'hevc'
        if 'h264' in codec or 'avc' in codec:
            return 'h264'
        if 'av1' in codec:
            return 'av1'
        return codec or 'unknown'

    def _is_failed_stream(validation_value):
        value = str(validation_value or '').strip().lower()
        return value in ('fail', 'failed', 'error', 'timeout')

    def _bitrate_ceiling_for_resolution(total_pixels):
        if not total_pixels:
            return 20000
        if total_pixels <= 640 * 360:
            return 1500
        if total_pixels <= 854 * 480:
            return 2500
        if total_pixels <= 1280 * 720:
            return 5000
        if total_pixels <= 1920 * 1080:
            return 8000
        if total_pixels <= 2560 * 1440:
            return 12000
        return 20000

    scored_records = []
    for record in records:
        ordering = record.get('ordering_score')
        if ordering in (None, '', 'N/A'):
            ordering = record.get('score')
        score_value = _safe_float(ordering)
        if score_value is None:
            score_value = 0.0
        scored_records.append((record, score_value))

    provider_scores = {}
    provider_failures = {}
    for record, score_value in scored_records:
        provider_id = record.get('m3u_account') or 'unknown'
        provider_scores.setdefault(provider_id, []).append(score_value)
        validation_value = record.get('validation_result') or record.get('status')
        if _is_failed_stream(validation_value):
            provider_failures[provider_id] = provider_failures.get(provider_id, 0) + 1

    provider_stats = {}
    for provider_id, scores in provider_scores.items():
        max_score = max(scores) if scores else 0.0
        weak_count = sum(1 for score in scores if max_score > 0 and score <= max_score * 0.7)
        strong_count = sum(1 for score in scores if max_score > 0 and score >= max_score * 0.9)
        provider_stats[provider_id] = {
            'max_score': max_score,
            'weak_count': weak_count,
            'strong_count': strong_count,
            'fail_count': provider_failures.get(provider_id, 0),
            'total': len(scores)
        }

    duplicate_groups = {}
    for record, score_value in scored_records:
        provider_id = record.get('m3u_account') or 'unknown'
        parsed = _parse_resolution(record.get('resolution'))
        resolution_key = tuple(parsed) if parsed else None
        codec_key = _normalize_codec(record.get('video_codec'))
        bitrate = _safe_float(record.get('avg_bitrate_kbps'))
        duplicate_groups.setdefault((provider_id, resolution_key, codec_key), []).append(
            (record, score_value, bitrate)
        )

    def _record_key(record):
        stream_id = record.get('stream_id')
        if stream_id is not None:
            return str(stream_id)
        stream_url = record.get('stream_url')
        if stream_url:
            return str(stream_url)
        return str(record.get('stream_name') or '')

    duplicate_penalties = {}
    for group_key, group_records in duplicate_groups.items():
        if len(group_records) < 2:
            continue
        group_records.sort(
            key=lambda item: (
                item[1],
                _safe_float(item[0].get('avg_bitrate_kbps')) or 0.0,
                _stable_tiebreaker(item[0].get('stream_id') or item[0].get('stream_url') or '')
            ),
            reverse=True
        )
        best_record, _best_score, best_bitrate = group_records[0]
        if best_bitrate is None:
            continue
        threshold = max(250.0, best_bitrate * 0.05)
        for record, _score_value, bitrate in group_records[1:]:
            if bitrate is None:
                continue
            if abs(bitrate - best_bitrate) <= threshold:
                # Near-duplicate suppression is tiny and deterministic; all streams remain present. (Safe)
                duplicate_penalties[_record_key(record)] = -0.002

    def _tie_breaker(record, score_value):
        provider_id = record.get('m3u_account') or 'unknown'
        stats = provider_stats.get(provider_id, {})
        tie_bias = 0.0

        # Slot-1 conservatism: prefer efficient codecs and modest bitrate when scores are close. (Safe)
        codec_key = _normalize_codec(record.get('video_codec'))
        if codec_key == 'hevc':
            tie_bias += 0.002

        parsed = _parse_resolution(record.get('resolution'))
        total_pixels = parsed[0] * parsed[1] if parsed else 0
        bitrate = _safe_float(record.get('avg_bitrate_kbps'))
        if bitrate and total_pixels:
            ceiling = _bitrate_ceiling_for_resolution(total_pixels)
            normalized_bitrate = min(bitrate, ceiling) / max(ceiling, 1.0)
            tie_bias += (1.0 - normalized_bitrate) * 0.002

        # Provider variance dampening applies only when a provider shows mixed quality. (Safe)
        if stats:
            mixed_quality = (
                stats.get('strong_count', 0) > 0
                and (stats.get('weak_count', 0) > 0 or stats.get('fail_count', 0) > 0)
            )
            if mixed_quality and score_value >= stats.get('max_score', 0.0) * 0.9:
                tie_bias -= 0.002
            elif not mixed_quality:
                tie_bias += 0.001

        tie_bias += duplicate_penalties.get(_record_key(record), 0.0)
        return tie_bias

    def _is_close(a, b):
        if a is None or b is None:
            return False
        delta = abs(a - b)
        threshold = max(0.02, 0.01 * max(a, b))
        return delta <= threshold and delta <= similar_delta

    sortable = []
    for record, score_value in scored_records:
        tie_value = _record_key(record)
        sortable.append({
            'record': record,
            'score_value': score_value,
            'tie_bias': _tie_breaker(record, score_value),
            'tie_value': tie_value
        })

    sortable.sort(
        key=lambda item: (item['score_value'], _stable_tiebreaker(item['tie_value'])),
        reverse=True
    )

    ordered_records = []
    if sortable:
        current_group = [sortable[0]]
        for item in sortable[1:]:
            if _is_close(current_group[-1]['score_value'], item['score_value']):
                current_group.append(item)
            else:
                current_group.sort(
                    key=lambda grouped: (
                        grouped['score_value'] + grouped['tie_bias'],
                        grouped['score_value'],
                        _stable_tiebreaker(grouped['tie_value'])
                    ),
                    reverse=True
                )
                ordered_records.extend(current_group)
                current_group = [item]
        current_group.sort(
            key=lambda grouped: (
                grouped['score_value'] + grouped['tie_bias'],
                grouped['score_value'],
                _stable_tiebreaker(grouped['tie_value'])
            ),
            reverse=True
        )
        ordered_records.extend(current_group)

    if ordered_records:
        def _is_hd_and_reject_free(record):
            parsed = _parse_resolution(record.get('resolution'))
            if not parsed:
                return False
            width, height = parsed
            if width < 1280 or height < 720:
                return False
            reject = record.get('reject_count')
            if reject is None:
                return False
            try:
                return int(reject) == 0
            except (ValueError, TypeError):
                return False

        has_hd_reject_free = any(_is_hd_and_reject_free(item['record']) for item in ordered_records)
        if has_hd_reject_free and ordered_records:
            first_record = ordered_records[0]['record']
            first_parsed = _parse_resolution(first_record.get('resolution'))
            if first_parsed and (first_parsed[0] < 1280 or first_parsed[1] < 720):
                for idx in range(1, len(ordered_records)):
                    if _is_hd_and_reject_free(ordered_records[idx]['record']):
                        ordered_records[0], ordered_records[idx] = ordered_records[idx], ordered_records[0]
                        break

        # TTFF-based slot-1 protection: fast-starting HD stream should hold slot-1
        # If slot-1 is slow (>1500ms), find a faster HD alternative
        first_record = ordered_records[0]['record']
        first_ttff = _normalize_numeric(
            first_record.get('ttff_ms') or first_record.get('avg_ttff_ms')
        )
        if first_ttff and first_ttff > 1500:
            for idx in range(1, len(ordered_records)):
                candidate = ordered_records[idx]['record']
                cand_ttff = _normalize_numeric(
                    candidate.get('ttff_ms') or candidate.get('avg_ttff_ms')
                )
                # Skip if candidate is also slow
                if not cand_ttff or cand_ttff > 1500:
                    continue
                # Skip if candidate is below 720p (non-HD)
                cand_parsed = _parse_resolution(candidate.get('resolution'))
                if cand_parsed and cand_parsed[1] < 720:
                    continue
                # Fast HD candidate found - swap to slot-1
                ordered_records[0], ordered_records[idx] = ordered_records[idx], ordered_records[0]
                break

    return [item['record'].get('stream_id') for item in ordered_records]


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

        # Log detailed score breakdown for each stream in final order
        if final_ids and logging.getLogger().isEnabledFor(logging.INFO):
            id_to_record = {r.get('stream_id'): r for r in records_for_ordering}
            channel_name = None
            channel_number = None
            if channel_lookup:
                ch_meta = channel_lookup.get(_safe_int(channel_id), {})
                channel_name = ch_meta.get('name')
                channel_number = ch_meta.get('channel_number')
            # Fall back to getting channel info from records if not in lookup
            if not channel_name and records_for_ordering:
                channel_name = records_for_ordering[0].get('channel_name')
            if not channel_number and records_for_ordering:
                channel_number = records_for_ordering[0].get('channel_number')
            header = f"Channel {channel_number or channel_id}"
            if channel_name:
                header += f" ({channel_name})"
            logging.info(f"{header} - Stream scores after reorder:")
            for slot, sid in enumerate(final_ids, start=1):
                rec = id_to_record.get(sid, {})
                _, breakdown = _continuous_ordering_score_breakdown(rec)
                stream_name = rec.get('stream_name') or 'Unknown'
                provider = rec.get('m3u_account_name') or rec.get('m3u_account') or 'unknown'
                ttff_val = breakdown['ttff_ms']
                ttff_str = f"{int(ttff_val)}ms" if ttff_val and not math.isnan(ttff_val) else 'N/A'
                bitrate_val = breakdown['bitrate_kbps']
                bitrate_str = f"{int(bitrate_val)}kbps" if bitrate_val and not math.isnan(bitrate_val) else 'N/A'
                logging.info(
                    f"  #{slot}: {stream_name} [{provider}] "
                    f"| Score: {breakdown['final_score']:.2f} "
                    f"| TTFF: {ttff_str} ({breakdown['ttff_score']}) "
                    f"| Bitrate: {bitrate_str} ({breakdown['bitrate_score']}) "
                    f"| Res: {breakdown['resolution'] or 'N/A'} ({breakdown['resolution_score']}) "
                    f"| FPS: {breakdown['fps'] or 'N/A'} ({breakdown['fps_score']}) "
                    f"| Codec: {breakdown['video_codec'] or 'N/A'} ({breakdown['codec_score']}) "
                    f"| Audio: {breakdown['audio_codec'] or 'N/A'} ({breakdown['audio_score']}) "
                    f"| Validation: {breakdown['validation_result'] or 'N/A'} ({breakdown['validation_penalty']})"
                )

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
                # Compute and store score breakdown for visibility in job history
                _, breakdown = _continuous_ordering_score_breakdown(record)
                merged.update({
                    'order': idx,
                    'quality_tier': 'ordered',
                    'score_breakdown': breakdown,
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

# Refresh learning DB: stored at data/refresh_learning.sqlite (per working dir).
# Tables:
#   refresh_learning_decisions: raw accept/reject events per channel/signature.
#   refresh_learning_stats: per-channel aggregate counts used for preview annotations.
def _refresh_learning_db_path(config):
    """Return the sqlite path for refresh learning decisions/stats."""
    # Refresh learning must not be job-scoped so per-channel learning persists across runs.
    return config.resolve_path('data/refresh_learning.sqlite')


def _normalize_refresh_signature(stream_name):
    """Normalize stream names into deterministic signatures for refresh learning."""
    if not isinstance(stream_name, str):
        return ""
    cleaned = stream_name.strip().lower()
    cleaned = re.sub(r'[^a-z0-9\s]', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def _ensure_refresh_learning_db(config):
    """Ensure refresh learning sqlite schema exists."""
    db_path = _refresh_learning_db_path(config)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_learning_decisions (
                channel_id TEXT NOT NULL,
                stream_id TEXT,
                stream_name TEXT,
                stream_signature TEXT NOT NULL,
                decision TEXT NOT NULL,
                decided_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_learning_stats (
                channel_id TEXT NOT NULL,
                stream_signature TEXT NOT NULL,
                accept_count INTEGER NOT NULL DEFAULT 0,
                reject_count INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, stream_signature)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_baseline (
                channel_id TEXT NOT NULL,
                stream_signature TEXT NOT NULL,
                stream_name TEXT NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, stream_signature)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_rejections (
                channel_id TEXT NOT NULL,
                stream_signature TEXT NOT NULL,
                stream_name TEXT NOT NULL,
                rejected_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, stream_signature)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_regex_trust (
                channel_id TEXT NOT NULL,
                pattern TEXT NOT NULL,
                accepted_count INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, pattern)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_selectors (
                channel_id TEXT NOT NULL,
                selector_index INTEGER NOT NULL,
                selector_text TEXT NOT NULL,
                source TEXT NOT NULL,
                confirmed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, selector_index)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_exclusions (
                channel_id TEXT NOT NULL,
                stream_name TEXT NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, stream_name)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_injected_state (
                channel_id TEXT NOT NULL PRIMARY KEY,
                injected_includes TEXT,
                injected_excludes TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        return conn
    except Exception:
        conn.close()
        raise


def _load_refresh_selectors(config, channel_id):
    selectors = []
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        rows = conn.execute(
            """
            SELECT selector_index, selector_text, source, confirmed
            FROM refresh_selectors
            WHERE channel_id = ?
            ORDER BY selector_index ASC
            """,
            (str(channel_id),)
        ).fetchall()
        for row in rows:
            selectors.append({
                'text': row[1],
                'source': row[2],
                'confirmed': bool(row[3]),
            })
    except Exception as exc:
        logging.warning("Failed to load refresh selectors: %s", exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return selectors


def _save_refresh_selectors(config, channel_id, selectors):
    cleaned = []
    for selector in selectors or []:
        if not isinstance(selector, dict):
            continue
        text = selector.get('text')
        if not isinstance(text, str) or not text.strip():
            continue
        source = selector.get('source') if selector.get('source') in ('user', 'system') else 'user'
        confirmed = bool(selector.get('confirmed'))
        cleaned.append({
            'text': text,
            'source': source,
            'confirmed': confirmed,
        })
        if len(cleaned) >= 4:
            break
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        conn.execute(
            "DELETE FROM refresh_selectors WHERE channel_id = ?",
            (str(channel_id),)
        )
        now = datetime.now().isoformat()
        for idx, selector in enumerate(cleaned):
            conn.execute(
                """
                INSERT INTO refresh_selectors (
                    channel_id, selector_index, selector_text, source, confirmed, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(channel_id),
                    idx,
                    selector['text'],
                    selector['source'],
                    1 if selector['confirmed'] else 0,
                    now,
                )
            )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logging.warning("Failed to persist refresh selectors: %s", exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _load_refresh_exclusions(config, channel_id):
    exclusions = []
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        rows = conn.execute(
            """
            SELECT stream_name
            FROM refresh_exclusions
            WHERE channel_id = ?
            ORDER BY LOWER(stream_name), stream_name
            """,
            (str(channel_id),)
        ).fetchall()
        exclusions = [row[0] for row in rows if row and isinstance(row[0], str)]
    except Exception as exc:
        logging.warning("Failed to load refresh exclusions: %s", exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return exclusions


def _add_refresh_exclusions(config, channel_id, stream_names):
    names = [name for name in (stream_names or []) if isinstance(name, str) and name.strip()]
    if not names:
        return
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        now = datetime.now().isoformat()
        for name in names:
            conn.execute(
                """
                INSERT OR IGNORE INTO refresh_exclusions (
                    channel_id, stream_name, added_at
                ) VALUES (?, ?, ?)
                """,
                (
                    str(channel_id),
                    name,
                    now,
                )
            )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logging.warning("Failed to persist refresh exclusions: %s", exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _load_refresh_injected_state(config, channel_id):
    payload = {
        'injected_includes': [],
        'injected_excludes': [],
    }
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        row = conn.execute(
            """
            SELECT injected_includes, injected_excludes
            FROM refresh_injected_state
            WHERE channel_id = ?
            """,
            (str(channel_id),)
        ).fetchone()
        if row:
            for key, value in zip(payload.keys(), row):
                if isinstance(value, str) and value:
                    try:
                        parsed = json.loads(value)
                    except json.JSONDecodeError:
                        parsed = []
                    payload[key] = [
                        name for name in parsed if isinstance(name, str) and name
                    ]
    except Exception as exc:
        logging.warning("Failed to load refresh injected state: %s", exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return payload


def _save_refresh_injected_state(config, channel_id, injected_includes=None, injected_excludes=None):
    includes = [
        name for name in (injected_includes or [])
        if isinstance(name, str) and name
    ]
    excludes = [
        name for name in (injected_excludes or [])
        if isinstance(name, str) and name
    ]
    now = datetime.now().isoformat()
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        conn.execute(
            """
            INSERT INTO refresh_injected_state (
                channel_id, injected_includes, injected_excludes, updated_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                injected_includes = excluded.injected_includes,
                injected_excludes = excluded.injected_excludes,
                updated_at = excluded.updated_at
            """,
            (
                str(channel_id),
                json.dumps(includes, ensure_ascii=False),
                json.dumps(excludes, ensure_ascii=False),
                now,
            )
        )
        conn.commit()
    except Exception as exc:
        logging.warning("Failed to persist refresh injected state: %s", exc)
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _remove_refresh_exclusion(config, channel_id, stream_name):
    if not isinstance(stream_name, str) or not stream_name.strip():
        return
    conn = None
    try:
        conn = _ensure_refresh_learning_db(config)
        conn.execute(
            "DELETE FROM refresh_exclusions WHERE channel_id = ? AND stream_name = ?",
            (str(channel_id), stream_name)
        )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logging.warning("Failed to remove refresh exclusion: %s", exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _load_refresh_learning_stats(config, channel_id):
    """Load per-channel learning stats keyed by signature."""
    stats = {}
    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh learning DB unavailable: %s", exc)
        return stats

    try:
        cursor = conn.execute(
            """
            SELECT stream_signature, accept_count, reject_count
            FROM refresh_learning_stats
            WHERE channel_id = ?
            """,
            (str(channel_id),)
        )
        for signature, accept_count, reject_count in cursor.fetchall():
            stats[signature] = {
                'accept_count': int(accept_count or 0),
                'reject_count': int(reject_count or 0),
            }
    finally:
        conn.close()
    return stats


def _has_refresh_learning_stats(config, channel_id):
    """Return True when refresh learning stats exist for the channel."""
    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh learning DB unavailable: %s", exc)
        return False

    try:
        cursor = conn.execute(
            """
            SELECT 1
            FROM refresh_learning_stats
            WHERE channel_id = ?
            LIMIT 1
            """,
            (str(channel_id),)
        )
        return cursor.fetchone() is not None
    finally:
        conn.close()


def _load_refresh_baseline(config, channel_id):
    """Load the baseline stream names for a channel keyed by normalized signature."""
    baseline = {}
    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh baseline DB unavailable: %s", exc)
        return baseline

    try:
        cursor = conn.execute(
            """
            SELECT stream_signature, stream_name
            FROM refresh_baseline
            WHERE channel_id = ?
            """,
            (str(channel_id),)
        )
        for signature, stream_name in cursor.fetchall():
            baseline[signature] = stream_name
    finally:
        conn.close()
    return baseline


def _add_refresh_baseline_entries(config, channel_id, stream_names):
    """Insert baseline stream names for a channel (idempotent)."""
    if not stream_names:
        return 0
    entries = []
    added_at = datetime.now().isoformat()
    for stream_name in stream_names:
        signature = _normalize_refresh_signature(stream_name)
        if not signature:
            continue
        entries.append((str(channel_id), signature, stream_name, added_at))

    if not entries:
        return 0

    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh baseline DB unavailable: %s", exc)
        return 0

    try:
        conn.execute("BEGIN")
        conn.executemany(
            """
            INSERT OR IGNORE INTO refresh_baseline (
                channel_id, stream_signature, stream_name, added_at
            ) VALUES (?, ?, ?, ?)
            """,
            entries
        )
        conn.commit()
        return conn.total_changes
    except Exception as exc:
        conn.rollback()
        logging.warning("Failed to persist refresh baseline entries: %s", exc)
        return 0
    finally:
        conn.close()


def _load_refresh_rejections(config, channel_id):
    """Load permanently rejected stream signatures for a channel."""
    rejected = set()
    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh rejection DB unavailable: %s", exc)
        return rejected

    try:
        cursor = conn.execute(
            """
            SELECT stream_signature
            FROM refresh_rejections
            WHERE channel_id = ?
            """,
            (str(channel_id),)
        )
        for (signature,) in cursor.fetchall():
            rejected.add(signature)
    finally:
        conn.close()
    return rejected


def _record_refresh_rejections(config, channel_id, stream_names):
    """Persist rejected stream names (idempotent)."""
    if not stream_names:
        return 0
    entries = []
    rejected_at = datetime.now().isoformat()
    for stream_name in stream_names:
        signature = _normalize_refresh_signature(stream_name)
        if not signature:
            continue
        entries.append((str(channel_id), signature, stream_name, rejected_at))

    if not entries:
        return 0

    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh rejection DB unavailable: %s", exc)
        return 0

    try:
        conn.execute("BEGIN")
        conn.executemany(
            """
            INSERT OR IGNORE INTO refresh_rejections (
                channel_id, stream_signature, stream_name, rejected_at
            ) VALUES (?, ?, ?, ?)
            """,
            entries
        )
        conn.commit()
        return conn.total_changes
    except Exception as exc:
        conn.rollback()
        logging.warning("Failed to persist refresh rejections: %s", exc)
        return 0
    finally:
        conn.close()


def _load_refresh_regex_trust(config, channel_id):
    """Load regex trust counts for a channel."""
    trust = {}
    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh regex trust DB unavailable: %s", exc)
        return trust

    try:
        cursor = conn.execute(
            """
            SELECT pattern, accepted_count
            FROM refresh_regex_trust
            WHERE channel_id = ?
            """,
            (str(channel_id),)
        )
        for pattern, accepted_count in cursor.fetchall():
            trust[pattern] = int(accepted_count or 0)
    finally:
        conn.close()
    return trust


def _record_refresh_regex_accepts(config, channel_id, pattern_counts):
    """Persist accepted counts per regex pattern."""
    if not pattern_counts:
        return 0
    entries = []
    updated_at = datetime.now().isoformat()
    for pattern, count in pattern_counts.items():
        if count <= 0:
            continue
        entries.append((str(channel_id), pattern, int(count), updated_at))

    if not entries:
        return 0

    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Refresh regex trust DB unavailable: %s", exc)
        return 0

    try:
        conn.execute("BEGIN")
        for channel_id_value, pattern, count, updated_at_value in entries:
            conn.execute(
                """
                INSERT INTO refresh_regex_trust (
                    channel_id, pattern, accepted_count, last_seen_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(channel_id, pattern) DO UPDATE SET
                    accepted_count = accepted_count + ?,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    channel_id_value,
                    pattern,
                    count,
                    updated_at_value,
                    count,
                )
            )
        conn.commit()
        return conn.total_changes
    except Exception as exc:
        conn.rollback()
        logging.warning("Failed to persist refresh regex accept counts: %s", exc)
        return 0
    finally:
        conn.close()


def _build_refresh_regex_patterns(baseline_names, trust_counts):
    """Build deterministic regex patterns derived from baseline names."""
    patterns = []
    quality_tokens = {
        'UHD',
        'HD',
        'FHD',
        'SD',
        '4K',
        'HDR',
        'HLG',
        'DV',
        'DOLBY',
        'DOLBYVISION',
        'HEVC',
        'H265',
    }

    for name in baseline_names:
        if not isinstance(name, str) or not name.strip():
            continue
        cleaned = name.strip()
        tokens = cleaned.split()
        last_token = tokens[-1] if tokens else ''
        normalized_last = re.sub(r'[^A-Za-z0-9]', '', last_token or '').upper()
        allow_suffix = normalized_last not in quality_tokens

        escaped = re.escape(cleaned)
        suffix = r'(?:\s+(?:UHD|HD|FHD|SD|4K|HDR|HLG|DV|DOLBY(?:\s+VISION)?|HEVC|H(?:\.?265)))'
        if allow_suffix:
            pattern_text = rf'^{escaped}{suffix}?$'
        else:
            pattern_text = rf'^{escaped}$'

        accepted_count = int(trust_counts.get(pattern_text, 0) or 0)
        patterns.append({
            'pattern': pattern_text,
            'regex': re.compile(pattern_text, flags=re.IGNORECASE),
            'trusted': accepted_count >= 2,
            'accepted_count': accepted_count,
        })
    return patterns


def _build_refresh_learning_metadata(signature, stats):
    """Compute learning metadata for a given signature."""
    accept_count = 0
    reject_count = 0
    if signature and signature in stats:
        entry = stats[signature] or {}
        accept_count = int(entry.get('accept_count', 0) or 0)
        reject_count = int(entry.get('reject_count', 0) or 0)

    confidence = (accept_count + 1) / (accept_count + reject_count + 2)
    has_reject = reject_count > 0
    overrode_reject = has_reject and confidence >= 0.9

    return {
        'checked_default': True,
        'learned_accept': accept_count > 0,
        'learned_reject': has_reject and not overrode_reject,
        'overrode_reject': overrode_reject,
        'confidence': float(confidence),
        'signature': signature,
    }


def _record_refresh_learning_decisions(config, channel_id, matching_streams, allowed_set):
    """Persist accept/reject decisions for refresh candidates."""
    if allowed_set is None:
        return

    decisions = []
    decided_at = datetime.now().isoformat()

    for stream in matching_streams:
        stream_id = stream.get('id')
        stream_name = stream.get('name')
        signature = stream.get('signature') or _normalize_refresh_signature(stream_name)
        if not signature:
            continue
        decision = 'accept' if (stream_id is not None and int(stream_id) in allowed_set) else 'reject'
        decisions.append({
            'channel_id': str(channel_id),
            'stream_id': str(stream_id) if stream_id is not None else None,
            'stream_name': stream_name,
            'signature': signature,
            'decision': decision,
            'decided_at': decided_at,
        })

    if not decisions:
        return

    try:
        conn = _ensure_refresh_learning_db(config)
    except Exception as exc:
        logging.warning("Failed to open refresh learning DB: %s", exc)
        return

    try:
        conn.execute("BEGIN")
        conn.executemany(
            """
            INSERT INTO refresh_learning_decisions (
                channel_id, stream_id, stream_name, stream_signature, decision, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    entry['channel_id'],
                    entry['stream_id'],
                    entry['stream_name'],
                    entry['signature'],
                    entry['decision'],
                    entry['decided_at'],
                )
                for entry in decisions
            ]
        )

        for entry in decisions:
            accept_delta = 1 if entry['decision'] == 'accept' else 0
            reject_delta = 1 if entry['decision'] == 'reject' else 0
            conn.execute(
                """
                INSERT INTO refresh_learning_stats (
                    channel_id, stream_signature, accept_count, reject_count, last_seen_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(channel_id, stream_signature) DO UPDATE SET
                    accept_count = accept_count + ?,
                    reject_count = reject_count + ?,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    entry['channel_id'],
                    entry['signature'],
                    accept_delta,
                    reject_delta,
                    entry['decided_at'],
                    accept_delta,
                    reject_delta,
                )
            )

        conn.commit()
    except Exception as exc:
        conn.rollback()
        logging.warning("Failed to persist refresh learning decisions: %s", exc)
    finally:
        conn.close()

def refresh_channel_streams(api, config, channel_id, base_search_text=None, include_filter=None, exclude_filter=None, allowed_stream_ids=None, preview=False, stream_name_regex=None, stream_name_regex_override=None, all_streams_override=None, all_channels_override=None, provider_names=None, excluded_stream_names=None):
    """
    Find and add all matching streams from all providers for a specific channel.

    Refresh matching uses per-channel wildcard selectors and injected includes/excludes.
    Confirmed selectors are ORed together and unioned, injected includes are added,
    and injected excludes are subtracted last.

    Args:
        api: DispatcharrAPI instance
        config: Config instance
        channel_id: ID of the channel to refresh
        base_search_text: Optional literal search text supplied by the user (unused).
        include_filter: Unused (legacy).
        exclude_filter: Unused (legacy).
        allowed_stream_ids: Optional explicit selection from a prior preview (legacy).
    Returns:
        dict with stats about streams found/added
    """
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
    search_name = base_search_text if base_search_text is not None else ''
    logging.info(f"Channel: {channel_name}")
    if isinstance(search_name, str) and search_name.strip():
        logging.info(f"Using literal search text: {search_name}")
    
    # Get current streams for this channel.
    current_streams = api.fetch_channel_streams(channel_id)
    current_stream_ids = {s['id'] for s in current_streams} if current_streams else set()

    injected_includes = []
    for stream in current_streams or []:
        name = stream.get('name')
        if not isinstance(name, str) or not name:
            continue
        injected_includes.append(name)

    refresh_settings_config = Config('config.yaml')
    selectors = _load_refresh_selectors(refresh_settings_config, channel_id)
    confirmed_selectors = [
        selector.get('text')
        for selector in selectors
        if isinstance(selector, dict)
        and selector.get('confirmed')
        and isinstance(selector.get('text'), str)
        and selector.get('text').strip()
    ]
    injected_excludes = _load_refresh_exclusions(refresh_settings_config, channel_id)
    has_explicit_excluded = isinstance(excluded_stream_names, list) and len(excluded_stream_names) > 0
    if has_explicit_excluded:
        _add_refresh_exclusions(refresh_settings_config, channel_id, excluded_stream_names)
        injected_excludes = _load_refresh_exclusions(refresh_settings_config, channel_id)
    _save_refresh_injected_state(
        refresh_settings_config,
        channel_id,
        injected_includes=injected_includes,
        injected_excludes=injected_excludes,
    )

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
    
    # Preview responses should list all candidates that pass selector matching.
    _PREVIEW_STREAM_LIMIT = None

    total_matching = 0
    preview_truncated = False

    def _normalize_stream_id(value):
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            if isinstance(value, float) and not value.is_integer():
                cleaned = str(value).strip()
                return cleaned or None
            return str(int(value))
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            if cleaned.isdigit():
                return str(int(cleaned))
            return cleaned
        cleaned = str(value).strip()
        return cleaned or None

    allowed_set = None
    explicit_empty_selection = isinstance(allowed_stream_ids, list) and len(allowed_stream_ids) == 0
    if allowed_stream_ids is not None:
        allowed_set = set()
        for sid in allowed_stream_ids:
            normalized = _normalize_stream_id(sid)
            if normalized is None:
                continue
            allowed_set.add(normalized)
        if not allowed_set and not explicit_empty_selection:
            allowed_set = None

    # Build filtered streams (used for actual update) and a capped list of
    # detailed streams for preview/UI.
    filtered_streams = []   # used when preview=False (full set)
    preview_streams = []    # capped list returned to UI when preview=True
    filtered_ids = []       # full list of matched stream IDs (used for counts/deltas)
    filtered_count = 0      # accurate count after all filtering (and allowed_set)

    provider_lookup = provider_names if isinstance(provider_names, dict) else None
    matching_streams = []
    excluded_streams = []
    matched_keys = set()
    selector_values = [str(s) for s in confirmed_selectors if isinstance(s, str)]
    selector_count = len(selector_values)
    injected_include_set = {name.casefold() for name in injected_includes if isinstance(name, str)}
    injected_exclude_set = {name for name in injected_excludes if isinstance(name, str)}
    pending_exclude_set = {
        name
        for name in (excluded_stream_names or [])
        if isinstance(name, str)
    }
    injected_exclude_set |= pending_exclude_set

    all_stream_ids = {stream.get('id') for stream in all_streams if stream.get('id') is not None}
    extra_current_streams = [
        stream for stream in (current_streams or [])
        if stream.get('id') is None or stream.get('id') not in all_stream_ids
    ]
    if extra_current_streams:
        all_streams = list(all_streams) + extra_current_streams

    def _wildcard_match(value, pattern):
        if not isinstance(value, str) or not isinstance(pattern, str):
            return False
        value_cf = value.casefold()
        pattern_cf = pattern.casefold()
        if pattern_cf == '':
            return value_cf == ''
        if '*' not in pattern_cf:
            return value_cf == pattern_cf
        if pattern_cf == '*':
            return True
        parts = pattern_cf.split('*')
        if not pattern_cf.startswith('*'):
            first = parts.pop(0)
            if not value_cf.startswith(first):
                return False
            start = len(first)
        else:
            start = 0
        end_required = None
        if not pattern_cf.endswith('*'):
            end_required = parts.pop() if parts else ''
        for part in parts:
            if part == '':
                continue
            idx = value_cf.find(part, start)
            if idx == -1:
                return False
            start = idx + len(part)
        if end_required is not None:
            if end_required == '':
                return True
            idx = value_cf.rfind(end_required)
            if idx == -1:
                return False
            if idx < start:
                return False
            return value_cf.endswith(end_required)
        return True

    def _stream_match_key(stream_id, stream_name, provider_id, provider_name):
        # Deduplicate per-provider only: same channel name/ID from different
        # providers should both be retained, while duplicates within a provider
        # collapse to a single match.
        provider_id_value = None
        if provider_id is not None:
            provider_id_str = str(provider_id).strip()
            if provider_id_str:
                provider_id_value = provider_id_str
        provider_key = ('provider_id', provider_id_value) if provider_id_value is not None else ('provider_name', str(provider_name))
        if stream_id is not None:
            return ('id', str(stream_id), provider_key)
        return ('fallback', str(stream_name), provider_key)

    def _add_matched_stream(stream, stream_name, stream_id, provider_id, provider_name, match_source):
        nonlocal total_matching, filtered_count, preview_truncated
        key = _stream_match_key(stream_id, stream_name, provider_id, provider_name)
        if key in matched_keys:
            return
        matched_keys.add(key)

        total_matching += 1
        matching_streams.append({
            'id': stream_id,
            'name': stream_name,
            'match_source': match_source,
        })

        detailed_stream = {
            'id': stream_id,
            'name': stream_name,
            'provider_id': provider_id,
            'provider_name': provider_name,
            # URL is not used by the UI and can be large/sensitive; omit.
            'resolution': stream.get('resolution'),
            'bitrate_kbps': stream.get('bitrate_kbps') or stream.get('ffmpeg_output_bitrate'),
            'match_source': match_source,
        }

        filtered_count += 1
        if stream_id is not None:
            filtered_ids.append(stream_id)
        if preview:
            if _PREVIEW_STREAM_LIMIT is None or len(preview_streams) < _PREVIEW_STREAM_LIMIT:
                preview_streams.append(detailed_stream)
            else:
                preview_truncated = True
        else:
            filtered_streams.append(detailed_stream)

    def _record_excluded_stream(stream, stream_name, stream_id, provider_id, provider_name, match_source):
        if not preview:
            return
        excluded_streams.append({
            'id': stream_id,
            'name': stream_name,
            'provider_id': provider_id,
            'provider_name': provider_name,
            'match_source': match_source,
            'reason': 'excluded'
        })

    def _matches_any_selector(stream_name):
        if not selector_values:
            return False
        return any(_wildcard_match(stream_name, selector) for selector in selector_values)

    # PHASE 1: SELECTOR MATCHES
    for stream in all_streams:
        stream_name = stream.get('name', '')
        stream_id = stream.get('id')
        provider_id = stream.get('m3u_account')
        provider_name = stream.get('m3u_account_name')

        if provider_name is None and provider_lookup and provider_id is not None:
            provider_name = provider_lookup.get(str(provider_id))

        if not isinstance(stream_name, str):
            continue

        match_source = None
        stream_id_key = _normalize_stream_id(stream_id)
        if allowed_set is not None:
            if stream_id_key is None or stream_id_key not in allowed_set:
                continue
            match_source = 'selected'
        elif _matches_any_selector(stream_name):
            match_source = 'selector'
        elif injected_include_set and stream_name.casefold() in injected_include_set:
            match_source = 'include'

        if not match_source:
            continue
        if stream_name in injected_exclude_set:
            if allowed_set is not None:
                if stream_id_key is not None and stream_id_key in allowed_set:
                    pass
                else:
                    _record_excluded_stream(
                        stream,
                        stream_name,
                        stream_id,
                        provider_id,
                        provider_name,
                        match_source,
                    )
                    continue
            else:
                _record_excluded_stream(
                    stream,
                    stream_name,
                    stream_id,
                    provider_id,
                    provider_name,
                    match_source,
                )
                continue

        _add_matched_stream(
            stream,
            stream_name,
            stream_id,
            provider_id,
            provider_name,
            match_source,
        )

    # PHASE 2: ENSURE INJECTED INCLUDES
    if injected_include_set:
        for stream in all_streams:
            stream_name = stream.get('name', '')
            if not isinstance(stream_name, str):
                continue
            stream_id = stream.get('id')
            provider_id = stream.get('m3u_account')
            provider_name = stream.get('m3u_account_name')
            if provider_name is None and provider_lookup and provider_id is not None:
                provider_name = provider_lookup.get(str(provider_id))
            stream_id_key = _normalize_stream_id(stream_id)
            if allowed_set is not None:
                if stream_id_key is None or stream_id_key not in allowed_set:
                    continue
            if stream_name.casefold() not in injected_include_set:
                continue
            if stream_name in injected_exclude_set:
                if allowed_set is not None:
                    if stream_id_key is not None and stream_id_key in allowed_set:
                        pass
                    else:
                        _record_excluded_stream(
                            stream,
                            stream_name,
                            stream_id,
                            provider_id,
                            provider_name,
                            'include',
                        )
                        continue
                else:
                    _record_excluded_stream(
                        stream,
                        stream_name,
                        stream_id,
                        provider_id,
                        provider_name,
                        'include',
                    )
                    continue
            _add_matched_stream(
                stream,
                stream_name,
                stream_id,
                provider_id,
                provider_name,
                'include',
            )

    logging.info(f"Found {total_matching} matching streams")

    filtered_id_set = set(filtered_ids)
    added_ids = filtered_id_set - current_stream_ids
    removed_ids = current_stream_ids - filtered_id_set
    no_change = len(added_ids) == 0 and len(removed_ids) == 0

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
        'removed': len(removed_ids),
        'added': len(added_ids),
        'streams': preview_streams if preview else filtered_streams,
        'channel_name': channel_name,
        'base_search_text': search_name,
        'preview_limit': _PREVIEW_STREAM_LIMIT if preview else None,
        'preview_truncated': bool(preview_truncated) if preview else False,
        'previous_streams': previous_streams,
        'final_streams': final_streams,
        'final_stream_ids': final_stream_ids,
        'no_change': no_change,
        'injected_includes': injected_includes if preview else None,
        'injected_excludes': injected_excludes if preview else None,
        'selector_count': selector_count if preview else None,
        'excluded_streams': excluded_streams if preview else None
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
