"""
stream_analysis.py
Core stream analysis library for Dispatcharr-Maid
"""

import csv
import json
import logging
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
                    return set(data.get('processed_ids', []))
            except:
                return set()
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
            self.processed_ids.add(stream_id)
            
            # Checkpoint every 10 streams
            if self.processed % 10 == 0:
                self.save_checkpoint()
    
    def is_processed(self, stream_id):
        """Check if stream was already processed"""
        return stream_id in self.processed_ids
    
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
                'timeout': 30,
                'workers': 8,
                'retries': 1,
                'retry_delay': 10,
                'enable_capability_test': False,
                'capability_test_duration': 5,
                'capability_test_timeout': 12,
                'capability_max_width': 1920,
                'capability_max_height': 1080,
                'capability_startup_grace': 1.5,
            },
            'scoring': {
                'fps_bonus_points': 100,
                'hevc_boost': 1.5,
                'resolution_scores': {
                    '3840x2160': 100,
                    '1920x1080': 80,
                    '1280x720': 50,
                    '960x540': 20
                },
                'capability_pass_points': 180,
                'capability_timeout_penalty': 350,
                'capability_failure_penalty': 450,
                'capability_50fps_bonus': 60,
                'capability_full_run_bonus': 30,
                'capability_throughput_weight': 0.5,
            },
            'filters': {
                'channel_group_ids': [],
                'start_channel': 1,
                'end_channel': 99999,
                'stream_last_measured_days': 1,
                'remove_duplicates': True
            }
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


def _run_capability_test(url, analysis_cfg):
    """Run a short ffmpeg encode test to gauge server capability."""

    duration = float(analysis_cfg.get('capability_test_duration', 5) or 5)
    timeout = float(analysis_cfg.get('capability_test_timeout', max(8, duration + 5)))
    max_width = int(analysis_cfg.get('capability_max_width', 1920) or 1920)
    max_height = int(analysis_cfg.get('capability_max_height', 1080) or 1080)
    startup_grace = float(analysis_cfg.get('capability_startup_grace', 1.5) or 1.5)
    user_agent = analysis_cfg.get('user_agent', 'VLC/3.0.14')

    scale_filter = f"scale='if(gt(iw,{max_width}),{max_width},iw)':'if(gt(ih,{max_height}),{max_height},ih)'"

    command = [
        'ffmpeg',
        '-user_agent', user_agent,
        '-analyzeduration', '1000000',
        '-probesize', '1000000',
        '-loglevel', 'error',
        '-i', url,
        '-map', '0:v:0',
        '-map', '0:a?',
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-profile:v', 'high',
        '-level', '4.1',
        '-vf', scale_filter,
        '-crf', '23',
        '-c:a', 'aac',
        '-ac', '2',
        '-b:a', '128k',
        '-t', str(duration),
        '-progress', 'pipe:1',
        '-f', 'mpegts',
        '-y', '/dev/null'
    ]

    metrics = {
        'status': 'not_run',
        'frames': 0,
        'fps': 0.0,
        'elapsed': 0.0,
        'reason': None,
        'score': 0.0,
    }

    start = time.time()
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout
        )
        metrics['elapsed'] = time.time() - start

        # Parse ffmpeg -progress key/value lines from stdout
        progress = {}
        for line in result.stdout.splitlines():
            if '=' not in line:
                continue
            key, value = line.split('=', 1)
            progress[key.strip()] = value.strip()

        try:
            metrics['frames'] = int(progress.get('frame') or 0)
        except ValueError:
            metrics['frames'] = 0
        try:
            metrics['fps'] = float(progress.get('fps') or 0)
        except ValueError:
            metrics['fps'] = 0.0

        if result.returncode != 0:
            metrics['status'] = 'error'
            metrics['reason'] = result.stderr.strip()[:200] or 'ffmpeg exited non-zero'
        elif metrics['frames'] <= 0 and metrics['elapsed'] >= startup_grace:
            metrics['status'] = 'no_frames'
            metrics['reason'] = 'No frames produced during capability test'
        else:
            metrics['status'] = 'ok'

    except subprocess.TimeoutExpired:
        metrics['elapsed'] = timeout
        metrics['status'] = 'timeout'
        metrics['reason'] = f'Capability test exceeded {timeout}s'
    except Exception as exc:
        metrics['elapsed'] = time.time() - start
        metrics['status'] = 'error'
        metrics['reason'] = f'Capability test failed: {exc}'

    scoring_cfg = analysis_cfg.get('scoring') or {}
    pass_points = scoring_cfg.get('capability_pass_points', 180)
    timeout_penalty = scoring_cfg.get('capability_timeout_penalty', 350)
    failure_penalty = scoring_cfg.get('capability_failure_penalty', 450)
    fps_bonus = scoring_cfg.get('capability_50fps_bonus', 60)
    full_run_bonus = scoring_cfg.get('capability_full_run_bonus', 30)
    throughput_weight = scoring_cfg.get('capability_throughput_weight', 0.5)

    if metrics['status'] == 'ok':
        metrics['score'] = pass_points
        if metrics['fps'] >= 50:
            metrics['score'] += fps_bonus
        elif metrics['fps'] >= 45:
            metrics['score'] += fps_bonus / 2

        # Reward making it through the planned window (avoid rewards on early aborts)
        if metrics['elapsed'] >= duration * 0.9:
            metrics['score'] += full_run_bonus

        # Throughput bonus based on frames processed per second
        if metrics['elapsed'] > 0 and metrics['frames'] > 0:
            throughput = metrics['frames'] / metrics['elapsed']
            metrics['score'] += min(pass_points * throughput_weight, throughput)
    elif metrics['status'] == 'timeout':
        metrics['score'] = -abs(timeout_penalty)
    else:
        metrics['score'] = -abs(failure_penalty)

    return metrics


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
    capability_enabled = bool(analysis_cfg.get('enable_capability_test', False))
    capability_scoring = config.get('scoring') or {}
    
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

    # Capability test (optional, opt-in)
    if capability_enabled:
        capability_metrics = _run_capability_test(
            url,
            {**analysis_cfg, 'scoring': capability_scoring}
        )
    else:
        capability_metrics = {
            'status': 'not_run',
            'frames': 0,
            'fps': 0.0,
            'elapsed': 0.0,
            'reason': 'Capability test disabled',
            'score': 0.0,
        }

    row['capability_status'] = capability_metrics.get('status')
    row['capability_reason'] = capability_metrics.get('reason')
    row['capability_frames'] = capability_metrics.get('frames')
    row['capability_fps'] = capability_metrics.get('fps')
    row['capability_elapsed'] = capability_metrics.get('elapsed')
    row['capability_score'] = capability_metrics.get('score')

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
    days_to_keep = filters.get('stream_last_measured_days', 7)
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
        'err_decode', 'err_discontinuity', 'err_timeout',
        'capability_status', 'capability_reason', 'capability_frames',
        'capability_fps', 'capability_elapsed', 'capability_score'
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
        df_final = pd.read_csv(output_csv)
        df_final['stream_id'] = pd.to_numeric(df_final['stream_id'], errors='coerce')
        df_final.dropna(subset=['stream_id'], inplace=True)
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

    # Convert types
    df['bitrate_kbps'] = pd.to_numeric(df['bitrate_kbps'], errors='coerce')
    df['frames_decoded'] = pd.to_numeric(df['frames_decoded'], errors='coerce')
    df['frames_dropped'] = pd.to_numeric(df['frames_dropped'], errors='coerce')
    if 'capability_score' in df.columns:
        df['capability_score'] = pd.to_numeric(df['capability_score'], errors='coerce').fillna(0)
    else:
        df['capability_score'] = 0
    
    # Group by stream and calculate averages
    summary = df.groupby('stream_id').agg(
        avg_bitrate_kbps=('bitrate_kbps', 'mean'),
        avg_frames_decoded=('frames_decoded', 'mean'),
        avg_frames_dropped=('frames_dropped', 'mean'),
        capability_score=('capability_score', 'mean')
    ).reset_index()
    
    # Merge with latest metadata
    latest_meta = df.drop_duplicates(subset='stream_id', keep='last')
    summary = pd.merge(
        summary, 
        latest_meta.drop(columns=['bitrate_kbps', 'frames_decoded', 'frames_dropped']), 
        on='stream_id'
    )

    # Calculate dropped frame percentage
    summary['dropped_frame_percentage'] = (
        summary['avg_frames_dropped'] / summary['avg_frames_decoded'] * 100
    ).fillna(0)
    
    # Scoring
    resolution_scores = scoring_cfg.get('resolution_scores', {
        '3840x2160': 100, '1920x1080': 80, '1280x720': 50, '960x540': 20
    })
    summary['resolution_score'] = (
        summary['resolution'].astype(str).str.strip()
        .map(resolution_scores).fillna(0)
    )
    
    fps_bonus = scoring_cfg.get('fps_bonus_points', 100)
    summary['fps_bonus'] = 0
    summary.loc[pd.to_numeric(summary['fps'], errors='coerce').fillna(0) >= 50, 'fps_bonus'] = fps_bonus
    
    # HEVC boost
    hevc_boost = scoring_cfg.get('hevc_boost', 1.5)
    summary['scoring_bitrate'] = summary['avg_bitrate_kbps']
    if hevc_boost != 1.0:
        summary.loc[summary['video_codec'] == 'hevc', 'scoring_bitrate'] *= hevc_boost
    
    summary['max_bitrate_for_channel'] = summary.groupby('channel_id')['scoring_bitrate'].transform('max')
    summary['bitrate_score'] = (
        summary['scoring_bitrate'] / (summary['max_bitrate_for_channel'] * 0.01)
    ).fillna(0)
    
    summary['dropped_frames_penalty'] = summary['dropped_frame_percentage'] * 1
    
    # Error penalties
    error_columns = ['err_decode', 'err_discontinuity', 'err_timeout']
    for col in error_columns:
        summary[col] = pd.to_numeric(summary[col], errors='coerce').fillna(0)
    summary['error_penalty'] = summary[error_columns].sum(axis=1) * 25

    if 'capability_status' not in summary.columns:
        summary['capability_status'] = 'not_run'
    else:
        summary['capability_status'] = summary['capability_status'].fillna('not_run')
    if 'capability_score' not in summary.columns:
        summary['capability_score'] = 0

    summary['capability_score'] = pd.to_numeric(summary['capability_score'], errors='coerce').fillna(0)
    
    # Calculate final score
    summary['score'] = (
        summary['bitrate_score'] +
        summary['resolution_score'] +
        summary['fps_bonus'] +
        summary['capability_score'] -
        summary['dropped_frames_penalty'] -
        summary['error_penalty']
    )
    summary.loc[summary['avg_bitrate_kbps'].isna(), 'score'] = -1
    
    # Sort by channel and score
    df_sorted = summary.sort_values(by=['channel_number', 'score'], ascending=[True, False])
    
    # Prepare final columns
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
        'error_penalty', 'capability_status', 'capability_score', 'capability_reason'
    ]
    for col in final_columns:
        if col not in df_sorted.columns:
            df_sorted[col] = 'N/A'
    
    df_sorted = df_sorted[final_columns]
    df_sorted.to_csv(output_csv, index=False, na_rep='N/A')
    logging.info(f"Scored streams saved to {output_csv}")
    
    # Update stream stats on server
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
            
            # Clean None values
            stats = {k: v for k, v in stats.items() if pd.notna(v)}
            
            if stats:
                try:
                    api.update_stream_stats(int(stream_id), stats)
                except Exception as e:
                    logging.warning(f"Could not update stats for stream {stream_id}: {e}")


def reorder_streams(api, config, input_csv=None):
    """Reorder streams in Dispatcharr based on scores"""

    logging.info("Reordering streams in Dispatcharr...")

    input_csv = input_csv or config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')
    
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        logging.error(f"Input CSV not found: {input_csv}")
        return
    
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
        logging.warning("No streams to reorder")
        return
    
    df['stream_id'] = pd.to_numeric(df['stream_id'], errors='coerce')
    df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
    df.dropna(subset=['stream_id', 'channel_id'], inplace=True)
    df['stream_id'] = df['stream_id'].astype(int)
    df['channel_id'] = df['channel_id'].astype(int)
    
    grouped = df.groupby("channel_id")
    
    for channel_id, group in grouped:
        sorted_stream_ids = group["stream_id"].tolist()
        
        # Get current streams from API
        current_streams = api.fetch_channel_streams(channel_id)
        if not current_streams:
            logging.warning(f"Could not fetch streams for channel {channel_id}")
            continue
        
        current_ids_set = {s['id'] for s in current_streams}
        validated_ids = [sid for sid in sorted_stream_ids if sid in current_ids_set]
        
        # Add any new streams not in CSV
        csv_ids_set = set(sorted_stream_ids)
        new_ids = [sid for sid in current_ids_set if sid not in csv_ids_set]
        final_ids = validated_ids + new_ids
        
        if not final_ids:
            logging.warning(f"No valid streams for channel {channel_id}")
            continue
        
        try:
            api.update_channel_streams(channel_id, final_ids)
            logging.info(f"Reordered channel {channel_id}")
        except Exception as e:
            logging.error(f"Failed to reorder channel {channel_id}: {e}")
    
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
            'resolution': None
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
        'preview_truncated': bool(preview_truncated) if preview else False
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
