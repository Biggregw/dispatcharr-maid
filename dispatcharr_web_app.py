#!/usr/bin/env python3
"""
dispatcharr_web_app.py
Full interactive web application for Dispatcharr Maid
Run everything from the browser - no CLI needed!
"""

import json
import html
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from queue import Queue

import pandas as pd
import yaml
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

from api_utils import DispatcharrAPI
from provider_data import refresh_provider_data
from stream_analysis import (
    refresh_channel_streams,
    Config,
    fetch_streams,
    analyze_streams,
    score_streams,
    reorder_streams
)
from job_workspace import create_job_workspace

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

app = Flask(__name__)

CORS(app)

_dispatcharr_auth_state = {
    'authenticated': False,
    'last_error': None
}


# Check authentication lazily on first UI request to avoid crashing when Dispatcharr is temporarily unavailable.
def _ensure_dispatcharr_ready():
    """Lazily validate Dispatcharr auth on first UI request to avoid startup crashes."""
    if _dispatcharr_auth_state['authenticated']:
        return True, None

    try:
        api = DispatcharrAPI()
        api.login()
        api.fetch_channel_groups()
        _dispatcharr_auth_state['authenticated'] = True
        _dispatcharr_auth_state['last_error'] = None
        return True, None
    except Exception as exc:
        _dispatcharr_auth_state['last_error'] = str(exc)
        logging.error("Dispatcharr authentication check failed: %s", exc)
        return False, _dispatcharr_auth_state['last_error']


def _render_auth_error(error_message):
    message = error_message or 'Dispatcharr credentials are invalid or unavailable.'
    message = html.escape(str(message))
    return (
        f"""<!DOCTYPE html>
        <html lang='en'>
        <head><meta charset='utf-8'><title>Dispatcharr Authentication Required</title></head>
        <body>
            <h1>Dispatcharr Authentication Required</h1>
            <p>{message}</p>
            <p>Please update the Dispatcharr connection details and reload this page.</p>
        </body>
        </html>""",
        503
    )

# Job management
jobs = {}  # {job_id: Job}
job_lock = threading.Lock()


def _get_latest_job_with_workspace():
    """Return the most recent job that has an assigned workspace"""
    with job_lock:
        jobs_with_workspace = [j for j in jobs.values() if j.workspace]
        if not jobs_with_workspace:
            return None
        return max(jobs_with_workspace, key=lambda j: j.started_at)


def _build_config_from_job(job):
    """Create a Config instance scoped to a job workspace"""
    if job and job.workspace:
        workspace_path = Path(job.workspace)
        return Config(workspace_path / 'config.yaml', working_dir=workspace_path)
    return Config('config.yaml')


def _build_exclude_filter(exclude_filter, exclude_plus_one):
    filters = [item.strip() for item in (exclude_filter or '').split(',') if item.strip()]
    if exclude_plus_one:
        plus_one_pattern = r'\+ ?1'
        if plus_one_pattern not in filters:
            filters.append(plus_one_pattern)
    return ','.join(filters) if filters else None


def _ensure_provider_map(api, config):
    """
    Ensure provider_map.json exists in the workspace.
    This fetches provider accounts from Dispatcharr and saves the ID->name mapping.
    Called for ALL job types so historical results always have provider names.
    """
    dispatcharr_cfg = config.get('dispatcharr') or {}
    if not dispatcharr_cfg.get('refresh_provider_data', False):
        return
    refresh_provider_data(api, config, force=False)


def _job_ran_analysis(job_type):
    """Return True if the job type includes an analysis step."""
    return job_type in {'full', 'full_cleanup', 'analyze'}


def _provider_names_path(config):
    return config.resolve_path('provider_names.json')


def _load_provider_names(config):
    """Load provider display names, preferring Dispatcharr mappings with optional overrides."""
    provider_names = _load_provider_map(config)
    
    # Try workspace-specific provider_names.json first
    provider_path = _provider_names_path(config)
    
    # Fall back to root directory if not found in workspace
    if not os.path.exists(provider_path):
        root_provider_path = 'provider_names.json'
        if os.path.exists(root_provider_path):
            provider_path = root_provider_path
        else:
            return provider_names
    
    try:
        with open(provider_path, 'r') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            provider_names.update({str(key): str(value) for key, value in data.items()})
            return provider_names
        logging.warning("provider_names.json must be a JSON object mapping provider_id to display_name.")
        return provider_names
    except Exception as exc:
        logging.warning("Could not load provider_names.json: %s", exc)
        return provider_names


def _provider_map_path(config):
    return config.resolve_path('provider_map.json')


def _load_provider_map(config):
    """Load Dispatcharr-sourced provider mappings for summary display."""
    provider_path = _provider_map_path(config)
    
    # Fall back to root directory if not found in workspace
    if not os.path.exists(provider_path):
        root_provider_path = 'provider_map.json'
        if os.path.exists(root_provider_path):
            provider_path = root_provider_path
        else:
            return {}
    
    try:
        with open(provider_path, 'r') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(key): str(value) for key, value in data.items()}
        logging.warning("provider_map.json must be a JSON object mapping provider_id to display_name.")
        return {}
    except Exception as exc:
        logging.warning("Could not load provider_map.json: %s", exc)
        return {}


def _provider_metadata_path(config):
    return config.resolve_path('provider_metadata.json')


def _load_provider_metadata(config):
    """Load Dispatcharr-sourced provider metadata for capacity visibility."""
    root_provider_path = Path(app.root_path) / 'provider_metadata.json'
    provider_path = None
    if config:
        workspace_path = Path(_provider_metadata_path(config))
        if workspace_path.exists():
            provider_path = workspace_path

    if provider_path is None:
        provider_path = root_provider_path

    if not provider_path.exists():
        return {}

    try:
        with open(provider_path, 'r') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(key): value for key, value in data.items()}
        logging.warning("provider_metadata.json must be a JSON object mapping provider_id to metadata.")
        return {}
    except Exception as exc:
        logging.warning("Could not load provider_metadata.json: %s", exc)
        return {}


def _build_capacity_summary(df, provider_metadata):
    if not provider_metadata:
        return None

    capacities = []
    for meta in provider_metadata.values():
        max_streams = None
        if isinstance(meta, dict):
            max_streams = meta.get('max_streams')
        if isinstance(max_streams, (int, float)) and max_streams > 0:
            capacities.append(int(max_streams))

    if not capacities:
        return None

    total_capacity = sum(capacities)
    channel_count = None
    if 'channel_id' in df.columns:
        channel_count = int(df['channel_id'].nunique())

    warning = None
    if channel_count is not None and channel_count > total_capacity:
        warning = (
            "Configured channels exceed total provider capacity. "
            "If all channels are watched simultaneously, provider connection limits may be hit."
        )

    return {
        'total_capacity': total_capacity,
        'channel_count': channel_count,
        'provider_count': len(capacities),
        'warning': warning
    }


def _extract_analyzed_streams(results):
    if isinstance(results, dict):
        total = results.get('total')
        if isinstance(total, (int, float)):
            return int(total)
    return 0


def _build_results_payload(results, analysis_ran, job_type, provider_names=None, provider_metadata=None, capacity_summary=None):
    payload = {
        'success': True,
        'results': results,
        'analysis_ran': analysis_ran,
        'analyzed_streams': _extract_analyzed_streams(results),
        'job_type': job_type
    }
    if provider_names is not None:
        payload['provider_names'] = provider_names
    if provider_metadata is not None:
        payload['provider_metadata'] = provider_metadata
    if capacity_summary is not None:
        payload['capacity_summary'] = capacity_summary
    return payload


def _get_job_results(job_id):
    with job_lock:
        job = jobs.get(job_id)

    if job:
        results = job.result_summary
        job_type = job.job_type
        config = _build_config_from_job(job)
        specific_channel_ids = job.channels
    else:
        history = get_job_history()
        history_job = next((entry for entry in history if entry.get('job_id') == job_id), None)
        if not history_job:
            return None, None, None, None, 'Job not found'
        results = history_job.get('result_summary')
        job_type = history_job.get('job_type')
        workspace = history_job.get('workspace')
        specific_channel_ids = history_job.get('channels')
        if workspace:
            workspace_path = Path(workspace)
            config = Config(workspace_path / 'config.yaml', working_dir=workspace_path)
        else:
            config = Config('config.yaml')

    # Treat "missing" as None (an empty dict is still a valid payload).
    # If the summary is missing, try reconstructing from the job workspace.
    if results is None or (isinstance(results, dict) and not results and _job_ran_analysis(job_type)):
        try:
            summary = generate_job_summary(config, specific_channel_ids=specific_channel_ids)
        except Exception:
            summary = None
        if summary:
            results = summary
        elif results is None:
            return None, None, None, None, 'No results available'

    analysis_ran = _job_ran_analysis(job_type) if job_type else False
    return results, analysis_ran, job_type, config, None


class Job:
    """Represents a running or completed job"""

    def __init__(self, job_id, job_type, groups, channels=None, base_search_text=None, include_filter=None, exclude_filter=None, streams_per_provider=1, exclude_4k=False, exclude_plus_one=False, group_names=None, channel_names=None, workspace=None, selected_stream_ids=None):
        self.job_id = job_id
        self.job_type = job_type  # 'full', 'full_cleanup', 'fetch', 'analyze', etc.
        self.groups = groups
        self.channels = channels
        self.group_names = group_names or "Unknown"
        self.channel_names = channel_names or "All channels"
        self.base_search_text = base_search_text
        self.include_filter = include_filter
        self.exclude_filter = exclude_filter
        self.streams_per_provider = streams_per_provider  # Specific channel IDs if selected
        self.exclude_4k = exclude_4k  # Exclude 4K/UHD streams during refresh
        self.exclude_plus_one = exclude_plus_one
        self.selected_stream_ids = selected_stream_ids
        self.status = 'queued'  # queued, running, completed, failed, cancelled
        self.progress = 0
        self.total = 0
        self.failed = 0
        self.current_step = ''
        self.started_at = datetime.now().isoformat()
        self.completed_at = None
        self.error = None
        self.cancel_requested = False
        self.thread = None
        self.result_summary = None
        self.workspace = workspace
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'groups': self.groups,
            'channels': self.channels,
            'group_names': self.group_names,
            'channel_names': self.channel_names,
            'base_search_text': self.base_search_text,
            'selected_stream_ids': self.selected_stream_ids,
            'exclude_plus_one': self.exclude_plus_one,
            'status': self.status,
            'progress': self.progress,
            'total': self.total,
            'failed': self.failed,
            'current_step': self.current_step,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'error': self.error,
            'result_summary': self.result_summary,
            'workspace': self.workspace
        }


def get_job_history():
    """Get job history from file"""
    history_file = 'logs/job_history.json'
    
    if not os.path.exists(history_file):
        return []
    
    try:
        with open(history_file, 'r') as f:
            return json.load(f)
    except:
        return []


def save_job_to_history(job):
    """Save completed job to history"""
    history_file = 'logs/job_history.json'
    Path(history_file).parent.mkdir(parents=True, exist_ok=True)
    
    history = get_job_history()
    
    # Add this job
    history.insert(0, job.to_dict())
    
    # Keep only last 50 jobs
    history = history[:50]
    
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)


def progress_callback(job, progress_data):
    """Callback function for progress updates"""
    job.progress = progress_data.get('processed', 0)
    job.total = progress_data.get('total', 0)
    job.failed = progress_data.get('failed', 0)


def run_job_worker(job, api, config):
    """Background worker that executes the job"""
    task_name = None
    try:
        job.status = 'running'
        
        # Update config with selected groups and channels
        config.set('filters', 'channel_group_ids', job.groups)
        
        # Add specific channel IDs if selected
        if job.channels:
            config.set('filters', 'specific_channel_ids', job.channels)
        else:
            # Clear any previous specific selections
            filters = config.get('filters')
            if filters and 'specific_channel_ids' in filters:
                del config.config['filters']['specific_channel_ids']
        
        config.save()
        
        # ===== FIX: Ensure provider_map.json exists for ALL job types =====
        # This ensures historical job results can display provider names correctly
        _ensure_provider_map(api, config)
        # ==================================================================
        
        # Execute based on job type
        if job.job_type in ['full', 'full_cleanup']:
            if job.job_type == 'full_cleanup':
                task_name = "Quality Check & Cleanup"
                logging.info("TASK_START: Quality Check & Cleanup")
            # Step 1: Fetch
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Fetching streams...'
            fetch_streams(api, config)
            
            # Step 2: Analyze (FORCE re-analysis of all streams)
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Analyzing streams (forcing fresh analysis)...'
            
            # Save original setting and force re-analysis
            original_days = config.get('filters', 'stream_last_measured_days', 1)
            config.set('filters', 'stream_last_measured_days', 0)
            config.save()
            
            def progress_wrapper(progress_data):
                progress_callback(job, progress_data)
                return not job.cancel_requested  # Return False to cancel
            
            analyzed_count = 0
            try:
                analyzed_count = analyze_streams(
                    config,
                    progress_callback=progress_wrapper,
                    force_full_analysis=(job.job_type == 'full_cleanup')
                ) or 0
            finally:
                # Restore original setting
                config.set('filters', 'stream_last_measured_days', original_days)
                config.save()
            
            if job.job_type == 'full_cleanup' and not analyzed_count:
                logging.warning("Full cleanup analysis executed zero streams; stopping before scoring/cleanup.")
                job.status = 'failed'
                job.current_step = 'Error: Analysis executed zero streams'
                return
            
            # Step 3: Score
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Scoring streams...'
            score_streams(api, config, update_stats=True)
            
            # Step 4: Reorder
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Reordering streams...'
            reorder_streams(api, config)
            
            # Step 5: Cleanup (if requested)
            if job.job_type == 'full_cleanup':
                if job.cancel_requested:
                    job.status = 'cancelled'
                    return
                
                job.current_step = f'Cleaning up (keeping top {job.streams_per_provider} per provider)...'
                cleanup_stats = cleanup_streams_by_provider(api, job.groups, config, job.streams_per_provider, job.channels)
                
                # Add cleanup stats to result summary
                if not job.result_summary:
                    job.result_summary = {}
                job.result_summary['cleanup_stats'] = cleanup_stats
                job.result_summary['final_stream_count'] = cleanup_stats.get('total_streams_after', 0)
        
        elif job.job_type == 'fetch':
            job.current_step = 'Fetching streams...'
            fetch_streams(api, config)
        
        elif job.job_type == 'analyze':
            job.current_step = 'Analyzing streams...'
            
            def progress_wrapper(progress_data):
                progress_callback(job, progress_data)
                return not job.cancel_requested
            
            analyze_streams(config, progress_callback=progress_wrapper)
        
        elif job.job_type == 'score':
            job.current_step = 'Scoring streams...'
            score_streams(api, config, update_stats=True)
        
        elif job.job_type == 'reorder':
            job.current_step = 'Reordering streams...'
            reorder_streams(api, config)
        
        elif job.job_type == 'refresh_optimize':
            task_name = "Refresh Channel Streams"
            logging.info("TASK_START: Refresh Channel Streams")
            # Validate single channel selection
            if not job.channels or len(job.channels) != 1:
                job.status = 'failed'
                job.current_step = 'Error: Must select exactly 1 channel'
                return
            
            channel_id = job.channels[0]
            
            # ONLY refresh - find and replace all streams with matching ones
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Searching all providers for matching streams...'
            effective_exclude_filter = _build_exclude_filter(job.exclude_filter, job.exclude_plus_one)
            refresh_result = refresh_channel_streams(
                api,
                config,
                channel_id,
                job.base_search_text,
                job.include_filter,
                effective_exclude_filter,
                job.exclude_4k,
                job.selected_stream_ids
            )
            
            if 'error' in refresh_result:
                job.status = 'failed'
                job.current_step = f"Error: {refresh_result['error']}"
                return
            
            # Done - just report what happened
            removed = refresh_result.get('removed', 0)
            added = refresh_result.get('added', 0)
            job.current_step = f"Replaced {removed} old streams with {added} matching streams"
            
            # Store refresh-specific summary (skip CSV-based summary)
            job.result_summary = {
                'job_type': 'refresh',
                'channel_id': channel_id,
                'previous_count': removed,
                'new_count': added,
                'total_matching': refresh_result.get('total_matching', 0),
                'include_filter': job.include_filter,
                'exclude_filter': effective_exclude_filter,
                'base_search_text': refresh_result.get('base_search_text')
            }
            
            # Small delay to ensure frontend polling catches the final status
            import time
            time.sleep(1)
        
        elif job.job_type == 'cleanup':
            job.current_step = f'Cleaning up (keeping top {job.streams_per_provider} per provider)...'
            cleanup_stats = cleanup_streams_by_provider(api, job.groups, config, job.streams_per_provider, job.channels)
            
            # Store cleanup stats for history
            job.result_summary = {
                'cleanup_stats': cleanup_stats,
                'final_stream_count': cleanup_stats.get('total_streams_after', 0)
            }
        
        # Job completed successfully
        job.status = 'completed' if not job.cancel_requested else 'cancelled'
        job.current_step = 'Completed' if not job.cancel_requested else 'Cancelled'
        job.completed_at = datetime.now().isoformat()
        
        # Generate analysis summary when analysis ran
        if _job_ran_analysis(job.job_type):
            summary = generate_job_summary(config, specific_channel_ids=job.channels)
            if summary:
                if job.result_summary:
                    job.result_summary.update(summary)
                else:
                    job.result_summary = summary
        
    except Exception as e:
        job.status = 'failed'
        job.error = str(e)
        job.completed_at = datetime.now().isoformat()
    
    finally:
        if task_name:
            logging.info(f"TASK_END: {task_name}")
        # Save to history
        save_job_to_history(job)


def cleanup_streams_by_provider(api, selected_group_ids, config, streams_per_provider=1, specific_channel_ids=None):
    """Keep only the top N streams from each provider for each channel, sorted by quality score"""
    
    # Track stats
    stats = {
        'channels_processed': 0,
        'total_streams_before': 0,
        'total_streams_after': 0
    }
    
    # Load scored streams to get quality rankings
    scored_file = config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')
    stream_scores = {}
    
    if os.path.exists(scored_file):
        try:
            df = pd.read_csv(scored_file)
            # Create lookup: stream_id -> quality_score
            for _, row in df.iterrows():
                stream_id = int(row['stream_id']) if pd.notna(row.get('stream_id')) else None
                score = float(row['quality_score']) if pd.notna(row.get('quality_score')) else 0
                if stream_id:
                    stream_scores[stream_id] = score
        except Exception as e:
            logging.warning(f"Could not load scores: {e}")
    
    channels = api.fetch_channels()
    
    # Filter by specific channels if provided, otherwise use groups
    if specific_channel_ids:
        filtered_channels = [
            ch for ch in channels 
            if ch.get('id') in specific_channel_ids
        ]
    else:
        filtered_channels = [
            ch for ch in channels 
            if ch.get('channel_group_id') in selected_group_ids
        ]
    
    for channel in filtered_channels:
        channel_id = channel['id']
        stream_ids = channel.get('streams', [])
        
        if not stream_ids or len(stream_ids) <= streams_per_provider:
            continue
        
        stats['channels_processed'] += 1
        stats['total_streams_before'] += len(stream_ids)
        
        # Fetch details for each stream and group by provider
        streams_by_provider = {}
        
        for stream_id in stream_ids:
            stream_details = api.fetch_stream_details(stream_id)
            
            if stream_details:
                provider_id = stream_details.get('m3u_account')
                
                if provider_id is not None:
                    if provider_id not in streams_by_provider:
                        streams_by_provider[provider_id] = []
                    
                    # Store stream with its score
                    score = stream_scores.get(stream_id, 0)
                    streams_by_provider[provider_id].append({
                        'id': stream_id,
                        'score': score
                    })
        
        # Keep top N streams from each provider, grouped by rank
        streams_by_rank = {}  # rank -> [stream_ids]
        
        for provider_id, provider_streams in streams_by_provider.items():
            # Sort by score (highest first)
            sorted_streams = sorted(provider_streams, key=lambda x: x['score'], reverse=True)
            
            # Keep top N and organize by rank
            for rank, stream in enumerate(sorted_streams[:streams_per_provider]):
                if rank not in streams_by_rank:
                    streams_by_rank[rank] = []
                streams_by_rank[rank].append(stream)
        
        # Build final list: all rank 0 (best) sorted by score, then all rank 1 (2nd best) sorted, etc
        streams_to_keep = []
        for rank in sorted(streams_by_rank.keys()):
            # Sort this rank's streams by score (highest first)
            rank_streams = sorted(streams_by_rank[rank], key=lambda x: x['score'], reverse=True)
            streams_to_keep.extend([s['id'] for s in rank_streams])
        
        if streams_to_keep and len(streams_to_keep) < len(stream_ids):
            try:
                api.update_channel_streams(channel_id, streams_to_keep)
                stats['total_streams_after'] += len(streams_to_keep)
                logging.info(f"Channel {channel_id}: kept {len(streams_to_keep)} streams ({streams_per_provider} per provider)")
            except Exception as e:
                logging.warning(f"Failed to update channel {channel_id}: {e}")
        else:
            stats['total_streams_after'] += len(stream_ids)
    
    return stats


def generate_job_summary(config, specific_channel_ids=None):
    """Generate comprehensive summary of last analysis, optionally filtered by channel IDs"""
    measurements_file = config.resolve_path('csv/03_iptv_stream_measurements.csv')
    scored_file = config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')
    if not os.path.exists(measurements_file):
        return None
    
    try:
        df = pd.read_csv(measurements_file)
        scored_df = None
        if os.path.exists(scored_file):
            scored_df = pd.read_csv(scored_file)
        
        if len(df) == 0:
            return None

        if scored_df is not None and len(scored_df) == 0:
            scored_df = None
        
        # Filter by specific channels if provided
        if specific_channel_ids:
            df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
            df = df[df['channel_id'].isin(specific_channel_ids)]
            
            if len(df) == 0:
                return None
            if scored_df is not None and 'channel_id' in scored_df.columns:
                scored_df['channel_id'] = pd.to_numeric(scored_df['channel_id'], errors='coerce')
                scored_df = scored_df[scored_df['channel_id'].isin(specific_channel_ids)]
        
        total = len(df)
        successful = len(df[df['status'] == 'OK'])
        failed = total - successful
        
        stats_df = scored_df if scored_df is not None else df

        # Provider breakdown
        provider_stats = {}
        provider_stats_df = None
        if 'm3u_account' in stats_df.columns:
            provider_stats_df = stats_df
        elif 'm3u_account' in df.columns:
            provider_stats_df = df

        if provider_stats_df is not None:
            quality_column = None
            if 'quality_score' in provider_stats_df.columns:
                quality_column = 'quality_score'
            elif 'score' in provider_stats_df.columns:
                quality_column = 'score'

            for provider_id in provider_stats_df['m3u_account'].unique():
                if pd.isna(provider_id):
                    continue
                provider_df = provider_stats_df[provider_stats_df['m3u_account'] == provider_id]
                provider_total = len(provider_df)
                provider_success = len(provider_df[provider_df['status'] == 'OK'])
                
                # Get average quality score for successful streams (exclude unknown/negative)
                success_df = provider_df[provider_df['status'] == 'OK']
                avg_score = 0
                neutral_quality = 70
                unknown_penalty = 5
                if quality_column and len(success_df) > 0:
                    quality_series = pd.to_numeric(success_df[quality_column], errors='coerce')
                    known_quality = quality_series[quality_series >= 0]
                    if not known_quality.empty:
                        avg_score = known_quality.mean()
                    else:
                        avg_score = neutral_quality - unknown_penalty
                else:
                    avg_score = neutral_quality - unknown_penalty

                success_rate = round(provider_success / provider_total * 100, 1) if provider_total > 0 else 0
                success_weight = (success_rate / 100) ** 2
                weighted_score = avg_score * success_weight

                provider_key = str(int(provider_id)) if isinstance(provider_id, (int, float)) else str(provider_id)
                provider_stats[provider_key] = {
                    'total': provider_total,
                    'successful': provider_success,
                    'failed': provider_total - provider_success,
                    'success_rate': success_rate,
                    'avg_quality': round(avg_score, 1),
                    'weighted_score': round(weighted_score, 1)
                }
        
        # Quality distribution
        quality_dist = {
            'excellent': 0,  # 90-100
            'good': 0,       # 70-89
            'fair': 0,       # 50-69
            'poor': 0        # <50
        }
        
        if 'quality_score' in df.columns:
            success_df = df[df['status'] == 'OK']
            quality_series = pd.to_numeric(success_df['quality_score'], errors='coerce')
            known_quality = quality_series[quality_series >= 0]
            for score in known_quality:
                if score >= 90:
                    quality_dist['excellent'] += 1
                elif score >= 70:
                    quality_dist['good'] += 1
                elif score >= 50:
                    quality_dist['fair'] += 1
                else:
                    quality_dist['poor'] += 1
        
        # Resolution breakdown
        resolution_dist = {}
        if 'resolution' in df.columns:
            success_df = df[df['status'] == 'OK']
            for res in success_df['resolution'].value_counts().items():
                resolution_dist[res[0]] = int(res[1])
        
        # Error analysis
        error_types = {}
        failed_df = df[df['status'] != 'OK']
        if len(failed_df) > 0:
            # Use status column for error types
            for error in failed_df['status'].value_counts().items():
                if pd.notna(error[0]) and error[0] != 'OK':
                    error_types[error[0]] = int(error[1])
        
        # Channel breakdown
        channel_stats = {}
        if 'channel_number' in stats_df.columns:
            for channel_num in stats_df['channel_number'].unique():
                if pd.isna(channel_num):
                    continue
                channel_df = stats_df[stats_df['channel_number'] == channel_num]
                # Use stream_name as channel name (first occurrence)
                channel_name = channel_df['stream_name'].iloc[0] if 'stream_name' in stats_df.columns and len(channel_df) > 0 else f"Channel {int(channel_num)}"
                channel_total = len(channel_df)
                channel_success = len(channel_df[channel_df['status'] == 'OK'])
                
                channel_stats[str(int(channel_num))] = {
                    'name': channel_name,
                    'total': channel_total,
                    'successful': channel_success,
                    'failed': channel_total - channel_success,
                    'success_rate': round(channel_success / channel_total * 100, 1) if channel_total > 0 else 0
                }
        provider_metadata = _load_provider_metadata(config)
        capacity_summary = _build_capacity_summary(df, provider_metadata)

        return {
            'total': total,
            'successful': successful,
            'failed': failed,
            'success_rate': round(successful / total * 100, 1) if total > 0 else 0,
            'provider_stats': provider_stats,
            'quality_distribution': quality_dist,
            'resolution_distribution': resolution_dist,
            'error_types': error_types,
            'channel_stats': channel_stats,
            'capacity_summary': capacity_summary,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error generating summary: {e}")
        return None


# API Endpoints

@app.route('/')
def index():
    """Main application page"""
    auth_ok, auth_error = _ensure_dispatcharr_ready()
    if not auth_ok:
        return _render_auth_error(auth_error)
    return render_template('app.html')


@app.route('/health')
def health_check():
    """Lightweight health endpoint that does not require Dispatcharr connectivity."""
    return jsonify({'status': 'ok'}), 200


@app.route('/results')
def results():
    """Results dashboard page"""
    auth_ok, auth_error = _ensure_dispatcharr_ready()
    if not auth_ok:
        return _render_auth_error(auth_error)
    return render_template('results.html')


@app.route('/api/groups')
def api_groups():
    """Get all channel groups with channel counts"""
    try:
        api = DispatcharrAPI()
        api.login()
        
        groups = api.fetch_channel_groups()
        channels = api.fetch_channels()
        
        # Count channels per group
        group_counts = {}
        for channel in channels:
            group_id = channel.get('channel_group_id')
            if group_id:
                group_counts[group_id] = group_counts.get(group_id, 0) + 1
        
        # Add counts to groups
        for group in groups:
            group['channel_count'] = group_counts.get(group['id'], 0)
        
        # Filter to groups with channels
        groups = [g for g in groups if g.get('channel_count', 0) > 0]
        
        return jsonify({'success': True, 'groups': groups})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/channels')
def api_channels():
    """Get channels for selected groups"""
    try:
        group_ids_str = request.args.get('groups', '')
        
        if not group_ids_str:
            return jsonify({'success': False, 'error': 'No groups specified'}), 400
        
        group_ids = [int(x.strip()) for x in group_ids_str.split(',') if x.strip()]
        
        api = DispatcharrAPI()
        api.login()
        
        all_channels = api.fetch_channels()
        
        # Filter by selected groups
        filtered_channels = [
            ch for ch in all_channels 
            if ch.get('channel_group_id') in group_ids
        ]
        
        # Sort by group, then by channel number
        filtered_channels.sort(key=lambda x: (x.get('channel_group_id', 0), x.get('channel_number', 0)))
        
        # Group channels by group_id
        channels_by_group = {}
        for channel in filtered_channels:
            group_id = channel.get('channel_group_id')
            if group_id not in channels_by_group:
                channels_by_group[group_id] = []

            channels_by_group[group_id].append({
                'id': channel['id'],
                'channel_number': channel.get('channel_number'),
                'name': channel.get('name', 'Unknown'),
                'channel_group_id': group_id,
                'base_search_text': channel.get('name', 'Unknown')
            })
        
        return jsonify({'success': True, 'channels': channels_by_group})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/refresh-preview', methods=['POST'])
def api_refresh_preview():
    """Preview matching streams for a channel before refreshing"""
    try:
        data = request.get_json()
        channel_id = data.get('channel_id')
        base_search_text = data.get('base_search_text')
        include_filter = data.get('include_filter')
        exclude_filter = data.get('exclude_filter')
        exclude_plus_one = data.get('exclude_plus_one', False)
        exclude_4k = data.get('exclude_4k', False)

        if not channel_id:
            return jsonify({'success': False, 'error': 'Channel ID is required'}), 400

        api = DispatcharrAPI()
        api.login()
        config = Config('config.yaml')

        effective_exclude_filter = _build_exclude_filter(exclude_filter, exclude_plus_one)
        preview = refresh_channel_streams(
            api,
            config,
            int(channel_id),
            base_search_text,
            include_filter,
            effective_exclude_filter,
            exclude_4k,
            preview=True
        )

        if 'error' in preview:
            return jsonify({'success': False, 'error': preview.get('error')}), 400

        return jsonify({'success': True, 'preview': preview})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/start-job', methods=['POST'])
def api_start_job():
    """Start a new job"""
    try:
        data = request.get_json()
        print(f'DEBUG: Received job request: {data}')
        import sys; sys.stdout.flush()
        
        job_type = data.get('job_type')
        groups = data.get('groups', [])
        channels = data.get('channels')  # Optional: specific channel IDs
        group_names = data.get('group_names', 'Unknown')
        channel_names = data.get('channel_names', 'All channels')
        base_search_text = data.get('base_search_text')
        include_filter = data.get('include_filter')
        exclude_filter = data.get('exclude_filter')
        streams_per_provider = data.get('streams_per_provider', 1)
        exclude_4k = data.get('exclude_4k', False)
        exclude_plus_one = data.get('exclude_plus_one', False)
        selected_stream_ids = data.get('selected_stream_ids')
        
        if not job_type or not groups:
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400
        
        # Create job
        job_id = str(uuid.uuid4())
        workspace, config_path = create_job_workspace(job_id)
        job = Job(job_id, job_type, groups, channels, base_search_text, include_filter, exclude_filter, streams_per_provider, exclude_4k, exclude_plus_one, group_names, channel_names, str(workspace), selected_stream_ids)

        # Initialize API and config
        api = DispatcharrAPI()
        api.login()
        config = Config(config_path, working_dir=workspace)
        
        # Start job in background thread
        job.thread = threading.Thread(
            target=run_job_worker, 
            args=(job, api, config),
            daemon=True
        )
        
        # Store job
        with job_lock:
            jobs[job_id] = job
        
        # Start thread
        job.thread.start()
        
        return jsonify({'success': True, 'job_id': job_id, 'job': job.to_dict()})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/job/<job_id>')
def api_get_job(job_id):
    """Get job status"""
    with job_lock:
        job = jobs.get(job_id)
    
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    
    return jsonify({'success': True, 'job': job.to_dict()})


@app.route('/api/job/<job_id>/cancel', methods=['POST'])
def api_cancel_job(job_id):
    """Cancel a running job"""
    with job_lock:
        job = jobs.get(job_id)
    
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    
    if job.status == 'running':
        job.cancel_requested = True
        return jsonify({'success': True, 'message': 'Cancellation requested'})
    else:
        return jsonify({'success': False, 'error': 'Job is not running'}), 400


@app.route('/api/jobs')
def api_get_jobs():
    """Get all active jobs"""
    with job_lock:
        active_jobs = [job.to_dict() for job in jobs.values()]
    
    return jsonify({'success': True, 'jobs': active_jobs})


@app.route('/api/job-history')
def api_job_history():
    """Get job history"""
    history = get_job_history()
    return jsonify({'success': True, 'history': history})


@app.route('/api/results/detailed/<job_id>')
def api_job_results(job_id):
    """Get detailed analysis results for a specific job"""
    try:
        results, analysis_ran, job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404

        provider_names = _load_provider_names(config) if config else {}
        provider_metadata = _load_provider_metadata(config) if config else {}
        capacity_summary = results.get('capacity_summary') if isinstance(results, dict) else None
        return jsonify(_build_results_payload(
            results,
            analysis_ran,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary
        ))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/job/<job_id>')
def api_job_scoped_results(job_id):
    """Get detailed analysis results scoped to a specific job."""
    try:
        results, analysis_ran, job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404

        provider_names = _load_provider_names(config) if config else {}
        provider_metadata = _load_provider_metadata(config) if config else {}
        capacity_summary = results.get('capacity_summary') if isinstance(results, dict) else None
        return jsonify(_build_results_payload(
            results,
            analysis_ran,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary
        ))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/detailed')
def api_detailed_results():
    """Get detailed analysis results from the most recent completed job"""
    try:
        # First, check for recent completed job with summary
        with job_lock:
            completed_jobs = [j for j in jobs.values() if j.status == 'completed' and j.result_summary]
            if completed_jobs:
                # Get most recent
                latest_job = max(completed_jobs, key=lambda j: j.started_at)
                provider_names = _load_provider_names(_build_config_from_job(latest_job))
                capacity_summary = None
                if isinstance(latest_job.result_summary, dict):
                    capacity_summary = latest_job.result_summary.get('capacity_summary')
                provider_metadata = _load_provider_metadata(_build_config_from_job(latest_job))
                return jsonify(_build_results_payload(
                    latest_job.result_summary,
                    _job_ran_analysis(latest_job.job_type),
                    latest_job.job_type,
                    provider_names,
                    provider_metadata,
                    capacity_summary
                ))
        
        # Fall back to CSV-based summary if no recent jobs
        latest_job = _get_latest_job_with_workspace()
        if latest_job is None:
            config = Config('config.yaml')
            summary = generate_job_summary(config)
        else:
            config = _build_config_from_job(latest_job)
            summary = generate_job_summary(config)
        
        if not summary:
            return jsonify({'success': False, 'error': 'No results available'}), 404
        
        job_type = latest_job.job_type if latest_job else None
        provider_names = _load_provider_names(config)
        provider_metadata = _load_provider_metadata(config)
        capacity_summary = summary.get('capacity_summary') if isinstance(summary, dict) else None
        return jsonify(_build_results_payload(
            summary,
            True,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary
        ))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/csv')
def api_export_csv():
    """Export results as CSV"""
    latest_job = _get_latest_job_with_workspace()
    config = _build_config_from_job(latest_job)
    measurements_file = config.resolve_path('csv/03_iptv_stream_measurements.csv')
    
    if not os.path.exists(measurements_file):
        return jsonify({'success': False, 'error': 'No results available'}), 404
    
    try:
        # Read and return CSV
        with open(measurements_file, 'r') as f:
            csv_data = f.read()
        
        from flask import Response
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=stream_analysis_results.csv'}
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/config')
def api_get_config():
    """Get current configuration"""
    try:
        config = Config('config.yaml')
        
        return jsonify({
            'success': True,
            'config': {
                'analysis': config.get('analysis'),
                'scoring': config.get('scoring'),
                'filters': config.get('filters')
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/config', methods=['POST'])
def api_update_config():
    """Update configuration"""
    try:
        data = request.get_json()
        config = Config('config.yaml')
        
        if 'analysis' in data:
            for key, value in data['analysis'].items():
                config.set('analysis', key, value)
        
        if 'scoring' in data:
            for key, value in data['scoring'].items():
                config.set('scoring', key, value)
        
        config.save()
        
        return jsonify({'success': True, 'message': 'Configuration updated'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    print("\n" + "="*70)
    print("🧹 DISPATCHARR MAID - WEB APPLICATION")
    print("="*70)
    print("\nStarting web application...")
    print("Access the dashboard at: http://localhost:5000")
    print("Or from another device: http://YOUR-SERVER-IP:5000")
    print("\n✨ Full interactive mode - no CLI needed!")
    print("   • Select channel groups")
    print("   • Select specific channels")
    print("   • Run jobs with one click")
    print("   • Monitor progress in real-time")
    print("\nPress Ctrl+C to stop")
    print("="*70 + "\n")
    
    # Run on all interfaces
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
