#!/usr/bin/env python3
"""
web_monitor.py
Minimal web dashboard for monitoring Dispatcharr Maid progress
"""

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, jsonify

app = Flask(__name__)


def get_current_progress():
    """Read current progress from checkpoint file"""
    checkpoint_file = 'logs/checkpoint.json'
    
    if not os.path.exists(checkpoint_file):
        return None
    
    try:
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
        
        processed = data.get('processed_count', len(data.get('processed_ids', [])))
        failed = data.get('failed_count', 0)
        timestamp = data.get('timestamp', '')
        
        # Try to determine total from CSV if available
        total = None
        if os.path.exists('csv/02_grouped_channel_streams.csv'):
            try:
                df = pd.read_csv('csv/02_grouped_channel_streams.csv')
                total = len(df)
            except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError):
                pass
        
        return {
            'active': True,
            'processed': processed,
            'total': total,
            'failed': failed,
            'timestamp': timestamp,
            'percent': (processed / total * 100) if total and total > 0 else 0
        }
    
    except (OSError, json.JSONDecodeError, ValueError, KeyError) as e:
        return None


def get_last_run_summary():
    """Get summary of last completed run"""
    measurements_file = 'csv/03_iptv_stream_measurements.csv'
    
    if not os.path.exists(measurements_file):
        return None
    
    try:
        df = pd.read_csv(measurements_file)
        
        if df.empty:
            return None
        
        # Get timestamp of last run
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        last_timestamp = df['timestamp'].max()
        
        # Calculate stats
        total = len(df)
        successful = len(df[df['status'] == 'OK'])
        failed = total - successful
        
        # Provider breakdown
        df['provider'] = df['stream_url'].str.extract(r'//([^/]+)')[0]
        provider_stats = df.groupby('provider').agg({
            'status': lambda x: (x == 'OK').sum(),
            'stream_id': 'count'
        }).rename(columns={'status': 'success', 'stream_id': 'total'})
        provider_stats['fail_rate'] = (
            (provider_stats['total'] - provider_stats['success']) / provider_stats['total'] * 100
        )
        
        # Top 5 providers
        top_providers = provider_stats.nlargest(5, 'success')
        top_list = [
            {
                'name': provider,
                'success': int(row['success']),
                'total': int(row['total']),
                'fail_rate': round(row['fail_rate'], 1)
            }
            for provider, row in top_providers.iterrows()
        ]
        
        # Problematic providers (>10% failure)
        bad_providers = provider_stats[provider_stats['fail_rate'] > 10].nlargest(5, 'fail_rate')
        bad_list = [
            {
                'name': provider,
                'success': int(row['success']),
                'total': int(row['total']),
                'fail_rate': round(row['fail_rate'], 1)
            }
            for provider, row in bad_providers.iterrows()
        ]
        
        return {
            'timestamp': last_timestamp.isoformat() if pd.notna(last_timestamp) else 'Unknown',
            'total': total,
            'successful': successful,
            'failed': failed,
            'success_rate': round(successful / total * 100, 1) if total > 0 else 0,
            'top_providers': top_list,
            'problematic_providers': bad_list
        }
    
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError, KeyError) as e:
        print(f"Error reading summary: {e}")
        return None


def get_channel_groups():
    """Get list of channel groups from CSV"""
    groups_file = 'csv/00_channel_groups.csv'
    
    if not os.path.exists(groups_file):
        return []
    
    try:
        df = pd.read_csv(groups_file)
        return [
            {'id': int(row['id']), 'name': row['name']}
            for _, row in df.iterrows()
        ]
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError, KeyError):
        return []


def get_config_info():
    """Get current configuration"""
    import yaml
    
    config_file = 'config.yaml'
    if not os.path.exists(config_file):
        return None
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        filters = config.get('filters', {})
        analysis = config.get('analysis', {})
        
        return {
            'groups': filters.get('channel_group_ids', []),
            'channel_range': f"{filters.get('start_channel', 1)}-{filters.get('end_channel', 99999)}",
            'workers': analysis.get('workers', 8),
            'duration': analysis.get('duration', 10)
        }
    except (OSError, yaml.YAMLError, ValueError, AttributeError, KeyError):
        return None


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    """API endpoint for current status"""
    current_progress = get_current_progress()
    last_run = get_last_run_summary()
    config = get_config_info()
    groups = get_channel_groups()
    
    return jsonify({
        'current_progress': current_progress,
        'last_run': last_run,
        'config': config,
        'groups': groups,
        'server_time': datetime.now().isoformat()
    })


if __name__ == '__main__':
    print("\n" + "="*70)
    print("DISPATCHARR MAID - WEB MONITOR")
    print("="*70)
    print("\nStarting web server...")
    print("Access the dashboard at: http://localhost:5000")
    print("Or from another device: http://YOUR-SERVER-IP:5000")
    print("\nPress Ctrl+C to stop")
    print("="*70 + "\n")
    
    # Run on all interfaces so it's accessible remotely
    app.run(host='0.0.0.0', port=5000, debug=False)
