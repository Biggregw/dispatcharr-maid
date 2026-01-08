#!/usr/bin/env python3
"""
interactive_maid.py
Interactive interface for Dispatcharr stream management
Formerly interactive_stream_sorter.py - now uses direct function calls
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd

from api_utils import DispatcharrAPI
from job_workspace import create_job_workspace
from stream_analysis import (
    Config,
    fetch_streams,
    analyze_streams,
    score_streams,
    reorder_streams
)


def count_channels_per_group(groups, channels):
    """Count how many channels belong to each group"""
    group_counts = {group['id']: 0 for group in groups}
    
    for channel in channels:
        group_id = channel.get('channel_group_id')
        if group_id in group_counts:
            group_counts[group_id] += 1
    
    for group in groups:
        group['channel_count'] = group_counts.get(group['id'], 0)
    
    return groups


def display_menu(groups):
    """Display groups in a numbered menu"""
    print("\n" + "="*70)
    print("DISPATCHARR CHANNEL GROUPS")
    print("="*70)
    
    for idx, group in enumerate(groups, 1):
        group_id = group.get('id')
        group_name = group.get('name', 'Unknown')
        channel_count = group.get('channel_count', 0)
        print(f"{idx:2d}. {group_name:<40} ({channel_count:3d} channels) [ID: {group_id}]")
    
    print("="*70)


def parse_selection(selection_str, max_num):
    """Parse user selection string into list of indices"""
    selection_str = selection_str.strip().lower()
    
    if selection_str == 'all':
        return list(range(1, max_num + 1))
    
    selected = set()
    
    try:
        parts = selection_str.split(',')
        for part in parts:
            part = part.strip()
            
            if '-' in part:
                start, end = part.split('-')
                start, end = int(start), int(end)
                if 1 <= start <= max_num and 1 <= end <= max_num:
                    selected.update(range(start, end + 1))
            else:
                num = int(part)
                if 1 <= num <= max_num:
                    selected.add(num)
        
        return sorted(list(selected))
    
    except ValueError:
        print("Invalid input format!")
        return []


def get_user_selection(groups):
    """Handle the interactive selection process"""
    while True:
        display_menu(groups)
        
        print("\nSelection Options:")
        print("  Enter numbers (e.g., '1,3,5')")
        print("  Enter range (e.g., '1-4')")
        print("  Enter 'all' for all groups")
        print("  Enter 'q' to quit")
        
        selection = input("\nYour selection: ").strip()
        
        if selection.lower() == 'q':
            print("Cancelled by user")
            sys.exit(0)
        
        indices = parse_selection(selection, len(groups))
        
        if not indices:
            print("No valid groups selected. Try again.")
            continue
        
        print("\n" + "="*70)
        print("SELECTED GROUPS:")
        print("="*70)
        
        selected_groups = []
        for idx in indices:
            group = groups[idx - 1]
            selected_groups.append(group)
            print(f"  {group['name']} ({group.get('channel_count', 0)} channels)")
        
        print("="*70)
        
        confirm = input("\nProceed with these groups? (y/n): ").strip().lower()
        
        if confirm == 'y':
            return selected_groups
        else:
            print("\nLet's try again...\n")


def cleanup_streams_by_provider(api, selected_group_ids):
    """Keep only the top stream from each provider for each channel"""
    print("\n" + "="*70)
    print("CLEANING UP - KEEPING TOP STREAM PER PROVIDER PER CHANNEL")
    print("="*70 + "\n")
    
    channels = api.fetch_channels()

    # Performance: fetch provider IDs for all streams once (avoid N+1 API calls)
    stream_provider_map = {}
    try:
        stream_provider_map = api.fetch_stream_provider_map()
    except Exception as e:
        print(f"Warning: could not build stream provider map; using per-stream lookups: {e}")
    
    filtered_channels = [
        ch for ch in channels 
        if ch.get('channel_group_id') in selected_group_ids
    ]
    
    print(f"Processing {len(filtered_channels)} channels in selected groups\n")
    
    cleaned_count = 0
    skipped_count = 0
    total_removed = 0
    
    for channel in filtered_channels:
        channel_id = channel['id']
        channel_name = channel['name']
        channel_number = channel.get('channel_number', 'N/A')
        stream_ids = channel.get('streams', [])
        
        if not stream_ids or len(stream_ids) <= 1:
            skipped_count += 1
            continue
        
        print(f"  Channel {channel_number}: {channel_name} ({len(stream_ids)} streams)")
        
        # Group streams by provider (prefer cached provider map; fallback only when needed)
        streams_by_provider = {}
        
        for stream_id in stream_ids:
            sid = int(stream_id)
            provider_id = stream_provider_map.get(sid)

            if provider_id is None:
                stream_details = api.fetch_stream_details(sid)
                if stream_details:
                    provider_id = stream_details.get('m3u_account')

            if provider_id is None:
                continue

            if provider_id not in streams_by_provider:
                streams_by_provider[provider_id] = []
            streams_by_provider[provider_id].append(sid)
        
        # Keep only the first (top-ranked) stream from each provider
        streams_to_keep = []
        removed_this_channel = 0
        
        for provider_id, provider_streams in streams_by_provider.items():
            if provider_streams:
                # Keep the first stream (top-ranked after reordering)
                streams_to_keep.append(provider_streams[0])
                removed_count = len(provider_streams) - 1
                
                if removed_count > 0:
                    print(f"     Provider {provider_id}: keeping stream {provider_streams[0]}, removing {removed_count}")
                    removed_this_channel += removed_count
        
        if removed_this_channel > 0:
            try:
                api.update_channel_streams(channel_id, streams_to_keep)
                cleaned_count += 1
                total_removed += removed_this_channel
                print(f"     ✓ Updated channel - removed {removed_this_channel} streams")
            except Exception as e:
                print(f"     ✗ Error: {e}")
        else:
            skipped_count += 1
            print(f"     Already optimal (1 stream per provider)")
        
        print()
    
    print("="*70)
    print(f"Cleanup complete!")
    print(f"   Cleaned: {cleaned_count} channels")
    print(f"   Skipped: {skipped_count} channels")
    print(f"   Total streams removed: {total_removed}")
    print("="*70 + "\n")


def generate_summary_report(config):
    """Generate and display analysis summary"""
    measurements_file = config.resolve_path('csv/03_iptv_stream_measurements.csv')
    
    if not os.path.exists(measurements_file):
        print("\nNo measurements file found - skipping summary")
        return
    
    try:
        df = pd.read_csv(measurements_file)
        
        total_streams = len(df)
        successful = len(df[df['status'] == 'OK'])
        failed = total_streams - successful
        
        # Provider breakdown
        df['provider'] = df['stream_url'].str.extract(r'//([^/]+)')[0]
        provider_stats = df.groupby('provider').agg({
            'status': lambda x: (x == 'OK').sum(),
            'stream_id': 'count'
        }).rename(columns={'status': 'success', 'stream_id': 'total'})
        provider_stats['fail_rate'] = (
            (provider_stats['total'] - provider_stats['success']) / provider_stats['total'] * 100
        )
        
        print("\n" + "="*70)
        print("ANALYSIS SUMMARY")
        print("="*70)
        print(f"Total Streams Analyzed: {total_streams:,}")
        print(f"Successful: {successful:,} ({successful/total_streams*100:.1f}%)")
        print(f"Failed: {failed:,} ({failed/total_streams*100:.1f}%)")
        
        print("\nTop 5 Providers by Success Count:")
        top_providers = provider_stats.nlargest(5, 'success')
        for provider, row in top_providers.iterrows():
            print(f"  {provider}: {int(row['success'])}/{int(row['total'])} "
                  f"({100-row['fail_rate']:.1f}% success)")
        
        if len(provider_stats[provider_stats['fail_rate'] > 10]) > 0:
            print("\nProblematic Providers (>10% failure rate):")
            bad_providers = provider_stats[provider_stats['fail_rate'] > 10].nlargest(5, 'fail_rate')
            for provider, row in bad_providers.iterrows():
                print(f"  {provider}: {int(row['success'])}/{int(row['total'])} "
                      f"({row['fail_rate']:.1f}% failure rate)")
        
        print("="*70 + "\n")
    
    except Exception as e:
        print(f"\nCould not generate summary: {e}")


def run_full_pipeline(api, config, selected_ids, with_cleanup=False):
    """Run the complete analysis pipeline"""
    start_time = time.time()
    
    try:
        # Fetch
        print("\n" + "="*70)
        print("STEP 1: FETCHING STREAMS")
        print("="*70)
        fetch_streams(api, config)
        
        # Analyze
        print("\n" + "="*70)
        print("STEP 2: ANALYZING STREAMS")
        print("="*70)
        print("This may take a while. Progress will be shown below.\n")
        analyze_streams(config)
        
        # Score
        print("\n" + "="*70)
        print("STEP 3: SCORING AND SORTING STREAMS")
        print("="*70)
        score_streams(api, config, update_stats=True)
        
        # Reorder
        print("\n" + "="*70)
        print("STEP 4: REORDERING STREAMS IN DISPATCHARR")
        print("="*70)
        reorder_streams(api, config)
        
        # Cleanup (optional)
        if with_cleanup:
            cleanup_streams_by_provider(api, selected_ids)
        
        # Summary
        generate_summary_report(config)
        
        # Final stats
        elapsed = time.time() - start_time
        print("\n" + "="*70)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"Total time: {elapsed/60:.1f} minutes")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main execution flow"""
    print("\n" + "="*70)
    print("DISPATCHARR SCOUT - Interactive Stream Manager")
    print("="*70)
    
    # Check for .env file
    if not Path('.env').exists():
        print("\n✗ Error: .env file not found!")
        print("Please create a .env file with your Dispatcharr credentials.")
        print("\nExample:")
        print("  DISPATCHARR_BASE_URL=http://your-server:9191")
        print("  DISPATCHARR_USER=your-username")
        print("  DISPATCHARR_PASS=your-password")
        print("  DISPATCHARR_TOKEN=")
        sys.exit(1)
    
    # Create isolated workspace
    job_id = datetime.now().strftime('cli-%Y%m%d-%H%M%S')
    workspace, config_path = create_job_workspace(job_id)

    # Load configuration
    config = Config(config_path, working_dir=workspace)
    
    # Initialize API
    print("\nConnecting to Dispatcharr...")
    try:
        api = DispatcharrAPI()
        api.login()
        print("✓ Login successful")
    except Exception as e:
        print(f"✗ Login failed: {e}")
        sys.exit(1)
    
    # Fetch channel groups
    print("\nFetching channel groups...")
    try:
        groups = api.fetch_channel_groups()
        
        if not groups:
            print("✗ No channel groups found!")
            sys.exit(1)
        
        print(f"✓ Found {len(groups)} channel group(s)")
        
    except Exception as e:
        print(f"✗ Error fetching groups: {e}")
        sys.exit(1)
    
    # Count channels per group
    print("Counting channels per group...")
    try:
        channels = api.fetch_channels()
        groups = count_channels_per_group(groups, channels)
        
        # Filter to groups with channels
        groups = [g for g in groups if g.get('channel_count', 0) > 0]
        
        if not groups:
            print("✗ No channel groups with channels found!")
            sys.exit(1)
        
        print(f"✓ Found {len(channels)} total channels across {len(groups)} active groups")
        
    except Exception as e:
        print(f"✗ Error counting channels: {e}")
        sys.exit(1)
    
    # User selects groups
    selected_groups = get_user_selection(groups)
    selected_ids = [g['id'] for g in selected_groups]
    
    # Update config with selected groups
    config.set('filters', 'channel_group_ids', selected_ids)
    config.save()
    print(f"\n✓ Configuration updated with selected groups: {selected_ids}")
    
    # Main menu
    while True:
        print("\n" + "="*70)
        print("WHAT WOULD YOU LIKE TO DO?")
        print("="*70)
        print("1. Full pipeline + cleanup (fetch → analyze → score → reorder → cleanup)")
        print("2. Full pipeline (fetch → analyze → score → reorder)")
        print("3. Fetch only")
        print("4. Analyze only")
        print("5. Score only")
        print("6. Reorder only")
        print("7. Cleanup only (keep top stream per provider)")
        print("8. Generate summary report")
        print("9. Change group selection")
        print("0. Exit")
        
        choice = input("\nEnter choice (0-9): ").strip()
        
        if choice == '0':
            print("\nGoodbye!")
            sys.exit(0)
        
        elif choice == '1':
            run_full_pipeline(api, config, selected_ids, with_cleanup=True)
        
        elif choice == '2':
            run_full_pipeline(api, config, selected_ids, with_cleanup=False)
        
        elif choice == '3':
            print("\n" + "="*70)
            print("FETCHING STREAMS")
            print("="*70)
            try:
                fetch_streams(api, config)
                print("\n✓ Fetch complete!")
            except Exception as e:
                print(f"\n✗ Fetch failed: {e}")
        
        elif choice == '4':
            print("\n" + "="*70)
            print("ANALYZING STREAMS")
            print("="*70)
            print("This may take a while. Progress will be shown below.\n")
            try:
                analyze_streams(config)
                print("\n✓ Analysis complete!")
                generate_summary_report(config)
            except Exception as e:
                print(f"\n✗ Analysis failed: {e}")
        
        elif choice == '5':
            print("\n" + "="*70)
            print("SCORING AND SORTING STREAMS")
            print("="*70)
            try:
                score_streams(api, config, update_stats=True)
                print("\n✓ Scoring complete!")
            except Exception as e:
                print(f"\n✗ Scoring failed: {e}")
        
        elif choice == '6':
            print("\n" + "="*70)
            print("REORDERING STREAMS IN DISPATCHARR")
            print("="*70)
            try:
                reorder_streams(api, config)
                print("\n✓ Reordering complete!")
            except Exception as e:
                print(f"\n✗ Reordering failed: {e}")
        
        elif choice == '7':
            cleanup_streams_by_provider(api, selected_ids)
        
        elif choice == '8':
            generate_summary_report(config)
        
        elif choice == '9':
            # Re-select groups
            selected_groups = get_user_selection(groups)
            selected_ids = [g['id'] for g in selected_groups]
            config.set('filters', 'channel_group_ids', selected_ids)
            config.save()
            print(f"\n✓ Configuration updated with new groups: {selected_ids}")
        
        else:
            print("Invalid choice. Please try again.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
