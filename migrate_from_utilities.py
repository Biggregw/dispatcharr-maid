#!/usr/bin/env python3
"""
migrate_from_utilities.py
Helper script to migrate from Dispatcharr-Utilities to Dispatcharr-Maid
"""

import os
import shutil
from pathlib import Path


def migrate():
    """Migrate configuration and data from old setup"""
    
    print("="*70)
    print("MIGRATION HELPER: Dispatcharr-Utilities → Dispatcharr-Maid")
    print("="*70 + "\n")
    
    old_path = Path('../Dispatcharr-Utilities')
    
    if not old_path.exists():
        print("❌ Could not find Dispatcharr-Utilities folder at:", old_path.absolute())
        print("\nPlease ensure this script is run from the Dispatcharr_Maid folder")
        print("and that Dispatcharr-Utilities is in the parent directory.")
        return False
    
    print(f"✓ Found old installation at: {old_path.absolute()}\n")
    
    # Migrate .env file
    old_env = old_path / '.env'
    new_env = Path('.env')
    
    if old_env.exists() and not new_env.exists():
        print("Migrating .env file...")
        shutil.copy2(old_env, new_env)
        print(f"  ✓ Copied .env")
    elif new_env.exists():
        print("⚠ .env already exists - skipping (not overwriting)")
    else:
        print("⚠ No .env file found in old installation")
    
    # Migrate CSV data
    old_csv = old_path / 'csv'
    new_csv = Path('csv')
    
    if old_csv.exists():
        print("\nMigrating CSV data...")
        new_csv.mkdir(exist_ok=True)
        
        csv_files = [
            '00_channel_groups.csv',
            '01_channels_metadata.csv',
            '02_grouped_channel_streams.csv',
            '03_iptv_stream_measurements.csv',
            '04_fails.csv',
            '05_iptv_streams_scored_sorted.csv'
        ]
        
        copied = 0
        for csv_file in csv_files:
            old_file = old_csv / csv_file
            new_file = new_csv / csv_file
            
            if old_file.exists():
                shutil.copy2(old_file, new_file)
                size = old_file.stat().st_size / 1024  # KB
                print(f"  ✓ Copied {csv_file} ({size:.1f} KB)")
                copied += 1
        
        if copied > 0:
            print(f"\n  Total: Copied {copied} CSV files")
        else:
            print("  No CSV files found to migrate")
    
    # Convert config.ini to config.yaml
    old_config = old_path / 'config.ini'
    new_config = Path('config.yaml')
    
    if old_config.exists() and not new_config.exists():
        print("\nConverting config.ini to config.yaml...")
        try:
            import configparser
            import yaml
            
            parser = configparser.ConfigParser()
            parser.read(old_config)
            
            # Extract settings
            settings = parser['script_settings']
            
            # Build new config
            new_config_data = {
                'analysis': {
                    'duration': 10,
                    'idet_frames': 500,
                    'timeout': 30,
                    'workers': 8,
                    'retries': 1,
                    'retry_delay': 10
                },
                'scoring': {
                    'fps_bonus_points': int(settings.get('fps_bonus_points', 100)),
                    'hevc_boost': float(settings.get('hevc_boost', 1.5)),
                    'resolution_scores': {
                        '3840x2160': 100,
                        '1920x1080': 80,
                        '1280x720': 50,
                        '960x540': 20
                    }
                },
                'filters': {
                    'channel_group_ids': [
                        int(x.strip()) for x in settings.get('channel_group_ids', '').split(',')
                        if x.strip().isdigit()
                    ],
                    'start_channel': int(settings.get('start_channel', 1)),
                    'end_channel': int(settings.get('end_channel', 99999)),
                    'stream_last_measured_days': int(settings.get('stream_last_measured_days', 1)),
                    'remove_duplicates': settings.getboolean('remove_duplicates', True)
                }
            }
            
            with open(new_config, 'w') as f:
                yaml.dump(new_config_data, f, default_flow_style=False)
            
            print(f"  ✓ Converted config.ini → config.yaml")
            print(f"     Channel groups: {new_config_data['filters']['channel_group_ids']}")
            print(f"     Channel range: {new_config_data['filters']['start_channel']}-{new_config_data['filters']['end_channel']}")
        
        except Exception as e:
            print(f"  ✗ Could not convert config.ini: {e}")
            print("    You can manually configure config.yaml")
    
    elif new_config.exists():
        print("\n⚠ config.yaml already exists - skipping conversion")
    
    # Summary
    print("\n" + "="*70)
    print("MIGRATION SUMMARY")
    print("="*70)
    print("\n✓ Migration complete!")
    print("\nWhat was migrated:")
    print("  • .env file (credentials)")
    print("  • CSV data files (analysis results)")
    print("  • Configuration settings")
    
    print("\nYou can now:")
    print("  1. Delete the old Dispatcharr-Utilities folder (optional)")
    print("  2. Run: python3 interactive_maid.py")
    
    print("\nNote: The old Dispatcharr-Utilities folder is still intact.")
    print("      Only copies were made - nothing was deleted.")
    print("="*70 + "\n")
    
    return True


if __name__ == '__main__':
    try:
        import yaml
        migrate()
    except ImportError:
        print("Error: PyYAML is required for migration")
        print("Install it with: pip install PyYAML")
    except Exception as e:
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
