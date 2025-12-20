#!/usr/bin/env python3
"""Update provider names with real names from Dispatcharr"""
import pandas as pd
from api_utils import DispatcharrAPI

# Initialize API
api = DispatcharrAPI()

# Fetch M3U accounts properly
print("Fetching M3U accounts...")
try:
    provider_map = api.fetch_m3u_account_map()
    print(f"Found {len(provider_map)} providers:")
    for id, name in provider_map.items():
        print(f"  ID {id}: {name}")
    
except Exception as e:
    print(f"API failed: {e}")
    print("Using manual mapping...")
    # Manual mapping based on your screenshots
    # We'll need to figure out which ID is which
    provider_map = {}

# Read CSV
df = pd.read_csv('csv/03_iptv_stream_measurements.csv')

# Update names
if provider_map:
    df['m3u_account_name'] = df['m3u_account'].map(provider_map)
    df.to_csv('csv/03_iptv_stream_measurements.csv', index=False)
    print("\n✅ Updated provider names!")
    print("\nProvider breakdown:")
    for name, count in df['m3u_account_name'].value_counts().items():
        print(f"   {name}: {count} streams")
else:
    print("❌ Could not fetch provider names from API")
    print("   Please check Dispatcharr API access")
