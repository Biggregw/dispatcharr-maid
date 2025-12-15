#!/usr/bin/env python3
"""Fetch provider names - try multiple API endpoints"""
import pandas as pd
from api_utils import DispatcharrAPI

api = DispatcharrAPI()
df = pd.read_csv('csv/03_iptv_stream_measurements.csv')

unique_provider_ids = df['m3u_account'].dropna().unique()
print(f"Fetching names for {len(unique_provider_ids)} providers...\n")

provider_map = {}
for provider_id in unique_provider_ids:
    provider_id = int(provider_id)
    
    # Try different endpoint patterns
    endpoints = [
        f'/api/m3u-accounts/{provider_id}/',
        f'/m3u-accounts/{provider_id}/',
        f'/api/m3u/{provider_id}/',
    ]
    
    found = False
    for endpoint in endpoints:
        try:
            account = api.get(endpoint)
            if account and isinstance(account, dict):
                name = account.get('name') or account.get('title') or f"Provider {provider_id}"
                provider_map[provider_id] = name
                print(f"✓ ID {provider_id}: {name} (from {endpoint})")
                found = True
                break
        except:
            continue
    
    if not found:
        provider_map[provider_id] = f"Provider {provider_id}"
        print(f"✗ ID {provider_id}: Could not fetch name")

df['m3u_account_name'] = df['m3u_account'].map(provider_map)
df.to_csv('csv/03_iptv_stream_measurements.csv', index=False)

print(f"\n✅ Updated!")
print(f"\nProvider breakdown:")
for name, count in df['m3u_account_name'].value_counts().items():
    print(f"   {name}: {count} streams")
