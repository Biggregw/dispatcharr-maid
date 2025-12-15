#!/usr/bin/env python3
"""Test channel matching against real Dispatcharr data"""
import re
from api_utils import DispatcharrAPI

def normalize(text):
    """Remove spaces and lowercase"""
    return text.replace(" ", "").lower()

def strip_quality(text):
    """Remove quality indicators"""
    quality_terms = ['hd', 'sd', 'fhd', '4k', 'uhd', 'hevc', 'h264', 'h265']
    result = text
    for term in quality_terms:
        result = re.sub(rf'\b{term}\b', '', result, flags=re.IGNORECASE)
    return result.strip()

def matches_channel(selected_channel, stream_name, regional_filter=None, exclude_filter=None):
    """Check if stream matches selected channel"""
    selected_base = strip_quality(selected_channel)
    selected_normalized = normalize(selected_base)
    stream_normalized = normalize(stream_name)
    
    if selected_normalized not in stream_normalized:
        return False, "no substring match"
    
    selected_has_timeshift = re.search(r'\+\d', selected_channel)
    stream_has_timeshift = re.search(r'\+\d', stream_name)
    
    if selected_has_timeshift and not stream_has_timeshift:
        return False, "timeshift mismatch"
    if not selected_has_timeshift and stream_has_timeshift:
        return False, "timeshift mismatch"
    
    # Apply regional INCLUDE filter if provided
    if regional_filter:
        wildcards = [w.strip() for w in regional_filter.split(',')]
        stream_lower = stream_name.lower()
        
        matched = False
        for wildcard in wildcards:
            pattern = wildcard.replace('*', '.*')
            if re.search(pattern, stream_lower):
                matched = True
                break
        
        if not matched:
            return False, "no regional match"
    
    # Apply EXCLUDE filter if provided
    if exclude_filter:
        excludes = [e.strip() for e in exclude_filter.split(',')]
        stream_lower = stream_name.lower()
        
        for exclude in excludes:
            pattern = exclude.replace('*', '.*')
            if re.search(pattern, stream_lower):
                return False, f"excluded by '{exclude}'"
    
    return True, "✓"

# Connect to Dispatcharr
print("Connecting to Dispatcharr...")
api = DispatcharrAPI()

# Get user input
print("\n" + "="*70)
channel_input = input("Enter channel name (e.g., 'ITV', 'BBC One'): ").strip()
regional_input = input("Include filter (e.g., 'york*' or 'york*,lond*', blank for all): ").strip()
exclude_input = input("Exclude filter (e.g., 'lincoln*', blank for none): ").strip()
print("="*70)

# Fetch ALL streams (paginated)
print("\nFetching all streams from Dispatcharr...")
all_streams = []
page = 1
next_url = '/api/channels/streams/?limit=100'

while next_url:
    result = api.get(next_url)
    if not result or 'results' not in result:
        break
    
    all_streams.extend(result['results'])
    print(f"  Fetched page {page}: {len(all_streams)} total streams so far...")
    
    if result.get('next'):
        next_url = result['next'].split('/api/')[-1]
        next_url = '/api/' + next_url
    else:
        next_url = None
    
    page += 1

print(f"✓ Loaded {len(all_streams)} total streams")

# Test matching
print(f"\nSearching for matches to: '{channel_input}'")
if regional_input:
    print(f"Include filter: '{regional_input}'")
if exclude_input:
    print(f"Exclude filter: '{exclude_input}'")

print("\n" + "="*70)
print("MATCHING STREAMS:")
print("="*70)

matches = []
for stream in all_streams:
    stream_name = stream.get('name', '')
    stream_id = stream.get('id')
    m3u_account = stream.get('m3u_account')
    
    result, reason = matches_channel(
        channel_input, 
        stream_name, 
        regional_input or None,
        exclude_input or None
    )
    
    if result:
        matches.append({
            'id': stream_id,
            'name': stream_name,
            'provider': m3u_account
        })

# Display results
if matches:
    # Group by provider
    by_provider = {}
    for m in matches:
        provider = m['provider'] or 'Unknown'
        if provider not in by_provider:
            by_provider[provider] = []
        by_provider[provider].append(m)
    
    print(f"\nFound {len(matches)} matching streams across {len(by_provider)} providers:\n")
    
    for provider_id in sorted(by_provider.keys()):
        streams = by_provider[provider_id]
        print(f"Provider {provider_id}: ({len(streams)} streams)")
        for s in streams[:5]:
            print(f"  • [{s['id']}] {s['name']}")
        if len(streams) > 5:
            print(f"  ... and {len(streams) - 5} more")
        print()
else:
    print("\n❌ No matching streams found")

print("="*70)
print(f"Total matches: {len(matches)}")
