#!/usr/bin/env python3
"""Remove unwanted BBC One streams from a channel"""
from api_utils import DispatcharrAPI

api = DispatcharrAPI()

# Get all channels
channels = api.fetch_channels()

# Find BBC One channels
bbc_channels = [c for c in channels if 'BBC One' in c.get('name', '')]

print("BBC One channels found:")
for i, ch in enumerate(bbc_channels, 1):
    streams = api.fetch_channel_streams(ch['id'])
    stream_count = len(streams) if streams else 0
    print(f"{i}. [{ch['id']}] {ch['name']} - {stream_count} streams")

print("\nWhich channel do you want to clean up? (enter number): ", end='')
choice = int(input().strip())

if choice < 1 or choice > len(bbc_channels):
    print("Invalid choice")
    exit(1)

selected = bbc_channels[choice - 1]
channel_id = selected['id']
channel_name = selected['name']

print(f"\nSelected: {channel_name}")

# Get current streams
current_streams = api.fetch_channel_streams(channel_id)
print(f"Current stream count: {len(current_streams)}")

# Ask what to keep
print("\nWhat do you want to KEEP? Options:")
print("1. Keep only streams matching a pattern (e.g., 'yorkshire')")
print("2. Keep only top N streams by provider")
print("3. Remove ALL streams and start fresh")
print("\nChoice: ", end='')
keep_choice = int(input().strip())

if keep_choice == 1:
    pattern = input("Enter pattern to keep (e.g., 'yorkshire', 'london'): ").strip().lower()
    filtered = [s for s in current_streams if pattern in s['name'].lower()]
    print(f"\nFound {len(filtered)} streams matching '{pattern}'")
    
elif keep_choice == 2:
    n = int(input("How many per provider? "))
    # Group by provider
    by_provider = {}
    for s in current_streams:
        provider = s.get('m3u_account')
        if provider not in by_provider:
            by_provider[provider] = []
        by_provider[provider].append(s)
    
    # Keep top N from each
    filtered = []
    for provider, streams in by_provider.items():
        filtered.extend(streams[:n])
    print(f"\nKeeping {len(filtered)} streams (top {n} per provider)")
    
elif keep_choice == 3:
    filtered = []
    print("\nRemoving ALL streams")
else:
    print("Invalid choice")
    exit(1)

# Show what will be kept
print(f"\nStreams to KEEP: {len(filtered)}")
print(f"Streams to REMOVE: {len(current_streams) - len(filtered)}")

confirm = input("\nProceed? (yes/no): ").strip().lower()
if confirm != 'yes':
    print("Cancelled")
    exit(0)

# Update channel with filtered streams
filtered_ids = [s['id'] for s in filtered]
api.update_channel_streams(channel_id, filtered_ids)

print(f"\n✅ Done! Channel now has {len(filtered)} streams")
