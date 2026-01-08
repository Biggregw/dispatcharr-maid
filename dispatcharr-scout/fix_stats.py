import pandas as pd

# Test with actual CSV
df = pd.read_csv('csv/03_iptv_stream_measurements.csv')

print("CSV Columns:", df.columns.tolist())
print("\nChannel stats calculation:")

# Calculate channel stats with existing columns
channel_stats = {}
if 'channel_number' in df.columns:
    for channel_num in df['channel_number'].unique():
        if pd.isna(channel_num):
            continue
        channel_df = df[df['channel_number'] == channel_num]
        
        # Get channel name from stream_name (first occurrence)
        channel_name = "Unknown"
        if 'stream_name' in df.columns:
            first_stream = channel_df['stream_name'].iloc[0] if len(channel_df) > 0 else "Unknown"
            channel_name = first_stream
        
        channel_total = len(channel_df)
        channel_success = len(channel_df[channel_df['status'] == 'OK'])
        
        channel_stats[str(int(channel_num))] = {
            'name': channel_name,
            'total': channel_total,
            'successful': channel_success,
            'failed': channel_total - channel_success,
            'success_rate': round(channel_success / channel_total * 100, 1) if channel_total > 0 else 0
        }

print(f"Found {len(channel_stats)} channels")
for num, stats in list(channel_stats.items())[:3]:
    print(f"  Channel {num}: {stats}")
