# Web Monitor Quick Start Guide

## What It Does

The web monitor provides a **live dashboard** to monitor your Dispatcharr Maid analysis progress from any browser - even your phone!

**Features:**
- 📊 Real-time progress tracking during analysis
- 📈 Last run summary with provider statistics  
- ⚙️ Current configuration display
- 🔄 Auto-refreshes every 2 seconds
- 📱 Mobile-friendly
- 🌐 Access from any device on your network

---

## Installation

The web monitor is already included! Just install Flask:

```bash
cd ~/Dispatcharr_Maid
source venv/bin/activate
pip install Flask
```

---

## How to Use

### Step 1: Start an Analysis Job

In one terminal window:

```bash
cd ~/Dispatcharr_Maid
source venv/bin/activate
python3 interactive_maid.py
```

Select your groups and start a job (option 1, 2, or 4).

### Step 2: Start the Web Monitor

In a **second terminal window** (or screen/tmux session):

```bash
cd ~/Dispatcharr_Maid
source venv/bin/activate
python3 web_monitor.py
```

You'll see:
```
======================================================================
DISPATCHARR MAID - WEB MONITOR
======================================================================

Starting web server...
Access the dashboard at: http://localhost:5000
Or from another device: http://YOUR-SERVER-IP:5000

Press Ctrl+C to stop
======================================================================
```

### Step 3: Open in Browser

**On the same server:**
```
http://localhost:5000
```

**From another device (laptop, phone, etc.):**
```
http://192.168.1.100:5000
```
*(Replace with your server's actual IP)*

---

## What You'll See

### Current Job Status
- Progress bar showing completion percentage
- Streams processed / total
- Failed count
- Real-time updates every 2 seconds

### Last Completed Run
- Total streams analyzed
- Success/failure breakdown
- Top performing providers
- Problematic providers (>10% failure rate)

### Current Configuration
- Selected channel groups
- Channel number range
- Number of workers
- Analysis duration per stream

---

## Tips

### Use Screen or Tmux
To keep both running even if you disconnect SSH:

```bash
# Install screen if needed
sudo apt install screen

# Start a screen session
screen -S dispatcharr

# Run interactive_maid.py
cd ~/Dispatcharr_Maid
source venv/bin/activate
python3 interactive_maid.py

# Detach: Press Ctrl+A then D

# Start another screen for web monitor
screen -S webmonitor

# Run web_monitor.py
cd ~/Dispatcharr_Maid
source venv/bin/activate
python3 web_monitor.py

# Detach: Press Ctrl+A then D

# Now you can close SSH - both keep running!

# To re-attach later:
screen -r dispatcharr   # Check analysis
screen -r webmonitor    # Check web monitor
```

### Access from Your Phone

1. Make sure your phone is on the same network as your server
2. Open your phone's browser
3. Go to `http://YOUR-SERVER-IP:5000`
4. Bookmark it for easy access!

### Firewall Note

If you can't access from another device, you may need to allow port 5000:

```bash
sudo ufw allow 5000/tcp
```

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'flask'"

Install Flask:
```bash
source venv/bin/activate
pip install Flask
```

### Can't Access from Another Device

1. Check server IP: `ip addr show`
2. Check firewall: `sudo ufw status`
3. Allow port: `sudo ufw allow 5000/tcp`

### Dashboard Shows "No active job running"

This is normal! Start an analysis job in `interactive_maid.py` and the dashboard will update.

### Dashboard Shows Old Data

The dashboard reads from:
- `logs/checkpoint.json` (current progress)
- `csv/03_iptv_stream_measurements.csv` (last run)

If these files are old, the dashboard will show old data.

---

## Screenshots

### During Analysis
```
📊 Current Job Status
⚡ RUNNING
[████████████████░░░░] 72.3%
Processed: 3,912 | Total: 5,432 | Failed: 87
```

### Last Run Summary
```
📈 Last Completed Run
✓ COMPLETED
Total: 5,432 | Successful: 5,187 (95.5%) | Failed: 245

Top Providers:
provider1.example.com: 1,230/1,234 (99.7% success)
provider2.example.com: 980/987 (99.3% success)

⚠️ Problematic Providers (>10% failure):
badprovider.example.com: 123/456 (73.0% failure)
```

---

## Security Note

The web monitor has **no authentication**. It's designed for:
- Home networks (trusted users only)
- Read-only monitoring (can't start/stop jobs)

**Don't expose port 5000 to the internet!**

If you need to access remotely, use SSH tunneling:

```bash
# On your laptop (not the server)
ssh -L 5000:localhost:5000 root@your-server-ip

# Then open in browser:
http://localhost:5000
```

---

## Stopping the Web Monitor

Press `Ctrl+C` in the terminal where web_monitor.py is running.

Or if using screen:
```bash
screen -r webmonitor
# Press Ctrl+C
```

---

Enjoy your new dashboard! 🎉
