# Dispatcharr Maid - Interactive Web Application Guide

## 🎉 What This Is

A **complete web-based control panel** for Dispatcharr Maid. No CLI needed - do everything from your browser!

### Key Features
✅ **Select channel groups** with checkboxes  
✅ **Select specific channels** within those groups  
✅ **One-click job execution** - Full pipeline, analyze only, etc.  
✅ **Live progress tracking** - Real-time updates with progress bars  
✅ **Job management** - Run and monitor jobs  
✅ **Job history** - See all past runs  
✅ **Mobile-friendly** - Access from phone/tablet  
✅ **No CLI required** - Everything in the browser!

---

## 🚀 Quick Start

### Step 1: Deploy with Docker

```bash
cd ~/dispatcharr-maid

# Setup credentials
cp .env.template .env
nano .env
# Set: DISPATCHARR_BASE_URL=http://dispatcharr:9191
# Add your username and password

# Build and start
docker-compose up -d

# Check it's running
docker-compose ps
```

### Step 2: Access Web App

Open in browser:
```
http://YOUR-SERVER-IP:5000
```

### Step 3: Use the Application

1. **Select Groups** → Click checkboxes for groups you want
2. **Select Channels** → Optionally refine to specific channels
3. **Run Jobs** → Click action button (Full Pipeline, Analyze Only, etc.)
4. **Monitor Progress** → Watch real-time updates
5. **View History** → See all past job results

That's it! 🎊

---

## 📱 User Interface Guide

### Main Workflow

```
┌─────────────────────────────────────────────────────────┐
│  Step 1: Select Groups                                 │
├─────────────────────────────────────────────────────────┤
│  ☑ UK TV (45 channels)                                 │
│  ☑ US TV (78 channels)                                 │
│  ☐ Sports (23 channels)                                │
│                                                         │
│  [Next: Select Channels →]                             │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Step 2: Select Channels (Optional)                    │
├─────────────────────────────────────────────────────────┤
│  🔍 Search: "BBC"                                       │
│                                                         │
│  UK TV                                                  │
│    ☑ 001 - BBC One                                     │
│    ☑ 002 - BBC Two                                     │
│    ☐ 003 - ITV                                         │
│    [Select All UK TV] [Clear]                          │
│                                                         │
│  US TV                                                  │
│    ☐ 101 - ABC                                         │
│    ☑ 102 - CBS                                         │
│    [Select All US TV] [Clear]                          │
│                                                         │
│  3 channels selected                                   │
│  [← Back] [Next: Run Jobs →]                           │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Step 3: Run Jobs                                       │
├─────────────────────────────────────────────────────────┤
│  Groups: UK TV, US TV                                  │
│  Channels: 3 specific channels                         │
│                                                         │
│  [🚀 Full Pipeline + Cleanup]  [📊 Full Pipeline]      │
│  [📥 Fetch Only]  [🔍 Analyze Only]  [⭐ Score Only]   │
│  [🔄 Reorder Only]  [🧹 Cleanup Only]                  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  📊 Current Job                                         │
├─────────────────────────────────────────────────────────┤
│  Status: RUNNING                                        │
│  Analyzing streams...                                   │
│                                                         │
│  [███████████░░░░░] 72.3%                              │
│                                                         │
│  Processed: 218   Total: 302   Failed: 5              │
│                                                         │
│  [❌ Cancel Job]                                        │
└─────────────────────────────────────────────────────────┘
```

---

## 🎯 Features in Detail

### 1. Group Selection

**What it does:**
- Shows all your channel groups from Dispatcharr
- Displays channel count for each group
- Click to select/deselect

**How to use:**
1. Click checkboxes to select groups
2. Click "Next" when ready
3. You can select multiple groups

**Example:**
```
☑ UK TV (45 channels)
☑ Sports Premium (23 channels)
☐ Movies (156 channels)
```

---

### 2. Channel Selection (Optional)

**What it does:**
- Shows all channels in your selected groups
- Lets you pick specific channels to analyze
- Search/filter by name or number
- Select all or clear per group

**When to use:**
- **Testing:** Select just 5-10 channels to test first
- **Targeted:** Only analyze problematic channels
- **Fast:** Skip channels you know are fine

**When to skip:**
- Want to analyze ALL channels in selected groups
- First time running - analyze everything

**How to use:**
1. **Search:** Type "BBC" to filter only BBC channels
2. **Select:** Check the channels you want
3. **Bulk actions:** "Select All UK TV" or "Clear"
4. **Skip:** Just click "Next" without selecting anything = all channels

**Example workflow:**
```
Search: "Sky Sports"
→ Shows only Sky Sports channels
→ Click "Select All UK TV"
→ Selected: 15 Sky Sports channels

OR

→ Click "Next" without selecting anything
→ All 68 channels will be analyzed
```

---

### 3. Job Actions

**Available jobs:**

| Button | What It Does | When to Use |
|--------|--------------|-------------|
| 🚀 **Full Pipeline + Cleanup** | Fetch → Analyze → Score → Reorder → Remove duplicates | First run or weekly maintenance |
| 📊 **Full Pipeline** | Fetch → Analyze → Score → Reorder | Regular updates |
| 📥 **Fetch Only** | Download stream metadata | Quick refresh |
| 🔍 **Analyze Only** | Test stream quality with ffmpeg | Added new channels |
| ⭐ **Score Only** | Recalculate scores | Changed scoring settings |
| 🔄 **Reorder Only** | Update stream order in Dispatcharr | After scoring |
| 🧹 **Cleanup Only** | Keep best stream per provider | Remove duplicates |

**Recommended:**
- **First time:** Full Pipeline + Cleanup
- **Weekly:** Full Pipeline
- **Quick test:** Analyze Only on 5-10 channels

---

### 4. Live Progress Tracking

**During analysis, you'll see:**
```
Status: RUNNING
Analyzing streams...

[██████████████░░░░░░] 68.5%

Processed: 3,427    Total: 5,000
Failed: 42          Remaining: 1,573

[❌ Cancel Job]
```

**Features:**
- Real-time updates every 2 seconds
- Progress bar
- Processed/total/failed counts
- Cancel button (if needed)

**After completion:**
```
Status: COMPLETED
Completed

Total Analyzed: 5,000
Successful: 4,823 (96.5%)
Failed: 177 (3.5%)
```

---

### 5. Job History

**What you see:**
```
Dec 13, 14:30 - Full Pipeline ✓ (95.5% success)
Dec 12, 09:15 - Analyze Only ✓ (97.2% success)
Dec 11, 22:10 - Full Pipeline ✗ (Cancelled)
```

**Features:**
- Last 50 jobs stored
- Shows job type, time, status, success rate
- Persists across container restarts

---

## 🎓 Usage Examples

### Example 1: First Time Setup

```
1. Open: http://YOUR-SERVER-IP:5000

2. Step 1: Select all your groups
   ☑ UK TV
   ☑ US TV  
   ☑ Sports
   [Next →]

3. Step 2: Skip channel selection
   [Next →]

4. Step 3: Run full pipeline
   Click: [🚀 Full Pipeline + Cleanup]

5. Watch progress
   Wait 30-60 minutes
   See completion summary
```

---

### Example 2: Test on Subset First

```
1. Step 1: Select one group
   ☑ UK TV
   [Next →]

2. Step 2: Select specific channels
   Search: "BBC"
   ☑ BBC One
   ☑ BBC Two
   ☑ BBC News
   [Next →]

3. Step 3: Analyze only
   Click: [🔍 Analyze Only]

4. Check results
   If good → Run on all channels
   If issues → Troubleshoot first
```

---

### Example 3: Weekly Maintenance

```
1. Step 1: Select groups
   ☑ All your groups
   [Next →]

2. Step 2: Skip
   [Next →]

3. Step 3: Full pipeline
   Click: [📊 Full Pipeline]

4. Monitor from phone
   Open on mobile while doing other things
   Check occasionally
```

---

### Example 4: Fix Specific Channels

```
1. Step 1: Select group with issues
   ☑ Sports Premium
   [Next →]

2. Step 2: Select problem channels
   Search: "Sky Sports"
   ☑ Sky Sports F1
   ☑ Sky Sports Premier League
   [Next →]

3. Step 3: Re-analyze
   Click: [🔍 Analyze Only]

4. Then re-score and reorder
   Click: [⭐ Score Only]
   Click: [🔄 Reorder Only]
```

---

## 🐳 Docker Management

### Start/Stop

```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# Restart
docker-compose restart

# View logs
docker-compose logs -f dispatcharr-maid-web
```

### Update

```bash
# Get new version
cd ~/dispatcharr-maid
# Extract new Dispatcharr_Maid_Docker.zip

# Rebuild
docker-compose build

# Restart
docker-compose up -d
```

### Check Status

```bash
# Is it running?
docker-compose ps

# Resource usage
docker stats dispatcharr-maid-web

# View logs
docker logs -f dispatcharr-maid-web
```

---

## 🌐 Access Options

### Local Network

```
http://YOUR-SERVER-IP:5000
```

### Via Nginx Proxy Manager (HTTPS)

1. Open NPM: `http://YOUR-SERVER-IP:81`
2. Add Proxy Host:
   - Domain: `maid.yourdomain.com`
   - Forward to: `dispatcharr-maid-web`
   - Port: `5000`
3. Enable SSL

Access: `https://maid.yourdomain.com`

### Via Portainer

1. Open Portainer: `http://YOUR-SERVER-IP:9000`
2. Containers → `dispatcharr-maid-web`
3. View logs, restart, check resources

---

## 🔧 Configuration

### Change Port

Edit `docker-compose.yml`:

```yaml
ports:
  - "8080:5000"  # Use port 8080 instead
```

Then: `docker-compose up -d`

### Adjust Analysis Settings

The web app uses `config.yaml` for settings:

```yaml
analysis:
  duration: 10      # Seconds per stream
  workers: 8        # Concurrent threads
  retries: 1        # Retry failed streams

scoring:
  fps_bonus_points: 100
  hevc_boost: 1.5
```

After editing: `docker-compose restart`

---

## 📱 Mobile Usage

### Bookmark on Phone

1. Open `http://YOUR-SERVER-IP:5000` on phone
2. Tap Share → Add to Home Screen
3. Now it's an app icon!

### Mobile Workflow

1. Start job from desktop
2. Check progress on phone while elsewhere
3. Get notification (in history) when done
4. Review results from phone

---

## 🐛 Troubleshooting

### Can't Access Web App

**Check container:**
```bash
docker-compose ps
# Should show: dispatcharr-maid-web   Up
```

**Check port:**
```bash
sudo netstat -tlnp | grep 5000
# Should show something listening
```

**Check firewall:**
```bash
sudo ufw allow 5000/tcp
```

**View logs:**
```bash
docker-compose logs dispatcharr-maid-web
```

---

### "Failed to load groups"

**Check .env file:**
```bash
docker exec dispatcharr-maid-web cat .env
```

Should show:
```
DISPATCHARR_BASE_URL=http://dispatcharr:9191
DISPATCHARR_USER=your-username
DISPATCHARR_PASS=your-password
```

**Test connection:**
```bash
docker exec dispatcharr-maid-web ping -c 3 dispatcharr
```

---

### Job Stuck in "Running"

**Check logs:**
```bash
docker-compose logs -f dispatcharr-maid-web
```

**Cancel and retry:**
- Click "Cancel Job" button
- Wait for cancellation
- Start new job

**Restart container:**
```bash
docker-compose restart dispatcharr-maid-web
```

---

### Progress Not Updating

**Refresh browser:**
- Hard refresh: Ctrl+Shift+R (Windows) or Cmd+Shift+R (Mac)

**Check connection:**
- Make sure you're on same network
- Check firewall isn't blocking port 5000

**Check logs:**
```bash
docker-compose logs dispatcharr-maid-web
```

---

## 💾 Data Management

### Where Data is Stored

| Data | Location | Purpose |
|------|----------|---------|
| CSV files | `./csv/` | Analysis results |
| Logs | `./logs/` | Progress logs, checkpoints |
| Job history | `./logs/job_history.json` | Past jobs |
| Config | `./config.yaml` | Settings |
| Credentials | `./.env` | API access |

### Backup

```bash
cd ~/dispatcharr-maid

# Backup everything
tar -czf backup-$(date +%Y%m%d).tar.gz csv/ logs/ config.yaml .env

# Backup just results
tar -czf csv-backup-$(date +%Y%m%d).tar.gz csv/
```

### Clear Old Data

```bash
# Clear CSV files (will regenerate)
rm -rf csv/*

# Clear logs
rm -rf logs/*

# Clear job history
rm logs/job_history.json
```

---

## 🎯 Best Practices

### First Run
1. Test on 5-10 channels first
2. Check results look good
3. Then run on all channels

### Regular Maintenance
1. Run weekly: Full Pipeline
2. Check job history for failures
3. Investigate failed providers

### Troubleshooting
1. Run Analyze Only on problem channels
2. Check logs for specific errors
3. Adjust config if needed

### Performance
- **Few channels:** Use 8-16 workers
- **Many channels:** Use 4-8 workers (prevents overwhelming)
- **Slow streams:** Increase timeout to 45-60 seconds

---

## ⚡ Advanced Features

### Cancel Long-Running Jobs

Click "Cancel Job" button - job will finish current stream then stop.

### Search Channels

Use search box to filter:
- By name: "BBC", "Sky", "Sport"
- By number: "001", "10", "200"

### Bulk Selection

- "Select All UK TV" - Selects all channels in that group
- "Clear" - Deselects all in that group

### Job History Tracking

- Last 50 jobs saved
- Shows success rates
- Helps identify patterns

---

## 🔒 Security Notes

**No Authentication:**
- Designed for home network use
- Don't expose port 5000 to internet

**If You Need Remote Access:**
Use SSH tunnel:
```bash
ssh -L 5000:localhost:5000 root@your-server
```

Then open: `http://localhost:5000`

**Or Use Nginx Proxy Manager:**
- Add authentication in NPM
- Use HTTPS with SSL cert
- More secure than SSH tunnel

---

## 🎊 Summary

### What You Can Do Now

✅ **Access from anywhere** - Browser on desktop, phone, tablet  
✅ **Select groups** - Click checkboxes  
✅ **Refine to channels** - Pick specific channels or use all  
✅ **Run jobs** - One click to start  
✅ **Monitor progress** - Real-time updates  
✅ **Track history** - See all past runs  
✅ **No CLI** - Everything in browser  

### Quick Commands

```bash
# Start: docker-compose up -d
# Access: http://YOUR-SERVER-IP:5000
# Stop: docker-compose down
# Logs: docker-compose logs -f
# Update: docker-compose build && docker-compose up -d
```

---

Enjoy your new web-based control panel! 🚀
