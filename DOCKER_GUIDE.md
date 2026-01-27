# Dispatcharr Maid - Docker Deployment Guide

## 🐳 What This Does

Runs Dispatcharr Maid in Docker containers that:
- ✅ **Connect to your existing Dispatcharr** on the same network
- ✅ **Persist data** (CSV files, logs, config)
- ✅ **Web UI always available** on port 5000 (control panel + results)
- ✅ **Optional CLI on-demand** (exec into the same container if you prefer terminal workflows)
- ✅ **Auto-restart** with your server
- ✅ **Managed via Portainer** (optional)

---

## 📋 Prerequisites

- Docker and docker-compose installed ✅ 
- Dispatcharr running ✅ 
- Your Dispatcharr credentials

---

## 🚀 Quick Start

### Step 1: Setup Files

```bash
# Create directory
mkdir -p ~/dispatcharr-maid
cd ~/dispatcharr-maid

# Download the latest Dispatcharr-Maid release zip from GitHub
wget https://github.com/Biggregw/dispatcharr-maid/archive/refs/heads/main.zip -O Dispatcharr_Maid.zip

# Extract the zip file
unzip Dispatcharr_Maid.zip

# Move the files to the directory
mv dispatcharr-maid-main/* .
rm -rf dispatcharr-maid-main
ls
```

#### You should now see files such as:
- `Dockerfile`
- `docker-compose.yml`
- `api_utils.py`
- `stream_analysis.py`
- `interactive_maid.py`
- `web_monitor.py`
- `templates/`
- `config.yaml.example`
- `.env.example`
- `requirements.txt`

---

### Step 2: Configure Credentials

Create and edit the `.env` file with your Dispatcharr credentials:

```bash
cp .env.example .env
nano .env
```

Create your local `config.yaml` (required):

```bash
cp config.yaml.example config.yaml
nano config.yaml
```

**Important:** The `.env` file should look like this:

```ini
# Since we're on the same Docker network, use container name!
DISPATCHARR_BASE_URL=http://dispatcharr:9191
DISPATCHARR_USER=admin
DISPATCHARR_PASS=your_actual_password
DISPATCHARR_TOKEN=
```

**Key points:**
- Use `http://dispatcharr:9191` (the container name), not `localhost` or an IP!
- Dispatcharr-Maid must run on the same Docker network as Dispatcharr
- The provided `docker-compose.yml` attaches the service to the existing `dispatcharr_default` network, which must already exist

---

### Step 3: Build and Start

```bash
# Build the containers
docker-compose build

# Start everything
docker-compose up -d

# Note: You may see warnings about unset variables - this is a known issue and safe to ignore
# WARNING: The xxxxxxxxx variable is not set. Defaulting to a blank string.
# Containers will start successfully

# Check they're running
docker-compose ps
```

You should see:
```
NAME                    STATUS              PORTS
dispatcharr-maid-web    Up (healthy)        0.0.0.0:5000->5000/tcp
```

---

### Step 4: Access Web UI

Open in your browser:
```
http://YOUR-SERVER-IP:5000
```

You should see the Dispatcharr Maid dashboard!

---

## 🎯 How to Use

### Run Analysis Jobs

**Recommendation:** Select one Group and one Channel to work on at a time for optimal results.

#### 1. Navigate to the Dashboard
Open `http://YOUR-SERVER-IP:5000` in your browser.

#### 2. Select Your Groups
A check has been made to list all Groups that exist in channels within Dispatcharr (not all groups from your provider).

- You will be presented with each of these groups and can select single or multiple groups
- **Recommended:** Select a single group to make best use of functionality
- The group(s) you select will be examined in the next step

#### 3. Click "Select Channels"
All channels that exist in the selected groups will now be displayed.

- Select the channel(s) you want (e.g., "BBC One")
- **Recommended:** Select a single channel at a time

#### 4. Click "Run Jobs"
If running for a single channel (e.g., "BBC One"), you will see the matching chain at the top of the screen.

**Selection & Matching:**
- **Primary Match**: the exact search sent to providers (editable)
- **Include / Exclude filters**: `*` wildcards supported
- **Exclude +1 channels**
- **Advanced Regex (optional)** with a **regex-only** toggle if you want regex to be the only rule
- **No server-side capability testing:** Dispatcharr-Maid assumes client-side decode (e.g., Firestick) and proxied playback without transcoding; FFmpeg capability checks were removed.

Providers are not treated as failure domains; diversity comes from the tiered ordering policy rather than round robin.

#### 5. Click "Refresh Channel Streams"
- A box will appear showing matching streams as they're queried
- Allow time for the search to complete and present results
- A preview box will display where you can unselect any streams you don't want added if your filtering wasn't quite right

#### 6. Add Selected Streams
Leave the streams you want selected and click **"Add Selected Streams"**

- The "Current Job" section will display a message indicating it's searching for streams
- Wait for results to show how many matching streams were found
- At this point, all selected streams have been added to your channel in Dispatcharr

#### 7. Click "Quality Check (Apply Changes)" (Optional but Recommended)
This performs a full probe of each stream and scores them, then **orders** your channel using a continuous, deterministic scoring model.

Tip: If you want to review the ordering first, use **"Quality Check (Read-Only)"** to compute the same ordering without applying changes.

Choose an **Analysis Profile** (Fast / Balanced / Deep) before running quality checks; advanced YAML parameters remain available for power users.

Ordering is based on a continuous score (resolution, bitrate, FPS, codecs, validation confidence). Providers can influence position but never inclusion.

The web dashboard updates every 2 seconds with:
- Live progress bar
- Streams processed/failed
- ETA
- Last run statistics

---

### Summary

You have now:
1. Added all streams from all providers to your selected channel, filtered by your search criteria
2. Tested each stream for quality and speed
3. Ordered all streams deterministically for stable channel definitions

---

## 📊 Monitoring

### View Logs

```bash
# Web UI logs
docker-compose logs -f dispatcharr-maid-web
```

### Access Data Files

All your CSV files and logs are in the local folders:

```bash
cd ~/dispatcharr-maid

# CSV files
ls -lh csv/

# Logs and checkpoints
ls -lh logs/

# Refresh learning data (selectors + injected excludes)
ls -lh data/

# Config
cat config.yaml
```

These folders are **mounted into the containers**, so data persists even if you recreate containers.

---

## 🔧 Container Management

### Start/Stop

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart all services
docker-compose restart
```

### Update

Use the provided automation if available (e.g., `./update-maid.sh`) to pull updates, rebuild, and restart with preserved data. If you prefer manual steps:

```bash
# Pull latest code
cd ~/dispatcharr-maid
wget https://github.com/Biggregw/dispatcharr-maid/archive/refs/heads/main.zip -O Dispatcharr_Maid.zip
unzip -o Dispatcharr_Maid.zip
mv dispatcharr-maid-main/* .
rm -rf dispatcharr-maid-main Dispatcharr_Maid.zip

# Rebuild containers (required when dependencies or assets change)
docker-compose build

# Restart with new version (fast restart without rebuild skips code changes)
docker-compose up -d
```

### View Status

```bash
# Check container status
docker-compose ps

# Check resource usage
docker stats dispatcharr-maid-web

# Check network connectivity
docker exec dispatcharr-maid-web ping -c 3 dispatcharr
```

---

## 🌐 Network Architecture

```
dispatcharr_default network 
├── dispatcharr 
├── dispatcharr-redis 
└── dispatcharr-maid-web (auto-assigned IP, exposed on host: 0.0.0.0:5000)
```

All containers can communicate by name:
- `dispatcharr-maid-web` → `http://dispatcharr:9191` ✅
- `dispatcharr` → `dispatcharr-redis:6379` ✅

---

## 📦 Using Portainer

If you have Portainer running, here's how to manage Dispatcharr Maid through it:

1. **Open Portainer:** `http://YOUR-SERVER-IP:9000`
2. **Go to Containers**
3. **Find:**
   - `dispatcharr-maid-web` (Web UI)

**From Portainer you can:**
- ✅ Start/stop containers
- ✅ View logs in real-time
- ✅ Access console (exec into container)
- ✅ Check resource usage
- ✅ Restart containers

**To run CLI from Portainer (optional):**
1. Click on `dispatcharr-maid-web` container
2. Click "Console"
3. Select "Custom" → enter `/bin/bash`
4. Click "Connect"
5. Run: `python3 interactive_maid.py`

---

## 🔍 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs dispatcharr-maid-web

# Common issues:
# - .env file missing
# - Wrong network name
# - Port conflict
```

### Can't Connect to Dispatcharr

```bash
# Test network connectivity
docker exec dispatcharr-maid-web ping dispatcharr

# Check .env has correct URL
docker exec dispatcharr-maid-web cat .env

# Should show: DISPATCHARR_BASE_URL=http://dispatcharr:9191
```

### Web Monitor Not Accessible

```bash
# Check if it's running
docker-compose ps dispatcharr-maid-web

# Check if port 5000 is available
sudo netstat -tlnp | grep 5000

# View web UI logs
docker-compose logs dispatcharr-maid-web
```

### Data Not Persisting

```bash
# Check volumes are mounted
docker inspect dispatcharr-maid-web | grep -A 10 Mounts

# Make sure you're in the right directory
pwd  # Should be ~/dispatcharr-maid

# Check local folders exist
ls -la csv/ logs/ data/
```

---

## 💾 Data Persistence

The following folders are **bind-mounted** from your host to the containers:

| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `./csv/` | `/app/csv/` | All CSV data files |
| `./logs/` | `/app/logs/` | Logs and checkpoints |
| `./data/` | `/app/data/` | Refresh learning (selectors + injected excludes) |
| `./config.yaml` | `/app/config.yaml` | Configuration |
| `./.env` | `/app/.env` | Credentials |

**This means:**
- ✅ Data survives container restarts
- ✅ You can view files directly on host
- ✅ Easy to backup (just backup these folders)
- ✅ Easy to edit config (edit local file, restart container)

---

## 🔄 Auto-Start on Boot

The `restart: unless-stopped` policy means:
- ✅ Containers auto-start when server reboots
- ✅ Auto-restart if they crash
- ✅ Won't restart if you manually stop them

To prevent auto-start:
```bash
# Change restart policy
docker update --restart=no dispatcharr-maid-web
```

---

## 🎛️ Advanced Configuration

### Change Web Monitor Port

Edit `docker-compose.yml`:

```yaml
ports:
  - "8080:5000"  # Access on port 8080 instead of 5000
```

Then: `docker-compose up -d`

### Resource Limits

Edit `docker-compose.yml`:

```yaml
services:
  dispatcharr-maid-web:
    # ... existing config ...
    deploy:
      resources:
        limits:
          cpus: '4.0'
          memory: 4G
        reservations:
          cpus: '2.0'
          memory: 2G
```

### Run Analysis on a Schedule

Use cron to trigger analysis:

```bash
# Edit crontab
crontab -e

# Add this line (run daily at 2am):
0 2 * * * docker exec dispatcharr-maid-web python3 -c "from stream_analysis import Config, fetch_streams, analyze_streams, score_streams, reorder_streams; from api_utils import DispatcharrAPI; api = DispatcharrAPI(); api.login(); config = Config(); fetch_streams(api, config); analyze_streams(config); score_streams(api, config); reorder_streams(api, config)" >> /var/log/dispatcharr-maid-cron.log 2>&1
```

---

## 🎯 Nginx Proxy Manager Integration

You can add HTTPS access to the web UI:

1. **Open NPM:** `http://YOUR-SERVER-IP:81`
2. **Add Proxy Host:**
   - Domain: `maid.yourdomain.com` (or use local domain)
   - Forward to: `dispatcharr-maid-web`
   - Port: `5000`
3. **Enable SSL** (if you have a domain)

Now access via: `https://maid.yourdomain.com`

### Optional: Provider usage (viewing activity) from NPM access logs

If your IPTV client uses the **Xtream connection served by Dispatcharr**, you can
derive provider-usage stats from Nginx Proxy Manager access logs.

1. **Mount NPM logs into the Maid container** (read-only).
   - NPM logs live at `/data/logs/` inside the NPM container.
   - On the host, that corresponds to your NPM data volume/bind mount (varies by install).
2. Set `usage.access_log_dir` in `config.yaml` (see `config.yaml.example`).
3. Call the endpoint:
   - `GET /api/usage/providers?days=7&proxy_host=1`

**Security note:** Xtream URLs often include credentials in the URL; treat access logs as sensitive.

---

## 📊 Comparison: Docker vs. Manual

| Aspect | Manual (venv) | Docker |
|--------|---------------|--------|
| Setup | `python3 -m venv`, `source activate` | `docker-compose up -d` |
| Dependencies | Manual pip install | Automatic |
| Updates | Pull code, reinstall deps | Rebuild image |
| Persistence | Local files | Mounted volumes |
| Auto-start | Manual/systemd | Built-in |
| Isolation | System Python | Containerized |
| Portability | Environment-dependent | Portable |
| Management | CLI only | CLI + Portainer |

**Docker advantages:**
- ✅ Easier management
- ✅ Better isolation
- ✅ Auto-restart capability
- ✅ Consistent environment

---

## 🚀 Next Steps

1. **Start using it:**
   ```bash
   docker-compose up -d
   docker-compose exec dispatcharr-maid-web python3 interactive_maid.py
   ```

2. **Access dashboard:**
   ```
   http://YOUR-SERVER-IP:5000
   ```

3. **Monitor via Portainer:**
   ```
   http://YOUR-SERVER-IP:9000
   ```

4. **Optional: Add to Nginx Proxy Manager** for HTTPS access

---

## 🆘 Getting Help

**Check container logs:**
```bash
docker-compose logs -f
```

**Test Dispatcharr connection:**
```bash
docker exec dispatcharr-maid-web python3 -c "from api_utils import DispatcharrAPI; api = DispatcharrAPI(); api.login(); print('✅ Connected!')"
```

**Verify files are mounted:**
```bash
docker exec dispatcharr-maid-web ls -la /app/csv /app/logs /app/jobs
```

**Rebuild from scratch:**
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

Enjoy your Dockerized Dispatcharr Maid! 🎉🐳
