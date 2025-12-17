# Dispatcharr Maid - Docker Deployment Guide

## 🐳 What This Does

Runs Dispatcharr Maid in Docker containers that:
- ✅ **Connect to your existing Dispatcharr** on the same network
- ✅ **Persist data** (CSV files, logs, config)
- ✅ **Web monitor always available** on port 5000
- ✅ **Run CLI on-demand** when you need to analyze
- ✅ **Auto-restart** with your server
- ✅ **Managed via Portainer** (you already have it!)

---

## 📋 Prerequisites

- Docker and docker-compose installed ✅ (you have this)
- Dispatcharr running ✅ (you have this)
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

# Extract the Dispatcharr_Maid.zip
unzip Dispatcharr_Maid.zip

# Move the extracted files into the current directory
mv dispatcharr-maid-main/* .
rmdir dispatcharr-maid-main

# You should now see files such as
Dockerfile
docker-compose.yml
api_utils.py
stream_analysis.py
interactive_maid.py
web_monitor.py
templates/
config.yaml
.env.template
requirements.txt


### Step 2: Configure Credentials

```bash
# Copy template
cp .env.template .env

# Edit with your Dispatcharr credentials
nano .env
```

**Important:** The `.env` file should look like this:

```ini
# Since we're on the same Docker network, use container name!
DISPATCHARR_BASE_URL=http://dispatcharr:9191
DISPATCHARR_USER=admin
DISPATCHARR_PASS=your_actual_password
DISPATCHARR_TOKEN=
```

**Key point:** Use `http://dispatcharr:9191` (the container name), not `localhost` or an IP!

### Step 3: Build and Start

```bash
# Build the containers
docker-compose build

# Start everything
docker-compose up -d

# Check they're running
docker-compose ps
```

You should see:
```
NAME                    STATUS              PORTS
dispatcharr-maid        Up                  
dispatcharr-maid-web    Up (healthy)        0.0.0.0:5000->5000/tcp
```

### Step 4: Access Web Monitor

Open in your browser:
```
http://YOUR-SERVER-IP:5000
```

You should see the Dispatcharr Maid dashboard!

---

## 🎯 How to Use

### Run Analysis Jobs

**Method 1: Interactive Mode (Recommended)**

```bash
# Start interactive session
docker-compose exec dispatcharr-maid python3 interactive_maid.py

# You'll see the menu:
# 1. Full pipeline + cleanup
# 2. Full pipeline
# etc...

# Select your groups and run!
```

**Method 2: Direct Commands**

```bash
# Fetch only
docker-compose exec dispatcharr-maid python3 interactive_maid.py

# Or use docker exec directly
docker exec -it dispatcharr-maid python3 interactive_maid.py
```

### Monitor Progress

While analysis is running, just open:
```
http://YOUR-SERVER-IP:5000
```

The web dashboard updates every 2 seconds with:
- Live progress bar
- Streams processed/failed
- ETA
- Last run statistics

### View Logs

```bash
# Web monitor logs
docker-compose logs -f dispatcharr-maid-web

# Main container logs
docker-compose logs -f dispatcharr-maid

# All logs
docker-compose logs -f
```

### Access Data Files

All your CSV files and logs are in the local folders:

```bash
cd ~/dispatcharr-maid

# CSV files
ls -lh csv/

# Logs and checkpoints
ls -lh logs/

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

# Stop just the CLI (keep web monitor running)
docker-compose stop dispatcharr-maid

# Start just the CLI
docker-compose start dispatcharr-maid
```

### Update

```bash
# Pull latest code
cd ~/dispatcharr-maid
# (extract new Dispatcharr_Maid.zip)

# Rebuild containers
docker-compose build

# Restart with new version
docker-compose up -d
```

### View Status

```bash
# Check container status
docker-compose ps

# Check resource usage
docker stats dispatcharr-maid dispatcharr-maid-web

# Check network connectivity
docker exec dispatcharr-maid ping -c 3 dispatcharr
```

---

## 🌐 Network Architecture

```
dispatcharr_default network (172.18.0.0/16)
├── dispatcharr (172.18.0.3:9191)
├── dispatcharr-redis (172.18.0.2:6379)
├── dispatcharr-maid (auto-assigned IP)
└── dispatcharr-maid-web (auto-assigned IP)
    └── Exposed on host: 0.0.0.0:5000
```

All containers can communicate by name:
- `dispatcharr-maid` → `http://dispatcharr:9191` ✅
- `dispatcharr` → `dispatcharr-redis:6379` ✅

---

## 📦 Using Portainer

You already have Portainer running! Here's how to manage Dispatcharr Maid through it:

1. **Open Portainer:** `http://YOUR-SERVER-IP:9000`
2. **Go to Containers**
3. **Find:**
   - `dispatcharr-maid` (CLI container)
   - `dispatcharr-maid-web` (Web monitor)

**From Portainer you can:**
- ✅ Start/stop containers
- ✅ View logs in real-time
- ✅ Access console (exec into container)
- ✅ Check resource usage
- ✅ Restart containers

**To run analysis from Portainer:**
1. Click on `dispatcharr-maid` container
2. Click "Console"
3. Select "Custom" → enter `/bin/bash`
4. Click "Connect"
5. Run: `python3 interactive_maid.py`

---

## 🔍 Troubleshooting

### Container won't start

```bash
# Check logs
docker-compose logs dispatcharr-maid

# Common issues:
# - .env file missing
# - Wrong network name
# - Port conflict
```

### Can't connect to Dispatcharr

```bash
# Test network connectivity
docker exec dispatcharr-maid ping dispatcharr

# Check .env has correct URL
docker exec dispatcharr-maid cat .env

# Should show: DISPATCHARR_BASE_URL=http://dispatcharr:9191
```

### Web monitor not accessible

```bash
# Check if it's running
docker-compose ps dispatcharr-maid-web

# Check if port 5000 is available
sudo netstat -tlnp | grep 5000

# View web monitor logs
docker-compose logs dispatcharr-maid-web
```

### Data not persisting

```bash
# Check volumes are mounted
docker inspect dispatcharr-maid | grep -A 10 Mounts

# Make sure you're in the right directory
pwd  # Should be ~/dispatcharr-maid

# Check local folders exist
ls -la csv/ logs/
```

---

## 💾 Data Persistence

The following folders are **bind-mounted** from your host to the containers:

| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `./csv/` | `/app/csv/` | All CSV data files |
| `./logs/` | `/app/logs/` | Logs and checkpoints |
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
  dispatcharr-maid:
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
0 2 * * * docker exec dispatcharr-maid python3 -c "from stream_analysis import *; from api_utils import *; config = Config(); api = DispatcharrAPI(); api.login(); fetch_streams(api, config); analyze_streams(config); score_streams(api, config); reorder_streams(api, config)" >> /var/log/dispatcharr-maid-cron.log 2>&1
```

---

## 🎯 Nginx Proxy Manager Integration

You already have Nginx Proxy Manager! You can add HTTPS access to the web monitor:

1. **Open NPM:** `http://YOUR-SERVER-IP:81`
2. **Add Proxy Host:**
   - Domain: `maid.yourdomain.com` (or use local domain)
   - Forward to: `dispatcharr-maid-web`
   - Port: `5000`
3. **Enable SSL** (if you have a domain)

Now access via: `https://maid.yourdomain.com`

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

**Docker wins for:**
- ✅ Easier management
- ✅ Better isolation
- ✅ Auto-restart
- ✅ Consistent environment

---

## 🚀 Next Steps

1. **Start using it:**
   ```bash
   docker-compose up -d
   docker-compose exec dispatcharr-maid python3 interactive_maid.py
   ```

2. **Access dashboard:**
   ```
   http://YOUR-SERVER-IP:5000
   ```

3. **Monitor via Portainer:**
   ```
   http://YOUR-SERVER-IP:9000
   ```

4. **Optional: Add to Nginx Proxy Manager** for HTTPS

---

## 🆘 Getting Help

**Check container logs:**
```bash
docker-compose logs -f
```

**Test Dispatcharr connection:**
```bash
docker exec dispatcharr-maid python3 -c "from api_utils import *; api = DispatcharrAPI(); api.login(); print('✓ Connected!')"
```

**Verify files are mounted:**
```bash
docker exec dispatcharr-maid ls -la /app/csv /app/logs
```

**Rebuild from scratch:**
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

Enjoy your Dockerized Dispatcharr Maid! 🎉🐳
