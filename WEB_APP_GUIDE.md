# Dispatcharr Maid - Web App Guide

This guide matches the current **web UI** shipped in this repo (`dispatcharr_web_app.py` + `templates/app.html` + `templates/results.html`).

---

## 🚀 Quick Start (Docker)

From the repo directory:

```bash
cp .env.example .env
cp config.yaml.example config.yaml

# Optional: nicer provider display names in the UI/results
cp provider_names.json.example provider_names.json

nano .env
nano config.yaml

docker-compose up -d --build
```

Open:

- Control panel: `http://YOUR-SERVER-IP:5000/`
- Results: `http://YOUR-SERVER-IP:5000/results`

---

## 🧭 Main workflow (what the UI actually does)

### 1) Select Groups

- Tick one or more channel groups.
- Click **Next: Select Channels**.

### 2) (Optional) Select Channels

- Use the search box to filter by name/number.
- Either:
  - Select specific channels, or
  - Select none (means “all channels in the chosen groups” for quality jobs).

Click **Next: Run Jobs**.

### 3) Run Jobs (enrichment + analysis)

This step is where you define and run Jobs. The matching model is always:

**Primary Match → Include / Exclude → optional Advanced Regex (with a regex-only toggle)**

- **Refresh Channel Streams** is intended for **one channel at a time** so you can inspect the preview list.
- Click **Refresh Channel Streams**, adjust the matching chain above, and preview the results.
- Untick any streams you do *not* want, then click **Add Selected Streams**.
- Jobs are saved automatically when you run them. You can still click **Save Job Definition** from the preview to capture one-off tweaks, then manage the Saved Job from the Run Jobs screen (view, re-run, rename via re-saving, or delete).

Dispatcharr-Maid assumes client-side decode (e.g., Firestick) with proxied playback only; FFmpeg capability testing and transcoding are out of scope.

### 4) Quality Check (scoring + ordering)

Pick an **Analysis Profile** (Fast / **Balanced** (default) / Deep) to decide how deep the probe should go. Raw analysis parameters stay under **Advanced analysis settings**, and YAML imports remain supported if you want to override everything.

- **Quality Check (Apply Changes)**: runs analysis + scoring, then updates Dispatcharr immediately with a deterministic ordering (no streams removed).
- **Quality Check (Read-Only)**: makes **no changes** and computes the same ordering for inspection.

---

## 📊 Results page (what you can do there)

Open `http://YOUR-SERVER-IP:5000/results`.

- **Run picker**: switch between completed jobs
- **Export CSV**: download results for the selected job
- Tabs:
  - **Providers / Channels / Errors**: summary tables
  - **Streams**: stream list with a “generate minimal regex” helper
  - **Ordering visibility**: review the ordered stream list for each channel

---

## 🔒 Security note

The web UI is intended for trusted networks. Do not expose port 5000 to the public internet; use a reverse proxy with auth, VPN, or SSH tunneling if you need remote access.
