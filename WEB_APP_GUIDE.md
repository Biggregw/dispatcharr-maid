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

### 3) Refresh Channel Streams (enrichment step)

This is intended for **one channel at a time** (because it uses a single base search term and shows you a preview list).

1. Click **Refresh Channel Streams**
2. Adjust:
   - **Base Search Text** (what the app searches for across providers)
   - **Include Filter / Exclude Filter** (supports `*` wildcards, comma-separated)
   - **Exclude +1 channels**
   - **Exclude 4K/UHD streams** (enabled by default)
3. The modal shows a **preview list** of matching streams
4. Untick any streams you do *not* want, then click **Add Selected Streams**
5. Optional: click **Save Regex** to keep your selection logic for later

### 4) Quality Check (ranking + cleanup)

Set **Streams Per Provider** (e.g. `2`) then choose one:

- **Quality Check (Apply Changes)**: runs analysis + scoring, then updates Dispatcharr immediately (reorder + cleanup based on “Streams Per Provider”).
- **Quality Check (Preview Plan)**: makes **no changes**. It generates a plan you can review and commit later from the Results page.

---

## 📊 Results page (what you can do there)

Open `http://YOUR-SERVER-IP:5000/results`.

- **Run picker**: switch between completed jobs
- **Export CSV**: download results for the selected job
- Tabs:
  - **Providers / Channels / Errors**: summary tables
  - **Streams**: stream list with a “generate minimal regex” helper
  - **Planned Changes**: for “Preview Plan” runs; you can **commit selected channels** to Dispatcharr from here

---

## 🔒 Security note

The web UI is intended for trusted networks. Do not expose port 5000 to the public internet; use a reverse proxy with auth, VPN, or SSH tunneling if you need remote access.
