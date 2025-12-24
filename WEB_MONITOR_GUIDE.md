# Web monitor guide (legacy / optional)

This repo contains **two** different web experiences:

- **Primary UI (recommended)**: `dispatcharr_web_app.py` (control panel + results) on `http://YOUR-SERVER-IP:5000/`
- **Legacy “monitor-only” dashboard**: `web_monitor.py` (read-only status page)

Most users should use the primary UI. This guide covers `web_monitor.py` for cases where you only want a lightweight dashboard for CLI runs.

---

## What `web_monitor.py` shows

The monitor page reads local files from the current working directory:

- `logs/checkpoint.json` (current progress)
- `csv/03_iptv_stream_measurements.csv` (last run summary, if present)
- `config.yaml` (basic settings display)

If those files don’t exist (or you’re running jobs in per-job workspaces under `jobs/<job_id>/`), the monitor will show “no data”.

---

## Running it

### Docker (same container)

If you are using the default Docker setup, you can run the legacy monitor by overriding the compose command:

- Edit `docker-compose.yml` and change:
  - `command: python3 dispatcharr_web_app.py`
  - to `command: python3 web_monitor.py`

Then:

```bash
docker-compose up -d --build
```

### Local (venv)

If you run locally, install dependencies and start it:

```bash
pip install -r requirements.txt
python3 web_monitor.py
```

---

## Security note

The monitor has **no authentication**. Keep it on a trusted network (or put it behind a reverse proxy / VPN / SSH tunnel).
