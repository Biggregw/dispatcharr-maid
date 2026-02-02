# Results Page Guide (matches current UI)

The results page is served by `dispatcharr_web_app.py` and rendered by `templates/results.html`.

Open:

- `http://YOUR-SERVER-IP:5000/results`
- Or for a specific run: `http://YOUR-SERVER-IP:5000/results?job_id=<job_id>`

---

## What you’ll see

### Overall Summary

Shows totals for the selected run:

- Total analyzed
- Successful / failed
- Success rate
- Optional capacity warning (if provider max streams metadata is available)

### Run picker (history)

At the top-right of the results page there’s a **Run** dropdown. It lists completed **Job Runs** from `logs/job_history.json` and lets you switch between runs.

Notes about how history is stored/pruned so it matches the UI behavior:

- The app keeps the **50 most recent jobs** in history.
- If you enable the optional retention policy (see below), jobs older than the retention window are removed from history **and** their workspaces are deleted from disk.

### Export CSV

The **📥 Export CSV** button downloads results for the currently selected run:

- `GET /api/results/csv` (latest)
- `GET /api/results/csv?job_id=<job_id>` (job-scoped)

---

## Tabs

### Providers

Shows per-provider totals, success rate, and average quality. If `provider_names.json` exists, provider IDs are shown using your friendly names.

Optional toggle:

- **Show all providers (including 0 streams)**: includes providers that exist in your provider map even if they didn’t appear in this run.

### Channels

Per-channel totals and success rate for the run.

### Errors

A frequency list of error strings captured during analysis (timeouts, bad URLs, ffmpeg probe failures, etc.).

### Streams

Loads stream rows for the run and includes a helper to **generate a minimal regex** for future Primary Match/Advanced Regex use:

- Tick streams you want to match
- Untick streams you want excluded
- Click **Generate minimal regex**

This is useful when you want a stable "keep these, exclude those" filter for refresh/enrichment.

---

## Where results are stored

Each web job writes to a per-job workspace:

- `jobs/<job_id>/csv/` (run CSVs)
- `jobs/<job_id>/logs/` (job logs/artifacts)

The web UI also maintains job history in:

- `logs/job_history.json`

You can optionally enable pruning:

- `web.results_retention_days` in `config.yaml`, or
- environment variable `DISPATCHARR_MAID_RESULTS_RETENTION_DAYS` (takes precedence)
