# Dispatcharr Maid

Dispatcharr Maid provides housekeeping tools for the Dispatcharr IPTV stream project. It bundles a CLI and web UI for fetching,
analyzing, and reordering streams so you can keep channel lists healthy.

The web UI has **no user login**. Access relies on your network exposure and valid Dispatcharr API credentials only.

## Assumptions
- You already run Dispatcharr and can reach its API from the machine running this app.
- Dispatcharr credentials are provided via environment variables (see `.env.example`).
- The UI surfaces a blocking error page if Dispatcharr is unreachable or credentials are invalid, but the container still starts.

## Quick start (Docker Compose)

1. **Clone & prepare configuration**
   ```bash
   git clone <repo-url>
   cd dispatcharr-maid
   cp .env.example .env
   # Fill in your Dispatcharr URL/username/password in .env
   ```

2. **Launch the web UI**
   ```bash
   docker compose up
   ```
   Open http://localhost:5000. If Dispatcharr credentials are missing or unreachable, a clear error page is shown instead of crashing.

3. **Using the CLI instead** (optional)
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   cp .env.example .env  # populate credentials first
   python interactive_maid.py
   ```

## Common scripts
- `stream_analysis.py`: Core library for fetching, scoring, and reordering streams with checkpointing.
- `interactive_maid.py`: Terminal workflow that uses the analysis library to clean up channel lists.
- `dispatcharr_web_app.py`: Flask app exposing the same operations through a web dashboard.
- `web_monitor.py`: Helper for monitoring web-related tasks in Docker deployments.
- `add_provider_tracking.py`, `update_provider_names.py`: Utilities to enrich CSV output with provider names.

## Troubleshooting
- If API requests fail, confirm your credentials are loaded from `.env` and URLs match your Dispatcharr instance.
- For CSV-related errors, ensure the `csv/` directory exists and files are writable.
- Logs are written under `logs/`; check them for stack traces when runs fail.

## Known limitations
- The UI depends entirely on Dispatcharr API access; most features will be blocked until valid credentials are supplied.
- Jobs can take time on large channel sets; monitor progress in the UI and avoid restarting the container mid-run.
- No built-in user authentication—expose the service only to trusted networks.
