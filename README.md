# Dispatcharr Maid

Dispatcharr Maid provides housekeeping tools for the Dispatcharr IPTV stream project. It bundles a CLI and web UI for fetching, analyzing, and reordering streams so you can keep channel lists healthy.

## Quick start

1. **Clone & install dependencies**
   ```bash
   git clone <repo-url>
   cd dispatcharr-maid
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment**
   - Set `DISPATCHARR_BASE_URL` and `DISPATCHARR_TOKEN` in a server-side `.env` file. The web UI no longer prompts for tokens, so these values must be present before you start the app.
   - Adjust `config.yaml` to point at your Dispatcharr endpoints and data directories.

3. **Run the interactive CLI**
   ```bash
   python interactive_maid.py
   ```
   The CLI walks you through fetching streams, analyzing them, and writing results.

4. **Run the web app**
   ```bash
   export FLASK_APP=dispatcharr_web_app.py
   flask run --host 0.0.0.0 --port 5000
   ```
   Open http://localhost:5000 to queue jobs and monitor analysis from your browser.

5. **Docker option**
   ```bash
   docker build -t dispatcharr-maid .
   docker run --rm -p 5000:5000 dispatcharr-maid
   ```
   The image runs the interactive workflow defined in `Dockerfile`; adjust the command or compose file as needed.

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
