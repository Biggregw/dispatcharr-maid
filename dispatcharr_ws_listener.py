"""Background listener for Dispatcharr WebSocket playback updates."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse, urlunparse

from api_utils import DispatcharrAPI
from observation_store import Observation, ObservationStore


class DispatcharrWebSocketListener:
    """Passive listener that records playback observations from Dispatcharr."""

    def __init__(self, observation_store: ObservationStore, ws_path: Optional[str] = None):
        self.observation_store = observation_store
        self.ws_path = ws_path or "/ws/updates/"
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="dispatcharr-ws-listener", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _run(self):
        backoff_seconds = 3
        while not self._stop_event.is_set():
            try:
                self._listen_once()
                backoff_seconds = 3
            except Exception as exc:  # pragma: no cover - defensive guard
                logging.warning("Dispatcharr WebSocket listener error: %s", exc, exc_info=True)
            if self._stop_event.is_set():
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)

    def _listen_once(self):
        try:
            api = DispatcharrAPI()
        except Exception as exc:
            logging.warning("Dispatcharr WebSocket setup failed: %s", exc)
            time.sleep(5)
            return

        try:
            headers = api._get_headers()
        except Exception as exc:
            logging.warning("Dispatcharr WebSocket auth failed: %s", exc)
            time.sleep(5)
            return

        try:
            import websocket  # type: ignore
        except Exception as exc:
            logging.warning("Dispatcharr WebSocket client unavailable: %s", exc)
            time.sleep(5)
            return

        url = self._build_ws_url(api.base_url)
        auth_header = headers.get("Authorization")
        extra_headers = [f"Authorization: {auth_header}"] if auth_header else None

        logging.info("Connecting to Dispatcharr WebSocket at %s", url)

        ws = websocket.WebSocketApp(
            url,
            header=extra_headers,
            on_message=self._handle_message,
            on_error=self._handle_error,
            on_close=self._handle_close,
            on_open=self._handle_open,
        )

        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:  # pragma: no cover - defensive guard
            logging.warning("Dispatcharr WebSocket connection failed: %s", exc, exc_info=True)

    def _handle_open(self, *_):  # pragma: no cover - logging only
        logging.info("Dispatcharr WebSocket connected")

    def _handle_close(self, *_):  # pragma: no cover - logging only
        logging.info("Dispatcharr WebSocket disconnected")

    def _handle_error(self, _, error):  # pragma: no cover - logging only
        logging.warning("Dispatcharr WebSocket error: %s", error)

    def _handle_message(self, _, message: str):
        try:
            payload = json.loads(message)
        except Exception:
            logging.debug("Dispatcharr WS message skipped (non-JSON)")
            return

        if not self._is_channel_stats_update(payload):
            return

        channels = (((payload or {}).get("data") or {}).get("stats") or {}).get("channels")
        if not isinstance(channels, Iterable):
            logging.debug("Dispatcharr WS message skipped (missing channels list)")
            return

        observations = self._build_playback_observations(channels)
        if observations:
            self.observation_store.extend(observations)

    def _is_channel_stats_update(self, payload) -> bool:
        if not isinstance(payload, dict):
            return False
        if payload.get("type") != "update":
            return False
        data = payload.get("data")
        if not isinstance(data, dict) or data.get("type") != "channel_stats":
            return False
        return True

    def _build_playback_observations(self, channels: Iterable) -> list[Observation]:
        observations: list[Observation] = []
        now = datetime.now(timezone.utc).isoformat()

        for channel in channels:
            if not isinstance(channel, dict):
                continue
            channel_id_raw = channel.get("channel_id")
            if channel_id_raw in (None, ""):
                continue
            try:
                channel_id = int(channel_id_raw)
            except Exception:
                logging.debug("Dispatcharr WS channel skipped (invalid channel_id)")
                continue
            observations.append(
                Observation(
                    ts=now,
                    event="observed",
                    channel_id=channel_id,
                    source="dispatcharr_ws",
                )
            )
        return observations

    def _build_ws_url(self, base_url: str) -> str:
        parsed = urlparse(base_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        path = self.ws_path if self.ws_path.startswith("/") else f"/{self.ws_path}"
        cleaned = path if path.endswith("/") else f"{path}/"
        new_path = urljoin(parsed.path.rstrip("/") + "/", cleaned.lstrip("/"))
        rebuilt = parsed._replace(scheme=scheme, path=new_path)
        return urlunparse(rebuilt)


def start_dispatcharr_ws_listener(store: ObservationStore) -> DispatcharrWebSocketListener:
    listener = DispatcharrWebSocketListener(store)
    try:
        listener.start()
    except Exception as exc:  # pragma: no cover - defensive guard
        logging.warning("Failed to start Dispatcharr WebSocket listener: %s", exc, exc_info=True)
    return listener
