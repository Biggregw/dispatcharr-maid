"""
api_utils.py
Consolidated API utilities for Dispatcharr interaction
"""

import logging
import os

import requests
from dotenv import load_dotenv


class DispatcharrAuthError(Exception):
    """Raised when authentication is missing or invalid."""


class DispatcharrConnectionError(Exception):
    """Raised when Dispatcharr cannot be reached."""


class DispatcharrAPI:
    """Unified API client for Dispatcharr"""

    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv('DISPATCHARR_BASE_URL', '').rstrip('/')

        if not self.base_url:
            raise ValueError("Missing DISPATCHARR_BASE_URL in environment")

    def _get_token(self):
        token = os.getenv('DISPATCHARR_TOKEN')
        if token:
            return token

        raise DispatcharrAuthError(
            "DISPATCHARR_TOKEN must be set in the server environment (.env)"
        )

    def _ensure_token(self):
        # Explicitly call _get_token so any missing token surfaces as an auth error
        self._get_token()

    def _get_headers(self):
        """Get authorization headers"""
        token = self._get_token()
        return {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _request(self, method, endpoint, payload=None, timeout=30):
        url = f"{self.base_url}{endpoint}"

        self._ensure_token()

        try:
            response = requests.request(
                method,
                url,
                headers=self._get_headers(),
                json=payload,
                timeout=timeout
            )
        except requests.exceptions.RequestException as exc:
            raise DispatcharrConnectionError(f"{method.upper()} {url} failed: {exc}") from exc

        if response.status_code in (401, 403):
            raise DispatcharrAuthError(f"Authentication failed for {method.upper()} {url}")

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise DispatcharrConnectionError(f"{method.upper()} {url} failed: {exc}") from exc

        return response.json()

    def check_status(self):
        """Verify Dispatcharr availability and token validity."""
        base_ping = f"{self.base_url}/"

        try:
            requests.get(base_ping, timeout=10)
        except requests.exceptions.RequestException as exc:
            raise DispatcharrConnectionError(f"Cannot reach Dispatcharr at {self.base_url}: {exc}") from exc

        self._ensure_token()

        # Validate authentication by fetching channel groups (no token refresh or mutation)
        try:
            self.fetch_channel_groups()
        except DispatcharrAuthError:
            raise DispatcharrAuthError("Dispatcharr token rejected")
        except DispatcharrConnectionError:
            raise

        return {'reachable': True, 'authenticated': True}

    def get(self, endpoint, timeout=30):
        """GET request with authentication"""
        return self._request('get', endpoint, timeout=timeout)

    def patch(self, endpoint, payload, timeout=30):
        """PATCH request with authentication"""
        return self._request('patch', endpoint, payload=payload, timeout=timeout)

    def post(self, endpoint, payload, timeout=30):
        """POST request with authentication"""
        return self._request('post', endpoint, payload=payload, timeout=timeout)

    # Convenience methods for common operations

    def fetch_channel_groups(self):
        """Fetch all channel groups"""
        return self.get('/api/channels/groups/')

    def fetch_channels(self):
        """Fetch all channels"""
        return self.get('/api/channels/channels/')

    def fetch_channel_streams(self, channel_id):
        """Fetch streams for a specific channel"""
        return self.get(f'/api/channels/channels/{channel_id}/streams/')

    def fetch_stream_details(self, stream_id):
        """Fetch details for a specific stream"""
        try:
            return self.get(f'/api/channels/streams/{stream_id}/')
        except DispatcharrAuthError:
            raise
        except Exception as exc:  # noqa: BLE001
            logging.warning(f"Could not fetch stream {stream_id}: {exc}")
            return None

    def update_channel_streams(self, channel_id, stream_ids):
        """Update the stream list for a channel"""
        return self.patch(
            f'/api/channels/channels/{channel_id}/',
            {'streams': stream_ids}
        )

    def update_stream_stats(self, stream_id, stats):
        """Update statistics for a stream"""
        return self.patch(
            f'/api/channels/streams/{stream_id}/',
            {'stream_stats': stats}
        )
