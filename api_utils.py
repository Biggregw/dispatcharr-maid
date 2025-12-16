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
        self.username = os.getenv('DISPATCHARR_USER')
        self.password = os.getenv('DISPATCHARR_PASS')
        self.token = os.getenv('DISPATCHARR_TOKEN')

        if not self.base_url:
            raise ValueError("Missing DISPATCHARR_BASE_URL in environment")

    def login(self):
        """Authenticate with Dispatcharr (CLI bootstrap only)."""
        if not self.username or not self.password:
            raise DispatcharrAuthError(
                "DISPATCHARR_USER and DISPATCHARR_PASS are required to obtain a token"
            )

        url = f"{self.base_url}/api/accounts/token/"

        try:
            response = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                json={'username': self.username, 'password': self.password},
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            token = data.get('access') or data.get('token')

            if not token:
                raise DispatcharrAuthError("No access token found in response")

            self.token = token
            logging.info("Login successful - token obtained")
            return True

        except requests.exceptions.RequestException as exc:
            raise DispatcharrConnectionError(f"Login failed: {exc}") from exc

    def _ensure_token(self):
        if not self.token:
            raise DispatcharrAuthError("DISPATCHARR_TOKEN is required for API calls")

    def _get_headers(self):
        """Get authorization headers"""
        self._ensure_token()
        return {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _request(self, method, endpoint, payload=None, timeout=30):
        self._ensure_token()
        url = f"{self.base_url}{endpoint}"

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

        auth_url = f"{self.base_url}/api/channels/groups/"
        try:
            response = requests.get(auth_url, headers=self._get_headers(), timeout=10)
        except requests.exceptions.RequestException as exc:
            raise DispatcharrConnectionError(f"Authenticated check failed: {exc}") from exc

        if response.status_code in (401, 403):
            raise DispatcharrAuthError("Dispatcharr token rejected")

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise DispatcharrConnectionError(f"Authenticated check failed: {exc}") from exc

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
