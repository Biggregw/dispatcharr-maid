"""
api_utils.py
Consolidated API utilities for Dispatcharr interaction
"""

import os
import sys
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv, set_key


class DispatcharrAPI:
    """Unified API client for Dispatcharr"""
    
    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv('DISPATCHARR_BASE_URL', '').rstrip('/')
        self.username = os.getenv('DISPATCHARR_USER')
        self.password = os.getenv('DISPATCHARR_PASS')
        self.token = os.getenv('DISPATCHARR_TOKEN')

        if not self.base_url:
            raise ValueError("Missing DISPATCHARR_BASE_URL in environment or .env file.")

        if not self.token and not all([self.username, self.password]):
            raise ValueError(
                "Missing credentials. Provide DISPATCHARR_TOKEN or both DISPATCHARR_USER and DISPATCHARR_PASS."
            )
    
    def login(self):
        """Authenticate with Dispatcharr and save token"""
        if not all([self.username, self.password]):
            raise ValueError("Username and password are required to obtain a token.")

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
            self.token = data.get('access') or data.get('token')
            
            if not self.token:
                raise ValueError("No access token found in response")
            
            # Update .env with new token
            env_path = Path('.env')
            if env_path.exists():
                set_key(env_path, 'DISPATCHARR_TOKEN', self.token)
            
            logging.info("Login successful - token saved")
            return True
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Login failed: {e}")
    
    def _get_headers(self):
        """Get authorization headers"""
        if not self.token:
            self.login()
        
        return {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    def _refresh_token(self):
        """Refresh authentication token"""
        logging.info("Token expired - refreshing...")
        return self.login()
    
    def get(self, endpoint, timeout=30):
        """GET request with authentication and retry logic"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=timeout)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            response = e.response
            if response is not None and response.status_code == 401:
                # Token expired, refresh and retry
                if self._refresh_token():
                    response = requests.get(url, headers=self._get_headers(), timeout=timeout)
                    response.raise_for_status()
                    return response.json()
            status = response.status_code if response is not None else "unknown"
            raise Exception(f"GET {url} failed ({status}): {e}")
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"GET {url} failed: {e}")

    def get_raw(self, endpoint, timeout=30):
        """GET request returning raw response for inspection/logging"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.get(url, headers=self._get_headers(), timeout=timeout)
            if response.status_code == 401:
                if self._refresh_token():
                    response = requests.get(url, headers=self._get_headers(), timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise Exception(f"GET {url} failed: {e}")
    
    def patch(self, endpoint, payload, timeout=30):
        """PATCH request with authentication and retry logic"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.patch(
                url, 
                json=payload, 
                headers=self._get_headers(), 
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            response = e.response
            if response is not None and response.status_code == 401:
                # Token expired, refresh and retry
                if self._refresh_token():
                    response = requests.patch(
                        url, 
                        json=payload, 
                        headers=self._get_headers(), 
                        timeout=timeout
                    )
                    response.raise_for_status()
                    return response.json()
            status = response.status_code if response is not None else "unknown"
            snippet = ""
            if response is not None:
                try:
                    snippet = (response.text or "").strip()
                except Exception:
                    snippet = ""
            if snippet:
                snippet = snippet.replace("\n", " ")[:200]
                raise Exception(f"PATCH {url} failed ({status}): {snippet}")
            raise Exception(f"PATCH {url} failed ({status}): {e}")
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"PATCH {url} failed: {e}")
    
    def post(self, endpoint, payload, timeout=30):
        """POST request with authentication and retry logic"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.post(
                url, 
                json=payload, 
                headers=self._get_headers(), 
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            response = e.response
            if response is not None and response.status_code == 401:
                # Token expired, refresh and retry
                if self._refresh_token():
                    response = requests.post(
                        url, 
                        json=payload, 
                        headers=self._get_headers(), 
                        timeout=timeout
                    )
                    response.raise_for_status()
                    return response.json()
            status = response.status_code if response is not None else "unknown"
            raise Exception(f"POST {url} failed ({status}): {e}")
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"POST {url} failed: {e}")
    
    # Convenience methods for common operations
    
    def fetch_channel_groups(self):
        """Fetch all channel groups"""
        return self.get('/api/channels/groups/')
    
    def fetch_channels(self):
        """Fetch all channels"""
        return self.get('/api/channels/channels/')

    def fetch_channel(self, channel_id):
        """Fetch a single channel by ID"""
        return self.get(f'/api/channels/channels/{channel_id}/')
    
    def fetch_channel_streams(self, channel_id):
        """Fetch streams for a specific channel"""
        return self.get(f'/api/channels/channels/{channel_id}/streams/')

    def fetch_stream_provider_map(self, limit=100):
        """
        Fetch all streams and build a stream_id -> m3u_account map.

        This avoids N+1 API calls when you only need provider/account IDs.
        Returns a dict of {int(stream_id): m3u_account}.
        """
        stream_provider_map = {}
        next_url = f'/api/channels/streams/?limit={int(limit)}'

        while next_url:
            payload = self.get(next_url)

            # DRF-style pagination: {results: [...], next: "..."}
            if isinstance(payload, dict) and 'results' in payload:
                results = payload.get('results') or []
                next_link = payload.get('next')
            # Fallback: unpaginated list
            elif isinstance(payload, list):
                results = payload
                next_link = None
            else:
                raise ValueError("Unexpected streams response shape; expected dict or list.")

            for stream in results:
                if not isinstance(stream, dict):
                    continue
                stream_id = stream.get('id')
                m3u_account = stream.get('m3u_account')
                if stream_id is None or m3u_account is None:
                    continue
                stream_provider_map[int(stream_id)] = m3u_account

            if next_link:
                # Convert absolute next URL to API-relative endpoint
                next_url = next_link.split('/api/')[-1]
                next_url = '/api/' + next_url
            else:
                next_url = None

        return stream_provider_map
    
    def fetch_stream_details(self, stream_id):
        """Fetch details for a specific stream"""
        try:
            return self.get(f'/api/channels/streams/{stream_id}/')
        except Exception as e:
            logging.warning(f"Could not fetch stream {stream_id}: {e}")
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
