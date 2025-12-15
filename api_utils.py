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
        
        if not all([self.base_url, self.username, self.password]):
            raise ValueError(
                "Missing credentials in .env file. "
                "Required: DISPATCHARR_BASE_URL, DISPATCHARR_USER, DISPATCHARR_PASS"
            )
    
    def login(self):
        """Authenticate with Dispatcharr and save token"""
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
            if e.response.status_code == 401:
                # Token expired, refresh and retry
                if self._refresh_token():
                    response = requests.get(url, headers=self._get_headers(), timeout=timeout)
                    response.raise_for_status()
                    return response.json()
            raise Exception(f"GET {url} failed: {e}")
        
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
            if e.response.status_code == 401:
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
            raise Exception(f"PATCH {url} failed: {e}")
        
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
            if e.response.status_code == 401:
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
            raise Exception(f"POST {url} failed: {e}")
        
        except requests.exceptions.RequestException as e:
            raise Exception(f"POST {url} failed: {e}")
    
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
