"""
Simplified HTTP utilities for OP-ETL focused on maintainability.
Handles the 90% use case with clear, straightforward code.
"""

import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Optional

import urllib3
from urllib3.util.retry import Retry
from urllib3.util.timeout import Timeout

logger = logging.getLogger(__name__)

# Simple constants
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
MAX_RESPONSE_SIZE_MB = 50


class SimpleResponse:
    """Simple response wrapper for HTTP responses."""

    def __init__(self, status: int, data: bytes, headers: dict | None = None):
        self.status = status
        self.data = data
        self.headers = headers or {}

    @property
    def text(self) -> str:
        """Get response as text."""
        return self.data.decode('utf-8', errors='replace')

    def json(self) -> dict:
        """Parse response as JSON."""
        return json.loads(self.text)


class SimpleHttpClient:
    """Simplified HTTP client for common operations."""

    def __init__(self, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES):
        self.timeout = timeout
        self.retries = retries

        # Create urllib3 pool manager with retry strategy
        retry_strategy = Retry(
            total=retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )

        self.http = urllib3.PoolManager(
            timeout=Timeout(connect=10, read=timeout),
            retries=retry_strategy
        )

    def get(self, url: str, headers: dict | None = None) -> Optional[SimpleResponse]:
        """Perform HTTP GET request."""
        try:
            logger.debug(f"[HTTP] GET {url}")

            response = self.http.request(
                'GET', url,
                headers=headers or {}
            )

            # Check response size
            if len(response.data) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                logger.warning(f"[HTTP] Response too large: {len(response.data)} bytes")
                return None

            return SimpleResponse(
                status=response.status,
                data=response.data,
                headers=dict(response.headers)
            )

        except Exception as e:
            logger.error(f"[HTTP] GET failed for {url}: {e}")
            return None

    def download(self, url: str, output_path: Path) -> bool:
        """Download file to specified path."""
        response = None
        try:
            logger.info(f"[HTTP] Downloading {url} to {output_path}")

            response = self.http.request(
                'GET', url,
                preload_content=False
            )

            if response.status != 200:
                logger.error(f"[HTTP] Download failed with status {response.status}")
                return False

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Stream download to file
            with open(output_path, 'wb') as f:
                downloaded = 0
                for chunk in response.stream(1024 * 1024):  # 1MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Check file size limit
                    if downloaded > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                        logger.error(f"[HTTP] Download too large: {downloaded} bytes")
                        output_path.unlink(missing_ok=True)
                        return False

            logger.info(f"[HTTP] Downloaded {downloaded} bytes to {output_path}")
            return True

        except Exception as e:
            logger.error(f"[HTTP] Download failed for {url}: {e}")
            return False
        finally:
            if response is not None and hasattr(response, 'release_conn'):
                response.release_conn()


# Global client instance
_client = SimpleHttpClient()


def safe_json_parse(content: bytes, max_size_mb: int = MAX_RESPONSE_SIZE_MB) -> Optional[Dict[str, Any]]:
    """Parse JSON content safely with size checks."""
    try:
        # Check size
        if len(content) > max_size_mb * 1024 * 1024:
            logger.warning(f"[HTTP] JSON content too large: {len(content)} bytes")
            return None

        # Parse JSON
        text = content.decode('utf-8', errors='replace')
        return json.loads(text)

    except Exception as e:
        logger.error(f"[HTTP] JSON parse failed: {e}")
        return None


def safe_xml_parse(content: bytes, max_size_mb: int = MAX_RESPONSE_SIZE_MB) -> Optional[ET.Element]:
    """Parse XML content safely with size checks."""
    try:
        # Check size
        if len(content) > max_size_mb * 1024 * 1024:
            logger.warning(f"[HTTP] XML content too large: {len(content)} bytes")
            return None

        # Parse XML
        return ET.fromstring(content)

    except Exception as e:
        logger.error(f"[HTTP] XML parse failed: {e}")
        return None


def validate_response_content(response: SimpleResponse) -> bool:
    """Basic response validation."""
    if response is None:
        return False

    if response.status != 200:
        logger.warning(f"[HTTP] Non-200 status: {response.status}")
        return False

    if len(response.data) == 0:
        logger.warning("[HTTP] Empty response")
        return False

    return True


class RecursionSafeSession:
    """Backward compatibility wrapper for existing code."""

    def __init__(self):
        self.client = SimpleHttpClient()

    def safe_get(self, url: str, **kwargs) -> Optional[SimpleResponse]:
        """Compatible with existing safe_get calls."""
        return self.client.get(url)


def download_with_retries(url: str, output_path: Path, **kwargs) -> bool:
    """Download file with retries - backward compatibility function."""
    return _client.download(url, output_path)


# Convenience functions for common operations
def http_get(url: str, **kwargs) -> Optional[SimpleResponse]:
    """Get HTTP response."""
    return _client.get(url)


def http_get_json(url: str, **kwargs) -> Optional[Dict[str, Any]]:
    """Get and parse JSON response."""
    response = _client.get(url)
    if response and validate_response_content(response):
        return safe_json_parse(response.data)
    return None


def http_get_xml(url: str, **kwargs) -> Optional[ET.Element]:
    """Get and parse XML response."""
    response = _client.get(url)
    if response and validate_response_content(response):
        return safe_xml_parse(response.data)
    return None


def http_download(url: str, output_path: Path, **kwargs) -> bool:
    """Download file to path."""
    return _client.download(url, output_path)
