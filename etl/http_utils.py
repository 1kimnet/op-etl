"""
Robust HTTP utilities for OP-ETL pipeline.
Addresses recursion depth errors and provides resilient downloading.
"""

import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Optional, Union

import requests

log = logging.getLogger(__name__)

# Constants for safety limits
MAX_RESPONSE_SIZE_MB = 100
MAX_JSON_DEPTH = 50
MAX_XML_ELEMENTS = 50000
MAX_JSON_ARRAY_SAMPLE_SIZE = 100
DEFAULT_RECURSION_LIMIT = 3000
DEFAULT_TIMEOUT = 60


class RecursionSafeSession:
    """HTTP session with simplified configuration to avoid recursion issues."""

    def __init__(self, max_retries: int = 3, backoff_factor: float = 0.5):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Ensure recursion limit is set high enough for deeply nested API responses
        current_limit = sys.getrecursionlimit()
        if current_limit < DEFAULT_RECURSION_LIMIT:
            sys.setrecursionlimit(DEFAULT_RECURSION_LIMIT)
            log.debug(f"[HTTP] Increased recursion limit from {current_limit} to {DEFAULT_RECURSION_LIMIT}")

    def safe_get(self, url: str, timeout: int = DEFAULT_TIMEOUT,
                 **kwargs) -> Optional[requests.Response]:
        """Perform a safe GET request with minimal configuration."""
        log.debug(f"[HTTP] Requesting: {url}")

        try:
            # Use a fresh session for each request to avoid recursion issues
            session = requests.Session()

            # Set simple user agent
            session.headers.update({
                'User-Agent': 'op-etl/1.0 (geospatial-data-pipeline)'
            })

            # Make the request with basic retry logic
            for attempt in range(self.max_retries + 1):
                try:
                    response = session.get(url, timeout=timeout, **kwargs)

                    # Validate response size
                    content_length = response.headers.get('content-length')
                    if content_length:
                        size_mb = int(content_length) / (1024 * 1024)
                        if size_mb > MAX_RESPONSE_SIZE_MB:
                            log.warning(f"[HTTP] Response too large: {size_mb:.1f}MB > {MAX_RESPONSE_SIZE_MB}MB")
                            return None

                    response.raise_for_status()
                    log.debug(f"[HTTP] Success: {url} ({response.status_code})")
                    return response

                except requests.exceptions.RequestException as e:
                    if attempt < self.max_retries:
                        wait_time = self.backoff_factor * (2 ** attempt)
                        log.debug(f"[HTTP] Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                        time.sleep(wait_time)
                    else:
                        log.error(f"[HTTP] Request failed after {self.max_retries + 1} attempts for {url}: {e}")
                        return None

            return None

        except RecursionError as e:
            log.error(f"[HTTP] Recursion error for {url}: {e}")
            return None
        except Exception as e:
            log.error(f"[HTTP] Unexpected error for {url}: {e}")
            return None


def safe_json_parse(content: Union[str, bytes], max_depth: int = MAX_JSON_DEPTH) -> Optional[Dict]:
    """Safely parse JSON with simplified error handling."""
    try:
        if isinstance(content, bytes):
            content = content.decode('utf-8', errors='replace')

        # Check content size
        if len(content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning(f"[JSON] Content too large: {len(content)} bytes")
            return None

        try:
            data = json.loads(content)

            # Check depth (simplified check)
            if _json_depth(data) > max_depth:
                log.warning(f"[JSON] Structure too deep: > {max_depth} levels")
                return None

            return data

        except RecursionError as e:
            log.error(f"[JSON] Recursion error during parsing: {e}")
            return None
        except json.JSONDecodeError as e:
            log.error(f"[JSON] Parse error: {e}")
            return None

    except Exception as e:
        log.error(f"[JSON] Unexpected error: {e}")
        return None


def safe_xml_parse(content: Union[str, bytes], max_elements: int = MAX_XML_ELEMENTS) -> Optional[ET.Element]:
    """Safely parse XML with simplified error handling."""
    try:
        if isinstance(content, str):
            content = content.encode('utf-8')

        # Check content size
        if len(content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning(f"[XML] Content too large: {len(content)} bytes")
            return None

        try:
            # Use iterparse to limit elements processed
            from io import BytesIO
            element_count = 0

            # First pass: count elements
            for event, elem in ET.iterparse(BytesIO(content), events=('start',)):
                element_count += 1
                if element_count > max_elements:
                    log.warning(f"[XML] Too many elements: > {max_elements}")
                    return None

            # Second pass: parse normally if within limits
            root = ET.fromstring(content)
            return root

        except RecursionError as e:
            log.error(f"[XML] Recursion error during parsing: {e}")
            return None
        except ET.ParseError as e:
            log.error(f"[XML] Parse error: {e}")
            return None

    except Exception as e:
        log.error(f"[XML] Unexpected error: {e}")
        return None


def download_with_retries(url: str, output_path: Path,
                          max_retries: int = 3,
                          timeout: int = DEFAULT_TIMEOUT) -> bool:
    """Download a file with retries and safety checks."""
    session = RecursionSafeSession(max_retries=max_retries)

    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"[DOWNLOAD] Attempt {attempt}/{max_retries}: {url}")

            response = session.safe_get(url, timeout=timeout)
            if not response:
                continue

            # Save with streaming to handle large files
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'wb') as f:
                total_size = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)

                        # Safety check for file size
                        if total_size > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                            log.warning("[DOWNLOAD] File too large, stopping download")
                            output_path.unlink(missing_ok=True)
                            return False

            # Verify download
            if output_path.exists() and output_path.stat().st_size > 0:
                log.info(f"[DOWNLOAD] Success: {output_path.name} ({output_path.stat().st_size} bytes)")
                return True

        except Exception as e:
            log.warning(f"[DOWNLOAD] Attempt {attempt} failed: {e}")

        # Wait before retry
        if attempt < max_retries:
            wait_time = session.backoff_factor * (2 ** (attempt - 1))
            log.info(f"[DOWNLOAD] Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)

    log.error(f"[DOWNLOAD] Failed after {max_retries} attempts: {url}")
    return False


def _json_depth(obj: Any, current_depth: int = 0) -> int:
    """Calculate approximate JSON structure depth."""
    if current_depth > MAX_JSON_DEPTH:
        return current_depth

    if isinstance(obj, dict):
        if not obj:
            return current_depth
        return max(_json_depth(v, current_depth + 1) for v in obj.values())
    elif isinstance(obj, list):
        if not obj:
            return current_depth
        return max(_json_depth(item, current_depth + 1) for item in obj[:MAX_JSON_ARRAY_SAMPLE_SIZE])  # Limit check to first MAX_JSON_ARRAY_SAMPLE_SIZE items
    else:
        return current_depth


def validate_response_content(response: requests.Response) -> bool:
    """Validate response content before parsing."""
    try:
        # Check content type
        content_type = response.headers.get('content-type', '').lower()

        # Check for known problematic patterns
        if 'text/html' in content_type and len(response.content) > 1024 * 1024:
            log.warning("[VALIDATE] Large HTML response (likely error page)")
            return False

        # Check for suspiciously large JSON/XML
        if (any(t in content_type for t in ['json', 'xml']) and
                len(response.content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024):
            log.warning(f"[VALIDATE] Oversized {content_type} response")
            return False

        return True

    except Exception as e:
        log.warning(f"[VALIDATE] Validation error: {e}")
        return False
