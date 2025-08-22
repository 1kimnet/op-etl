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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# Constants for safety limits
MAX_RESPONSE_SIZE_MB = 100
MAX_JSON_DEPTH = 50
MAX_XML_ELEMENTS = 50000
MAX_JSON_ARRAY_SAMPLE_SIZE = 100
DEFAULT_RECURSION_LIMIT = 3000
DEFAULT_TIMEOUT = 60


class RecursionSafeSession:
    """HTTP session recursion depth protection and robust error handling."""

    def __init__(self, max_retries: int = 3, backoff_factor: float = 0.5):
        self.session = requests.Session()
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Configure retry strategy
        retry_strategy = Retry(  # type: ignore
            # t0ype ignore used because the Retry class from urllib3.util.retry
            # does not have a __init__ method that matches expected signature.
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=backoff_factor,
            raise_on_status=False
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set user agent
        self.session.headers.update({
            'User-Agent': 'op-etl/1.0 (geospatial-data-pipeline)'
        })

    def safe_get(self, url: str, timeout: int = DEFAULT_TIMEOUT,
                 **kwargs) -> Optional[requests.Response]:
        """Perform a safe GET request with recursion protection."""
        log.debug(f"[HTTP] Requesting: {url}")

        original_limit = None
        try:
            # Temporarily increase recursion limit if needed
            original_limit = sys.getrecursionlimit()
            if original_limit < DEFAULT_RECURSION_LIMIT:
                sys.setrecursionlimit(DEFAULT_RECURSION_LIMIT)
                log.debug(f"[HTTP] Increased recursion limit from {original_limit} to {DEFAULT_RECURSION_LIMIT}")

            response = self.session.get(url, timeout=timeout, stream=True, **kwargs)

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

        except RecursionError as e:
            log.error(f"[HTTP] Recursion error for {url}: {e}")
            return None
        except requests.exceptions.Timeout as e:
            log.error(f"[HTTP] Timeout for {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            log.error(f"[HTTP] Request failed for {url}: {e}")
            return None
        except Exception as e:
            log.error(f"[HTTP] Unexpected error for {url}: {e}")
            return None
        finally:
            # Restore original recursion limit
            if original_limit is not None and original_limit < DEFAULT_RECURSION_LIMIT:
                sys.setrecursionlimit(original_limit)


def safe_json_parse(content: Union[str, bytes], max_depth: int = MAX_JSON_DEPTH) -> Optional[Dict]:
    """Safely parse JSON with recursion depth protection."""
    original_limit = None
    try:
        if isinstance(content, bytes):
            content = content.decode('utf-8', errors='replace')

        # Check content size
        if len(content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning(f"[JSON] Content too large: {len(content)} bytes")
            return None

        # Temporarily increase recursion limit
        original_limit = sys.getrecursionlimit()
        if original_limit < DEFAULT_RECURSION_LIMIT:
            sys.setrecursionlimit(DEFAULT_RECURSION_LIMIT)

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
        finally:
            if original_limit is not None and original_limit < DEFAULT_RECURSION_LIMIT:
                sys.setrecursionlimit(original_limit)

    except Exception as e:
        log.error(f"[JSON] Unexpected error: {e}")
        return None


def safe_xml_parse(content: Union[str, bytes], max_elements: int = MAX_XML_ELEMENTS) -> Optional[ET.Element]:
    """Safely parse XML with recursion depth protection."""
    original_limit = None
    try:
        if isinstance(content, str):
            content = content.encode('utf-8')

        # Check content size
        if len(content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning(f"[XML] Content too large: {len(content)} bytes")
            return None

        # Temporarily increase recursion limit
        original_limit = sys.getrecursionlimit()
        if original_limit < DEFAULT_RECURSION_LIMIT:
            sys.setrecursionlimit(DEFAULT_RECURSION_LIMIT)

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
        finally:
            if original_limit is not None and original_limit < DEFAULT_RECURSION_LIMIT:
                sys.setrecursionlimit(original_limit)

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
