"""
Robust HTTP utilities for OP-ETL pipeline.
Fixed recursion issues with response header handling.
"""

import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union

import requests

log = logging.getLogger(__name__)

# Constants for safety limits
MAX_RESPONSE_SIZE_MB = 100
DEFAULT_TIMEOUT = 60
MAX_JSON_DEPTH = 100  # Explicit JSON recursion depth limit
DEFAULT_RECURSION_LIMIT = 3000


@dataclass
class SimpleResponse:
    """A simple dataclass to hold response data, avoiding recursion issues."""
    status_code: int
    headers: Dict[str, str]  # Changed to str instead of Any
    content: bytes
    url: str

    def json(self) -> Any:
        """Safely parse response content as JSON."""
        return safe_json_parse(self.content)


class RecursionSafeSession:
    """A robust, simplified HTTP session that avoids recursion issues."""

    def __init__(self, max_retries: int = 3, backoff_factor: float = 0.5):
        # Ensure recursion limit is set high enough as a safety net
        current_limit = sys.getrecursionlimit()
        if current_limit < DEFAULT_RECURSION_LIMIT:
            sys.setrecursionlimit(DEFAULT_RECURSION_LIMIT)
            log.debug(f"[HTTP] Increased recursion limit from {current_limit} to {DEFAULT_RECURSION_LIMIT}")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def safe_get(self, url: str, timeout: int = DEFAULT_TIMEOUT,
                 **kwargs) -> Optional[SimpleResponse]:
        """Perform a safe GET request with retries and return a SimpleResponse."""
        log.debug(f"[HTTP] Requesting: {url}")

        for attempt in range(self.max_retries + 1):
            try:
                # Create session with proper cleanup
                session = requests.Session()
                try:
                    session.headers.update({
                        'User-Agent': 'op-etl/1.0 (geospatial-data-pipeline)',
                        'Accept': 'application/json, text/html, */*',
                        'Accept-Encoding': 'gzip, deflate',
                        'Connection': 'close'
                    })

                    # Make request without 'with' statement to avoid context manager issues
                    response = session.get(url, timeout=timeout, stream=True, **kwargs)

                    try:
                        response.raise_for_status()

                        # Check content length
                        content_length = response.headers.get('content-length')
                        if content_length and int(content_length) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                            log.warning(f"[HTTP] Response too large: {content_length} bytes")
                            return None

                        # Read content in chunks
                        content = b''
                        total_size = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                total_size += len(chunk)
                                if total_size > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                                    log.warning(f"[HTTP] Response size exceeded limit: {total_size} bytes")
                                    return None
                                content += chunk

                        log.debug(f"[HTTP] Success: {url} ({response.status_code}, {len(content)} bytes)")

                        # Extract headers safely - convert to simple dict of strings
                        # This avoids recursion issues with requests' CaseInsensitiveDict
                        safe_headers = {}
                        try:
                            for key, value in response.headers.items():
                                # Convert both key and value to strings
                                safe_headers[str(key)] = str(value)
                        except Exception as e:
                            log.debug(f"[HTTP] Header extraction warning: {e}")
                            # Fall back to minimal headers
                            safe_headers = {
                                'content-type': response.headers.get('content-type', ''),
                                'content-length': str(len(content))
                            }

                        # Create response object with safe headers
                        return SimpleResponse(
                            status_code=response.status_code,
                            headers=safe_headers,
                            content=content,
                            url=response.url
                        )
                    finally:
                        # Ensure response is closed
                        response.close()
                finally:
                    # Ensure session is closed
                    session.close()

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor * (2 ** attempt)
                    log.debug(f"[HTTP] Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    log.error(f"[HTTP] Request failed after {self.max_retries + 1} attempts for {url}: {e}")
                    return None
            except Exception as e:
                log.error(f"[HTTP] Unexpected error for {url}: {e}")
                return None

        return None


def safe_json_parse(content: Union[str, bytes], max_size_mb: int = 50) -> Optional[Dict[str, Any]]:
    """Safely parse JSON with size and complexity limits."""
    try:
        # Add null check
        if content is None:
            log.warning("[JSON] Content is None")
            return None

        if isinstance(content, bytes):
            content = content.decode('utf-8', errors='replace')

        # Check content size
        if len(content) > max_size_mb * 1024 * 1024:
            log.warning(f"[JSON] Content too large: {len(content)} bytes")
            return None

        # Check for obviously problematic content
        if isinstance(content, str):
            brace_count = sum(1 for c in content if c == '{')
            bracket_count = sum(1 for c in content if c == '[')
            if brace_count > 50000 or bracket_count > 50000:
                log.warning("[JSON] Content appears to have excessive nesting")
                return None

        # Parse with standard library
        try:
            data = json.loads(content)

            # Validate depth after parsing
            if _check_json_depth(data) > MAX_JSON_DEPTH:
                log.warning(f"[JSON] Exceeds maximum nesting depth of {MAX_JSON_DEPTH}")
                return None

            return data
        except json.JSONDecodeError as e:
            log.error(f"[JSON] Parse error: {e}")
            return None
        except RecursionError as e:
            log.error(f"[JSON] Recursion error during parsing: {e}")
            return None

    except Exception as e:
        log.error(f"[JSON] Unexpected error: {e}")
        return None


def _check_json_depth(obj: Any, current_depth: int = 0) -> int:
    """Helper function to check the depth of a JSON object."""
    if current_depth > MAX_JSON_DEPTH:
        return current_depth

    if isinstance(obj, dict):
        if not obj:
            return current_depth + 1
        return max(_check_json_depth(v, current_depth + 1) for v in obj.values())
    elif isinstance(obj, list):
        if not obj:
            return current_depth + 1
        return max(_check_json_depth(v, current_depth + 1) for v in obj)
    else:
        return current_depth


def safe_xml_parse(content: Union[str, bytes], max_elements: int = 10000) -> Optional[ET.Element]:
    """Safely parse XML with element count limits."""
    try:
        if content is None:
            log.warning("[XML] Content is None")
            return None

        if isinstance(content, str):
            content = content.encode('utf-8')

        # Check content size
        if len(content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning(f"[XML] Content too large: {len(content)} bytes")
            return None

        # Check for XML bombs
        if isinstance(content, bytes):
            entity_count = content.count(b'<!ENTITY')
            if entity_count > 0:
                log.warning("[XML] Potentially dangerous XML with ENTITY declarations")
                return None

            element_count = content.count(b'<')
            if element_count > max_elements:
                log.warning(f"[XML] Too many elements: {element_count} > {max_elements}")
                return None

            try:
                parser = ET.XMLParser(encoding='utf-8')
                root = ET.fromstring(content, parser=parser)
                return root
            except ET.ParseError as e:
                log.error(f"[XML] Parse error: {e}")
                return None
            except RecursionError as e:
                log.error(f"[XML] Recursion error during parsing: {e}")
                return None
        else:
            log.warning("[XML] Content is not bytes after conversion")
            return None

    except Exception as e:
        log.error(f"[XML] Unexpected error: {e}")
        return None


def download_with_retries(url: str, output_path: Path,
                          max_retries: int = 3,
                          timeout: int = DEFAULT_TIMEOUT) -> bool:
    """Download a file with retries and safety checks."""
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"[DOWNLOAD] Attempt {attempt}/{max_retries}: {url}")

            # Create a fresh session for each download attempt
            session = requests.Session()
            try:
                session.headers.update({
                    'User-Agent': 'op-etl/1.0 (geospatial-data-pipeline)',
                    'Accept': '*/*',
                    'Connection': 'close'
                })

                # Use streaming to handle large files
                response = session.get(url, stream=True, timeout=timeout)
                try:
                    response.raise_for_status()

                    # Get content length if available
                    content_length = response.headers.get('content-length')
                    if content_length and int(content_length) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                        log.warning(f"[DOWNLOAD] File too large: {content_length} bytes")
                        return False

                    # Create directory if needed
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    # Stream directly to file
                    with open(output_path, 'wb') as f:
                        total_size = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                total_size += len(chunk)
                                if total_size > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
                                    log.warning(f"[DOWNLOAD] Size exceeded limit: {total_size} bytes")
                                    f.close()
                                    if output_path.exists():
                                        try:
                                            os.unlink(output_path)
                                        except OSError:
                                            pass
                                    return False
                                f.write(chunk)
                finally:
                    response.close()
            finally:
                session.close()

            # Verify download
            if output_path.exists() and output_path.stat().st_size > 0:
                log.info(f"[DOWNLOAD] Success: {output_path.name} ({output_path.stat().st_size} bytes)")
                return True
            else:
                log.warning("[DOWNLOAD] Downloaded file is empty or missing")

        except Exception as e:
            log.warning(f"[DOWNLOAD] Attempt {attempt} failed: {e}")

        # Wait before retry
        if attempt < max_retries:
            wait_time = 0.5 * (2 ** (attempt - 1))
            log.info(f"[DOWNLOAD] Waiting {wait_time:.1f}s before retry...")
            time.sleep(wait_time)

    log.error(f"[DOWNLOAD] Failed after {max_retries} attempts: {url}")
    return False


def validate_response_content(response: SimpleResponse) -> bool:
    """Basic response validation for SimpleResponse."""
    try:
        content_type = response.headers.get('content-type', '').lower()

        if len(response.content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning(f"[VALIDATE] Response too large: {len(response.content)} bytes")
            return False

        if 'text/html' in content_type and b'error' in response.content.lower()[:1024]:
            log.warning("[VALIDATE] Response appears to be an error page")
            return False

        return True

    except Exception as e:
        log.warning(f"[VALIDATE] Validation error: {e}")
        return False
