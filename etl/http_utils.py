"""
http_utils.py — Reliable HTTP utilities for OP-ETL (backward-compatible)

Design:
- urllib3-only core (no requests)
- No implicit Portal probing, redirects disabled by default
- Clear logging; fail fast, fail clearly
- Safe JSON/XML parsing with size and depth limits
- Backward-compatibility shims for:
  - RecursionSafeSession.safe_get(...)
  - download_with_retries(...)
  - safe_xml_parse(...)
  - validate_response_content(...)

Config knobs can be provided via the HttpClient(cfg=...) dict.
"""

from __future__ import annotations

import json
import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union

import urllib3
from urllib3 import PoolManager
from urllib3.util.retry import Retry
from urllib3.util.timeout import Timeout

# --------------------------------------------------------------------------------------
# Constants / defaults (match previous file names so other modules won't whine)
# --------------------------------------------------------------------------------------

MAX_RESPONSE_SIZE_MB = 100            # in-memory response cap
MAX_DOWNLOAD_SIZE_MB = 5_000          # download cap
DEFAULT_TIMEOUT = 60                  # seconds (read timeout)
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_READ_TIMEOUT = 60.0
DEFAULT_TOTAL_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_ALLOWED_METHODS = frozenset({"GET", "HEAD"})
DEFAULT_STATUS_FORCELIST = (429, 500, 502, 503, 504)
MAX_JSON_DEPTH = 100                  # kept for parity with older name
DEFAULT_FOLLOW_REDIRECTS = False

# Legacy recursion-related constants/functions kept for import-compat
DEFAULT_RECURSION_LIMIT = 3000

def get_current_recursion_depth() -> int:
    """Legacy shim: returns a constant depth. We don't play recursion games here."""
    return 0

def check_recursion_safety(threshold_ratio: float = 0.8) -> bool:
    """Legacy shim: always safe. Kept to avoid breaking imports."""
    return True

# Type helper
BytesLike = Union[str, bytes, bytearray, memoryview]

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# Normalizers and small helpers
# --------------------------------------------------------------------------------------

def _normalize_bytes(value: Optional[BytesLike]) -> Optional[bytes]:
    """Normalize str/bytes/bytearray/memoryview to bytes, or None."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8", errors="replace")
    return str(value).encode("utf-8", errors="replace")

def _to_text(value: Optional[BytesLike]) -> Optional[str]:
    """Normalize str/bytes/bytearray/memoryview to str, or None."""
    raw = _normalize_bytes(value)
    if raw is None:
        return None
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return raw.decode("latin-1", errors="replace")

def _json_depth(obj: Any, depth: int = 0, max_depth: int = MAX_JSON_DEPTH) -> int:
    """Return observed JSON depth; stop early if above max_depth."""
    if depth > max_depth:
        return depth
    if isinstance(obj, dict):
        if not obj:
            return depth + 1
        return max(_json_depth(v, depth + 1, max_depth) for v in obj.values())
    if isinstance(obj, list):
        if not obj:
            return depth + 1
        return max(_json_depth(v, depth + 1, max_depth) for v in obj)
    return depth

def _bytes_too_large(data: bytes, limit_mb: int) -> bool:
    return len(data) > limit_mb * 1024 * 1024

# --------------------------------------------------------------------------------------
# Response container
# --------------------------------------------------------------------------------------

@dataclass
class SimpleResponse:
    status_code: int
    headers: Dict[str, str]
    content: bytes
    url: str

    def text(self) -> str:
        return _to_text(self.content) or ""

    def json(self, max_depth: int = MAX_JSON_DEPTH) -> Optional[Dict[str, Any]]:
        return safe_json_parse(self.content, max_depth=max_depth)

# --------------------------------------------------------------------------------------
# Core client
# --------------------------------------------------------------------------------------

class HttpClient:
    """
    Thin wrapper around urllib3.PoolManager with sensible defaults.

    - Retries: idempotent methods with backoff; respects Retry-After
    - Timeouts: bounded via urllib3.Timeout
    - Redirects: disabled by default (avoid Portal sign-in flows)
    - Size caps: guard rails for responses and downloads
    """

    def __init__(
        self,
        *,
        total_retries: int = DEFAULT_TOTAL_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        allowed_methods = DEFAULT_ALLOWED_METHODS,
        status_forcelist = DEFAULT_STATUS_FORCELIST,
        follow_redirects: bool = DEFAULT_FOLLOW_REDIRECTS,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        num_pools: int = 20,
        headers: Optional[Dict[str, str]] = None,
        cfg: Optional[Dict[str, Any]] = None
    ) -> None:

        if cfg:
            total_retries   = cfg.get("http_total_retries", total_retries)
            backoff_factor  = cfg.get("http_backoff_factor", backoff_factor)
            allowed_methods = frozenset(cfg.get("http_allowed_methods", list(allowed_methods)))
            status_forcelist= tuple(cfg.get("http_status_forcelist", list(status_forcelist)))
            follow_redirects= bool(cfg.get("http_follow_redirects", follow_redirects))
            connect_timeout = float(cfg.get("http_connect_timeout", connect_timeout))
            read_timeout    = float(cfg.get("http_read_timeout", read_timeout))
            num_pools       = int(cfg.get("http_num_pools", num_pools))

        self.follow_redirects = follow_redirects

        retry = Retry(
            total=total_retries,
            connect=total_retries,
            read=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=allowed_methods,
            raise_on_status=False,
            respect_retry_after_header=True
        )

        self._timeout = Timeout(connect=connect_timeout, read=read_timeout)
        self._http: PoolManager = urllib3.PoolManager(
            num_pools=num_pools,
            retries=retry,
            timeout=self._timeout
        )

        self._default_headers = {
            "User-Agent": "op-etl/1.0 (geospatial-data-pipeline)",
            "Accept": "application/json, text/xml, text/plain, */*",
            "Accept-Encoding": "gzip, deflate"
        }
        if headers:
            self._default_headers.update(headers)

    def _build_url(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        from urllib.parse import urlencode
        qs = urlencode(params, doseq=True)
        return f"{url}&{qs}" if "?" in url else f"{url}?{qs}"

    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        if not headers:
            return dict(self._default_headers)
        out = dict(self._default_headers)
        out.update(headers)
        return out

    def _resolve_redirect_flag(self, allow_redirects: Optional[bool]) -> bool:
        if allow_redirects is None:
            return self.follow_redirects
        return bool(allow_redirects)

    # ---------------------- Public methods ----------------------

    def get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        allow_redirects: Optional[bool] = None,
        max_response_mb: int = MAX_RESPONSE_SIZE_MB
    ) -> Optional[SimpleResponse]:
        try:
            full_url = self._build_url(url, params)
            hdrs = self._merge_headers(headers)
            follow = self._resolve_redirect_flag(allow_redirects)

            log.info("GET %s", full_url)
            r = self._http.request("GET", full_url, headers=hdrs, redirect=follow, preload_content=False)
            try:
                status = int(r.status or 0)

                if not follow and 300 <= status < 400:
                    loc = r.headers.get("Location")
                    log.error("Redirect blocked: %s -> %s", full_url, loc)
                    r.release_conn()
                    return None

                content = r.read()
                r.release_conn()

                if _bytes_too_large(content, max_response_mb):
                    log.warning("Response too large: %s bytes (> %s MB)", len(content), max_response_mb)
                    return None

                headers_out = {str(k).lower(): str(v) for k, v in r.headers.items()}
                headers_out["content-length"] = str(len(content))

                return SimpleResponse(
                    status_code=status or 200,
                    headers=headers_out,
                    content=content,
                    url=getattr(r, "geturl", lambda: full_url)()
                )
            finally:
                try:
                    r.close()
                except Exception:
                    pass

        except urllib3.exceptions.MaxRetryError as e:
            log.error("HTTP retries exhausted for %s: %s", url, e)
            return None
        except Exception as e:
            log.error("HTTP error for %s: %s", url, e)
            return None

    def get_json(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        allow_redirects: Optional[bool] = None,
        max_response_mb: int = MAX_RESPONSE_SIZE_MB,
        max_json_depth: int = MAX_JSON_DEPTH
    ) -> Optional[Dict[str, Any]]:
        resp = self.get(url, params=params, headers=headers, allow_redirects=allow_redirects, max_response_mb=max_response_mb)
        if not resp:
            return None
        return safe_json_parse(resp.content, max_depth=max_json_depth)

    def get_xml(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        allow_redirects: Optional[bool] = None,
        max_response_mb: int = MAX_RESPONSE_SIZE_MB,
        max_elements: int = 10000
    ) -> Optional[ET.Element]:
        resp = self.get(url, params=params, headers=headers, allow_redirects=allow_redirects, max_response_mb=max_response_mb)
        if not resp:
            return None
        return safe_xml_parse(resp.content, max_elements=max_elements)

    def download_file(
        self,
        url: str,
        output_path: Path,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        allow_redirects: Optional[bool] = None,
        max_download_mb: int = MAX_DOWNLOAD_SIZE_MB,
        chunk_size: int = 1 << 15  # 32 KiB
    ) -> bool:
        try:
            full_url = self._build_url(url, params)
            hdrs = self._merge_headers(headers)
            follow = self._resolve_redirect_flag(allow_redirects)

            log.info("DOWNLOAD %s -> %s", full_url, output_path)
            r = self._http.request("GET", full_url, headers=hdrs, redirect=follow, preload_content=False)
            try:
                status = int(r.status or 0)

                if not follow and 300 <= status < 400:
                    loc = r.headers.get("Location")
                    log.error("Redirect blocked (download): %s -> %s", full_url, loc)
                    r.release_conn()
                    return False

                # size hint by header
                try:
                    length_header = r.headers.get("Content-Length")
                    if length_header and int(length_header) > max_download_mb * 1024 * 1024:
                        log.warning("File too large by header: %s bytes", length_header)
                        r.release_conn()
                        return False
                except Exception:
                    pass

                output_path.parent.mkdir(parents=True, exist_ok=True)

                total = 0
                with open(output_path, "wb") as f:
                    while True:
                        chunk = r.read(chunk_size)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_download_mb * 1024 * 1024:
                            log.warning("Download exceeded limit: %s bytes (> %s MB)", total, max_download_mb)
                            try:
                                f.close()
                            finally:
                                try:
                                    output_path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                            r.release_conn()
                            return False
                        f.write(chunk)

                r.release_conn()

                ok = output_path.exists() and output_path.stat().st_size > 0
                if ok:
                    log.info("DOWNLOAD OK: %s (%s bytes)", output_path.name, output_path.stat().st_size)
                else:
                    log.warning("Downloaded file missing or empty: %s", output_path)
                return ok

            finally:
                try:
                    r.close()
                except Exception:
                    pass

        except urllib3.exceptions.MaxRetryError as e:
            log.error("HTTP retries exhausted for %s: %s", url, e)
            return False
        except Exception as e:
            log.error("Download error for %s: %s", url, e)
            return False

# --------------------------------------------------------------------------------------
# Safe parsers (module-level)
# --------------------------------------------------------------------------------------

def safe_json_parse(content: BytesLike, *, max_size_mb: int = 50, max_depth: int = MAX_JSON_DEPTH) -> Optional[Dict[str, Any]]:
    """Safely parse JSON with size and depth limits."""
    try:
        text = _to_text(content)
        if text is None:
            log.warning("[JSON] Content is None")
            return None

        if not text.strip():
            log.warning("[JSON] Content is empty or whitespace-only")
            return None

        if len(text) > max_size_mb * 1024 * 1024:
            log.warning("[JSON] Content too large: %s bytes", len(text))
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

        data = json.loads(text)

        if _json_depth(data, 0, max_depth) > max_depth:
            log.warning("[JSON] Exceeds maximum nesting depth of %s", max_depth)
            return None

        return data

    except json.JSONDecodeError as e:
        log.error("[JSON] Parse error: %s", e)
        return None
    except Exception as e:
        log.error("[JSON] Unexpected error: %s", e)
        return None

def safe_xml_parse(content: BytesLike, *, max_elements: int = 10_000, max_size_mb: int = MAX_RESPONSE_SIZE_MB) -> Optional[ET.Element]:
    """Safely parse XML with element count and size limits."""
    try:
        raw = _normalize_bytes(content)
        if raw is None:
            log.warning("[XML] Content is None")
            return None

        if _bytes_too_large(raw, max_size_mb):
            log.warning("[XML] Content too large: %s bytes", len(raw))
            return None

        if raw.count(b"<!ENTITY") > 0:
            log.warning("[XML] Potentially dangerous XML with ENTITY declarations")
            return None

        if raw.count(b"<") > max_elements:
            log.warning("[XML] Too many elements: > %s", max_elements)
            return None

        parser = ET.XMLParser(encoding="utf-8")
        return ET.fromstring(raw, parser=parser)

    except ET.ParseError as e:
        log.error("[XML] Parse error: %s", e)
        return None
    except Exception as e:
        log.error("[XML] Unexpected error: %s", e)
        return None

def validate_response_content(response: SimpleResponse) -> bool:
    """Basic response validation for SimpleResponse (kept for backward-compat)."""
    try:
        if not response.content or len(response.content) == 0:
            log.warning("[VALIDATE] Response content is empty")
            return False

        content_type = response.headers.get("content-type", "").lower()

        if len(response.content) > MAX_RESPONSE_SIZE_MB * 1024 * 1024:
            log.warning("[VALIDATE] Response too large: %s bytes", len(response.content))
            return False

        if "text/html" in content_type and b"error" in response.content.lower()[:1024]:
            log.warning("[VALIDATE] Response appears to be an error page")
            return False

        return True
    except Exception as e:
        log.warning("[VALIDATE] Validation error: %s", e)
        return False

# --------------------------------------------------------------------------------------
# Legacy compatibility layer
# --------------------------------------------------------------------------------------

class RecursionSafeSession:
    """
    Backward-compatible shim around HttpClient.
    Provides .safe_get(...) returning SimpleResponse like the old class.
    """

    def __init__(self, max_retries: int = 3, backoff_factor: float = 0.5):
        self._client = HttpClient(
            total_retries=max_retries,
            backoff_factor=backoff_factor,
            follow_redirects=False,
            connect_timeout=DEFAULT_CONNECT_TIMEOUT,
            read_timeout=DEFAULT_READ_TIMEOUT,
        )

    def safe_get(self, url: str, timeout: int = DEFAULT_TIMEOUT, **kwargs) -> Optional[SimpleResponse]:
        # Map legacy args to new client
        params = kwargs.get("params")
        headers = kwargs.get("headers")
        allow_redirects = kwargs.get("allow_redirects", None)
        # HttpClient already has read timeout set; urllib3 Timeout is bound at PoolManager level
        return self._client.get(
            url,
            params=params,
            headers=headers,
            allow_redirects=allow_redirects,
            max_response_mb=MAX_RESPONSE_SIZE_MB
        )

def download_with_retries(
    url: str,
    output_path: Path,
    *,
    max_retries: int = 3,
    timeout: int = DEFAULT_TIMEOUT
) -> bool:
    """
    Backward-compatible function. Uses HttpClient.download_file with exponential backoff.
    """
    client = HttpClient(read_timeout=timeout)
    for attempt in range(1, max_retries + 1):
        try:
            log.info("[DOWNLOAD] Attempt %s/%s: %s", attempt, max_retries, url)
            ok = client.download_file(url, output_path)
            if ok:
                return True
            raise RuntimeError("download failed")
        except Exception as e:
            log.warning("[DOWNLOAD] Attempt %s failed: %s", attempt, e)
            if attempt < max_retries:
                wait = DEFAULT_BACKOFF_FACTOR * (2 ** (attempt - 1))
                log.info("[DOWNLOAD] Waiting %.1fs before retry...", wait)
                time.sleep(wait)
    log.error("[DOWNLOAD] Failed after %s attempts: %s", max_retries, url)
    return False

# --------------------------------------------------------------------------------------
# Convenience wrappers (optional)
# --------------------------------------------------------------------------------------

_default_client: Optional[HttpClient] = None

def _client() -> HttpClient:
    global _default_client
    if _default_client is None:
        _default_client = HttpClient()
    return _default_client

def http_get(url: str, **kwargs) -> Optional[SimpleResponse]:
    return _client().get(url, **kwargs)

def http_get_json(url: str, **kwargs) -> Optional[Dict[str, Any]]:
    return _client().get_json(url, **kwargs)

def http_get_xml(url: str, **kwargs) -> Optional[ET.Element]:
    return _client().get_xml(url, **kwargs)

def http_download(url: str, output_path: Path, **kwargs) -> bool:
    return _client().download_file(url, output_path, **kwargs)

# --------------------------------------------------------------------------------------
# Self-test (delete or keep for sanity checks)
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    s = RecursionSafeSession()
    r = s.safe_get("https://httpbin.org/get", params={"q": "gis"})
    if r:
        log.info("Status: %s, bytes: %s", r.status_code, len(r.content))
        data = r.json()
        log.info("Keys: %s", list(data.keys()) if data else "none")

    out = Path("tmp/example.json")
    ok = download_with_retries("https://httpbin.org/json", out, max_retries=2)
    log.info("Download ok: %s", ok)
