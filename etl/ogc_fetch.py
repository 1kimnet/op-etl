"""Fetch features from OGC API endpoints with a smile 😄.

This module provides a tiny helper to page through OGC API Features
collections and save each page into ``part_XXXX.geojson`` files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence
from urllib.parse import urljoin
import time

import requests


def _ensure_dir(path: Path) -> None:
    """Create ``path`` and parents if they do not exist."""
    path.mkdir(parents=True, exist_ok=True)


def _next_link(links: Optional[Sequence[dict]]) -> Optional[str]:
    """Return the ``href`` of the link with ``rel='next'`` if present."""
    if not links:
        return None
    for link in links:
        if not isinstance(link, dict):
            continue
        if link.get("rel") == "next" and link.get("href"):
            return link["href"]
    return None


def fetch_to_folder(
    base_collections_url: str,
    out_dir: Path,
    *,
    collections: Iterable[str],
    page_size: int = 1000,
    bbox: Optional[Sequence[float]] = None,
    bbox_sr: int = 3006,
    supports_bbox_crs: bool = False,
    timeout: tuple[int, int] = (10, 180),
) -> Path:
    """Download OGC API Features into ``out_dir`` as GeoJSON parts.

    Each collection is paged with ``limit`` and optional ``bbox`` filters.
    Pages are written verbatim. A tiny pause ⏳ keeps servers happy.
    """
    _ensure_dir(out_dir)
    parts = 0
    base = base_collections_url.rstrip("/") + "/"
    for coll in collections:
        items_url = urljoin(base, f"{coll}/items")
        params = {"limit": page_size}
        if bbox:
            params["bbox"] = ",".join(str(v) for v in bbox)
            if supports_bbox_crs:
                params["bbox-crs"] = f"EPSG:{bbox_sr}"
        url = items_url
        current = params
        while url:
            try:
                response = requests.get(url, params=current, timeout=timeout)
                response.raise_for_status()
                js = response.json()
            except requests.RequestException as e:
                print(f"Failed to fetch {url}: {e}")
                url = None
                continue
            features = js.get("features") or []
            if not features:
                break
            parts += 1
            out_file = out_dir / f"part_{parts:04d}.geojson"
            out_file.write_text(response.text, encoding="utf-8")
            url = _next_link(js.get("links"))
            current = None
            time.sleep(0.2)
    return out_dir
