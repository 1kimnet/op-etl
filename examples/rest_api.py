# etl/handlers/rest_api.py
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from ..exceptions import DataError, ErrorContext, NetworkError, SourceError
from ..models import Source
from ..utils import ensure_dirs, paths
from ..utils.circuit_breaker import CircuitBreaker, http_circuit_breaker
from ..utils.concurrent import get_layer_downloader
from ..utils.http_session import HTTPSessionHandler
from ..utils.naming import sanitize_for_filename
from ..utils.retry import RetryConfig, retry_with_backoff, smart_retry

log = logging.getLogger(__name__)

# Default BBOX from your document (SWEREF99 TM)
DEFAULT_BBOX_COORDS = "586206,6551160,647910,6610992"
DEFAULT_BBOX_SR = "3006"
DEFAULT_MAX_RECORDS = 5000

# Output format constants
GEOJSON_FORMAT = "geojson"
SWEREF99_TM_WKID = 3006


class RestApiDownloadHandler(HTTPSessionHandler):
    """Handles downloading data from ESRI REST API MapServer and FeatureServer Query endpoints."""

    def __init__(self, src: Source,
                 global_config: Optional[Dict[str, Any]] = None):
        self.src = src
        self.global_config = global_config or {}
        ensure_dirs()

        # Initialize HTTP session with connection pooling
        timeout = self.global_config.get("timeout", 30)
        self.timeout = timeout
        super().__init__(
            base_url=src.url,
            pool_connections=5,
            pool_maxsize=10,
            max_retries=3,
            timeout=timeout,
        )

        # Initialize retry configuration
        retry_config = self.global_config.get("retry", {})
        self.retry_config = RetryConfig(
            max_attempts=retry_config.get("max_attempts", 3),
            base_delay=retry_config.get("base_delay", 1.0),
            backoff_factor=retry_config.get("backoff_factor", 2.0),
            max_delay=retry_config.get("max_delay", 300.0),
        )

        # Initialize circuit breaker for this service
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=retry_config.get("circuit_breaker_threshold", 5),
            recovery_timeout=retry_config.get("circuit_breaker_timeout", 60.0),
            expected_exceptions=[Exception],
        )

        log.info(
            "🚀 Initializing RestApiDownloadHandler for source: %s",
            self.src.name)

    @retry_with_backoff()
    def _get_service_metadata(
            self, service_url: str) -> Optional[Dict[str, Any]]:
        """Fetches base metadata for the service (MapServer/FeatureServer) with retries."""
        return self._fetch_service_metadata_impl(service_url)

    @smart_retry("fetch_service_metadata")
    @http_circuit_breaker("rest_api_metadata", failure_threshold=3)
    def _fetch_service_metadata_impl(self, service_url: str) -> Dict[str, Any]:
        """Implementation of service metadata fetching with circuit breaker."""
        params = {"f": "json"}

        try:
            response = self.session.get(
                service_url, params=params, timeout=self.timeout
            )

            # Handle different HTTP status codes appropriately
            if response.status_code == 429:
                raise NetworkError(
                    f"Rate limit exceeded for {service_url}",
                    status_code=429,
                    url=service_url,
                    context=ErrorContext(
                        source_name=self.src.name,
                        url=service_url,
                        operation="fetch_metadata",
                    ),
                )
            elif 500 <= response.status_code < 600:
                raise SourceError(
                    f"Service temporarily unavailable: {response.status_code}",
                    available=False,
                    context=ErrorContext(
                        source_name=self.src.name,
                        url=service_url,
                        operation="fetch_metadata",
                        metadata={"status_code": response.status_code},
                    ),
                )
            elif 400 <= response.status_code < 500:
                raise NetworkError(
                    f"Client error: {response.status_code} {response.reason}",
                    status_code=response.status_code,
                    url=service_url,
                    context=ErrorContext(
                        source_name=self.src.name,
                        url=service_url,
                        operation="fetch_metadata",
                    ),
                )

            response.raise_for_status()

            try:
                return response.json()
            except json.JSONDecodeError as e:
                raise DataError(
                    f"Invalid JSON response from {service_url}: {e}",
                    data_type="json",
                    context=ErrorContext(
                        source_name=self.src.name,
                        url=service_url,
                        operation="parse_json",
                    ),
                ) from e

        except requests.exceptions.Timeout as e:
            raise NetworkError(
                f"Timeout fetching metadata from {service_url}",
                timeout=self.timeout,
                url=service_url,
                context=ErrorContext(
                    source_name=self.src.name,
                    url=service_url,
                    operation="fetch_metadata",
                ),
            ) from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(
                f"Connection error fetching metadata from {service_url}",
                url=service_url,
                context=ErrorContext(
                    source_name=self.src.name,
                    url=service_url,
                    operation="fetch_metadata",
                    metadata={"error": str(e)},
                ),
            ) from e
        except requests.exceptions.RequestException as e:
            raise NetworkError(
                f"Request failed for {service_url}: {e}",
                url=service_url,
                context=ErrorContext(
                    source_name=self.src.name,
                    url=service_url,
                    operation="fetch_metadata",
                    metadata={"error": str(e)},
                ),
            ) from e

    def _get_layer_metadata(self, layer_url: str) -> Optional[Dict[str, Any]]:
        """Fetches metadata for a specific layer."""
        try:
            params = {"f": "json"}
            response = self.session.get(
                layer_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            log.error(
                "❌ Failed to fetch layer metadata from %s: %s",
                layer_url,
                e)
            return None

    def _prepare_query_params(self) -> Dict[str, Any]:
        """Prepare common query parameters for REST requests."""
        use_bbox = self.global_config.get("use_bbox_filter", False)
        bbox_coords = self.src.raw.get("bbox", DEFAULT_BBOX_COORDS)
        bbox_sr = self.src.raw.get("bbox_sr", DEFAULT_BBOX_SR)

        params: Dict[str, Any] = {
            "where": self.src.raw.get("where_clause", "1=1"),
            "outFields": self.src.raw.get("out_fields", "*"),
            "returnGeometry": "true",
            "f": self.src.raw.get("format", "geojson"),
        }

        if use_bbox and bbox_coords:
            params["geometry"] = bbox_coords
            params["geometryType"] = "esriGeometryEnvelope"
            params["inSR"] = bbox_sr
            params["spatialRel"] = "esriSpatialRelIntersects"
            log.debug("Applying BBOX: %s (SRID: %s)", bbox_coords, bbox_sr)

        return params

    @smart_retry("request_page")
    def _request_page(
        self,
        query_url: str,
        params: Dict[str, Any],
        layer_name_sanitized: str,
        page_num: int,
    ) -> Optional[Dict[str, Any]]:
        """Execute a paginated request and return the JSON payload."""
        try:
            response_obj = self.session.get(
                query_url, params=params, timeout=120)
            response_obj.raise_for_status()
            return response_obj.json()
        except requests.exceptions.RequestException as e:
            raise NetworkError(
                f"Failed to download data for layer {layer_name_sanitized}, page {page_num}: {e}",
                url=query_url,
                context=ErrorContext(
                    source_name=self.src.name,
                    url=query_url,
                    operation="request_page",
                    metadata={
                        "layer": layer_name_sanitized,
                        "page": page_num},
                ),
            ) from e
        except json.JSONDecodeError as e:
            raise DataError(
                f"Failed to decode JSON for layer {layer_name_sanitized}, page {page_num}: {e}",
                data_type="json",
                context=ErrorContext(
                    source_name=self.src.name,
                    url=query_url,
                    operation="parse_json",
                    metadata={
                        "layer": layer_name_sanitized,
                        "page": page_num},
                ),
            ) from e

    def _append_features(
        self,
        data: Dict[str, Any],
        layer_name_sanitized: str,
        page_num: int,
        all_features: List[Dict[str, Any]],
        current_offset: int,
        max_record_count: int,
        effective_page_limit: int,
    ) -> tuple[bool, int, int]:
        """Append page features and determine if pagination should continue."""
        features = data.get("features", [])
        if not features:
            if page_num == 1:
                log.debug(
                    "ℹ️ No features returned for layer %s with current parameters.",
                    layer_name_sanitized,
                )
            else:
                log.debug(
                    "🏁 All features retrieved for layer %s (empty page).",
                    layer_name_sanitized,
                )
            return True, current_offset, 0

        all_features.extend(features)
        features_len = len(features)

        exceeded_transfer_limit = data.get("exceededTransferLimit", False)

        if exceeded_transfer_limit:
            log.debug(
                "⚠️ Exceeded transfer limit for layer %s, fetching next page.",
                layer_name_sanitized,
            )
            return False, current_offset + features_len, features_len

        if (
            features_len < effective_page_limit and effective_page_limit > 0
        ) or max_record_count == 0:
            log.debug(
                "🏁 All features likely retrieved for layer %s (less than page limit or server maxRecordCount is 0).",
                layer_name_sanitized,
            )
            return True, current_offset + features_len, features_len

        return False, current_offset + features_len, features_len

    def _write_output_data(
        self,
        output_path: Path,
        final_output_data: Dict[str, Any],
        layer_name_sanitized: str,
        features_written_total: int,
    ) -> None:
        """Persist fetched features to disk."""
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(final_output_data, f, ensure_ascii=False, indent=2)
            log.info(
                "✅ %s: %d features",
                layer_name_sanitized,
                features_written_total)
            log.debug(
                "💾 Successfully saved %d features for layer %s to %s",
                features_written_total,
                layer_name_sanitized,
                output_path,
            )
        except IOError as e:
            log.error(
                "❌ Failed to write data for layer %s to %s: %s",
                layer_name_sanitized,
                output_path,
                e,
            )

    def fetch(self) -> None:
        """Main fetch method for REST API sources."""
        if not self.src.enabled:
            log.info(
                "⏭️ Source '%s' (REST API) is disabled, skipping fetch.",
                self.src.name)
            return

        log.info(
            "🌐 Processing REST API source: '%s' from URL: %s",
            self.src.name,
            self.src.url,
        )

        service_meta = self._get_service_metadata(self.src.url)
        if not service_meta:
            log.error(
                "❌ Could not retrieve service metadata for %s. Skipping source.",
                self.src.name,
            )
            return

        layers_to_iterate_final: List[Dict[str, Any]] = []
        configured_layer_ids_from_yaml = self.src.raw.get("layer_ids")

        # Create a lookup for all layer details from the service metadata
        metadata_layers_details = {
            str(lyr.get("id")): lyr
            for lyr in service_meta.get("layers", [])
            if "id" in lyr
        }

        if configured_layer_ids_from_yaml:
            log.info(
                "Found explicit layer_ids in config: %s for source '%s'. Processing only these.",
                configured_layer_ids_from_yaml,
                self.src.name,
            )
            if not isinstance(configured_layer_ids_from_yaml, list):
                configured_layer_ids_from_yaml = [
                    configured_layer_ids_from_yaml]

            for lid_val in configured_layer_ids_from_yaml:
                lid_str = str(lid_val)
                layer_detail = metadata_layers_details.get(lid_str)

                if layer_detail:
                    layer_name = layer_detail.get("name", f"layer_{lid_str}")
                    layers_to_iterate_final.append(
                        {"id": lid_str, "name": layer_name, "metadata": layer_detail}
                    )
                else:
                    log.warning(
                        "Layer ID '%s' specified in config for source '%s' "
                        "was not found in the service's layer metadata list. "
                        "Will attempt to query it using this ID and a placeholder name.",
                        lid_str,
                        self.src.name,
                    )
                    layers_to_iterate_final.append(
                        {
                            "id": lid_str,
                            "name": f"layer_{lid_str}_cfg_only",
                            "metadata": None,
                        }
                    )
        elif "layers" in service_meta:
            log.info(
                "No explicit layer_ids in config for source '%s'. Discovering all layers from service metadata.",
                self.src.name,
            )
            for layer_id_str, layer_detail_from_meta in metadata_layers_details.items():
                layers_to_iterate_final.append(
                    {
                        "id": layer_id_str,
                        "name": layer_detail_from_meta.get(
                            "name", f"layer_{layer_id_str}"
                        ),
                        "metadata": layer_detail_from_meta,
                    }
                )

        # Fallback for single-layer FeatureServer
        elif (
            not layers_to_iterate_final
            and "/featureserver" in self.src.url.lower()
            and service_meta.get("type") == "Feature Layer"
        ):
            log.info(
                "Source '%s' appears to be a single-layer FeatureServer and no layers were previously identified. "
                "Adding layer from service root or URL.", self.src.name, )
            layer_id_from_url_match = re.search(r"/(\d+)/?$", self.src.url)
            fs_layer_id = (
                layer_id_from_url_match.group(1)
                if layer_id_from_url_match
                else service_meta.get("id", "0")
            )
            fs_layer_id_str = str(fs_layer_id)
            fs_layer_name = service_meta.get(
                "name", f"feature_layer_{fs_layer_id_str}")
            layers_to_iterate_final.append(
                {"id": fs_layer_id_str, "name": fs_layer_name, "metadata": service_meta}
            )

        if not layers_to_iterate_final:
            log.warning(
                "⚠️ No layers identified or specified to query for source '%s'. "
                "Check service metadata and `layer_ids` config.", self.src.name, )
            return

        log_layer_ids_to_query = [layer["id"]
                                  for layer in layers_to_iterate_final]
        log.info(
            "Source '%s': Will attempt to query %d layer(s): %s",
            self.src.name,
            len(layers_to_iterate_final),
            log_layer_ids_to_query,
        )

        # Use concurrent downloads for multiple layers
        if len(layers_to_iterate_final) > 1:
            self._fetch_layers_concurrent(layers_to_iterate_final)
        else:
            # Single layer - use original sequential approach
            for layer_info_to_query in layers_to_iterate_final:
                self._fetch_layer_data(
                    layer_info=layer_info_to_query,
                    layer_metadata_from_service=layer_info_to_query.get("metadata"),
                )

    def _fetch_layers_concurrent(
            self, layers_to_iterate: List[Dict[str, Any]]) -> None:
        """Fetch multiple layers concurrently for improved performance."""
        log.info(
            "🚀 Starting concurrent download of %d layers",
            len(layers_to_iterate))

        # Get concurrent downloader
        downloader = get_layer_downloader()

        # Enable parallel processing based on configuration
        use_concurrent = self.global_config.get(
            "enable_concurrent_downloads", True)
        max_workers = self.global_config.get("concurrent_download_workers", 5)

        if not use_concurrent:
            log.info("⚠️ Concurrent downloads disabled, falling back to sequential")
            for layer_info in layers_to_iterate:
                self._fetch_layer_data(
                    layer_info=layer_info,
                    layer_metadata_from_service=layer_info.get("metadata"),
                )
            return

        # Update worker count if specified
        if max_workers != downloader.manager.max_workers:
            downloader.manager.max_workers = max_workers

        # Execute concurrent downloads
        results = downloader.download_layers_concurrent(
            handler=self,
            layers_info=layers_to_iterate,
            fail_fast=self.global_config.get("fail_fast_downloads", False),
        )

        # Process results and log statistics
        successful_downloads = sum(1 for r in results if r.success)
        failed_downloads = len(results) - successful_downloads

        log.info(
            "🏁 Concurrent downloads completed: %d successful, %d failed",
            successful_downloads,
            failed_downloads,
        )

        # Log any failures
        for result in results:
            if not result.success:
                layer_name = result.metadata.get("task_name", "unknown")
                log.error(
                    "❌ Layer download failed: %s - %s",
                    layer_name,
                    result.error)

    def _determine_max_record_count(
        self,
        layer_id: str,
        layer_meta: Optional[Dict[str, Any]],
    ) -> tuple[int, Optional[Dict[str, Any]]]:
        """Resolve maxRecordCount using config or service metadata."""
        max_record_count_from_config = self.src.raw.get("max_record_count")
        if max_record_count_from_config is not None:
            try:
                max_record_count = int(max_record_count_from_config)
                log.debug(
                    "Using max_record_count from config: %d",
                    max_record_count)
                return max_record_count, layer_meta
            except ValueError:
                log.warning(
                    "Invalid 'max_record_count' in source.raw: '%s'. Falling back to metadata.",
                    max_record_count_from_config,
                )

        if not layer_meta:
            layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
            log.debug(
                "Fetching specific layer metadata for layer ID %s to determine maxRecordCount.",
                layer_id,
            )
            layer_meta = self._get_layer_metadata(layer_metadata_url)

        if layer_meta:
            if layer_meta.get("maxRecordCount") is not None:
                max_record_count = layer_meta["maxRecordCount"]
                log.debug(
                    "Service metadata maxRecordCount: %d",
                    max_record_count)
            elif layer_meta.get("standardMaxRecordCount") is not None:
                max_record_count = layer_meta["standardMaxRecordCount"]
                log.debug(
                    "Service metadata standardMaxRecordCount: %d",
                    max_record_count,
                )
            else:
                max_record_count = DEFAULT_MAX_RECORDS
                log.debug(
                    "maxRecordCount not found in layer metadata, using default: %d",
                    max_record_count,
                )
        else:
            max_record_count = 2000
            log.warning(
                "Could not fetch specific layer metadata for maxRecordCount, using default: %d",
                max_record_count,
            )

        if not isinstance(max_record_count, int):
            log.warning(
                "max_record_count ended up non-integer: '%s'. Defaulting to 2000.",
                max_record_count,
            )
            max_record_count = 2000

        return max_record_count, layer_meta

    def _pagination_loop(
        self,
        query_url: str,
        params: Dict[str, Any],
        layer_name_sanitized: str,
        max_record_count: int,
    ) -> tuple[List[Dict[str, Any]], int]:
        """Return all features for a layer via paginated requests."""
        current_offset = 0
        features_written_total = 0
        all_features: List[Dict[str, Any]] = []
        page_num = 1

        while True:
            effective_page_limit = 2000 if max_record_count == 0 else max_record_count
            log.debug(
                "Fetching page %d for layer %s (offset %d, limit %d)",
                page_num,
                layer_name_sanitized,
                current_offset,
                effective_page_limit,
            )

            page_params = params.copy()
            page_params["resultOffset"] = current_offset
            page_params["resultRecordCount"] = effective_page_limit

            data = self._request_page(
                query_url=query_url,
                params=page_params,
                layer_name_sanitized=layer_name_sanitized,
                page_num=page_num,
            )

            if data is None:
                break

            if "error" in data:
                log.error(
                    "❌ API_ERROR_REPORTED: Error from REST API for layer %s: %s",
                    layer_name_sanitized,
                    data["error"],
                )
                log.error(
                    "❌ API_ERROR_REPORTED: Breaking from pagination loop for this layer.", )
                break

            done, current_offset, features_len = self._append_features(
                data=data,
                layer_name_sanitized=layer_name_sanitized,
                page_num=page_num,
                all_features=all_features,
                current_offset=current_offset,
                max_record_count=max_record_count,
                effective_page_limit=effective_page_limit,
            )
            features_written_total += features_len
            if done:
                break

            page_num += 1

        return all_features, features_written_total

    def _add_crs_info(
        self,
        collection: Dict[str, Any],
        layer_id: str,
        layer_meta: Optional[Dict[str, Any]],
        output_format: str,
    ) -> Optional[Dict[str, Any]]:
        """Attach CRS metadata when appropriate."""
        if output_format != GEOJSON_FORMAT:
            return layer_meta

        if not layer_meta:
            layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
            log.debug(
                "Fetching specific layer metadata for layer ID %s (for CRS info).",
                layer_id,
            )
            layer_meta = self._get_layer_metadata(layer_metadata_url)

        if layer_meta and layer_meta.get("spatialReference"):
            sr_info = layer_meta.get("spatialReference")
            if sr_info and sr_info.get("wkid") == SWEREF99_TM_WKID:
                collection["crs"] = {
                    "type": "name",
                    "properties": {"name": "urn:ogc:def:crs:EPSG::3006"},
                }

        return layer_meta

    def _fetch_layer_data(
        self,
        layer_info: Dict[str, Any],
        layer_metadata_from_service: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Fetches data for a single layer."""
        layer_id = layer_info.get("id")
        if not layer_id:
            log.error("❌ Layer ID is missing from layer_info: %s", layer_info)
            return

        layer_name_original = layer_info.get("name", f"layer_{layer_id}")
        layer_name_sanitized = sanitize_for_filename(layer_name_original)

        query_url = f"{self.src.url.rstrip('/')}/{layer_id}/query"
        log.info("🚚 %s", layer_name_sanitized)
        log.debug(
            "Querying Layer ID: %s (Sanitized Name: %s, Original: %s) from %s",
            layer_id,
            layer_name_sanitized,
            layer_name_original,
            query_url,
        )

        max_record_count, layer_meta_to_use = self._determine_max_record_count(
            layer_id=layer_id,
            layer_meta=layer_metadata_from_service,
        )

        params = self._prepare_query_params()

        source_name_sanitized = sanitize_for_filename(self.src.name)
        staging_dir = (Path(str(paths.STAGING)) /
                       self.src.authority / source_name_sanitized)
        staging_dir.mkdir(parents=True, exist_ok=True)

        output_filename = f"{layer_name_sanitized}.{params['f']}"
        output_path = staging_dir / output_filename

        all_features, features_written_total = self._pagination_loop(
            query_url=query_url,
            params=params,
            layer_name_sanitized=layer_name_sanitized,
            max_record_count=max_record_count,
        )

        if not all_features:
            if features_written_total == 0:
                log.info("ℹ️ %s: no features", layer_name_sanitized)
            return

        final_output_data = {
            "type": "FeatureCollection",
            "features": all_features,
        }

        self._add_crs_info(
            collection=final_output_data,
            layer_id=layer_id,
            layer_meta=layer_meta_to_use,
            output_format=params["f"],
        )

        self._write_output_data(
            output_path=output_path,
            final_output_data=final_output_data,
            layer_name_sanitized=layer_name_sanitized,
            features_written_total=features_written_total,
        )

    def __enter__(self) -> "RestApiDownloadHandler":
        """Enter the context manager for use with 'with' statements."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager. No cleanup needed for REST API downloads."""
        pass
