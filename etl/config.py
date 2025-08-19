from pathlib import Path
import os
import yaml

from .download_http import slug
class ConfigError(Exception):
    pass


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"Missing required config file: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def load_config(
    config_path: str | os.PathLike | None = None,
    sources_path: str | os.PathLike | None = None,
) -> dict:
    """
    Load and merge config/config.yaml and config/sources.yaml.
    Honors env vars OPETL_CONFIG and OPETL_SOURCES and falls back to defaults.
    Validates required sections and sets safe defaults.
    """
    # Resolve paths: CLI args > env vars > defaults
    config_path = Path(
        config_path or os.environ.get("OPETL_CONFIG", "config/config.yaml")
    )
    sources_path = Path(
        sources_path or os.environ.get("OPETL_SOURCES", "config/sources.yaml")
    )

    cfg = _read_yaml(config_path)
    src = _read_yaml(sources_path)

    # Attach sources list (supports either top-level list or dict with key 'sources')
    sources = src.get("sources") if isinstance(src, dict) else src
    if not isinstance(sources, list):
        raise ConfigError(
            "sources.yaml must contain a top-level 'sources' list."
        )

    # Process sources to ensure they have required fields
    processed_sources = []
    for i, source in enumerate(sources):
        processed_source = source.copy()

        # Generate out_name from name if not provided
        if "out_name" not in processed_source:
            name = processed_source.get("name", f"source_{i}")
            processed_source["out_name"] = slug(name)

        processed_sources.append(processed_source)

    cfg["sources"] = processed_sources

    # Defaults to keep the rest of the pipeline sane
    gp = cfg.setdefault("geoprocess", cfg.pop("geoprocessing", {}))
    gp.setdefault("enabled", False)

    # Workspaces are required; provide a friendly error if missing
    if "workspaces" not in cfg:
        raise ConfigError(
            "Missing 'workspaces' in config.yaml. Expected keys: downloads, staging_gdb, sde_conn"
        )

    ws = cfg["workspaces"]
    for key in ("downloads", "staging_gdb"):
        if key not in ws:
            raise ConfigError(
                f"'workspaces.{key}' is required in config.yaml"
            )

    # Normalize parallel factor if provided as bare integer string
    ppf = gp.get("parallel_processing_factor")
    if isinstance(ppf, str) and ppf.isdigit():
        gp["parallel_processing_factor"] = f"{ppf}%"

    # Validation typo rescue (strict_modeQ -> strict_mode)
    val = cfg.setdefault("validation", {})
    if "strict_modeQ" in val and "strict_mode" not in val:
        val["strict_mode"] = val.pop("strict_modeQ")

    return cfg


def normalize_sources(cfg: dict) -> list[dict]:
    norm = []
    for s in cfg.get("sources", []):
        # enable flag
        if not s.get("enabled", True):
            continue

        t = s.get("type")
        out = {
            "name": s.get("name"),
            "authority": s.get("authority"),
            "type": t,                               # file | rest_api | ogc_api | atom_feed
            "url": s.get("url"),
            "staged_data_type": s.get("staged_data_type"),
            "include": s.get("include") or [],
            "download_format": s.get("download_format"),
            "raw": s.get("raw", {}) or {},
        }

        # normalize rest_api details
        if t == "rest_api":
            r = out["raw"]
            r.setdefault("format", "json")           # don't rely on geojson unless verified
            r.setdefault("where_clause", "1=1")
            r.setdefault("out_fields", "*")
            if "bbox" in r and isinstance(r["bbox"], str):
                r["bbox"] = [float(x) for x in r["bbox"].split(",")]

        # normalize ogc_api details
        if t == "ogc_api":
            r = out["raw"]
            r.setdefault("collections", [])
            r.setdefault("page_size", 1000)
            r.setdefault("supports_bbox_crs", True)

        norm.append(out)

    cfg["sources"] = norm
    return norm
