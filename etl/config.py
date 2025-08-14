from pathlib import Path
import os
import yaml

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
    cfg["sources"] = sources

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
