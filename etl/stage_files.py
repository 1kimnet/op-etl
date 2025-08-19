"""
Simple, conservative ingestion of downloaded file-based sources into staging.gdb.

Supports:
- ZIP archives containing a single shapefile (or a named shapefile via `include` in sources)
- GPKG files (imports first matching layer or named layer)
- Single shapefile (.shp)

Design: small, explicit rules; ambiguous archives are logged and skipped.
"""
from pathlib import Path
import zipfile
import logging
import arcpy
from .paths import staging_path


def _find_latest_file(download_dir: Path, pattern: str):
    # Find latest file matching a simple glob pattern (pattern can be '*' or name stem)
    files = sorted(download_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _import_shapefile(shp_path: Path, cfg: dict, out_name: str) -> bool:
    out_fc = staging_path(cfg, out_name)
    try:
        # Remove existing target if present (overwrite behavior)
        try:
            if arcpy.Exists(out_fc):
                arcpy.management.Delete(out_fc)
        except Exception:
            # best-effort delete; continue to attempt import
            logging.debug(f"[STAGE] Could not delete existing {out_fc} before import")
        out_fc_path = Path(out_fc)
        arcpy.conversion.FeatureClassToFeatureClass(
            str(shp_path),
            str(out_fc_path.parent),
            out_fc_path.name
        )
        logging.info(f"[STAGE] Imported shapefile {shp_path} -> {out_fc}")
        return True
    except Exception as e:
        logging.error(f"[STAGE] Failed to import shapefile {shp_path}: {e}")
        return False


def _import_gpkg(gpkg_path: Path, cfg: dict, out_name: str, layer_name: str | None = None) -> bool:
    out_fc = staging_path(cfg, out_name)
    try:
        # Remove existing target if present (overwrite behavior)
        try:
            if arcpy.Exists(out_fc):
                arcpy.management.Delete(out_fc)
        except Exception:
            logging.debug(f"[STAGE] Could not delete existing {out_fc} before import")
        if layer_name:
            src = f"{str(gpkg_path)}|layername={layer_name}"
        else:
            # ArcPy can reference gpkg with layer syntax if needed; try to copy first layer
            src = str(gpkg_path)
        out_fc_path = Path(out_fc)
        arcpy.conversion.FeatureClassToFeatureClass(src, str(out_fc_path.parent), out_fc_path.name)
        logging.info(f"[STAGE] Imported GPKG {gpkg_path} -> {out_fc}")
        return True
    except Exception as e:
        logging.error(f"[STAGE] Failed to import GPKG {gpkg_path}: {e}")
        return False


def _extract_hints(source: dict) -> tuple[str | None, str | None]:
    """Extract and normalize include and layer hints from source configuration."""
    include_hint = source.get('include')
    raw = source.get('raw') or {}
    layer_hint = raw.get('layer_name') or raw.get('layer')

    # normalize include_hint to a single string if possible
    include_hint = include_hint[0] if isinstance(include_hint, list) and include_hint else include_hint
    include_hint = include_hint.strip() if isinstance(include_hint, str) else None

    # normalize layer_hint to a single string if possible
    layer_hint = layer_hint[0] if isinstance(layer_hint, list) and layer_hint else layer_hint
    layer_hint = layer_hint.strip() if isinstance(layer_hint, str) else None

    return include_hint, layer_hint


def _find_candidates(auth_dir: Path, stem: str) -> list[Path]:
    """Find candidate files in the authority directory."""
    candidates = []

    def _find_latest_file_case_insensitive(directory, pattern_stem, extensions):
        files = []
        for p in directory.iterdir():
            if p.is_file():
                ext_lc = p.suffix.lower()
                name_lc = p.stem.lower()
                for ext in extensions:
                    if ext_lc == ext and (not pattern_stem or name_lc == pattern_stem.lower()):
                        files.append(p)
        if files:
            files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            return files[0]
        return None

    for ext in ('.zip', '.gpkg', '.shp'):
        if (f := _find_latest_file_case_insensitive(auth_dir, stem, [ext])) or \
           (f := _find_latest_file_case_insensitive(auth_dir, None, [ext])):
            candidates.append(f)

    return candidates


def _process_zip_file(file_path: Path, cfg: dict, source: dict, preferred_hint: str | None) -> bool:
    """Process a ZIP file and import its contents."""
    try:
        with zipfile.ZipFile(file_path, 'r') as zf:
            namelist = zf.namelist()
            shp_files = [n for n in namelist if n.lower().endswith('.shp')]
            gpkg_files = [n for n in namelist if n.lower().endswith('.gpkg')]

            target_dir = file_path.with_suffix('')
            target_dir.mkdir(parents=True, exist_ok=True)

            # Handle preferred hint for shapefiles
            if preferred_hint and (matched := [n for n in shp_files if preferred_hint.lower() in n.lower()]):
                zf.extractall(target_dir)
                shp_full = next(target_dir.glob(f"**/*{Path(matched[0]).name}"))
                return _import_shapefile(shp_full, cfg, source['out_name'])

            # Handle single shapefile
            if len(shp_files) == 1 and not gpkg_files:
                zf.extractall(target_dir)
                shp_full = next(target_dir.glob('**/*.shp'))
                return _import_shapefile(shp_full, cfg, source['out_name'])

            # Handle GPKG files
            if len(gpkg_files) >= 1:
                zf.extractall(target_dir)
                gpkg_full = next(target_dir.glob('**/*.gpkg'))
                layer_name = preferred_hint if preferred_hint else None
                return _import_gpkg(gpkg_full, cfg, source['out_name'], layer_name=layer_name)

            logging.warning(f"[STAGE] Zip contains multiple or no shapefiles/gpkg; skipping {file_path}")
            return False

    except Exception as e:
        logging.error(f"[STAGE] Error extracting zip {file_path}: {e}")
        return False


def _process_file(file_path: Path, cfg: dict, source: dict, preferred_hint: str | None) -> bool:
    """Process a single file based on its extension."""
    suffix = file_path.suffix.lower()

    if suffix == '.zip':
        return _process_zip_file(file_path, cfg, source, preferred_hint)
    elif suffix == '.gpkg':
        return _import_gpkg(file_path, cfg, source['out_name'])
    elif suffix == '.shp':
        return _import_shapefile(file_path, cfg, source['out_name'])
    else:
        logging.info(f"[STAGE] Unsupported downloaded file type for {file_path}; skipping")
        return False


def ingest_downloads(cfg: dict) -> None:
    downloads = Path(cfg['workspaces']['downloads'])

    for source in cfg.get('sources', []):
        if not source.get('include', True) or source.get('type') not in ('file', 'http'):
            continue

        auth_dir = downloads / (source.get('authority') or '')
        if not auth_dir.exists():
            logging.debug(f"[STAGE] No download dir for {source.get('name')} ({auth_dir})")
            continue

        stem = source.get('out_name') or ''
        include_hint, layer_hint = _extract_hints(source)
        preferred_hint = layer_hint or include_hint

        if not (candidates := _find_candidates(auth_dir, stem)):
            logging.info(f"[STAGE] No downloaded file found for source {source.get('name')} in {auth_dir}")
            continue

        _process_file(candidates[0], cfg, source, preferred_hint)
