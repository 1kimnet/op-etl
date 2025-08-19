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


def _import_shapefile(shp_path: Path, cfg, out_name: str) -> bool:
    out_fc = staging_path(cfg, out_name)
    try:
        # Remove existing target if present (overwrite behavior)
        try:
            if arcpy.Exists(out_fc):
                arcpy.management.Delete(out_fc)
        except Exception:
            # best-effort delete; continue to attempt import
            logging.debug(f"[STAGE] Could not delete existing {out_fc} before import")
        arcpy.conversion.FeatureClassToFeatureClass(str(shp_path), out_fc.rsplit('/', 1)[0], out_fc.rsplit('/', 1)[1])
        logging.info(f"[STAGE] Imported shapefile {shp_path} -> {out_fc}")
        return True
    except Exception as e:
        logging.error(f"[STAGE] Failed to import shapefile {shp_path}: {e}")
        return False


def _import_gpkg(gpkg_path: Path, cfg, out_name: str, layer_name: str | None = None) -> bool:
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


def ingest_downloads(cfg: dict) -> None:
    downloads = Path(cfg['workspaces']['downloads'])
    for s in cfg.get('sources', []):
        if not s.get('include', True):
            continue
        if s.get('type') not in ('file', 'http'):
            continue

        auth_dir = downloads / (s.get('authority') or '')
        if not auth_dir.exists():
            logging.debug(f"[STAGE] No download dir for {s.get('name')} ({auth_dir})")
            continue

        # Simple matching: look for files with source out_name or any standard extension
        stem = s.get('out_name') or ''
        include_hint = s.get('include')
        # Also support a raw.layer_name override (preferred)
        raw = s.get('raw') or {}
        layer_hint = raw.get('layer_name') or raw.get('layer')

        # normalize include_hint to a single string if possible, fallback to layer_hint
        if isinstance(include_hint, list) and include_hint:
            include_hint = include_hint[0]
        if isinstance(include_hint, str):
            include_hint = include_hint.strip()
        else:
            include_hint = None

        if isinstance(layer_hint, list) and layer_hint:
            layer_hint = layer_hint[0]
        if isinstance(layer_hint, str):
            layer_hint = layer_hint.strip()
        else:
            layer_hint = None

        # Choose preferred hint: layer_hint first, then include_hint
        preferred_hint = layer_hint or include_hint
        candidates = []
        # look for common extensions (case-insensitive)
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
                # Sort by modification time, newest first
                files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                return files[0]
            return None

        for ext in ('.zip', '.gpkg', '.shp'):
            f = _find_latest_file_case_insensitive(auth_dir, stem, [ext]) or _find_latest_file_case_insensitive(auth_dir, None, [ext])
            if f:
                candidates.append(f)

        if not candidates:
            logging.info(f"[STAGE] No downloaded file found for source {s.get('name')} in {auth_dir}")
            continue

        file_path = candidates[0]
        # handle zip
        if file_path.suffix.lower() == '.zip':
            try:
                with zipfile.ZipFile(file_path, 'r') as zf:
                    namelist = zf.namelist()
                    shp_files = [n for n in namelist if n.lower().endswith('.shp')]
                    gpkg_files = [n for n in namelist if n.lower().endswith('.gpkg')]
                    # If include_hint points to a shapefile name, prefer that
                    if preferred_hint:
                        matched = [n for n in shp_files if preferred_hint.lower() in n.lower()]
                        if matched:
                            target_dir = file_path.with_suffix('')
                            target_dir.mkdir(parents=True, exist_ok=True)
                            zf.extractall(target_dir)
                            shp_full = next(target_dir.glob(f"**/*{Path(matched[0]).name}"))
                            _import_shapefile(shp_full, cfg, s['out_name'])
                            continue

                    if len(shp_files) == 1 and not gpkg_files:
                        # extract the shapefile files (shp+shx+dbf etc.) to a temp dir
                        target_dir = file_path.with_suffix('')
                        target_dir.mkdir(parents=True, exist_ok=True)
                        zf.extractall(target_dir)
                        shp_full = next(target_dir.glob('**/*.shp'))
                        _import_shapefile(shp_full, cfg, s['out_name'])
                    elif len(gpkg_files) >= 1:
                        # If include_hint names a gpkg layer (e.g. layername), extract and try to import that layer
                        target_dir = file_path.with_suffix('')
                        target_dir.mkdir(parents=True, exist_ok=True)
                        zf.extractall(target_dir)
                        gpkg_full = next(target_dir.glob('**/*.gpkg'))
                        if layer_hint:
                            _import_gpkg(gpkg_full, cfg, s['out_name'], layer_name=layer_hint)
                        elif include_hint:
                            _import_gpkg(gpkg_full, cfg, s['out_name'], layer_name=include_hint)
                        else:
                            _import_gpkg(gpkg_full, cfg, s['out_name'])
                        continue
                    else:
                        logging.warning(f"[STAGE] Zip contains multiple or no shapefiles/gpkg; skipping {file_path}")
            except Exception as e:
                logging.error(f"[STAGE] Error extracting zip {file_path}: {e}")
            continue

        if file_path.suffix.lower() == '.gpkg':
            _import_gpkg(file_path, cfg, s['out_name'])
            continue

        if file_path.suffix.lower() == '.shp':
            _import_shapefile(file_path, cfg, s['out_name'])
            continue

        logging.info(f"[STAGE] Unsupported downloaded file type for {file_path}; skipping")
