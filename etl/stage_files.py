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
from .utils import best_shapefile_by_count


def _find_latest_file(download_dir: Path, pattern: str):
    # Find latest file matching a simple glob pattern (pattern can be '*' or name stem)
    """
    Return the most recently modified file in download_dir that matches the given glob pattern.

    Pattern is a pathlib-style glob (e.g. '*.zip', 'mystem*.gpkg', or a literal filename) evaluated against download_dir. Returns the Path to the newest matching file, or None if no matches are found.
    """
    files = sorted(download_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _import_shapefile(shp_path: Path, cfg, out_name: str) -> bool:
    """
    Import a .shp file into the staging geodatabase and overwrite any existing target.

    Detailed description:
    - Computes the destination feature class using staging_path(cfg, out_name).
    - Attempts a best-effort delete of an existing target before importing.
    - Converts the input shapefile into a feature class at the staging destination using ArcPy.
    - Catches errors and returns a boolean status rather than raising.

    Parameters:
        shp_path (Path): Path to the source .shp file.
        cfg (dict): Configuration mapping (used to resolve the staging path).
        out_name (str): Logical name for the output feature class in the staging geodatabase.

    Returns:
        bool: True if the shapefile was imported successfully; False on failure.
    """
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
    """
    Import a GeoPackage into the staging dataset at the path derived from cfg and out_name.

    Attempts to remove any existing target (best-effort) then copies either the whole GeoPackage (first layer) or a specific layer if layer_name is provided into staging_path(cfg, out_name).

    Parameters:
        layer_name (str | None): Optional exact layer name inside the GeoPackage to import. If omitted, the GeoPackage path is used and the first layer is imported.

    Returns:
        bool: True on successful import, False on any failure.
    """
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
            # List layers and pick the first one
            layers = _list_gpkg_layers(gpkg_path)
            if not layers:
                logging.error(f"[STAGE] No layers found in GPKG {gpkg_path}")
                return False
            if len(layers) > 1:
                logging.info("[STAGE] Multiple GPKG layers in %s; candidates: %s", gpkg_path, ", ".join(layers))
            src = f"{str(gpkg_path)}|layername={layers[0]}"
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
            auto_select = bool((source.get('raw') or {}).get('auto_select'))

            target_dir = file_path.with_suffix('')
            target_dir.mkdir(parents=True, exist_ok=True)

            # Handle preferred hint for shapefiles
            if preferred_hint and (matched := [n for n in shp_files if preferred_hint.lower() in n.lower()]):
                zf.extractall(target_dir)
                shp_full = next(target_dir.glob(f"**/*{Path(matched[0]).name}"), None)
                if shp_full:
                    return _import_shapefile(shp_full, cfg, source['out_name'])
                else:
                    logging.error(f"[STAGE] Expected shapefile not found after extraction: {matched[0]}")
                    return False

            # Handle shapefiles in archive
            if len(shp_files) >= 1 and not gpkg_files:
                zf.extractall(target_dir)
                shp_paths = list(target_dir.glob('**/*.shp'))
                if not shp_paths:
                    logging.error("[STAGE] Expected shapefile not found after extraction")
                    return False
                if len(shp_paths) == 1 or not auto_select:
                    if len(shp_paths) > 1:
                        logging.info("[STAGE] Multiple shapefiles in %s; candidates: %s", file_path, ", ".join(str(p) for p in shp_paths))
                    return _import_shapefile(shp_paths[0], cfg, source['out_name'])
                # auto-select best shapefile by feature count
                best = best_shapefile_by_count(shp_paths)
                if best is None:
                    logging.warning("[STAGE] auto_select could not pick shapefile (no counts>0); skipping %s", file_path)
                    return False
                logging.info("[STAGE] auto_select picked shapefile %s from %s", best, file_path)
                return _import_shapefile(best, cfg, source['out_name'])

            # Handle GPKG files
            if len(gpkg_files) >= 1:
                zf.extractall(target_dir)
                gpkg_full = next(target_dir.glob('**/*.gpkg'), None)
                if gpkg_full:
                    layer_name = preferred_hint if preferred_hint else None
                    if layer_name:
                        return _import_gpkg(gpkg_full, cfg, source['out_name'], layer_name=layer_name)
                    # No explicit layer provided
                    if auto_select:
                        # choose the gpkg layer with highest feature count
                        layer = _best_gpkg_layer(gpkg_full)
                        if not layer:
                            logging.warning("[STAGE] auto_select could not pick gpkg layer in %s; skipping", gpkg_full)
                            return False
                        logging.info("[STAGE] auto_select picked gpkg layer '%s' in %s", layer, gpkg_full)
                        return _import_gpkg(gpkg_full, cfg, source['out_name'], layer_name=layer)
                    # Try importing first layer; also log the available layers
                    layers = _list_gpkg_layers(gpkg_full)
                    if len(layers) > 1:
                        logging.info("[STAGE] Multiple GPKG layers in %s; candidates: %s", gpkg_full, ", ".join(layers))
                    return _import_gpkg(gpkg_full, cfg, source['out_name'])
                else:
                    logging.error("[STAGE] Expected GPKG file not found after extraction")
                    return False

            logging.warning(f"[STAGE] Zip contains multiple or no shapefiles/gpkg; skipping {file_path}")
            return False

    except Exception as e:
        logging.error(f"[STAGE] Error extracting zip {file_path}: {e}")
        return False


def _process_file(file_path: Path, cfg: dict, source: dict, preferred_hint: str | None) -> bool:
    """Process a single file based on its extension."""
    suffix = file_path.suffix.lower()
    auto_select = bool((source.get('raw') or {}).get('auto_select'))

    if suffix == '.zip':
        return _process_zip_file(file_path, cfg, source, preferred_hint)
    elif suffix == '.gpkg':
        # If explicit layer is provided, use it; otherwise apply auto_select if enabled
        if preferred_hint:
            return _import_gpkg(file_path, cfg, source['out_name'], layer_name=preferred_hint)
        if auto_select:
            layer = _best_gpkg_layer(file_path)
            if not layer:
                logging.warning("[STAGE] auto_select could not pick gpkg layer in %s; skipping", file_path)
                return False
            logging.info("[STAGE] auto_select picked gpkg layer '%s' in %s", layer, file_path)
            return _import_gpkg(file_path, cfg, source['out_name'], layer_name=layer)
        layers = _list_gpkg_layers(file_path)
        if len(layers) > 1:
            logging.info("[STAGE] Multiple GPKG layers in %s; candidates: %s", file_path, ", ".join(layers))
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
        if len(candidates) > 1:
            logging.info("[STAGE] Multiple candidate files for %s in %s: %s", source.get('name'), auth_dir, ", ".join(str(c) for c in candidates))
        _process_file(candidates[0], cfg, source, preferred_hint)

def _list_gpkg_layers(gpkg_path: Path) -> list[str]:
    """List feature layers inside a GPKG using arcpy.da.Walk (no env mutation)."""
    try:
        layers: list[str] = []
        for dirpath, dirnames, filenames in arcpy.da.Walk(str(gpkg_path), datatype="FeatureClass"):
            for f in filenames:
                layers.append(f)
        return layers
    except Exception:
        return []

def _best_gpkg_layer(gpkg_path: Path) -> str | None:
    """Return the gpkg layer name with the highest feature count (>0), or None, using full paths."""
    try:
        best_layer = None
        best_count = -1
        for dirpath, dirnames, filenames in arcpy.da.Walk(str(gpkg_path), datatype="FeatureClass"):
            for f in filenames:
                full = f"{str(gpkg_path)}|layername={f}"
                try:
                    res = arcpy.management.GetCount(full)
                    cnt = int(str(res.getOutput(0)))
                except Exception:
                    cnt = -1
                if cnt > best_count:
                    best_count = cnt
                    best_layer = f
        return best_layer if best_count > 0 else None
    except Exception:
        return None
