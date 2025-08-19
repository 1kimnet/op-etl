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
    """
    Ingest downloaded file-based sources from the configured downloads workspace into the staging dataset.

    Processes sources in cfg['sources'] that are included and of type 'file' or 'http'. For each source it:
    - Locates the latest matching downloaded file in downloads/<authority> by trying stem-based and generic patterns (*.zip, *.gpkg, *.shp).
    - Supports ZIP archives (prefers a hinted shapefile or single shapefile, or contained GeoPackage(s) with optional layer hints), GeoPackage (.gpkg) files (optionally importing a named layer), and single shapefiles (.shp).
    - Extracts ZIP contents to a sibling directory when needed and delegates ingestion to _import_shapefile or _import_gpkg.
    - Skips ambiguous archives or unsupported file types and continues on errors (errors are logged).

    Parameters:
        cfg (dict): Configuration containing at least:
            - workspaces: a mapping with 'downloads' pointing to the downloads directory.
            - sources: an iterable of source definitions; relevant source keys:
                - include (bool or list): whether to process the source (and optionally a shapefile name hint).
                - type (str): must be 'file' or 'http' to be processed.
                - authority (str): subdirectory under downloads where files are expected.
                - name (str): human-readable name used in logs.
                - out_name (str): target staging feature name (passed to import helpers).
                - raw (dict): optional, may contain 'layer_name' or 'layer' to prefer a specific layer inside a GeoPackage or ZIP.

    Returns:
        None
    """
    downloads = Path(cfg['workspaces']['downloads'])
    for s in cfg.get('sources', []):
        if not s.get('include', True):
            continue
        if s.get('type') not in ('file', 'http'):
            continue
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
