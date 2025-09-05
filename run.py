import argparse
import logging
import sys
import time
from pathlib import Path

from etl.config import ConfigError, load_config
from etl.paths import ensure_workspaces
from etl.workspace import create_clean_staging_gdb, remove_geodatabase_safely


# Download flow extracted for clarity
def _run_download(cfg, args):
    logging.info("Starting download process...")

    # Optional source filters
    sources = cfg["sources"]
    if args.authority:
        sources = [s for s in sources if s.get("authority") == args.authority]
    if args.type:
        sources = [s for s in sources if s.get("type") == args.type]

    filtered_cfg = cfg.copy()
    filtered_cfg["sources"] = sources

    use_unified = bool(cfg.get("use_unified_downloader", False))
    if use_unified:
        from etl import download as unified
        unified.run(filtered_cfg, authority=args.authority, type=args.type)
    else:
        from etl import download_atom, download_http, download_ogc, download_wfs

        # Check for simplified REST downloader
        use_simple_rest = bool(cfg.get("use_simplified_rest", False))
        if use_simple_rest:
            from etl import download_rest_simple as download_rest
            logging.info("Using simplified REST downloader")
        else:
            from etl import download_rest
            logging.info("Using full REST downloader")

        download_http.run(filtered_cfg)
        download_atom.run(filtered_cfg)
        download_ogc.run(filtered_cfg)
        download_wfs.run(filtered_cfg)
        download_rest.run(filtered_cfg)

    logging.info("Starting staging process...")
    use_simplified = bool(filtered_cfg.get("use_simplified_staging", False))
    if use_simplified:
        from etl.stage_simple import stage_all_downloads
        logging.info("Using simplified staging module")
    else:
        from etl.stage_files import stage_all_downloads
        logging.info("Using full staging module")
    stage_all_downloads(filtered_cfg)

    from etl.monitoring import get_error_patterns, log_pipeline_summary, save_pipeline_metrics
    log_pipeline_summary()

    metrics_file = Path("logs") / f"pipeline_metrics_{Path().resolve().name}_{int(time.time())}.json"
    save_pipeline_metrics(metrics_file)

    patterns = get_error_patterns()
    if patterns['recursion_errors']:
        logging.warning(f"Detected recursion errors in: {patterns['recursion_errors']}")
    if patterns['timeout_errors']:
        logging.warning(f"Detected timeout errors in: {patterns['timeout_errors']}")

    logging.info("Download process finished.")


# Generic step runner to avoid duplicate log lines
def _run_step(start_msg, runner, cfg, end_msg):
    logging.info(start_msg)
    runner(cfg)
    logging.info(end_msg)


def main():
    """Run the ETL pipeline with improved geodatabase management."""
    # Set increased recursion limit to handle deeply nested API responses
    sys.setrecursionlimit(3000)

    # 1) absolutely no logging.basicConfig here
    Path("logs").mkdir(exist_ok=True)  # safe to prep early

    # Parse arguments first to get config path
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None, help="Path to config.yaml")
    p.add_argument("--sources", default=None, help="Path to sources.yaml")
    p.add_argument("--download", action="store_true")
    p.add_argument("--process", action="store_true")
    p.add_argument("--load_sde", action="store_true")
    p.add_argument("--authority", help="Filter by authority")
    p.add_argument("--type", help="Filter by source type")
    args = p.parse_args()

    # Load configuration first
    try:
        cfg = load_config(args.config, args.sources)
    except ConfigError as e:
        # Use basic logging for config errors since we haven't configured logging yet
        print(f"Config error: {e}", file=sys.stderr)
        raise SystemExit(f"Config error: {e}") from e

    # 2) configure unified logging
    from etl.logging import setup_pipeline_logging
    # Map old YAML options if present; default to console INFO and file logs to logs/etl.log
    log_cfg = cfg.get("logging") or {}
    console_level = (log_cfg.get("level") or log_cfg.get("console_level") or "INFO").upper()
    file_path = None
    file_cfg = log_cfg.get("file") or {}
    if file_cfg.get("enabled", True):
        file_name = file_cfg.get("name") or "etl.log"
        file_path = Path("logs") / file_name
        file_level = (file_cfg.get("level") or "DEBUG").upper()
    else:
        file_level = "DEBUG"
    setup_pipeline_logging(console_level=console_level, file_path=file_path, file_level=file_level)

    # Suppress noisy urllib3 retry WARNINGs (e.g., connectionpool Retrying ...)
    for name in (
        "urllib3",
        "urllib3.connectionpool",
        "requests.packages.urllib3",
        "requests.packages.urllib3.connectionpool",
    ):
        logging.getLogger(name).setLevel(logging.ERROR)

    # 3) proceed with ETL; all modules just use logging.getLogger(__name__)
    logging.info("Starting ETL process...")

    ensure_workspaces(cfg)

    # Handle staging geodatabase cleanup and creation
    if cfg.get("cleanup_staging_before_run", False):
        staging_gdb_path = Path(cfg["workspaces"]["staging_gdb"]).resolve()

        # Remove existing geodatabase
        success = remove_geodatabase_safely(staging_gdb_path)
        if not success:
            logging.warning("Geodatabase removal had issues, but continuing...")

        # Create fresh geodatabase
        if not create_clean_staging_gdb(staging_gdb_path):
            raise SystemExit("Failed to create staging geodatabase")

    do_all = not any((args.download, args.process, args.load_sde))

    if args.download or do_all:
        _run_download(cfg, args)

    if args.process or do_all:
        from etl import process
        _run_step("Starting processing step...", process.run, cfg, "Processing step finished.")

    if args.load_sde or do_all:
        from etl import load_sde
        _run_step("Starting SDE loading process...", load_sde.run, cfg, "SDE loading process finished.")

    logging.info("ETL process finished successfully.")


if __name__ == "__main__":
    main()
