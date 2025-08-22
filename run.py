import argparse
import logging
import time
from pathlib import Path
from etl.config import load_config, ConfigError
from etl.paths import ensure_workspaces


def main():
    """Run the ETL pipeline with fixed downloader modules."""
    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/etl.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Starting ETL process...")

    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None, help="Path to config.yaml")
    p.add_argument("--sources", default=None, help="Path to sources.yaml")
    p.add_argument("--download", action="store_true")
    p.add_argument("--process", action="store_true")
    p.add_argument("--load_sde", action="store_true")
    p.add_argument("--authority", help="Filter by authority")
    p.add_argument("--type", help="Filter by source type")
    args = p.parse_args()

    try:
        cfg = load_config(args.config, args.sources)
    except ConfigError as e:
        logging.error(f"Config error: {e}")
        raise SystemExit(f"Config error: {e}")

    ensure_workspaces(cfg)

    do_all = not any((args.download, args.process, args.load_sde))

    if args.download or do_all:
        logging.info("Starting download process...")

        # Apply filters if specified
        sources = cfg["sources"]
        if args.authority:
            sources = [s for s in sources if s.get("authority") == args.authority]
        if args.type:
            sources = [s for s in sources if s.get("type") == args.type]

        # Create filtered config
        filtered_cfg = cfg.copy()
        filtered_cfg["sources"] = sources

        # Import downloaders
        from etl import download_http, download_atom, download_ogc, download_wfs, download_rest

        # Run each downloader separately
        # Each module now handles its own source filtering
        download_http.run(filtered_cfg)
        download_atom.run(filtered_cfg)
        download_ogc.run(filtered_cfg)
        download_wfs.run(filtered_cfg)
        download_rest.run(filtered_cfg)

        # Stage downloaded files
        logging.info("Starting staging process...")
        from etl.stage_files import stage_all_downloads
        stage_all_downloads(filtered_cfg)

        # Log monitoring summary
        from etl.monitoring import log_pipeline_summary, save_pipeline_metrics, get_error_patterns
        log_pipeline_summary()
        
        # Save metrics to file
        metrics_file = Path("logs") / f"pipeline_metrics_{Path().resolve().name}_{int(time.time())}.json"
        save_pipeline_metrics(metrics_file)
        
        # Check for error patterns
        patterns = get_error_patterns()
        if patterns['recursion_errors']:
            logging.warning(f"Detected recursion errors in: {patterns['recursion_errors']}")
        if patterns['timeout_errors']:
            logging.warning(f"Detected timeout errors in: {patterns['timeout_errors']}")

        logging.info("Download process finished.")

    if args.process or do_all:
        logging.info("Starting processing step...")
        from etl import process
        process.run(cfg)
        logging.info("Processing step finished.")

    if args.load_sde or do_all:
        logging.info("Starting SDE loading process...")
        from etl import load_sde
        load_sde.run(cfg)
        logging.info("SDE loading process finished.")

    logging.info("ETL process finished successfully.")


if __name__ == "__main__":
    main()