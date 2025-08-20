import argparse
import logging
from pathlib import Path
from etl.config import load_config, ConfigError
from etl.paths import ensure_workspaces


def main():
    """
    Run the ETL orchestration: configure logging, load configuration, prepare workspaces, and execute selected pipeline stages.

    This function:
    - Configures logging to both logs/etl.log and the console.
    - Parses command-line arguments to select which stages to run:
      --download, --process, --load_sde (if none specified, all stages run).
      Also accepts --config and --sources (paths to YAML files), optional --authority and --type filters, and a --plan flag (parsed but not acted on here).
    - Loads configuration via load_config; on configuration failure it raises SystemExit with an error message.
    - Ensures required workspaces via ensure_workspaces(cfg).
    - Conditionally runs pipeline stages. Each stage is lazily imported and invoked:
      - Download: runs download_http.run and download_rest.run, then stages file-based downloads via stage_files.stage_all_downloads.
      - Processing: runs process.run.
      - SDE loading: runs load_sde.run.

    Raises:
        SystemExit: if loading configuration fails (ConfigError).
    """
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
    p.add_argument("--type", help="Filter by source type (e.g., rest_api)")
    p.add_argument("--plan", action="store_true", help="Dry-run mode: print planned actions")
    args = p.parse_args()

    try:
        cfg = load_config(args.config, args.sources)
    except ConfigError as e:
        logging.error(f"Config error: {e}")
        # Fail fast with a clear message
        raise SystemExit(f"Config error: {e}")

    ensure_workspaces(cfg)

    do_all = not any((args.download, args.process, args.load_sde))

    if args.download or do_all:
        logging.info("Starting download process...")
        # Import lazily to avoid heavy imports if not needed
        from etl import download_http, download_rest
        from etl.stage_files import stage_all_downloads

        download_http.run(cfg)
        download_rest.run(cfg)

        # Stage file-based downloads into geodatabase
        logging.info("Starting staging process...")
        stage_all_downloads(cfg)

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