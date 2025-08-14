import argparse
import logging
from etl.config import load_config, ConfigError
from etl.paths import ensure_workspaces


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/etl.log"),
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
        download_http.run(cfg)
        download_rest.run(cfg)
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