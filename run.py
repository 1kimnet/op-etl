# run.py
import argparse
from etl.config import load_config
from etl.paths import ensure_workspaces
from etl import download_http, download_rest, process, load_sde

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--download", action="store_true")
    p.add_argument("--process", action="store_true")
    p.add_argument("--load_sde", action="store_true")
    args = p.parse_args()

    cfg = load_config()
    ensure_workspaces(cfg)

    do_all = not any([args.download, args.process, args.load_sde])

    if args.download or do_all:
        download_http.run_download(
            sources_path=cfg.get('sources_path', 'config/sources.yaml'),
            downloads_root=cfg.get('downloads_root', 'downloads'),
            log_csv=cfg.get('download_log', 'logs/download.csv')
        )
        download_rest.run(cfg)

    if args.process or do_all:
        process.run(cfg)

    if args.load_sde or do_all:
        load_sde.run(cfg)

if __name__ == "__main__":
    main()
