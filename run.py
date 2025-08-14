import argparse
from etl.config import load_config, ConfigError
from etl.paths import ensure_workspaces


def main():
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
        # Fail fast with a clear message
        raise SystemExit(f"Config error: {e}")

    ensure_workspaces(cfg)

    do_all = not any((args.download, args.process, args.load_sde))

    if args.download or do_all:
        # Import lazily to avoid heavy imports if not needed
        from etl import download_http, download_rest
        download_http.run(cfg)
        download_rest.run(cfg)

    if args.process or do_all:
        from etl import process
        process.run(cfg)

    if args.load_sde or do_all:
        from etl import load_sde
        load_sde.run(cfg)


if __name__ == "__main__":
    main()