import yaml

def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # defaults
    cfg.setdefault("geoprocess", {})
    cfg["geoprocess"].setdefault("enabled", False)
    return cfg
