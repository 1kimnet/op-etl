import logging
import requests
from pathlib import Path
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)

def download_atom_feed(url: str, download_dir: Path):
    """
    Downloads and parses an ATOM feed, then downloads the linked data.
    """
    try:
        log.debug(f"Fetching ATOM feed from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        log.debug(f"Successfully fetched ATOM feed from {url}")

        feed_content = response.content
        root = ET.fromstring(feed_content)

        # Find all links in the feed
        links = root.findall(".//{http://www.w3.org/2005/Atom}link")
        download_links = [link.get("href") for link in links if link.get("rel") == "enclosure"]

        if not download_links:
            log.warning(f"No download links found in ATOM feed: {url}")
            return

        for link_url in download_links:
            if not link_url:
                log.warning("Skipping empty or None download link")
                continue

            try:
                log.info(f"Downloading from {link_url}")
                file_response = requests.get(link_url, timeout=60, stream=True)
                file_response.raise_for_status()

                file_name = Path(link_url).name
                file_path = download_dir / file_name

                with open(file_path, "wb") as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                log.info(f"Successfully downloaded {file_name} to {file_path}")

            except requests.exceptions.RequestException as e:
                log.error(f"Failed to download file from {link_url}: {e}")

    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch ATOM feed from {url}: {e}")
    except ET.ParseError as e:
        log.error(f"Failed to parse ATOM feed from {url}: {e}")

def handle_atom_source(source: dict, download_dir: Path):
    """
    Handles a source of type 'atom'.
    """
    url = source.get("url")
    if not url:
        log.warning(f"ATOM source has no URL: {source.get('name')}")
        return

    log.info(f"Processing ATOM source: {source.get('name')}")
    download_atom_feed(url, download_dir)
