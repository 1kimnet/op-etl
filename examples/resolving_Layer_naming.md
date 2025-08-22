The primary issues were incorrectly separated Python code blocks and inconsistent list formatting. I have combined the script into a single block and standardized the lists for proper rendering.

Resolving Esri REST Layer Names
This document explains how to fetch human-readable names for layers from an Esri REST API service before downloading them. This prevents generic filenames like layer_0.geojson and instead allows for descriptive names like World_Time_Zones.geojson.

The Problem
When downloading data from an Esri FeatureServer or MapServer, you often iterate through layer IDs (0, 1, 2, etc.). If you use these IDs directly to name your output files, you lose the context of what the data represents. The solution is to first query the service's metadata, which contains the actual names for each layer ID.

Core Logic: layer_name_resolver.py
This Python script provides a simple, reusable class to discover layer names from a service URL.

Python

import requests
import logging
from typing import Any, Dict, List, Optional

# --- Basic Setup ---

# In a real application, you would use a more robust logging configuration

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

class LayerNameResolver:
    """
    A simplified handler to demonstrate fetching layer names from an Esri REST service.
    """

    def __init__(self, service_url: str, timeout: int = 30):
        """
        Initializes the resolver with the service URL.

        Args:
            service_url: The base URL of the Esri MapServer or FeatureServer.
            timeout: The timeout in seconds for HTTP requests.
        """
        self.service_url = service_url.rstrip('/')
        self.session = requests.Session()  # Use a session for connection pooling
        self.timeout = timeout
        log.info(f"Initialized resolver for service: {self.service_url}")

    def _get_service_metadata(self) -> Optional[Dict[str, Any]]:
        """
        Fetches the base metadata for the entire service (MapServer/FeatureServer).

        This metadata contains the list of all available layers and their properties.
        """
        params = {"f": "json"}
        try:
            log.info(f"Fetching service metadata from: {self.service_url}")
            response = self.session.get(self.service_url, params=params, timeout=self.timeout)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            metadata = response.json()
            log.info("Successfully fetched and parsed service metadata.")
            return metadata
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to fetch service metadata: {e}")
            return None
        except ValueError:  # Catches JSON decoding errors
            log.error("Failed to decode JSON from response.")
            return None

    def discover_layers(self) -> List[Dict[str, Any]]:
        """
        Discovers all layers from the service metadata and extracts their ID and name.

        Returns:
            A list of dictionaries, where each dictionary represents a layer
            with its 'id' and 'name'.
        """
        layers_to_process: List[Dict[str, Any]] = []
        service_meta = self._get_service_metadata()

        if not service_meta:
            log.error("Could not retrieve service metadata. Cannot discover layers.")
            return layers_to_process

        # The metadata contains a 'layers' key with a list of layer objects
        available_layers = service_meta.get("layers", [])

        if not available_layers:
            # This handles cases for single-layer FeatureServers where the root metadata
            # describes the layer itself.
            if service_meta.get("type") == "Feature Layer":
                log.info("Service appears to be a single-layer FeatureServer.")
                layer_id = service_meta.get("id", "0")
                # Use the service's name as the layer name
                layer_name = service_meta.get("name", f"layer_{layer_id}")
                layers_to_process.append({"id": str(layer_id), "name": layer_name})
            else:
                log.warning("No layers found in the service metadata.")
        else:
            # This is the standard case for MapServers or multi-layer FeatureServers
            log.info(f"Found {len(available_layers)} layers in service metadata.")
            for layer_details in available_layers:
                layer_id = layer_details.get("id")
                if layer_id is not None:
                    # Get the layer's name, but fall back to a generic name if it's missing
                    layer_name = layer_details.get("name", f"layer_{layer_id}")
                    layers_to_process.append({"id": str(layer_id), "name": layer_name})

        return layers_to_process

# --- Example Usage ---

if __name__ == "__main__":
    # Example URL for a public Esri FeatureServer
    # This service contains multiple layers with descriptive names.
    EXAMPLE_URL = "<https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/World_Time_Zones/FeatureServer>"

    print(f"--- Running example for: {EXAMPLE_URL} ---")
    resolver = LayerNameResolver(service_url=EXAMPLE_URL)
    discovered_layers = resolver.discover_layers()

    if discovered_layers:
        print("\n✅ Successfully discovered the following layers:")
        for layer in discovered_layers:
            print(f"  - Layer ID: {layer['id']}, Name: '{layer['name']}'")

        # You would now use this information in your download logic.
        # For example, when downloading data for layer '0', you would use the name
        # 'World Time Zones' to name the output file.
        first_layer = discovered_layers[0]
        layer_id_to_download = first_layer['id']
        output_filename = f"{first_layer['name'].replace(' ', '_')}.geojson"

        print(f"\n💡 Implementation idea:")
        print(f"  When downloading from .../FeatureServer/{layer_id_to_download},")
        print(f"  you would save the output as: '{output_filename}'")
    else:
        print("\n❌ Failed to discover any layers.")
How It Works: A Step-by-Step Explanation
Fetch Service Metadata: The process starts by making a single HTTP GET request to the base URL of the Esri REST service (e.g., .../FeatureServer). It adds the query parameter f=json, which tells the server to return a detailed JSON description of the service.

Parse the JSON Response: This JSON response contains all the service's properties. The most important key is "layers", which contains a list of objects, where each object represents a single layer.

Extract Layer ID and Name: The code iterates through the "layers" list. For each layer, it extracts:

"id": The unique numerical identifier (e.g., 0, 1). This is needed to query the layer's data.

"name": The human-readable name (e.g., "World Time Zones"). This is the name you want to use.

Handle Fallbacks:

Missing Name: If a layer object lacks a "name", the code creates a fallback like layer_0.

Single-Layer Services: Some services point directly to a single layer. The code handles this by checking the service's "type" and using the service's own metadata if the "layers" list is empty.

Instructions for Implementation
Follow these steps to integrate the logic into your pipeline.

Step 1: Identify the Service URL
Ensure you have the base URL for the service, not a specific layer.

Correct: https://.../FeatureServer

Incorrect: https://.../FeatureServer/0

Step 2: Discover Layers Before Downloading
Before your download loop, use the LayerNameResolver to get a list of all layers and their names.

Python

# In your main pipeline script

from layer_name_resolver import LayerNameResolver

my_service_url = "YOUR_SERVICE_URL_HERE"

resolver = LayerNameResolver(service_url=my_service_url)
layers_to_download = resolver.discover_layers()

if not layers_to_download:
    print("Stopping pipeline, no layers were found.")
    exit()
Step 3: Modify Your Download Loop
Loop through the layers_to_download list instead of a hardcoded range of numbers.

Python

# In your download logic

for layer_info in layers_to_download:
    layer_id = layer_info['id']
    layer_name = layer_info['name']

    # Construct the URL to query the specific layer's data
    query_url = f"{my_service_url}/{layer_id}/query"

    # Sanitize the layer name to make it a valid filename
    sanitized_name = layer_name.replace(' ', '_').replace('/', '-') # Basic example
    output_file = f"{sanitized_name}.geojson"

    print(f"Downloading data for layer '{layer_name}' (ID: {layer_id}) into {output_file}...")

    # --- Your existing download code goes here ---
    # Use 'query_url' to fetch the data and 'output_file' to save it.
    # For example:
    # response = requests.get(query_url, params={"where": "1=1", "f": "geojson", ...})
    # with open(output_file, 'w') as f:
    #     f.write(response.text)
    # ---------------------------------------------
