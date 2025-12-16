"""
Ticketmaster API Ingestion Script

This script fetches data from Ticketmaster API endpoints and lands raw JSON
responses to Unity Catalog Volumes for subsequent processing in the Bronze layer.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
import yaml


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TicketmasterAPIClient:
    """Client for interacting with Ticketmaster Discovery API"""

    def __init__(self, api_key: str, base_url: str = "https://app.ticketmaster.com/discovery/v2"):
        """
        Initialize the Ticketmaster API client

        Args:
            api_key: Ticketmaster API key
            base_url: Base URL for the API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to the Ticketmaster API with retry logic

        Args:
            endpoint: API endpoint (e.g., '/events.json')
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        if params is None:
            params = {}

        params['apikey'] = self.api_key

        url = f"{self.base_url}{endpoint}"
        logger.info(f"Fetching from {url} with params: {params}")

        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def fetch_paginated_data(
        self,
        endpoint: str,
        page_size: int = 200,
        max_pages: Optional[int] = None,
        additional_params: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Fetch all pages of data from an endpoint

        Args:
            endpoint: API endpoint
            page_size: Number of items per page
            max_pages: Maximum number of pages to fetch (None = all)
            additional_params: Additional query parameters

        Returns:
            List of all items across all pages
        """
        all_items = []
        page = 0

        if additional_params is None:
            additional_params = {}

        while True:
            if max_pages and page >= max_pages:
                logger.info(f"Reached max pages limit: {max_pages}")
                break

            params = {
                'size': page_size,
                'page': page,
                **additional_params
            }

            try:
                response_data = self._make_request(endpoint, params)

                # Extract items based on endpoint
                items_key = self._get_items_key(endpoint)
                embedded = response_data.get('_embedded', {})
                items = embedded.get(items_key, [])

                if not items:
                    logger.info(f"No more items found on page {page}")
                    break

                all_items.extend(items)
                logger.info(f"Fetched page {page} with {len(items)} items. Total so far: {len(all_items)}")

                # Check if there are more pages
                page_info = response_data.get('page', {})
                total_pages = page_info.get('totalPages', 1)

                if page >= total_pages - 1:
                    logger.info(f"Reached last page: {page}")
                    break

                page += 1

            except Exception as e:
                logger.error(f"Error fetching page {page}: {str(e)}")
                break

        return all_items

    def _get_items_key(self, endpoint: str) -> str:
        """Get the key name for items in the response based on endpoint"""
        if 'events' in endpoint:
            return 'events'
        elif 'venues' in endpoint:
            return 'venues'
        elif 'attractions' in endpoint:
            return 'attractions'
        elif 'classifications' in endpoint:
            return 'classifications'
        else:
            return 'items'


class VolumeWriter:
    """Writer for landing data to Unity Catalog Volumes"""

    def __init__(self, volume_path: str):
        """
        Initialize the volume writer

        Args:
            volume_path: Path to UC Volume (e.g., /Volumes/catalog/schema/volume)
        """
        self.volume_path = volume_path

    def write_json(self, data: List[Dict], entity_type: str, batch_timestamp: str) -> str:
        """
        Write data as JSON to the volume

        Args:
            data: List of items to write
            entity_type: Type of entity (events, venues, etc.)
            batch_timestamp: Timestamp for this batch

        Returns:
            Path to the written file
        """
        # Create directory structure: entity_type/year/month/day/
        date_obj = datetime.fromisoformat(batch_timestamp)
        date_path = date_obj.strftime("%Y/%m/%d")

        output_dir = f"{self.volume_path}/{entity_type}/{date_path}"
        os.makedirs(output_dir, exist_ok=True)

        # Create filename with timestamp
        timestamp_str = date_obj.strftime("%Y%m%d_%H%M%S")
        filename = f"{entity_type}_{timestamp_str}.json"
        output_path = f"{output_dir}/{filename}"

        # Write data
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Wrote {len(data)} items to {output_path}")
        return output_path


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    """Main ingestion workflow"""

    # Load configuration
    config = load_config()

    # Get API key from environment
    api_key = os.getenv('TICKETMASTER_API_KEY')
    if not api_key:
        raise ValueError("TICKETMASTER_API_KEY environment variable not set")

    # Initialize client and writer
    client = TicketmasterAPIClient(
        api_key=api_key,
        base_url=config['ticketmaster_api']['base_url']
    )

    volume_path = config['unity_catalog']['volume_path']
    writer = VolumeWriter(volume_path)

    # Batch timestamp for this run
    batch_timestamp = datetime.utcnow().isoformat()

    # Fetch and write data for each endpoint
    endpoints = config['ticketmaster_api']['endpoints']
    page_size = config['ticketmaster_api']['pagination']['page_size']
    max_pages = config['ticketmaster_api']['pagination']['max_pages']

    for entity_type, endpoint in endpoints.items():
        logger.info(f"Starting ingestion for {entity_type}")

        try:
            # Fetch data
            data = client.fetch_paginated_data(
                endpoint=endpoint,
                page_size=page_size,
                max_pages=max_pages
            )

            if data:
                # Write to volume
                output_path = writer.write_json(
                    data=data,
                    entity_type=entity_type,
                    batch_timestamp=batch_timestamp
                )
                logger.info(f"Successfully ingested {len(data)} {entity_type} to {output_path}")
            else:
                logger.warning(f"No data found for {entity_type}")

        except Exception as e:
            logger.error(f"Error processing {entity_type}: {str(e)}")
            continue

    logger.info("Ingestion complete")


if __name__ == "__main__":
    main()
