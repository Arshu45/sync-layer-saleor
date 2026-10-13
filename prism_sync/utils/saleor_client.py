"""
Saleor GraphQL Client utility for PRISM sync workers
"""

import logging
import os
import requests
from typing import Any, Dict, Optional

logger = logging.getLogger("saleor_client")


class SaleorClient:
    def __init__(self, endpoint: Optional[str] = None, token: Optional[str] = None):
        self.endpoint = endpoint or os.getenv("SALEOR_GRAPHQL", "http://127.0.0.1:8000/graphql/")
        self.token = token or os.getenv("SALEOR_APP_TOKEN", "")

    def execute(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables or {}},
            headers=headers,
            timeout=30,
        )

        if response.status_code != 200:
            raise RuntimeError(f"Saleor GraphQL HTTP {response.status_code}: {response.text}")

        res_json = response.json()
        if "errors" in res_json and res_json["errors"]:
            raise RuntimeError(f"Saleor GraphQL errors: {res_json['errors']}")

        return res_json.get("data", {})

    def get_first_product_type_id(self) -> Optional[str]:
        """Fetches the default ProductType ID in Saleor for creating new products."""
        query = """
        query GetProductTypes {
          productTypes(first: 1) {
            edges {
              node {
                id
                name
              }
            }
          }
        }
        """
        data = self.execute(query)
        edges = data.get("productTypes", {}).get("edges", [])
        if edges:
            return edges[0]["node"]["id"]
        return None

    def get_channel_ids(self) -> Dict[str, str]:
        """Returns map of channel slug -> channel ID."""
        query = """
        query GetChannels {
          channels {
            id
            slug
          }
        }
        """
        data = self.execute(query)
        channels = data.get("channels", [])
        return {ch["slug"]: ch["id"] for ch in channels}

