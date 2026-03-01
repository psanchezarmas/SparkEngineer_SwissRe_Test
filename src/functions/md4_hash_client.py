"""
MD4 Hash API Client
Fetches MD4 hashes for CLAIM_IDs from the external hashify API
"""
import requests
import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

BASE_URL = "https://api.hashify.net/hash/md4/hex"


class MD4HashClient:
    """Client for fetching MD4 hashes from hashify API."""
    
    def __init__(self, base_url: str = BASE_URL, timeout: int = 10, max_retries: int = 3):
        """
        Initialize the MD4 Hash client.
        
        Args:
            base_url: The base URL of the hashify API
            timeout: Request timeout in seconds
            max_retries: Number of retries for failed requests
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
    
    def fetch_hash(self, claim_id: str) -> Optional[Dict]:
        """
        Fetch MD4 hash for a given CLAIM_ID.
        
        Args:
            claim_id: The CLAIM_ID to hash
            
        Returns:
            Dictionary with API response and added stored_timestamp, or None if request fails
        """
        url = f"{self.base_url}?value={claim_id}"
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                # Add timestamp when stored
                data = response.json()
                data['stored_timestamp'] = datetime.utcnow().isoformat()
                
                logger.info(f"Successfully fetched hash for {claim_id}")
                return data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed for {claim_id}: {str(e)}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to fetch hash for {claim_id} after {self.max_retries} retries")
                    return None
        
        return None
    
    def fetch_hashes_batch(self, claim_ids: list) -> list:
        """
        Fetch MD4 hashes for multiple CLAIM_IDs.
        
        Args:
            claim_ids: List of CLAIM_IDs to hash
            
        Returns:
            List of dictionaries with API responses and timestamps
        """
        results = []
        for claim_id in claim_ids:
            data = self.fetch_hash(claim_id)
            if data:
                data['claim_id'] = claim_id
                results.append(data)
        
        return results
