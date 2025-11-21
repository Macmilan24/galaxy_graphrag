import requests
from config import settings
from src.utils.logger import get_logger
import time

logger = get_logger("embedding_service")

class EmbeddingService:
    def __init__(self):
        self.api_url = settings.HF_EMBEDDING_URL
        self.api_token = settings.HF_API_TOKEN
        
        if not self.api_url or not self.api_token:
            logger.warning("HF_EMBEDDING_URL or HF_API_TOKEN not set. Embeddings will be empty.")
            self.api_url = None

    def generate_embedding(self, text):
        """Generates embedding for a single string using HF Inference API."""
        if not text or not self.api_url:
            return []
        
        headers = {"Authorization": f"Bearer {self.api_token}"}
        payload = {"inputs": text}
        
        for attempt in range(3):
            try:
                response = requests.post(self.api_url, headers=headers, json=payload, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        if data and isinstance(data[0], list):
                            return data[0]
                        return data
                    return []
                elif response.status_code == 503:
                    # Model loading
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"HF API Error {response.status_code}: {response.text}")
                    return []
            except Exception as e:
                logger.error(f"Error generating embedding: {e}")
                time.sleep(1)
        return []

    def generate_embeddings_batch(self, texts, batch_size=10):
        """Generates embeddings for a list of strings."""
        if not texts or not self.api_url:
            return [[] for _ in texts]
            
        headers = {"Authorization": f"Bearer {self.api_token}"}
        embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            payload = {"inputs": batch}
            try:
                response = requests.post(self.api_url, headers=headers, json=payload, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    embeddings.extend(data)
                else:
                    logger.error(f"HF Batch API Error {response.status_code}: {response.text}")
                    embeddings.extend([[]] * len(batch))
            except Exception as e:
                logger.error(f"Error in batch embedding: {e}")
                embeddings.extend([[]] * len(batch))
            time.sleep(0.2) # Rate limit protection
            
        return embeddings
