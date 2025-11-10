import requests
from config import settings
import time

def get_embedding(text, max_retries=3):
    if not text or not isinstance(text, str):
        return None
    
    api_url = settings.HF_EMBEDDING_API_URL
    headers = {"Authorization": f"Bearer {settings.HF_API_TOKEN}"}
    payload = {"inputs": text}
    
    for attempt in range(max_retries):
        try:
            response = requests.post(api_url, headers=headers,json=payload)
            if response.status_code == 200:
                return response.json()
            else:
                time.sleep(1 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(1 * (attempt + 1))
            
    print(f"Failed to get embedding for text after {max_retries} attempts.")
    return None