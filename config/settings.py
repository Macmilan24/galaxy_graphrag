import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    

    HF_API_TOKEN = os.getenv("HF_API_TOKEN")
    HF_EMBEDDING_URL = os.getenv("HF_EMBEDDING_URL")
    
    GALAXY_URL = os.getenv("GALAXY_URL", "https://usegalaxy.org")
    GALAXY_API_KEY = os.getenv("GALAXY_API_KEY")
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    
    
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_RAW = os.path.join(PROJECT_ROOT, "data", "raw")
    DATA_PROCESSED = os.path.join(PROJECT_ROOT, "data", "processed")
    DATA_EMBEDDINGS = os.path.join(PROJECT_ROOT, "data", "embeddings")

settings = Settings()