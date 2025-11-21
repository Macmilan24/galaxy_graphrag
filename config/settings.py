import os
from dotenv import load_dotenv

load_dotenv()


GALAXY_URL = os.getenv("GALAXY_URL")
GALAXY_API_KEY = os.getenv("GALAXY_API_KEY")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

HF_EMBEDDING_URL = os.getenv("HF_EMBEDDING_URL")
HF_API_TOKEN = os.getenv("HF_API_TOKEN")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
