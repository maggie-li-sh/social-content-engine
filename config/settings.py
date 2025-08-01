"""
Configuration settings
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')

# Data Configuration
DEFAULT_CSV_PATH = 'data/2025-07-29 3_51pm.csv'
TOP_PERFORMERS_COUNT = int(os.getenv('TOP_PERFORMERS_COUNT', 10))

# Rate Limiting
API_DELAY_SECONDS = 20  # For OpenAI rate limits