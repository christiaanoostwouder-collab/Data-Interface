# backend/core/settings.py
import os
from pathlib import Path

# .env laden als aanwezig (optioneel)
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_PATH)
    except Exception:
        pass  # geen probleem als dotenv ontbreekt

# Simpele getters, geen third-party lib nodig
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
POLYMARKET_API = os.getenv("POLYMARKET_API", "https://data-api.polymarket.com")
