#!/usr/bin/env python3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check MDBLIST_API_KEY
mdblist_api_key = os.getenv("MDBLIST_API_KEY")
if mdblist_api_key:
    print(f"✅ MDBLIST_API_KEY found: {mdblist_api_key[:4]}...{mdblist_api_key[-4:]} (middle hidden)")
else:
    print("❌ MDBLIST_API_KEY not found in environment variables")

# Check TMDB_API_KEY
tmdb_api_key = os.getenv("TMDB_API_KEY")
if tmdb_api_key:
    print(f"✅ TMDB_API_KEY found: {tmdb_api_key[:4]}...{tmdb_api_key[-4:]} (middle hidden)")
else:
    print("❌ TMDB_API_KEY not found in environment variables")

# Check other settings
print(f"ENABLE_TMDB_MATCHING: {os.getenv('ENABLE_TMDB_MATCHING', 'Not set')}")
print(f"INCLUDE_YEAR: {os.getenv('INCLUDE_YEAR', 'Not set')}")