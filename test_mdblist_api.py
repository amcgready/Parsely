#!/usr/bin/env python3
import os
import requests
import time
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MDBLIST_API_KEY = os.getenv("MDBLIST_API_KEY")

if not MDBLIST_API_KEY:
    print("❌ Error: MDBLIST_API_KEY not found in .env file")
    exit(1)

print(f"🔑 Using API key: {MDBLIST_API_KEY[:4]}...{MDBLIST_API_KEY[-4:]} (hidden middle)")

# Test list URL: Universal Pictures Productions by amcgready
test_list_url = "https://mdblist.com/lists/amcgready/universal-pictures-productions"

# Define all potential API URL patterns to test
api_patterns = [
    # Standard formats from documentation
    "https://mdblist.com/api/lists/{username}/{list_id}",
    "https://mdblist.com/api/lists/{username}/{list_id}/items",
    "https://mdblist.com/api/users/{username}/lists/{list_id}",
    "https://mdblist.com/api/users/{username}/lists/{list_id}/items",
    
    # Alternative formats to try
    "https://mdblist.com/api/lists/user/{username}/{list_id}",
    "https://mdblist.com/api/lists/user/{username}/{list_id}/items",
]

# Extract username and list ID from the URL
username = "amcgready"
list_id = "universal-pictures-productions"
print(f"📝 Testing API access for list: {username}/{list_id}")

# Headers for API requests
headers = {
    "X-API-KEY": MDBLIST_API_KEY,
    "User-Agent": "Mozilla/5.0 (compatible; Parsely/1.0)"
}

results = []

# Test each API pattern
for pattern in api_patterns:
    api_url = pattern.format(username=username, list_id=list_id)
    print(f"\n🔍 Testing API URL: {api_url}")
    
    # Try the request with exponential backoff
    success = False
    for attempt in range(3):
        try:
            if attempt > 0:
                wait_time = 5 * (2 ** attempt)
                print(f"⏱️ Waiting {wait_time} seconds before retry #{attempt+1}...")
                time.sleep(wait_time)
            
            print(f"🔄 Attempt #{attempt+1}...")
            start_time = time.time()
            response = requests.get(api_url, headers=headers, timeout=30)
            elapsed = time.time() - start_time
            
            print(f"📊 Status Code: {response.status_code} (took {elapsed:.2f}s)")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    item_count = len(data.get("items", [])) if isinstance(data, dict) and "items" in data else 0
                    print(f"✅ Success! Found {item_count} items in response")
                    
                    if item_count > 0:
                        print(f"📝 First few titles:")
                        for i, item in enumerate(data["items"][:3]):
                            print(f"  - {item.get('title', '[No title]')}")
                    
                    results.append({
                        "url": api_url,
                        "status": "success",
                        "items": item_count,
                        "elapsed": elapsed
                    })
                    
                    success = True
                    break  # Exit retry loop for this URL
                except ValueError:
                    print("❌ Error: Response was not valid JSON")
                    print(f"📄 Response preview: {response.text[:200]}...")
            elif response.status_code == 503:
                print("⚠️ Service Unavailable (503) - API might be overloaded")
            elif response.status_code == 403:
                print("🔒 Forbidden (403) - API key might be invalid or expired")
            elif response.status_code == 404:
                print("🔍 Not Found (404) - Endpoint does not exist")
            else:
                print(f"⚠️ Unexpected status code: {response.status_code}")
                
            if response.text:
                try:
                    error_data = response.json()
                    print(f"📄 Error response: {json.dumps(error_data, indent=2)[:200]}")
                except:
                    print(f"📄 Response preview: {response.text[:200]}...")
        
        except requests.exceptions.Timeout:
            print("⏱️ Request timed out")
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
    
    if not success:
        results.append({
            "url": api_url,
            "status": "failed",
            "elapsed": elapsed if 'elapsed' in locals() else None
        })

# Print summary of results
print("\n" + "="*50)
print("📊 SUMMARY OF RESULTS")
print("="*50)

working_endpoints = [r for r in results if r["status"] == "success" and r.get("items", 0) > 0]
if working_endpoints:
    print(f"🎉 Found {len(working_endpoints)} working API endpoints!")
    for i, endpoint in enumerate(working_endpoints, 1):
        print(f"{i}. {endpoint['url']}")
        print(f"   - Items: {endpoint['items']}")
        print(f"   - Response time: {endpoint['elapsed']:.2f}s")
else:
    print("❌ No working API endpoints found.")
    print("\n📝 Recommendations:")
    print("1. Check if your API key is valid")
    print("2. Try again later as the service might be temporarily unavailable")
    print("3. Consider falling back to web scraping")

print("\n🔧 To fix your code, update the API URL format in fetch_mdblist_api() to use the working endpoint")