#!/usr/bin/env python3
import os
import re
import json
import time
import threading
import traceback
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --------- ENV MANAGEMENT ---------
ENV_FILE = ".env"
load_dotenv()

def get_env_flag(key, default="false"):
    return os.getenv(key, default).lower() == "true"

def get_env_string(key, default=""):
    return os.getenv(key, default)

def update_env_variable(key, value):
    value = "true" if value else "false"
    updated = False
    lines = []

    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r") as f:
            for line in f:
                if line.startswith(key + "="):
                    lines.append(f"{key}={value}\n")
                    updated = True
                else:
                    lines.append(line)
    if not updated:
        lines.append(f"{key}={value}\n")

    with open(ENV_FILE, "w") as f:
        f.writelines(lines)

    # Refresh in current session
    os.environ[key] = value

def update_env_string(key, value):
    updated = False
    lines = []

    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r") as f:
            for line in f:
                if line.startswith(key + "="):
                    lines.append(f"{key}={value}\n")
                    updated = True
                else:
                    lines.append(line)
    if not updated:
        lines.append(f"{key}={value}\n")

    with open(ENV_FILE, "w") as f:
        f.writelines(lines)

    # Refresh in current session
    os.environ[key] = value

# --------- CONFIG FLAGS ---------
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY not found in .env")

SCAN_HISTORY_FILE = "scan_history.json"
MONITOR_CONFIG_FILE = "monitor_config.json"
DEFAULT_MONITOR_INTERVAL = 1440  # 24 hours by default

def load_monitor_config():
    """Load the monitor configuration from file"""
    if os.path.exists(MONITOR_CONFIG_FILE):
        try:
            with open(MONITOR_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Warning: {MONITOR_CONFIG_FILE} contains invalid JSON. Creating new configuration.")
    
    # Return default empty configuration
    return {
        "monitor_interval": DEFAULT_MONITOR_INTERVAL,
        "last_run": None,
        "enabled": True,
        "monitored_lists": {}
    }

def save_monitor_config(config):
    """Save the monitor configuration to file"""
    with open(MONITOR_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")

def load_scan_history():
    if os.path.exists(SCAN_HISTORY_FILE):
        with open(SCAN_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_scan_history(history):
    """
    Save scan history to JSON file, merging with existing data rather than overwriting
    """
    # Load existing history if file exists
    existing_history = {}
    if os.path.exists(SCAN_HISTORY_FILE):
        try:
            with open(SCAN_HISTORY_FILE, "r", encoding="utf-8") as f:
                existing_history = json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Warning: {SCAN_HISTORY_FILE} contains invalid JSON. Creating new file.")
    
    # Merge new history with existing history
    # This updates existing keys and adds new ones
    for filename, entries in history.items():
        if filename in existing_history:
            # If the filename exists, add new entries to it
            existing_history[filename].update(entries)
        else:
            # If the filename is new, add the whole entry
            existing_history[filename] = entries
            
    # Write the merged history back to file
    with open(SCAN_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(existing_history, f, indent=2, ensure_ascii=False)

def clear_history(option="all", file_name=None):
    history = load_scan_history()
    if option == "all":
        history = {}
    elif option == "file" and file_name:
        history.pop(file_name, None)
    save_scan_history(history)

def load_titles_from_file(filepath):
    if not os.path.exists(filepath):
        return set()

    saved_titles = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            original = line.split("->")[0].split("[")[0].strip()
            if original:
                saved_titles.add(original)
    return saved_titles

def extract_titles_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('div.header.movie-title')
    return [card.get_text(strip=True).rsplit("(", 1)[0].replace(":", "").strip() for card in cards]

def extract_titles_from_trakt_html(html):
    """Extract titles from a Trakt list page"""
    soup = BeautifulSoup(html, 'html.parser')
    titles = []
    
    # Look for movie/show items in the list
    list_items = soup.select('div.grid-item')
    
    # For tracking duplicates to determine if this is the same page content
    seen_titles = set()
    duplicates_found = 0
    
    for item in list_items:
        # Find title element - looking for the specific structure in Trakt lists
        title_elem = item.select_one('a.titles-link h3')
        if title_elem:
            # Get the title text
            title = title_elem.get_text(strip=True)
            
            # Try to find the year separately
            year_elem = item.select_one('div.year')
            year = year_elem.get_text(strip=True) if year_elem else None
            
            # Create clean title
            if title:
                # If year was found separately, make sure it's not already in the title
                if year and f"({year})" in title:
                    title = title.replace(f"({year})", "").strip()
                
                # Check if we've already seen this title (indicates duplicate content)
                if title in seen_titles:
                    duplicates_found += 1
                else:
                    seen_titles.add(title)
                    titles.append(title)
    
    # If all titles were duplicates, this indicates we're likely seeing the same page again
    if duplicates_found > 0 and duplicates_found >= len(list_items) - 1:
        # Return a special flag to indicate duplicate content
        return "DUPLICATE_PAGE"
    
    return titles

def scrape_trakt_page(url, page):
    """Scrape a specific page from a Trakt list"""
    try:
        # Trakt uses a different pagination format
        page_url = url
        if page > 1:
            # For standard Trakt list pages, try using standard pagination format
            page_url = f"{url}?page={page}"
            
            # Special handling for specific URL patterns
            if "/users/" in url and "/lists/" in url:
                # Extract list ID for items API
                list_id = url.split("/lists/")[-1].split("/")[0]
                if list_id.isdigit():
                    # Try the API-style pagination for user lists
                    page_url = f"{url}/items?page={page}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        print(f"📄 Fetching Trakt page: {page_url}")
        response = requests.get(page_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Extract the titles from the HTML
        titles = extract_titles_from_trakt_html(response.text)
        
        # Check if we got the duplicate page marker
        if titles == "DUPLICATE_PAGE":
            print(f"⚠️ Detected duplicate content on page {page} - this is likely the end of the list")
            return page, "Error: End of list reached"
            
        print(f"🎬 Found {len(titles)} titles on Trakt page {page}")
        
        return page, titles
    except Exception as e:
        print(f"❌ Error fetching Trakt page {page}: {str(e)}")
        return page, f"Error: {str(e)}"

def scrape_letterboxd_page(url, page):
    """Scrape a specific page from a Letterboxd list"""
    try:
        # Letterboxd uses a different pagination format
        page_url = url
        if page > 1:
            # Remove trailing slash if present
            if url.endswith('/'):
                url = url[:-1]
                
            # Add pagination path
            page_url = f"{url}/page/{page}/"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        
        print(f"📄 Fetching Letterboxd page: {page_url}")
        response = requests.get(page_url, headers=headers, timeout=15)
        
        # For debugging, save the first page HTML
        if page == 1:
            debug_path = "letterboxd_page1.html"
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"ℹ️ Saved first page HTML to {debug_path} for reference")
        
        # Handle HTTP status codes
        if response.status_code != 200:
            print(f"⚠️ Letterboxd returned status code {response.status_code}")
            if response.status_code == 404 and page > 1:
                # This is normal for the last page
                return page, f"Error: 404 - End of list"
            else:
                return page, f"Error: HTTP {response.status_code}"
        
        # Extract the titles from the HTML
        titles = extract_titles_from_letterboxd_html(response.text)
        print(f"🎬 Found {len(titles)} titles on Letterboxd page {page}")
        
        # If no titles found on first page, save HTML and output details
        if not titles and page == 1:
            debug_path = "letterboxd_debug_failed.html"
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"⚠️ No titles found. Saved HTML to {debug_path} for debugging")
            print("❗ Please check the HTML for the correct structure and update the extraction code")
        
        return page, titles
    except Exception as e:
        print(f"❌ Error fetching Letterboxd page {page}: {str(e)}")
        return page, f"Error: {str(e)}"

def extract_titles_from_letterboxd_html(html):
    """Extract titles from a Letterboxd list page"""
    soup = BeautifulSoup(html, 'html.parser')
    titles = []
    
    # METHOD 1: Special handling for comparison lists (If you like this, watch this format)
    pairs_found = soup.select('div.film-pair')
    if pairs_found:
        print(f"📝 Detected comparison list format with {len(pairs_found)} pairs")
        
        for pair in pairs_found:
            # Each pair has two films
            film_posters = pair.select('div.film-poster')
            for poster in film_posters:
                if 'data-film-name' in poster.attrs:
                    title = poster['data-film-name']
                    year = poster.get('data-film-release-year', '')
                    
                    # Add title with year if available
                    if year:
                        titles.append(f"{title} ({year})")
                    else:
                        titles.append(title)
                        
            # If no posters with data attributes were found, try link extraction
            if not film_posters:
                film_links = pair.select('a.linked-film')
                for link in film_links:
                    title = link.get_text(strip=True)
                    if title:
                        titles.append(title)
        
        # If we found titles in this format, return them
        if titles:
            return titles
    
    # METHOD 2: Try standard formats - poster grid
    list_items = soup.select('li.poster-container div.film-poster')
    
    for item in list_items:
        if 'data-film-name' in item.attrs:
            title = item['data-film-name']
            year = item.get('data-film-release-year', '')
            
            # Add title with year if available
            if year:
                titles.append(f"{title} ({year})")
            else:
                titles.append(title)
    
    # METHOD 3: Try direct linked-film extraction (more aggressive)
    if not titles:
        linked_films = soup.select('a.linked-film')
        for link in linked_films:
            title = link.get_text(strip=True)
            if title and title not in titles:
                titles.append(title)
    
    # METHOD 4: Alternative structures for different list formats
    if not titles:
        # Try table view format
        list_items = soup.select('table.film-list td.film-title-wrapper a')
        for item in list_items:
            title = item.get_text(strip=True)
            if title and title not in titles:
                titles.append(title)
                
        # Try the standard film grid format
        if not titles:
            list_items = soup.select('div.film-detail h2.film-title a')
            for item in list_items:
                title = item.get_text(strip=True)
                if title and title not in titles:
                    titles.append(title)
                    
        # Try film-pair-content format
        if not titles:
            list_items = soup.select('div.film-pair-content h3.film-title a')
            for item in list_items:
                title = item.get_text(strip=True)
                if title and title not in titles:
                    # Check if there's a year element nearby
                    year_elem = item.find_next('small', class_='metadata')
                    if year_elem:
                        year_text = year_elem.get_text(strip=True)
                        if year_text and year_text.isdigit():
                            title = f"{title} ({year_text})"
                    titles.append(title)
    
        # Try any film-title links
        if not titles:
            list_items = soup.select('a.film-title')
            for item in list_items:
                title = item.get_text(strip=True)
                if title and title not in titles:
                    titles.append(title)
                    
    # METHOD 5: Extract from JSON-LD data
    if not titles:
        script_tags = soup.find_all('script', {'type': 'application/ld+json'})
        for script in script_tags:
            try:
                json_data = json.loads(script.string)
                if isinstance(json_data, dict) and 'itemListElement' in json_data:
                    for item in json_data['itemListElement']:
                        if 'item' in item and 'name' in item['item']:
                            title = item['item']['name']
                            if title and title not in titles:
                                titles.append(title)
            except:
                continue
    
    # METHOD 6: Super generic fallback approach
    if not titles:
        # Save HTML for debugging
        with open("letterboxd_debug.html", "w", encoding="utf-8") as f:
            f.write(str(soup))
            
        print("🔍 Using generic title extraction as fallback")
        
        # Look for ANY links that might contain film titles
        all_links = soup.select('a')
        for link in all_links:
            # Check if it has film-related classes
            classes = link.get('class', [])
            if any(cls in ['film-title', 'title-alt', 'frame', 'linked-film'] for cls in classes):
                title = link.get_text(strip=True)
                if title and len(title) > 1 and title not in titles:
                    titles.append(title)
    
    return titles

def extract_letterboxd_titles_using_selenium(url, quick_mode=True):
    """
    Extract titles from a Letterboxd page using Selenium for JavaScript-rendered content
    This is a fallback for when regular HTML scraping fails
    
    Parameters:
    - url: The Letterboxd list URL
    - quick_mode: If True, use optimized settings for faster extraction
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
    except ImportError:
        print("❌ Selenium is not installed. Run: pip install selenium")
        return []
        
    print("🌐 Starting Chrome in headless mode for JavaScript rendering...")
    
    # Configure Chrome options
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Speed optimizations
    if quick_mode:
        # Disable images for faster loading
        options.add_argument("--blink-settings=imagesEnabled=false")
        # Disable JavaScript that's not necessary for the initial page load
        options.add_argument("--disable-javascript")
        # Smaller window size for faster rendering
        options.add_argument("--window-size=800,600")
        # Disable extensions
        options.add_argument("--disable-extensions")
        # Disable various background networking
        options.add_argument("--disable-background-networking")
        # Disable background timer throttling
        options.add_argument("--disable-background-timer-throttling")
        # Disable backgrounding renders
        options.add_argument("--disable-backgrounding-occluded-windows")
        # Use low process priority
        options.add_argument("--disable-renderer-backgrounding")
    else:
        options.add_argument("--window-size=1920,1080")
        
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    titles = []
    
    try:
        # Initialize the driver
        driver = webdriver.Chrome(options=options)
        
        # Set page load timeout to prevent hanging
        driver.set_page_load_timeout(20)
        
        # Load the page
        print(f"📄 Loading page: {url}")
        driver.get(url)
        
        # Wait for content to load (reduced time)
        print("📊 Waiting for page to render...")
        time.sleep(1.5 if quick_mode else 3)  # Reduced wait time in quick mode
        
        # Extract from alt attributes directly (fastest method)
        print("🔍 Extracting titles from poster images...")
        
        # Direct extraction from img alt attributes (much faster than data attributes)
        img_elements = driver.find_elements(By.CSS_SELECTOR, 'li.poster-container div.film-poster img')
        for img in img_elements:
            alt_text = img.get_attribute('alt')
            if alt_text and alt_text not in titles:
                titles.append(alt_text)
                
        # If we got fewer than expected titles, try the data-film-name attributes
        if len(titles) < 50:  # Most lists have at least 50 films
            print("⚠️ Few titles found with quick method, trying data attributes...")
            film_posters = driver.find_elements(By.CSS_SELECTOR, 'li.poster-container div.film-poster')
            
            for poster in film_posters:
                title = poster.get_attribute('data-film-name')
                if title:
                    year = poster.get_attribute('data-film-release-year')
                    if year:
                        full_title = f"{title} ({year})"
                    else:
                        full_title = title
                        
                    if full_title not in titles:
                        titles.append(full_title)
                        
        print(f"✅ Extracted {len(titles)} titles using Selenium")
        
    except Exception as e:
        print(f"❌ Selenium error: {str(e)}")
    finally:
        # Close the browser
        try:
            driver.quit()
        except:
            pass
    
    return titles

def letterboxd_get_all_pages(url):
    """
    Get all titles from a Letterboxd list, optimized for speed
    
    Letterboxd loads all items on a single page for most lists,
    so we don't need to handle pagination separately
    """
    print(f"📋 Processing Letterboxd list: {url}")
    
    # First try with regular requests
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=5)  # Short timeout for quick failure
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        # Quick check - if we can find poster images, we might not need Selenium
        posters = soup.select('li.poster-container div.film-poster')
        title_elements = soup.select('div.film-detail h2.film-title a')
        
        if posters or title_elements:
            print("🔍 Found film elements in HTML, trying regular extraction...")
            titles = extract_titles_from_letterboxd_html(html)
            if titles:
                print(f"✅ Successfully extracted {len(titles)} titles without Selenium")
                return titles
    except:
        pass
        
    # Fall back to Selenium only if needed
    print("⚠️ Regular extraction failed, falling back to Selenium...")
    return extract_letterboxd_titles_using_selenium(url)

def scrape_letterboxd_page_with_selenium(url):
    """
    Scrape Letterboxd using Selenium for JavaScript-rendered content
    Returns a single page's content because pagination will be handled separately
    """
    titles = extract_letterboxd_titles_using_selenium(url)
    return 1, titles  # Always return page 1, all titles are extracted at once

def scrape_letterboxd(url):
    """
    Special handler for Letterboxd lists that handles everything in one go
    """
    print(f"🌐 Processing Letterboxd list: {url}")
    
    # First try standard HTML scraping for the first page
    _, titles = scrape_letterboxd_page(url, 1)
    
    # If we failed to get titles with regular method, use Selenium
    if not titles or (isinstance(titles, str) and titles.startswith("Error")):
        print("⚠️ Regular HTML scraping failed, switching to Selenium...")
        _, titles = scrape_letterboxd_page_with_selenium(url)
        
        # If we got titles with Selenium, don't try to paginate anymore
        if titles and not isinstance(titles, str):
            return titles
    
    # If we got titles with regular method, try to get more pages
    all_titles = []
    if titles and not isinstance(titles, str):
        all_titles.extend(titles)
        
        # Try next pages
        page = 2
        empty_count = 0
        
        while empty_count < 3:  # Stop after 3 empty pages
            try:
                _, page_titles = scrape_letterboxd_page(url, page)
                
                if page_titles and not isinstance(page_titles, str):
                    if page_titles:
                        all_titles.extend(page_titles)
                        empty_count = 0  # Reset empty counter
                    else:
                        empty_count += 1
                else:
                    empty_count += 1
                    
                page += 1
            except Exception as e:
                print(f"❌ Error on page {page}: {str(e)}")
                empty_count += 1
                page += 1
    
    return all_titles

def determine_site_type(url):
    """Determine the type of website from the URL"""
    if "trakt.tv" in url:
        return "trakt"
    elif "letterboxd.com" in url:
        return "letterboxd"
    elif "mdblist.com" in url:
        return "mdblist"
    else:
        return "unknown"

def scrape_page(url, page):
    """
    Scrape a specific page using the appropriate scraper based on the URL
    This replaces the original scrape_page function
    """
    site_type = determine_site_type(url)
    
    if site_type == "trakt":
        return scrape_trakt_page(url, page)
    elif site_type == "letterboxd":
        return scrape_letterboxd_page(url, page)
    elif site_type == "mdblist":
        # Original MDBList scraper logic
        try:
            full_url = f"{url}?append=yes&q_current_page={page}"
            response = requests.get(full_url, timeout=10)
            response.raise_for_status()
            return page, extract_titles_from_html(response.text)
        except Exception as e:
            return page, f"Error: {str(e)}"
    else:
        return page, f"Error: Unsupported site type for {url}"

def scrape_all_pages(base_url, max_empty_pages=5, delay=None):
    """Run the scraper for all pages of a URL"""
    # Determine the site type to adjust scraping behavior
    site_type = determine_site_type(base_url)
    print(f"🌐 Detected site type: {site_type}")
    
    # Special handling for Letterboxd lists
    if site_type == "letterboxd":
        print("ℹ️ Using specialized Letterboxd scraper")
        return scrape_letterboxd(base_url)
    
    all_titles = []
    empty_count = 0
    page = 1  # Start from page 1 instead of 0
    seen_titles = set()  # Track seen titles to detect duplicates
    
    # Get delay from environment or use default
    if delay is None:
        delay = float(os.getenv("PAGE_FETCH_DELAY", "0.5"))
    
    # Adjust concurrency based on settings
    parallel_enabled = get_env_flag("ENABLE_PARALLEL_PROCESSING", "true")
    
    # For Trakt lists, use lower parallelism to avoid duplicate issues
    max_concurrent_pages = 1 if site_type == "trakt" else (3 if parallel_enabled else 1)
    
    print(f"ℹ️ Using {delay}s delay between page batches")
    print(f"ℹ️ Processing {max_concurrent_pages} pages concurrently")
    
    while True:
        # Process several pages at once
        pages_to_fetch = [page + i for i in range(max_concurrent_pages)]
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_concurrent_pages) as executor:
            future_to_page = {executor.submit(scrape_page, base_url, p): p for p in pages_to_fetch}
            for future in as_completed(future_to_page):
                p, lines = future.result()
                results[p] = lines
        
        # Process results in order
        for p in pages_to_fetch:
            lines = results.get(p, [])
            
            # Check for errors
            if isinstance(lines, str) and lines.startswith("Error"):
                print(f"❌ Failed to fetch page {p}: {lines}")
                
                # Handle rate limiting specifically
                if "429" in lines:
                    print("⚠️ Rate limit detected! Increasing delay and retrying...")
                    # Retry with higher delay
                    time.sleep(5)  # Wait 5 seconds before retry
                    retry_result = scrape_page(base_url, p)
                    if not isinstance(retry_result[1], str):
                        lines = retry_result[1]
                        results[p] = lines
                    else:
                        print("❌ Retry failed, consider increasing PAGE_FETCH_DELAY in settings")
                        empty_count += 1
                else:
                    # For Trakt lists, specifically handle the end-of-list marker
                    if site_type == "trakt" and "End of list reached" in lines:
                        print("🛑 End of list reached. Stopping.")
                        return all_titles
                    
                    # For trakt and letterboxd, 404 on pages beyond the end is expected
                    if site_type in ["trakt", "letterboxd"] and "404" in lines and p > 1:
                        print("🛑 No more pages. Stopping.")
                        return all_titles
                    empty_count += 1
                
                # If too many empty/error pages, stop
                if empty_count >= max_empty_pages:
                    print("🛑 Too many errors or empty pages. Stopping.")
                    return all_titles
                continue
                
            if not lines or len(lines) == 0:
                empty_count += 1
                print(f"⚠️ No titles found on page {p} ({empty_count}/{max_empty_pages})")
                if empty_count >= max_empty_pages:
                    print("🛑 No more content. Stopping.")
                    return all_titles
            else:
                # Check for duplicates when adding titles
                new_titles = 0
                for title in lines:
                    if title not in seen_titles:
                        seen_titles.add(title)
                        all_titles.append(title)
                        new_titles += 1
                
                # For Trakt, if we got no new titles, increase empty count
                if site_type == "trakt" and new_titles == 0 and len(lines) > 0:
                    empty_count += 1
                    print(f"⚠️ All titles from page {p} were duplicates ({empty_count}/{max_empty_pages})")
                    if empty_count >= max_empty_pages:
                        print("🛑 No new content after several pages. Stopping.")
                        return all_titles
                else:
                    empty_count = 0
                    print(f"✅ Extracted {new_titles} new titles from page {p}")
        
        # Move to next batch of pages
        page += max_concurrent_pages
        time.sleep(delay)  # Use configured delay
    
    return all_titles

def search_tmdb_media(title, media_type, max_retries=3, delay=1):
    """
    Search TMDB for a specific media type (tv or movie)
    """
    url = f"https://api.themoviedb.org/3/search/{media_type}"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "language": "en-US"
    }

    def clean_title_for_fallback(t):
        return re.sub(r'[\[\]\"()–\-]', '', t).strip()

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=5)
            
            # Check for rate limiting
            if response.status_code == 429:
                # Rate limited - pause and retry with exponential backoff
                sleep_time = (2 ** attempt) * delay
                print(f"Rate limited. Pausing for {sleep_time}s before retry...")
                time.sleep(sleep_time)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if data["results"]:
                media = data["results"][0]
                
                # Handle different date field names for movies vs TV shows
                date_field = "first_air_date" if media_type == "tv" else "release_date"
                year = None
                if media.get(date_field):
                    year = media[date_field].split("-")[0]
                
                return {
                    "id": media["id"],
                    "year": year,
                    "type": media_type  # Add media type to help differentiate
                }
            elif attempt == 0:
                params["query"] = clean_title_for_fallback(title)
            else:
                time.sleep(delay)
                
        except requests.exceptions.RequestException:
            # Network error - pause and retry
            time.sleep(delay)
        except Exception:
            # Other error - pause and retry
            time.sleep(delay)

    return "[Error]"

def match_title_with_tmdb(title, max_retries=3, delay=1):
    """
    Match a title with TMDB by searching both TV shows and movies
    Returns either a dictionary with id and year, or "[Error]"
    """
    # Try TV show search first
    tv_result = search_tmdb_media(title, "tv", max_retries, delay)
    if tv_result != "[Error]":
        return tv_result
    
    # If no TV show match, try movie search
    movie_result = search_tmdb_media(title, "movie", max_retries, delay)
    return movie_result

def match_title_worker(title):
    result = match_title_with_tmdb(title)
    return (title, result)

def load_all_existing_titles():
    """
    Load all titles and their TMDB IDs from all existing lists in the output directory
    Returns a dictionary mapping titles to their TMDB IDs
    """
    title_map = {}
    root_dir = get_env_string("OUTPUT_ROOT_DIR", os.getcwd())
    
    # Walk through all files in the output directory
    for root, _, files in os.walk(root_dir):
        for file in files:
            if not file.endswith('.txt'):
                continue
            
            file_path = os.path.join(root, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                            
                        # Extract title and TMDB ID if present
                        title_part = line.split("->")[0].split("[")[0].strip()
                        tmdb_match = re.search(r'\[(?:movie:)?(\d+)\]', line)
                        
                        if title_part:
                            # If we have a TMDB ID, store it
                            if tmdb_match:
                                tmdb_id = tmdb_match.group(1)
                                media_type = "movie" if "movie:" in line else "tv"
                                
                                # Extract year if present
                                year = extract_year_from_title(title_part)
                                base_title = re.sub(r'\s*\(\d{4}\)\s*$', '', title_part)
                                
                                # Store with the clean base title as key
                                title_map[base_title] = {
                                    "id": tmdb_id,
                                    "year": year,
                                    "type": media_type
                                }
            except Exception as e:
                print(f"⚠️ Warning: Could not read file {file_path}: {str(e)}")
    
    print(f"📚 Loaded {len(title_map)} existing titles from all lists")
    return title_map

def process_scrape_results(titles, output_file, scan_history, enable_tmdb=True, include_year=True):
    """Process scraping results, checking existing lists before TMDB search"""
    # Use the helper function to get the full file path
    full_output_path = get_output_filepath(output_file)
    
    # Check if the output file exists and load existing titles
    existing_titles = load_titles_from_file(full_output_path)
    
    # If TMDB is enabled, load all existing titles from all lists
    all_title_map = {}
    if enable_tmdb:
        all_title_map = load_all_existing_titles()

    new_count = 0
    skipped_count = 0
    cached_count = 0
    tmdb_results = {}
    titles_to_write = []

    for title in titles:
        if title in existing_titles:
            skipped_count += 1
        else:
            titles_to_write.append(title)

    if enable_tmdb and titles_to_write:
        total_titles = len(titles_to_write)
        print(f"🔍 Matching {total_titles} titles with TMDB using threads...")
        
        # First, check which titles already have a TMDB mapping in our database
        titles_to_search = []
        for title in titles_to_write:
            # Look for the title in our existing database
            clean_title = re.sub(r'\s*\(\d{4}\)\s*$', '', title)
            if clean_title in all_title_map:
                tmdb_results[title] = all_title_map[clean_title]
                cached_count += 1
            else:
                titles_to_search.append(title)
        
        if cached_count > 0:
            print(f"✅ Found {cached_count} titles in existing lists, skipping TMDB search for these")
        
        if titles_to_search:
            # Adjust worker count based on number of titles
            max_workers = min(32, max(8, len(titles_to_search) // 5))
            print(f"⚡ Using {max_workers} worker threads for {len(titles_to_search)} API calls...")
            
            # Start health check
            health = show_health_check_start("TMDB matching", len(titles_to_search))
            
            # Show progress during API calls
            completed = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_title = {executor.submit(match_title_worker, title): title for title in titles_to_search}
                for future in as_completed(future_to_title):
                    title, result = future.result()
                    tmdb_results[title] = result
                    completed += 1
                    show_health_check_update(health, completed)
            
            # End health check
            show_health_check_end(health)

    with open(full_output_path, "a", encoding="utf-8") as f:
        for title in titles_to_write:
            if enable_tmdb:
                result = tmdb_results.get(title, "[Error]")

                if isinstance(result, dict):
                    year = f" ({result['year']})" if include_year and result.get("year") else ""
                    if result.get("type") == "movie":
                        f.write(f"{title}{year} [movie:{result['id']}]\n")
                    else:
                        f.write(f"{title}{year} [{result['id']}]\n")
                else:
                    f.write(f"{title} {result}\n")
            else:
                f.write(title + "\n")

            existing_titles.add(title)
            new_count += 1

    scan_history[output_file] = {title: {"tmdb_matched": enable_tmdb} for title in titles_to_write}
    save_scan_history(scan_history)
    
    # Return with cached info
    return new_count, skipped_count, cached_count

def get_output_filepath(filename):
    """Generate a full file path using the configured root directory"""
    root_dir = get_env_string("OUTPUT_ROOT_DIR", os.getcwd())
    
    # If filename already has a directory structure, preserve it under the root
    rel_path = os.path.normpath(filename)
    
    # Join the root directory with the relative path
    full_path = os.path.join(root_dir, rel_path)
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    
    return full_path

def scrape_url_worker(url):
    print(f"🌐 Processing: {url}")
    return url, scrape_all_pages(url)

def show_health_check_start(task, total_items, interval=3.0):
    """Start a health check for a long-running process"""
    import threading
    import time
    
    # Create a shared state object that can be modified by threads
    state = {"running": True, "processed": 0, "last_update": time.time()}
    
    def health_check_thread():
        start_time = time.time()
        while state["running"]:
            elapsed = time.time() - start_time
            processed = state["processed"]
            rate = processed / elapsed if elapsed > 0 else 0
            
            # Calculate ETA
            if rate > 0 and processed < total_items:
                eta_seconds = (total_items - processed) / rate
                if eta_seconds < 60:
                    eta = f"{eta_seconds:.1f}s"
                elif eta_seconds < 3600:
                    eta = f"{eta_seconds / 60:.1f}m"
                else:
                    eta = f"{eta_seconds / 3600:.1f}h"
                
            else:
                eta = "Unknown"
                
            # Print status
            print(f"\r⏳ {task}: {processed}/{total_items} ({processed/total_items*100:.1f}%) " +
                  f"| {rate:.1f} items/sec | ETA: {eta}", end="")
            
            time.sleep(interval)
    
    # Start the health check thread
    thread = threading.Thread(target=health_check_thread)
    thread.daemon = True
    thread.start()
    
    return state

def show_health_check_update(state, processed):
    """Update the health check with current progress"""
    state["processed"] = processed
    state["last_update"] = time.time()

def show_health_check_end(state):
    """End the health check"""
    state["running"] = False
    print()  # Print a newline to move past the last health check line

def find_error_entries(filepath):
    """Find all error entries in a file"""
    if not os.path.exists(filepath):
        return []
    
    errors = []
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if "[Error]" in line:
                    # Extract the title (everything before [Error])
                    title_part = line.split("[Error]")[0].strip()
                    errors.append({
                        "line_num": i,
                        "title": title_part,
                        "line": line
                    })
    except Exception as e:
        print(f"❌ Error reading file {filepath}: {str(e)}")
    
    return errors

def find_duplicate_entries_ultrafast(filepath, respect_years=True):
    """
    Ultra-optimized duplicate finder that reads the file only once,
    drastically improving performance for large files
    
    If respect_years is True, titles with different years are considered different entries
    """
    if not os.path.exists(filepath):
        return {}
    
    # Dictionary to track title occurrences with line numbers
    title_occurrences = {}
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                # Skip empty lines
                if not line.strip():
                    continue
                    
                try:
                    # Extract just the title part (before any "[" or "->")
                    title_part = line.split("->")[0].split("[")[0].strip()
                    
                    if title_part:
                        # If we're respecting years, include the year in the key if present
                        if respect_years:
                            year = extract_year_from_title(title_part)
                            # Remove year from title for cleaner display
                            base_title = re.sub(r'\s*\(\d{4}\)\s*$', '', title_part)
                            
                            if year:
                                key = f"{base_title} ({year})"
                            else:
                                key = base_title
                            
                        else:
                            key = title_part
                            
                        if key in title_occurrences:
                            title_occurrences[key].append({"line_num": i, "full_line": line.strip()})
                        else:
                            title_occurrences[key] = [{"line_num": i, "full_line": line.strip()}]
                except Exception:
                    continue  # Skip problematic lines
    except Exception as e:
        print(f"❌ Error reading file {filepath}: {str(e)}")
        return {}
    
    # Filter to only titles with multiple occurrences
    duplicates = {title: occurrences for title, occurrences in title_occurrences.items() 
                 if len(occurrences) > 1}
    
    return duplicates

def extract_year_from_title(title_line):
    """Extract year from a title line if present"""
    # Look for pattern like " (2023)" at the end of the title part
    match = re.search(r'\((\d{4})\)', title_line)
    if match:
        return match.group(1)
    return None

def remove_duplicate_lines(filepath, lines_to_keep):
    """
    Remove duplicate entries from a file, keeping only specified line numbers
    
    Args:
        filepath: Path to the file
        lines_to_keep: Set of line numbers to keep
    """
    try:
        # Read all lines
        with open(filepath, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        
        # Write back only the lines we want to keep
        with open(filepath, "w", encoding="utf-8") as f:
            for i, line in enumerate(all_lines, 1):
                if i in lines_to_keep or not line.strip():  # Keep empty lines and selected lines
                    f.write(line)
                    
        return True
    except Exception as e:
        print(f"❌ Error modifying file: {str(e)}")
        return False

def process_auto_fix_errors(errors, lines, file_path):
    """
    Process errors in a file with TMDB lookup, using cache when possible
    
    Args:
        errors: List of error entries
        lines: File content as list of lines
        file_path: Path to the file being processed
    
    Returns:
        Number of successfully fixed errors
    """
    if not errors:
        return 0
        
    # Load existing title mappings from all lists
    all_title_map = load_all_existing_titles()
    
    # Process errors in parallel
    error_titles = [(error['line_num'], error['title']) for error in errors]
    print(f"🔍 Processing {len(error_titles)} error entries...")
    
    # First check which titles are in our cache
    cached_fixes = 0
    titles_to_search = []
    line_num_to_title = {}
    
    for line_num, title in error_titles:
        clean_title = re.sub(r'\s*\(\d{4}\)\s*$', '', title)
        if clean_title in all_title_map:
            result = all_title_map[clean_title]
            
            # Update the line with the cached result
            year_str = f" ({result['year']})" if result.get('year') else ""
            
            if result.get('type') == 'movie':
                new_line = f"{title}{year_str} [movie:{result['id']}]\n"
            else:
                new_line = f"{title}{year_str} [{result['id']}]\n"
                
            lines[line_num - 1] = new_line
            cached_fixes += 1
        else:
            titles_to_search.append((line_num, title))
            line_num_to_title[line_num] = title
    
    if cached_fixes > 0:
        print(f"✅ Fixed {cached_fixes} entries using cached data from existing lists")
    
    if not titles_to_search:
        return cached_fixes
        
    # For remaining titles, search TMDB
    print(f"🔍 Searching TMDB for {len(titles_to_search)} remaining titles...")
    
    # Adjust worker count
    max_workers = min(20, max(5, len(titles_to_search) // 5))
    
    # Start health check for error fixing
    err_health = show_health_check_start("Fixing errors", len(titles_to_search))
    
    api_success_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_line = {}
        for line_num, title in titles_to_search:
            future = executor.submit(match_title_worker, title)
            future_to_line[future] = line_num
        
        completed = 0
        for future in as_completed(future_to_line):
            line_num = future_to_line[future]
            title = line_num_to_title[line_num]
            _, result = future.result()
            
            completed += 1
            show_health_check_update(err_health, completed)
            
            if isinstance(result, dict):
                # Construct the fixed line
                year_str = f" ({result['year']})" if result.get('year') else ""
                
                # Handle different media types
                if result.get('type') == 'movie':
                    new_line = f"{title}{year_str} [movie:{result['id']}]\n"
                else:
                    new_line = f"{title}{year_str} [{result['id']}]\n"
                
                # Update the line in the file content
                lines[line_num - 1] = new_line
                api_success_count += 1
    
    # End health check
    show_health_check_end(err_health)
    
    total_fixed = cached_fixes + api_success_count
    if total_fixed > 0:
        # Save changes
        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        
        print(f"✅ Fixed {total_fixed} errors: {cached_fixes} from cache, {api_success_count} from TMDB API")
    
    return total_fixed

def fix_errors_menu():
    """Menu for fixing errors in list files"""
    while True:
        clear_terminal()
        print("🔧 Fix Errors Menu")
        print("1. Fix errors in a single file")
        print("2. Fix errors in all monitored list files")
        print("3. Manual TMDB search for a title")
        print("4. Return to main menu")

        choice = input("\nChoose an option (1-4): ").strip()

        if choice == "1":
            filepath = input("Enter file path to fix: ").strip()
            full_path = get_output_filepath(filepath)
            
            if not os.path.exists(full_path):
                print(f"❌ File not found: {full_path}")
                input("Press Enter to continue...")
                continue
                
            print(f"🔍 Scanning for errors in {filepath}...")
            errors = find_error_entries(full_path)
            
            if not errors:
                print("✅ No errors found!")
                input("Press Enter to continue...")
                continue
                
            print(f"⚠️ Found {len(errors)} error entries")
            print("🤖 Auto-fix mode - will attempt to match all titles")
            
            # Ask for confirmation before starting the potentially time-consuming operation
            if input("Continue with auto-fixing? (y/N): ").lower() != 'y':
                continue
            
            # Read file content
            with open(full_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            # Process errors with caching
            total_fixed = process_auto_fix_errors(errors, lines, full_path)
            
            # Notify if some entries couldn't be fixed
            if total_fixed < len(errors):
                print(f"⚠️ {len(errors) - total_fixed} entries still need manual fixing")
            
            input("Press Enter to continue...")
        elif choice == "2":
            # Fix errors in all monitored lists
            config = load_monitor_config()
            if not config["monitored_lists"]:
                print("❌ No lists are currently being monitored.")
                input("Press Enter to continue...")
                continue
                
            print(f"📋 Found {len(config['monitored_lists'])} monitored lists")
            if input("Run error fixing on all monitored lists? (y/N): ").lower() != 'y':
                continue
                
            # Process each monitored list
            for output_file in config["monitored_lists"]:
                full_path = get_output_filepath(output_file)
                print(f"\n🔍 Processing {output_file}...")
                
                if not os.path.exists(full_path):
                    print(f"❌ File not found: {full_path}")
                    continue
                    
                # Find errors
                errors = find_error_entries(full_path)
                if not errors:
                    print("✅ No errors found!")
                    continue
                    
                print(f"⚠️ Found {len(errors)} error entries")
                
                # Read file content
                with open(full_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                
                # Process errors with caching
                total_fixed = process_auto_fix_errors(errors, lines, full_path)
                
                # Notify if some entries couldn't be fixed
                if total_fixed < len(errors):
                    print(f"⚠️ {len(errors) - total_fixed} entries still need manual fixing")
                    
            input("\nCompleted processing all lists. Press Enter to continue...")
        elif choice == "3":
            manual_tmdb_search()
        elif choice == "4":
            return
        else:
            input("❌ Invalid option. Press Enter to continue...")

def run_scraper():
    """Run the scraper for a single URL"""
    clear_terminal()
    print("🔍 Single URL Scraper")
    url = input("Enter URL to scrape: ").strip()
    
    if not url:
        print("❌ No URL provided.")
        return
    
    output_file = input("Enter output file name: ").strip()
    if not output_file:
        print("❌ No output file provided.")
        return
    
    # Get settings from environment
    enable_tmdb = get_env_flag("ENABLE_TMDB", "true")
    include_year = get_env_flag("INCLUDE_YEAR", "true")
    
    scan_history = load_scan_history()
    start_time = time.time()
    
    print(f"🌐 Scraping titles from {url}...")
    titles = scrape_all_pages(url)
    
    if not titles:
        print("❌ No titles found.")
        return
    
    print(f"✅ Found {len(titles)} titles")
    new_count, skipped_count, cached_count = process_scrape_results(
        titles, output_file, scan_history, 
        enable_tmdb=enable_tmdb, include_year=include_year
    )
    
    elapsed = time.time() - start_time
    print(f"✅ Added {new_count} new titles, skipped {skipped_count} existing titles")
    if cached_count > 0:
        print(f"🔄 Used {cached_count} cached TMDB lookups from existing lists")
    print(f"⏱️ Completed in {elapsed:.1f} seconds")

def run_batch_scraper():
    """Run the scraper for multiple URLs"""
    clear_terminal()
    print("📂 Batch URL Scraper")
    print("Enter one URL per line (empty line to finish):")
    
    urls = []
    while True:
        url = input("> ").strip()
        if not url:
            break
        urls.append(url)
    
    if not urls:
        print("❌ No URLs provided.")
        return
    
    output_file = input("Enter output file name: ").strip()
    if not output_file:
        print("❌ No output file provided.")
        return
    
    # Get settings from environment
    enable_tmdb = get_env_flag("ENABLE_TMDB", "true")
    include_year = get_env_flag("INCLUDE_YEAR", "true")
    
    scan_history = load_scan_history()
    start_time = time.time()
    
    all_titles = []
    
    # Scrape in parallel
    max_workers = min(10, len(urls))  # Cap at 10 threads
    print(f"⚡ Using {max_workers} worker threads for URL processing")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(scrape_url_worker, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                url, titles = future.result()
                if titles:
                    print(f"✅ Found {len(titles)} titles from {url}")
                    all_titles.extend(titles)
                else:
                    print(f"❌ No titles found from {url}")
            except Exception as e:
                print(f"❌ Error processing {url}: {str(e)}")
    
    if not all_titles:
        print("❌ No titles found from any URL.")
        return
    
    print(f"✅ Found {len(all_titles)} total titles from all URLs")
    new_count, skipped_count, cached_count = process_scrape_results(
        all_titles, output_file, scan_history, 
        enable_tmdb=enable_tmdb, include_year=include_year
    )
    
    elapsed = time.time() - start_time
    print(f"✅ Added {new_count} new titles, skipped {skipped_count} existing titles")
    if cached_count > 0:
        print(f"🔄 Used {cached_count} cached TMDB lookups from existing lists")
    print(f"⏱️ Completed in {elapsed:.1f} seconds")

def run_monitor_scraper():
    """User interface for monitoring lists"""
    clear_terminal()
    print("🔍 Monitor Scraper")
    
    config = load_monitor_config()
    
    # Check if any lists are configured
    if not config["monitored_lists"]:
        print("❌ No lists are currently being monitored.")
        print("Please add lists to monitor first.")
        if input("Would you like to add a list to monitor now? (y/N): ").lower() == 'y':
            add_monitor_url()
        else:
            input("Press Enter to continue...")
        return
    
    print(f"📋 Found {len(config['monitored_lists'])} monitored lists")
    print("1. Run monitor check now")
    print("2. Add a URL to monitor")
    print("3. View and manage monitored lists")
    print("4. Return to main menu")
    
    choice = input("\nChoose an option (1-4): ").strip()
    
    if choice == "1":
        # Run a manual check
        force_check = input("Force check all lists regardless of status? (y/N): ").lower() == 'y'
        run_monitor_check(force_check=force_check)
        input("\nCheck complete. Press Enter to continue...")
    elif choice == "2":
        add_monitor_url()
    elif choice == "3":
        manage_monitored_lists()
    elif choice == "4":
        return
    else:
        input("❌ Invalid option. Press Enter to continue...")

def add_monitor_url():
    """Add a URL to be monitored"""
    clear_terminal()
    print("🔗 Add URL to Monitor")
    
    url = input("Enter URL to monitor: ").strip()
    if not url:
        input("❌ No URL provided. Press Enter to continue...")
        return
    
    output_file = input("Enter output file for results: ").strip()
    if not output_file:
        input("❌ No output file provided. Press Enter to continue...")
        return
    
    # Determine site type from URL
    site_type = determine_site_type(url)
    if site_type == "unknown":
        print("⚠️ This URL type may not be supported. Supported sites: trakt.tv, letterboxd.com, mdblist.com")
        if input("Continue anyway? (y/N): ").lower() != 'y':
            return
    
    # Add the URL to the monitor configuration
    config = load_monitor_config()
    
    # Generate a unique ID for this monitor entry
    monitor_id = str(int(time.time() * 1000))
    
    # If this is the first URL for this output file, create a new entry
    if output_file not in config["monitored_lists"]:
        config["monitored_lists"][output_file] = {
            "urls": [],
            "last_check": None,
            "enabled": True
        }
    
    # Add the URL if it's not already in the list
    url_exists = False
    for existing_url in config["monitored_lists"][output_file]["urls"]:
        if existing_url["url"] == url:
            url_exists = True
            break
    
    if not url_exists:
        config["monitored_lists"][output_file]["urls"].append({
            "id": monitor_id,
            "url": url,
            "added": datetime.now().isoformat(),
            "last_check": None,
            "title_count": 0,  # Number of titles found in last check
            "total_added": 0,  # Total titles added from this URL over time
        })
    
    save_monitor_config(config)
    
    print(f"✅ Added URL to monitor with ID: {monitor_id}")
    print(f"🎯 New titles will be saved to: {output_file}")
    
    # Ask if user wants to run an initial check
    if input("Run an initial check now? (Y/n): ").lower() != 'n':
        run_monitor_check(specific_list=output_file, force_check=True)
    
    input("\nPress Enter to continue...")

def manage_monitored_lists():
    """View and manage monitored lists"""
    while True:
        clear_terminal()
        print("📋 Manage Monitored Lists")
        
        config = load_monitor_config()
        if not config["monitored_lists"]:
            print("❌ No lists are currently being monitored.")
            input("Press Enter to return to the previous menu...")
            return
        
        # Display list of monitored lists
        print(f"Found {len(config['monitored_lists'])} monitored lists:\n")
        lists = list(config["monitored_lists"].keys())
        
        for i, output_file in enumerate(lists, 1):
            list_config = config["monitored_lists"][output_file]
            status = "✅ Enabled" if list_config.get("enabled", True) else "❌ Disabled"
            url_count = len(list_config["urls"])
            last_check = list_config.get("last_check")
            
            if last_check:
                last_check_time = datetime.fromtimestamp(float(last_check))
                time_str = last_check_time.strftime("%Y-%m-%d %H:%M")
            else:
                time_str = "Never"
                
            print(f"{i}. {output_file} - {status} - {url_count} URLs - Last check: {time_str}")
        
        print("\nOptions:")
        print("0. Return to previous menu")
        print("1. View list details")
        print("2. Add URL to existing list")
        print("3. Remove URL from list")
        print("4. Enable/disable list")
        print("5. Delete list")
        print("6. Run check for specific list")
        print("7. Change monitor interval")
        
        choice = input("\nChoose an option: ").strip()
        
        if choice == "0":
            return
        elif choice == "1":
            # View list details
            list_num = input("Enter list number to view: ").strip()
            try:
                list_num = int(list_num)
                if 1 <= list_num <= len(lists):
                    view_list_details(lists[list_num - 1])
                else:
                    input("❌ Invalid list number. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        elif choice == "2":
            # Add URL to existing list
            list_num = input("Enter list number to add URL to: ").strip()
            try:
                list_num = int(list_num)
                if 1 <= list_num <= len(lists):
                    add_url_to_list(lists[list_num - 1])
                else:
                    input("❌ Invalid list number. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        elif choice == "3":
            # Remove URL from list
            list_num = input("Enter list number to remove URL from: ").strip()
            try:
                list_num = int(list_num)
                if 1 <= list_num <= len(lists):
                    remove_url_from_list(lists[list_num - 1])
                else:
                    input("❌ Invalid list number. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        elif choice == "4":
            # Enable/disable list
            list_num = input("Enter list number to toggle enable/disable: ").strip()
            try:
                list_num = int(list_num)
                if 1 <= list_num <= len(lists):
                    toggle_list_enabled(lists[list_num - 1])
                else:
                    input("❌ Invalid list number. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        elif choice == "5":
            # Delete list
            list_num = input("Enter list number to delete: ").strip()
            try:
                list_num = int(list_num)
                if 1 <= list_num <= len(lists):
                    delete_monitored_list(lists[list_num - 1])
                else:
                    input("❌ Invalid list number. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        elif choice == "6":
            # Run check for specific list
            list_num = input("Enter list number to check: ").strip()
            try:
                list_num = int(list_num)
                if 1 <= list_num <= len(lists):
                    force = input("Force check regardless of timing? (y/N): ").lower() == 'y'
                    run_monitor_check(specific_list=lists[list_num - 1], force_check=force)
                    input("Press Enter to continue...")
                else:
                    input("❌ Invalid list number. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        elif choice == "7":
            # Change monitor interval
            new_interval = input("Enter new interval in minutes (default 1440 = 24 hours): ").strip()
            try:
                new_interval = int(new_interval)
                if new_interval > 0:
                    config["monitor_interval"] = new_interval
                    save_monitor_config(config)
                    input(f"✅ Monitor interval updated to {new_interval} minutes. Press Enter to continue...")
                else:
                    input("❌ Interval must be greater than 0. Press Enter to continue...")
            except ValueError:
                input("❌ Invalid input. Press Enter to continue...")
        else:
            input("❌ Invalid option. Press Enter to continue...")

def view_list_details(output_file):
    """View details of a specific monitored list"""
    config = load_monitor_config()
    list_config = config["monitored_lists"].get(output_file)
    
    if not list_config:
        input(f"❌ List '{output_file}' not found. Press Enter to continue...")
        return
    
    clear_terminal()
    print(f"📋 List Details: {output_file}")
    
    # Basic list info
    status = "✅ Enabled" if list_config.get("enabled", True) else "❌ Disabled"
    print(f"Status: {status}")
    
    last_check = list_config.get("last_check")
    if last_check:
        last_check_time = datetime.fromtimestamp(float(last_check))
        print(f"Last check: {last_check_time.strftime('%Y-%m-%d %H:%M')}")
    else:
        print("Last check: Never")
    
    # URLs in this list
    print(f"\nURLs in this list ({len(list_config['urls'])}):")
    for i, url_entry in enumerate(list_config["urls"], 1):
        url = url_entry["url"]
        title_count = url_entry.get("title_count", 0)
        total_added = url_entry.get("total_added", 0)
        
        # Format the URL to fit nicely in console
        max_url_length = 60
        display_url = url if len(url) <= max_url_length else url[:max_url_length-3] + "..."
        
        print(f"{i}. {display_url}")
        print(f"   ID: {url_entry.get('id', 'N/A')}")
        print(f"   Last fetch: {title_count} titles")
        print(f"   Total added: {total_added} titles")
        
        # Show when this URL was last checked
        last_url_check = url_entry.get("last_check")
        if last_url_check:
            last_url_time = datetime.fromtimestamp(float(last_url_check))
            print(f"   Last checked: {last_url_time.strftime('%Y-%m-%d %H:%M')}")
        else:
            print("   Last checked: Never")
        
        # Add a spacer between URLs
        if i < len(list_config["urls"]):
            print()
    
    input("\nPress Enter to return...")

def add_url_to_list(output_file):
    """Add a URL to an existing monitored list"""
    config = load_monitor_config()
    if output_file not in config["monitored_lists"]:
        input(f"❌ List '{output_file}' not found. Press Enter to continue...")
        return
    
    clear_terminal()
    print(f"🔗 Add URL to '{output_file}'")
    
    url = input("Enter URL to monitor: ").strip()
    if not url:
        input("❌ No URL provided. Press Enter to continue...")
        return
    
    # Determine site type from URL
    site_type = determine_site_type(url)
    if site_type == "unknown":
        print("⚠️ This URL type may not be supported. Supported sites: trakt.tv, letterboxd.com, mdblist.com")
        if input("Continue anyway? (y/N): ").lower() != 'y':
            return
    
    # Generate a unique ID for this monitor entry
    monitor_id = str(int(time.time() * 1000))
    
    # Add the URL if it's not already in the list
    url_exists = False
    for existing_url in config["monitored_lists"][output_file]["urls"]:
        if existing_url["url"] == url:
            url_exists = True
            input(f"⚠️ This URL is already in the list. Press Enter to continue...")
            break
    
    if not url_exists:
        config["monitored_lists"][output_file]["urls"].append({
            "id": monitor_id,
            "url": url,
            "added": datetime.now().isoformat(),
            "last_check": None,
            "title_count": 0,
            "total_added": 0,
        })
        
        save_monitor_config(config)
        print(f"✅ Added URL to list with ID: {monitor_id}")
        
        # Ask if user wants to run an initial check
        if input("Run an initial check now? (Y/n): ").lower() != 'n':
            run_monitor_check(specific_list=output_file, force_check=True)
    
    input("\nPress Enter to continue...")

def remove_url_from_list(output_file):
    """Remove a URL from a monitored list"""
    config = load_monitor_config()
    if output_file not in config["monitored_lists"]:
        input(f"❌ List '{output_file}' not found. Press Enter to continue...")
        return
    
    list_config = config["monitored_lists"][output_file]
    if not list_config["urls"]:
        input("❌ This list has no URLs. Press Enter to continue...")
        return
    
    clear_terminal()
    print(f"🗑️ Remove URL from '{output_file}'")
    
    # Display URLs in this list
    print("URLs in this list:")
    for i, url_entry in enumerate(list_config["urls"], 1):
        url = url_entry["url"]
        print(f"{i}. {url}")
    
    # Get user choice
    try:
        choice = int(input("\nEnter number of URL to remove (0 to cancel): "))
        if choice == 0:
            return
        
        if 1 <= choice <= len(list_config["urls"]):
            # Ask for confirmation
            url_to_remove = list_config["urls"][choice - 1]["url"]
            if input(f"Are you sure you want to remove {url_to_remove}? (y/N): ").lower() == 'y':
                del list_config["urls"][choice - 1]
                save_monitor_config(config)
                print("✅ URL removed successfully")
                
                # If this was the last URL, ask if they want to delete the list
                if not list_config["urls"]:
                    if input("This list has no more URLs. Delete the entire list? (y/N): ").lower() == 'y':
                        del config["monitored_lists"][output_file]
                        save_monitor_config(config)
                        print(f"✅ List '{output_file}' deleted")
        else:
            print("❌ Invalid selection")
    except ValueError:
        print("❌ Invalid input, expected a number")
    
    input("\nPress Enter to continue...")

def toggle_list_enabled(output_file):
    """Toggle the enabled state of a monitored list"""
    config = load_monitor_config()
    if output_file not in config["monitored_lists"]:
        input(f"❌ List '{output_file}' not found. Press Enter to continue...")
        return
    
    # Toggle the enabled state
    current_state = config["monitored_lists"][output_file].get("enabled", True)
    config["monitored_lists"][output_file]["enabled"] = not current_state
    
    save_monitor_config(config)
    
    new_state = "enabled" if not current_state else "disabled"
    input(f"✅ List '{output_file}' is now {new_state}. Press Enter to continue...")

def delete_monitored_list(output_file):
    """Delete a monitored list"""
    config = load_monitor_config()
    if output_file not in config["monitored_lists"]:
        input(f"❌ List '{output_file}' not found. Press Enter to continue...")
        return
    
    # Ask for confirmation
    if input(f"Are you sure you want to delete the list '{output_file}'? (y/N): ").lower() == 'y':
        del config["monitored_lists"][output_file]
        save_monitor_config(config)
        input(f"✅ List '{output_file}' deleted. Press Enter to continue...")
    else:
        input("Operation cancelled. Press Enter to continue...")

def run_monitor_check(force_check=False, specific_list=None):
    """
    Run monitoring check for all configured lists or a specific list.
    
    Args:
        force_check (bool): If True, check all lists regardless of last check time
        specific_list (str): If provided, only check this specific list
    """
    clear_terminal()
    print("🔄 Running Monitor Check")
    
    config = load_monitor_config()
    if not config["monitored_lists"]:
        print("❌ No lists are currently being monitored.")
        return
    
    # Get monitor interval in minutes
    interval_minutes = config.get("monitor_interval", DEFAULT_MONITOR_INTERVAL)
    print(f"ℹ️ Monitor interval: {interval_minutes} minutes")
    
    # Track overall progress
    total_new_items = 0
    processed_lists = 0
    
    # Get the list of lists to process
    lists_to_process = []
    if specific_list:
        if specific_list in config["monitored_lists"]:
            lists_to_process = [specific_list]
        else:
            print(f"❌ List '{specific_list}' not found in monitored lists.")
            return
    else:
        lists_to_process = list(config["monitored_lists"].keys())
    
    # Process each list
    for output_file in lists_to_process:
        list_config = config["monitored_lists"][output_file]
        
        if not list_config.get("enabled", True) and not force_check:
            print(f"⏭️ Skipping disabled list: {output_file}")
            continue
        
        # Check if it's time to update this list
        last_check = list_config.get("last_check")
        current_time = datetime.now().timestamp()
        
        # Skip if it's not time yet, unless force_check is True
        if not force_check and last_check:
            last_check_time = float(last_check)
            time_since_check = (current_time - last_check_time) / 60  # Convert to minutes
            if time_since_check < interval_minutes:
                time_remaining = interval_minutes - time_since_check
                print(f"⏭️ Skipping {output_file} - checked {time_since_check:.1f} minutes ago (next check in {time_remaining:.1f} minutes)")
                continue
        
        print(f"\n📝 Processing list: {output_file}")
        scan_history = load_scan_history()
        
        # Process each URL for this list
        all_titles = []
        for url_entry in list_config["urls"]:
            url = url_entry["url"]
            print(f"🌐 Fetching: {url}")
            
            start_time = time.time()
            titles = scrape_all_pages(url)
            
            url_entry["last_check"] = current_time
            url_entry["title_count"] = len(titles) if titles else 0
            
            elapsed = time.time() - start_time
            print(f"✅ Found {len(titles) if titles else 0} titles in {elapsed:.1f} seconds")
            
            if titles:
                all_titles.extend(titles)
        
        # Update the last check time for this list
        list_config["last_check"] = current_time
        
        # Save merged results to the output file
        if all_titles:
            print(f"📊 Processing {len(all_titles)} total titles from all URLs")
            
            # Get settings from environment
            enable_tmdb = get_env_flag("ENABLE_TMDB", "true")
            include_year = get_env_flag("INCLUDE_YEAR", "true")
            
            new_count, skipped_count, cached_count = process_scrape_results(
                all_titles, output_file, scan_history,
                enable_tmdb=enable_tmdb, include_year=include_year
            )
            
            print(f"✅ Added {new_count} new titles to {output_file}")
            
            # Update total added count for each URL
            for url_entry in list_config["urls"]:
                url_entry["total_added"] = url_entry.get("total_added", 0) + (new_count // len(list_config["urls"]))
            
            total_new_items += new_count
        else:
            print("⚠️ No titles found from any URL in this list")
        
        processed_lists += 1
    
    # Update the last overall run time
    config["last_run"] = current_time
    save_monitor_config(config)
    
    print(f"\n✅ Monitor check complete: processed {processed_lists} lists, added {total_new_items} new items")
    return total_new_items

def main_menu():
    """Main menu for the application"""
    while True:
        clear_terminal()
        print("📋 Parsely - Media List Manager")
        print("1. Run Scraper (Single URL)")
        print("2. Batch Scraper (Multiple URLs)")
        print("3. Monitor Scraper")
        print("4. Fix Errors in Lists")
        print("5. Manage Duplicates in Lists")
        print("6. Batch Process Folders")
        print("7. Auto Fix Tool")
        print("8. Settings")
        print("9. Exit")

        choice = input("\nChoose an option (1-9): ").strip()

        if choice == "1":
            run_scraper()
        elif choice == "2":
            run_batch_scraper()
        elif choice == "3":
            # Call the run_monitor_scraper function instead of run_monitor_check directly
            run_monitor_scraper()
        elif choice == "4":
            fix_errors_menu()
        elif choice == "5":
            duplicates_menu()
        elif choice == "6":
            batch_process_menu()
        elif choice == "7":
            auto_fix_tool()
        elif choice == "8":
            show_settings()
        elif choice == "9":
            print("👋 Exiting.")
            break
        else:
            input("❌ Invalid option. Press Enter to continue...")

# This is the main entry point for the program
if __name__ == "__main__":
    # Check if a folder path was provided as an argument (drag and drop)
    import sys
    
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
        # A folder was dragged onto the script, process it
        process_dragged_folder(sys.argv[1])
    else:
        # Normal startup - show the main menu
        try:
            main_menu()
        except KeyboardInterrupt:
            print("\n👋 Program interrupted. Exiting.")
        except Exception as e:
            print(f"\n❌ Unexpected error: {str(e)}")
            import traceback
            traceback.print_exc()