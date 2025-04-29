#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import os
import time
import json
import re
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")

def load_scan_history():
    if os.path.exists(SCAN_HISTORY_FILE):
        with open(SCAN_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_scan_history(history):
    with open(SCAN_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)

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

def scrape_page(url, page):
    try:
        full_url = f"{url}?append=yes&q_current_page={page}"
        response = requests.get(full_url, timeout=10)
        response.raise_for_status()
        return page, extract_titles_from_html(response.text)
    except Exception as e:
        return page, f"Error: {str(e)}"

def scrape_all_pages(base_url, max_empty_pages=5, delay=None):
    all_lines = []
    empty_count = 0
    page = 0
    
    # Get delay from environment or use default
    if delay is None:
        delay = float(os.getenv("PAGE_FETCH_DELAY", "0.5"))
    
    # Adjust concurrency based on settings
    parallel_enabled = get_env_flag("ENABLE_PARALLEL_PROCESSING", "true")
    max_concurrent_pages = 3 if parallel_enabled else 1  # Reduce from 5 to 3 if parallel enabled
    
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
                        break
                else:
                    break
                
            if not lines:
                empty_count += 1
                print(f"⚠️ No titles found on page {p} ({empty_count}/{max_empty_pages})")
                if empty_count >= max_empty_pages:
                    print("🛑 No more content. Stopping.")
                    return all_lines
            else:
                empty_count = 0
                all_lines.extend(lines)
                print(f"✅ Extracted {len(lines)} titles from page {p}")
        
        # Move to next batch of pages
        page += max_concurrent_pages
        time.sleep(delay)  # Use configured delay
    
    return all_lines

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

def process_scrape_results(titles, output_file, scan_history, enable_tmdb=True, include_year=True):
    # Use the helper function to get the full file path
    full_output_path = get_output_filepath(output_file)
    
    # Check if the output file exists and load existing titles
    existing_titles = load_titles_from_file(full_output_path)

    new_count = 0
    skipped_count = 0
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
        
        # Adjust worker count based on number of titles
        max_workers = min(32, max(8, total_titles // 5))
        print(f"⚡ Using {max_workers} worker threads for API calls...")
        
        # Show progress during API calls
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_title = {executor.submit(match_title_worker, title): title for title in titles_to_write}
            for future in as_completed(future_to_title):
                title, result = future.result()
                tmdb_results[title] = result
                completed += 1
                if completed % 10 == 0 or completed == total_titles:
                    print(f"⏳ Progress: {completed}/{total_titles} titles matched ({completed/total_titles:.1%})")

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
    return new_count, skipped_count

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
    new_count, skipped_count = process_scrape_results(
        titles, output_file, scan_history, 
        enable_tmdb=enable_tmdb, include_year=include_year
    )
    
    elapsed = time.time() - start_time
    print(f"✅ Added {new_count} new titles, skipped {skipped_count} existing titles")
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
    new_count, skipped_count = process_scrape_results(
        all_titles, output_file, scan_history, 
        enable_tmdb=enable_tmdb, include_year=include_year
    )
    
    elapsed = time.time() - start_time
    print(f"✅ Added {new_count} new titles, skipped {skipped_count} existing titles")
    print(f"⏱️ Completed in {elapsed:.1f} seconds")

def run_monitor_scraper():
    """Run the monitor scraper to check for updates"""
    clear_terminal()
    print("🔍 Monitor Scraper (Coming Soon)")
    input("Press Enter to continue...")

def find_error_entries(filepath):
    """Find all [Error] entries in a file"""
    if not os.path.exists(filepath):
        return []
    
    error_entries = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if "[Error]" in line:
                    title = line.split("[Error]")[0].strip()
                    error_entries.append({
                        "line_num": i,
                        "title": title,
                        "line": line
                    })
    except Exception as e:
        print(f"❌ Error reading file {filepath}: {str(e)}")
    
    return error_entries

def extract_year_from_title(title_line):
    """Extract year from a title line if present"""
    # Look for pattern like " (2023)" at the end of the title part
    match = re.search(r'\((\d{4})\)', title_line)
    if match:
        return match.group(1)
    return None

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

def duplicates_menu():
    """Menu for managing duplicates in list files"""
    while True:
        clear_terminal()
        print("🔍 Manage Duplicates in Lists")
        print("1. Scan file for duplicates")
        print("2. Auto-fix duplicates (keep first occurrence)")
        print("3. Return to main menu")

        choice = input("\nChoose an option (1-3) [1]: ").strip() or "1"

        if choice == "1":
            filepath = input("Enter file path to scan: ").strip()
            full_path = get_output_filepath(filepath)
            
            if not os.path.exists(full_path):
                print(f"❌ File not found: {full_path}")
                input("Press Enter to continue...")
                continue
            
            # Ask if different years should be considered different titles
            respect_years = input("Treat titles with different years as different entries? (Y/n): ").strip().lower() != 'n'
                
            print(f"🔍 Scanning for duplicates in {filepath}...")
            duplicates = find_duplicate_entries_ultrafast(full_path, respect_years)
            
            if not duplicates:
                print("✅ No duplicates found!")
                input("Press Enter to continue...")
                continue
                
            print(f"⚠️ Found {len(duplicates)} titles with duplicates")
            
            # Keep track of lines to remove
            lines_to_remove = set()
            lines_to_keep = set()
            
            for title, occurrences in duplicates.items():
                print(f"\n📄 Title: {title}")
                print(f"   Found {len(occurrences)} occurrences:")
                for idx, occurrence in enumerate(occurrences, 1):
                    print(f"   {idx}. Line {occurrence['line_num']}: {occurrence['full_line']}")
                
                # Ask which occurrence to keep
                while True:
                    keep_idx = input(f"\nWhich occurrence to keep? (1-{len(occurrences)}) [1]: ").strip() or "1"
                    try:
                        keep_idx = int(keep_idx)
                        if 1 <= keep_idx <= len(occurrences):
                            break
                        print(f"❌ Please enter a number between 1 and {len(occurrences)}")
                    except ValueError:
                        print("❌ Please enter a valid number")
                
                # Mark the chosen line to keep, all others for removal
                for idx, occurrence in enumerate(occurrences, 1):
                    if idx == keep_idx:
                        lines_to_keep.add(occurrence['line_num'])
                    else:
                        lines_to_remove.add(occurrence['line_num'])
                
                if input("\nProcess next duplicate? (Y/n): ").strip().lower() == 'n':
                    break
            
            # Ask if the user wants to apply the changes
            if lines_to_remove:
                print(f"\n⚠️ Ready to remove {len(lines_to_remove)} duplicate lines and keep {len(lines_to_keep)} lines")
                if input("Apply changes? (y/N): ").strip().lower() == 'y':
                    # Create a set of all line numbers in the file
                    with open(full_path, "r", encoding="utf-8") as f:
                        total_lines = sum(1 for _ in f)
                    
                    # Lines to keep includes all lines NOT in lines_to_remove
                    all_lines_to_keep = set(range(1, total_lines + 1)) - lines_to_remove
                    
                    if remove_duplicate_lines(full_path, all_lines_to_keep):
                        print(f"✅ Successfully removed {len(lines_to_remove)} duplicates")
                    else:
                        print("❌ Failed to remove duplicates")
                else:
                    print("❌ Changes discarded")
            
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            filepath = input("Enter file path to fix: ").strip()
            full_path = get_output_filepath(filepath)
            
            if not os.path.exists(full_path):
                print(f"❌ File not found: {full_path}")
                input("Press Enter to continue...")
                continue
                
            # Ask if different years should be considered different titles
            respect_years = input("Treat titles with different years as different entries? (Y/n): ").strip().lower() != 'n'
            
            print(f"🔍 Scanning for duplicates in {filepath}...")
            duplicates = find_duplicate_entries_ultrafast(full_path, respect_years)
            
            if not duplicates:
                print("✅ No duplicates found!")
                input("Press Enter to continue...")
                continue
            
            print(f"⚠️ Found {len(duplicates)} titles with duplicates")
            print("🔧 Auto-fixing by keeping the first occurrence of each duplicate...")
            
            # Automatically create lists of lines to keep (first occurrence) and remove (all others)
            lines_to_keep = set()
            lines_to_remove = set()
            
            for title, occurrences in duplicates.items():
                # Keep the first occurrence
                lines_to_keep.add(occurrences[0]['line_num'])
                
                # Remove all other occurrences
                for occurrence in occurrences[1:]:
                    lines_to_remove.add(occurrence['line_num'])
            
            # Create a set of all line numbers in the file
            with open(full_path, "r", encoding="utf-8") as f:
                total_lines = sum(1 for _ in f)
            
            # Lines to keep includes all lines NOT in lines_to_remove
            all_lines_to_keep = set(range(1, total_lines + 1)) - lines_to_remove
            
            if remove_duplicate_lines(full_path, all_lines_to_keep):
                print(f"✅ Successfully removed {len(lines_to_remove)} duplicates")
            else:
                print("❌ Failed to remove duplicates")
                
            input("\nPress Enter to continue...")
            
        elif choice == "3":
            return
        else:
            input("❌ Invalid option. Press Enter to continue...")

def fix_errors_menu():
    """Menu for fixing errors in list files"""
    while True:
        clear_terminal()
        print("🔧 Fix Errors in Lists")
        print("1. Scan file for [Error] entries")
        print("2. Fix errors manually")
        print("3. Fix errors automatically via TMDB")
        print("4. Return to main menu")

        choice = input("\nChoose an option (1-4) [1]: ").strip() or "1"

        if choice == "1":
            filepath = input("Enter file path to scan: ").strip()
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
                
            print(f"⚠️ Found {len(errors)} error entries:")
            
            # Display the first 10 errors
            for i, error in enumerate(errors[:10], 1):
                print(f"  {i}. Line {error['line_num']}: {error['title']}")
            
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more")
                
            input("Press Enter to continue...")
            
        elif choice == "2":
            # Manual fix mode
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
            print("📝 Manual fix mode - you'll fix each entry one by one")
            
            # Read file content
            with open(full_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            errors_fixed = 0
            for error in errors:
                print(f"\n📄 Line {error['line_num']}: {error['line']}")
                print("Options:")
                print("1. Search TMDB for a matching title")
                print("2. Enter TMDB ID manually")
                print("3. Mark as movie with TMDB ID") 
                print("4. Skip this entry")
                print("5. Exit manual fixing")
                
                fix_choice = input("\nChoose an option (1-5) [1]: ").strip() or "1"
                
                if fix_choice == "1":
                    search_title = input(f"Enter search title [{error['title']}]: ").strip() or error['title']
                    print(f"🔍 Searching TMDB for: {search_title}")
                    
                    result = match_title_with_tmdb(search_title)
                    
                    if isinstance(result, dict):
                        year_str = f" ({result['year']})" if result.get('year') else ""
                        media_type = result.get('type', 'tv')  # Default to TV if not specified
                        
                        if media_type == "movie":
                            new_line = f"{error['title']}{year_str} [movie:{result['id']}]\n"
                            print(f"✅ Found movie match: {error['title']}{year_str} [movie:{result['id']}]")
                        else:
                            new_line = f"{error['title']}{year_str} [{result['id']}]\n"
                            print(f"✅ Found TV match: {error['title']}{year_str} [{result['id']}]")
                        
                        if input("Accept this match? (Y/n): ").lower() != 'n':
                            lines[error['line_num'] - 1] = new_line
                            errors_fixed += 1
                            print("✓ Applied fix")
                        else:
                            print("✗ Skipped")
                    else:
                        print("❌ No match found on TMDB")
                
                elif fix_choice == "2":
                    tmdb_id = input("Enter TMDB ID (for TV shows): ").strip()
                    if tmdb_id.isdigit():
                        # Optionally fetch the year
                        include_year = get_env_flag("INCLUDE_YEAR", "true")
                        year_str = ""
                        
                        if include_year:
                            # Attempt to fetch show details to get the year
                            try:
                                url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
                                params = {"api_key": TMDB_API_KEY}
                                response = requests.get(url, params=params, timeout=5)
                                
                                if response.status_code == 200:
                                    data = response.json()
                                    first_air_date = data.get("first_air_date", "")
                                    if first_air_date:
                                        year = first_air_date.split("-")[0]
                                        year_str = f" ({year})"
                            except Exception:
                                # If there's an error, just continue without year
                                pass
                        
                        new_line = f"{error['title']}{year_str} [{tmdb_id}]\n"
                        lines[error['line_num'] - 1] = new_line
                        errors_fixed += 1
                        print(f"✅ Updated to: {new_line.strip()}")
                    else:
                        print("❌ Invalid TMDB ID (must be a number)")
                
                elif fix_choice == "3":
                    # New option for movies
                    tmdb_id = input("Enter movie TMDB ID: ").strip()
                    if tmdb_id.isdigit():
                        # Optionally fetch the year
                        include_year = get_env_flag("INCLUDE_YEAR", "true")
                        year_str = ""
                        
                        if include_year:
                            # Attempt to fetch movie details to get the year
                            try:
                                url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
                                params = {"api_key": TMDB_API_KEY}
                                response = requests.get(url, params=params, timeout=5)
                                
                                if response.status_code == 200:
                                    data = response.json()
                                    release_date = data.get("release_date", "")
                                    if release_date:
                                        year = release_date.split("-")[0]
                                        year_str = f" ({year})"
                            except Exception:
                                # If there's an error, just continue without year
                                pass
                        
                        new_line = f"{error['title']}{year_str} [movie:{tmdb_id}]\n"
                        lines[error['line_num'] - 1] = new_line
                        errors_fixed += 1
                        print(f"✅ Updated to: {new_line.strip()}")
                    else:
                        print("❌ Invalid TMDB ID (must be a number)")
                
                elif fix_choice == "4":
                    print("⏭️ Skipped this entry")
                    continue
                
                elif fix_choice == "5":
                    print("⏹️ Exiting manual fix mode")
                    break
            
            # Save changes if any fixes were made
            if errors_fixed > 0:
                with open(full_path, "w", encoding="utf-8") as f:
                    f.writelines(lines)
                print(f"\n✅ Fixed {errors_fixed} out of {len(errors)} error entries")
            else:
                print("\n❌ No changes were made")
            
            input("Press Enter to continue...")
            
        elif choice == "3":
            # Automatic fix mode
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
            print("🤖 Auto-fix mode - will attempt to match all titles with TMDB")
            
            # Ask for confirmation before starting the potentially time-consuming operation
            if input("This may take some time. Continue? (y/N): ").lower() != 'y':
                continue
            
            # Read file content
            with open(full_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            # Process errors in parallel for better performance
            error_titles = [(error['line_num'], error['title']) for error in errors]
            total_errors = len(error_titles)
            
            print(f"🔍 Matching {total_errors} titles with TMDB using threads...")
            
            # Adjust worker count based on number of titles
            max_workers = min(20, max(5, total_errors // 5))
            print(f"⚡ Using {max_workers} worker threads for API calls...")
            
            fixed_count = 0
            success_count = 0
            
            # Show progress during API calls
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Create a map of line_num to future result
                future_to_line = {}
                for line_num, title in error_titles:
                    future = executor.submit(match_title_worker, title)
                    future_to_line[future] = line_num
                
                completed = 0
                for future in as_completed(future_to_line):
                    line_num = future_to_line[future]
                    title, result = future.result()
                    
                    completed += 1
                    if completed % 5 == 0 or completed == total_errors:
                        print(f"⏳ Progress: {completed}/{total_errors} titles matched ({completed/total_errors:.1%})")
                    
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
                        success_count += 1
            
            # Save changes if any fixes were made
            if success_count > 0:
                with open(full_path, "w", encoding="utf-8") as f:
                    f.writelines(lines)
                print(f"\n✅ Successfully fixed {success_count} out of {total_errors} error entries")
            else:
                print("\n❌ No entries could be fixed automatically")
            
            # Notify if some entries couldn't be fixed
            if success_count < total_errors:
                print(f"⚠️ {total_errors - success_count} entries still need manual fixing")
            
            input("Press Enter to continue...")
            
        elif choice == "4":
            return
        else:
            input("❌ Invalid option. Press Enter to continue...")

def process_folder_for_issues(folder_path):
    """
    Process all files in a folder for duplicates and errors
    
    Args:
        folder_path: Path to the folder containing list files
    
    Returns:
        tuple: (processed_files, fixed_duplicates, fixed_errors)
    """
    if not os.path.isdir(folder_path):
        print(f"❌ Not a valid folder: {folder_path}")
        return 0, 0, 0
    
    processed_files = 0
    total_duplicates_fixed = 0
    total_errors_fixed = 0  # Track fixed errors as well
    
    # Get all text files in the folder and subfolders
    file_list = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.txt'):
                file_list.append(os.path.join(root, file))
    
    if not file_list:
        print(f"❌ No text files found in folder: {folder_path}")
        return 0, 0, 0
    
    print(f"🔍 Found {len(file_list)} text files to process")
    
    for file_path in file_list:
        rel_path = os.path.relpath(file_path, folder_path)
        print(f"\n🔄 Processing file: {rel_path}")
        
        # Check for duplicates first
        duplicates = find_duplicate_entries_ultrafast(file_path, respect_years=True)
        if duplicates:
            # Handle duplicates (existing code)
            # ...
            pass
        else:
            print("✅ No duplicates found")
        
        # Now check for errors and try to fix them automatically
        errors = find_error_entries(file_path)
        if errors:
            print(f"⚠️ Found {len(errors)} error entries")
            
            # Ask if errors should be automatically fixed
            if input("Attempt to auto-fix errors with TMDB? (y/N): ").lower() == 'y':
                # Read file content
                with open(file_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                
                # Process errors in parallel
                error_titles = [(error['line_num'], error['title']) for error in errors]
                print(f"🔍 Matching {len(error_titles)} titles with TMDB...")
                
                # Adjust worker count
                max_workers = min(20, max(5, len(error_titles) // 5))
                
                success_count = 0
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_line = {}
                    for line_num, title in error_titles:
                        future = executor.submit(match_title_worker, title)
                        future_to_line[future] = line_num
                    
                    for future in as_completed(future_to_line):
                        line_num = future_to_line[future]
                        title, result = future.result()
                        
                        if isinstance(result, dict):
                            # Construct the fixed line
                            year_str = f" ({result['year']})" if result.get('year') else ""
                            new_line = f"{title}{year_str} [{result['id']}]\n"
                            
                            # Update the line in the file content
                            lines[line_num - 1] = new_line
                            success_count += 1
                
                # Save changes if any fixes were made
                if success_count > 0:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.writelines(lines)
                    print(f"✅ Successfully fixed {success_count} out of {len(errors)} error entries")
                    total_errors_fixed += success_count
                else:
                    print("❌ No entries could be fixed automatically")
            else:
                print("⏭️ Skipping error fixing for this file")
        else:
            print("✅ No errors found")
        
        processed_files += 1
    
    return processed_files, total_duplicates_fixed, total_errors_fixed

def batch_process_menu():
    """Menu for batch processing folders"""
    while True:
        clear_terminal()
        print("📁 Batch Process Folders")
        print("1. Process folder (find & fix duplicates, find errors)")
        print("2. Return to main menu")

        choice = input("\nChoose an option (1-2) [1]: ").strip() or "1"

        if choice == "1":
            folder_path = input("Enter folder path to process: ").strip()
            
            if not folder_path or not os.path.isdir(folder_path):
                print(f"❌ Invalid folder path: {folder_path}")
                input("Press Enter to continue...")
                continue
            
            start_time = time.time()
            print(f"🔍 Processing files in: {folder_path}")
            files, duplicates, errors = process_folder_for_issues(folder_path)
            
            elapsed = time.time() - start_time
            print("\n📊 Summary:")
            print(f"   - Processed {files} files")
            print(f"   - Fixed {duplicates} duplicate entries")
            print(f"   - Found {errors} error entries (manual fixing required)")
            print(f"\n⏱️ Completed in {elapsed:.1f} seconds")
            
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            return
        else:
            input("❌ Invalid option. Press Enter to continue...")

def process_dragged_folder(folder_path):
    """Process a folder dragged onto the application"""
    print(f"📁 Processing dragged folder: {folder_path}")
    
    # First, validate the folder
    if not os.path.isdir(folder_path):
        print(f"❌ Not a valid folder: {folder_path}")
        input("Press Enter to continue...")
        return
    
    # Process the folder
    start_time = time.time()
    files, duplicates, errors = process_folder_for_issues(folder_path)
    
    elapsed = time.time() - start_time
    print("\n📊 Summary:")
    print(f"   - Processed {files} files")
    print(f"   - Fixed {duplicates} duplicate entries")
    print(f"   - Found {errors} error entries (manual fixing required)")
    print(f"\n⏱️ Completed in {elapsed:.1f} seconds")
    
    input("\nPress Enter to exit...")

def main_menu():
    """Main menu for the application"""
    while True:
        clear_terminal()
        print("📋 Parsely - TV Show List Manager")
        print("1. Run Scraper (Single URL)")
        print("2. Batch Scraper (Multiple URLs)")
        print("3. Monitor Scraper")
        print("4. Fix Errors in Lists")
        print("5. Manage Duplicates in Lists")
        print("6. Batch Process Folders")
        print("7. Settings")
        print("8. Exit")

        choice = input("\nChoose an option (1-8): ").strip()

        if choice == "1":
            run_scraper()
        elif choice == "2":
            run_batch_scraper()
        elif choice == "3":
            run_monitor_scraper()
        elif choice == "4":
            fix_errors_menu()
        elif choice == "5":
            duplicates_menu()
        elif choice == "6":
            batch_process_menu()
        elif choice == "7":
            show_settings()
        elif choice == "8":
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