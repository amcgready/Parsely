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

def match_title_with_tmdb(title, max_retries=3, delay=1):
    url = "https://api.themoviedb.org/3/search/tv"
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
                show = data["results"][0]
                return {
                    "id": show["id"],
                    "year": show["first_air_date"].split("-")[0] if show.get("first_air_date") else None
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
        print("2. Return to main menu")

        choice = input("\nChoose an option (1-2): ").strip()

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
                
            print(f"⚠️ Found {len(errors)} error entries")
            input("Press Enter to continue...")
        elif choice == "2":
            return
        else:
            input("❌ Invalid option. Press Enter to continue...")

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
        print("6. Settings")
        print("7. Exit")

        choice = input("\nChoose an option (1-7): ").strip()

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
            show_settings()
        elif choice == "7":
            print("👋 Exiting.")
            break
        else:
            input("❌ Invalid option. Press Enter to continue...")

# This is the main entry point for the program
if __name__ == "__main__":
    # Start the main menu
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n👋 Program interrupted. Exiting.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()