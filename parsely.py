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

def process_file_queue(file_queue):
    """Process multiple files for both duplicates and errors"""
    # Settings
    respect_years = input("Treat titles with different years as different entries? (Y/n): ").strip().lower() != 'n'
    auto_fix_errors = input("Automatically fix error entries with TMDB? (Y/n): ").strip().lower() != 'n'
    
    # Stats tracking
    total_files = len(file_queue)
    total_duplicates_fixed = 0
    total_errors_fixed = 0
    start_time = time.time()
    
    # Start health check for overall processing
    overall_health = show_health_check_start("Processing files", total_files, interval=5.0)
    
    for idx, file_path in enumerate(file_queue):
        rel_path = os.path.basename(file_path)
        print(f"\n🔄 Processing file {idx+1}/{total_files}: {rel_path}")
        
        # ----- DUPLICATES SECTION -----
        print(f"🔍 Scanning for duplicates...")
        duplicates = find_duplicate_entries_ultrafast(file_path, respect_years)
        
        if duplicates:
            print(f"⚠️ Found {len(duplicates)} titles with duplicates")
            
            # Automatically create lists of lines to keep and remove
            lines_to_keep = set()
            lines_to_remove = set()
            
            # Start health check for duplicate processing
            dup_health = show_health_check_start("Processing duplicates", len(duplicates))
            
            for dup_idx, (title, occurrences) in enumerate(duplicates.items()):
                # Keep the first occurrence
                lines_to_keep.add(occurrences[0]['line_num'])
                
                # Remove all other occurrences
                duplicate_count = 0
                for occurrence in occurrences[1:]:
                    lines_to_remove.add(occurrence['line_num'])
                    duplicate_count += 1
                
                # Update health check
                show_health_check_update(dup_health, dup_idx + 1)
            
            # End health check for duplicates
            show_health_check_end(dup_health)
            
            # Create a set of all line numbers in the file
            with open(file_path, "r", encoding="utf-8") as f:
                total_lines = sum(1 for _ in f)
            
            # Lines to keep includes all lines NOT in lines_to_remove
            all_lines_to_keep = set(range(1, total_lines + 1)) - lines_to_remove
            
            if len(lines_to_remove) > 0:
                if remove_duplicate_lines(file_path, all_lines_to_keep):
                    print(f"✅ Successfully removed {len(lines_to_remove)} duplicates")
                    total_duplicates_fixed += len(lines_to_remove)
                else:
                    print("❌ Failed to remove duplicates")
            else:
                print("ℹ️ No duplicates need to be removed")
        else:
            print("✅ No duplicates found")
        
        # ----- ERRORS SECTION -----
        print(f"🔍 Scanning for errors...")
        errors = find_error_entries(file_path)
        
        if errors:
            print(f"⚠️ Found {len(errors)} error entries")
            
            if auto_fix_errors:
                # Read file content
                with open(file_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                
                # Process errors in parallel
                error_titles = [(error['line_num'], error['title']) for error in errors]
                print(f"🔍 Matching {len(error_titles)} titles with TMDB...")
                
                # Adjust worker count
                max_workers = min(20, max(5, len(error_titles) // 5))
                
                # Start health check for error fixing
                err_health = show_health_check_start("Fixing errors", len(error_titles))
                
                success_count = 0
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_line = {}
                    for line_num, title in error_titles:
                        future = executor.submit(match_title_worker, title)
                        future_to_line[future] = line_num
                    
                    completed = 0
                    for future in as_completed(future_to_line):
                        line_num = future_to_line[future]
                        title, result = future.result()
                        
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
                            success_count += 1
                
                # End health check
                show_health_check_end(err_health)
                
                # Save changes if any fixes were made
                if success_count > 0:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.writelines(lines)
                    print(f"✅ Successfully fixed {success_count} out of {len(errors)} error entries")
                    total_errors_fixed += success_count
                else:
                    print("❌ No entries could be fixed automatically")
            else:
                print("ℹ️ Skipping error fixing (auto-fix disabled)")
        else:
            print("✅ No errors found")
        
        # Update overall progress
        show_health_check_update(overall_health, idx + 1)
        print(f"✓ File processed: {rel_path}")
    
    # End overall health check
    show_health_check_end(overall_health)
    
    # Display summary
    elapsed = time.time() - start_time
    print("\n📊 SUMMARY:")
    print(f"   - Processed {total_files} files")
    print(f"   - Fixed {total_duplicates_fixed} duplicate entries")
    print(f"   - Fixed {total_errors_fixed} error entries")
    print(f"\n⏱️ Completed in {elapsed:.1f} seconds")
    
    input("\nPress Enter to continue...")

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

def auto_fix_tool():
    """Comprehensive tool to fix both duplicates and errors across multiple files in one operation"""
    while True:
        clear_terminal()
        print("🔧 Auto Fix Tool - Fix Duplicates and Errors")
        print("1. Add files to processing queue")
        print("2. Process queued files")
        print("3. Return to main menu")

        choice = input("\nChoose an option (1-3) [1]: ").strip() or "1"
        
        if choice == "1":
            file_queue = add_files_to_queue()
            if file_queue:
                if input(f"Process {len(file_queue)} queued files now? (Y/n): ").lower() != 'n':
                    process_file_queue(file_queue)
                    
        elif choice == "2":
            # Let user add files to the queue first
            file_queue = add_files_to_queue()
            if file_queue:
                process_file_queue(file_queue)
            else:
                print("❌ No files queued for processing.")
                input("Press Enter to continue...")
                
        elif choice == "3":
            return
        else:
            input("❌ Invalid option. Press Enter to continue...")

def add_files_to_queue():
    """Add multiple files to the processing queue"""
    clear_terminal()
    print("📂 Add Files to Processing Queue")
    print("Enter one file path per line (empty line to finish):")
    
    file_queue = []
    while True:
        file_path = input("> ").strip()
        if not file_path:
            break
            
        full_path = get_output_filepath(file_path)
        if os.path.exists(full_path):
            file_queue.append(full_path)
            print(f"✅ Added to queue: {file_path}")
        else:
            print(f"❌ File not found: {full_path}")
    
    if not file_queue:
        print("❌ No valid files added to queue.")
    else:
        print(f"✅ {len(file_queue)} files added to processing queue.")
    
    return file_queue

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
        print("7. Auto Fix Tool")
        print("8. Settings")
        print("9. Exit")

        choice = input("\nChoose an option (1-9): ").strip()

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