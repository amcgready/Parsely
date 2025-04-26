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
        except Exception:
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

def find_error_entries(filepath):
    """Find all lines with [Error] in a file"""
    if not os.path.exists(filepath):
        return []
    
    error_entries = []
    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            if "[Error]" in line:
                # Store the line number, original line, and just the title part
                title = line.split("[Error]")[0].strip()
                error_entries.append({"line_num": i, "line": line.strip(), "title": title})
    
    return error_entries

def fix_error_entries(output_file):
    """Interactively fix entries with errors in a file"""
    # Use the helper function to get the full file path
    full_output_path = get_output_filepath(output_file)
    
    if not os.path.exists(full_output_path):
        print(f"❌ File '{output_file}' not found.")
        return
    
    # Find all error entries
    error_entries = find_error_entries(full_output_path)
    
    if not error_entries:
        print(f"✅ No errors found in '{output_file}'.")
        return
    
    print(f"🔍 Found {len(error_entries)} entries with errors in '{output_file}'.")
    
    # Read the entire file into memory
    with open(full_output_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()
    
    # Process each error entry
    fixed_count = 0
    for i, entry in enumerate(error_entries, 1):
        print(f"\n{i}/{len(error_entries)}: {entry['line']}")
        print("Options:")
        print("1. Try to fix with TMDB search")
        print("2. Skip this entry")
        print("3. Skip all remaining entries")
        
        choice = input("Select an option (1-3): ").strip()
        
        if choice == "1":
            search_term = input(f"Enter search term (default: '{entry['title']}'): ").strip()
            if not search_term:
                search_term = entry['title']
                
            print(f"🔍 Searching TMDB for '{search_term}'...")
            result = match_title_with_tmdb(search_term)
            
            if isinstance(result, dict):
                year = f" ({result['year']})" if result.get("year") else ""
                new_line = f"{entry['title']}{year} [{result['id']}]\n"
                
                print(f"✅ Found match: {new_line.strip()}")
                confirm = input("Apply this fix? (y/n): ").strip().lower()
                
                if confirm == "y":
                    # Replace the line (account for 0-based indexing)
                    all_lines[entry['line_num'] - 1] = new_line
                    fixed_count += 1
                    print("✅ Entry updated.")
                else:
                    print("⏩ Entry not changed.")
            else:
                print("❌ No match found in TMDB.")
                manual_id = input("Enter TMDB ID manually (or leave blank to skip): ").strip()
                if manual_id and manual_id.isdigit():
                    new_line = f"{entry['title']} [{manual_id}]\n"
                    all_lines[entry['line_num'] - 1] = new_line
                    fixed_count += 1
                    print("✅ Entry updated with manual ID.")
                else:
                    print("⏩ Entry not changed.")
        
        elif choice == "2":
            print("⏩ Skipping this entry.")
            continue
            
        elif choice == "3":
            print("⏩ Skipping all remaining entries.")
            break
            
        else:
            print("❌ Invalid option, skipping this entry.")
    
    # Write the updated content back to the file
    if fixed_count > 0:
        with open(full_output_path, "w", encoding="utf-8") as f:
            f.writelines(all_lines)
        print(f"\n✅ Fixed {fixed_count} entries in '{output_file}'.")
    else:
        print("\n⚠️ No entries were changed.")

def fix_errors_menu():
    """Menu for fixing error entries in lists"""
    while True:
        clear_terminal()
        print("🔧 Fix Errors in Lists")
        print("1. Select file to fix")
        print("2. Scan directory for files with errors")
        print("3. Back to Main Menu")
        
        choice = input("\nSelect an option (1-3): ").strip()
        
        if choice == "1":
            output_file = input("Enter the name of the file to fix: ").strip()
            fix_error_entries(output_file)
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            # Get root directory from settings
            root_dir = get_env_string("OUTPUT_ROOT_DIR", os.getcwd())
            print(f"🔍 Scanning directory: {root_dir}")
            
            # Find all text files
            text_files = []
            for root, dirs, files in os.walk(root_dir):
                for file in files:
                    if file.endswith(".txt"):
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, root_dir)
                        text_files.append(rel_path)
            
            if not text_files:
                print("❌ No text files found.")
                input("Press Enter to continue...")
                continue
                
            # Check each file for errors
            files_with_errors = []
            for file_path in text_files:
                full_path = os.path.join(root_dir, file_path)
                error_entries = find_error_entries(full_path)
                if error_entries:
                    files_with_errors.append({
                        "path": file_path,
                        "error_count": len(error_entries)
                    })
            
            if not files_with_errors:
                print("✅ No files with errors found.")
                input("Press Enter to continue...")
                continue
                
            # Display files with errors
            print(f"\n📋 Found {len(files_with_errors)} files with errors:")
            for i, file_info in enumerate(files_with_errors, 1):
                print(f"{i}. {file_info['path']} ({file_info['error_count']} errors)")
            
            file_choice = input("\nEnter file number to fix (or 0 to cancel): ").strip()
            try:
                file_idx = int(file_choice) - 1
                if file_idx == -1:
                    continue
                if 0 <= file_idx < len(files_with_errors):
                    file_to_fix = files_with_errors[file_idx]['path']
                    fix_error_entries(file_to_fix)
                else:
                    print("❌ Invalid file number.")
                input("\nPress Enter to continue...")
            except ValueError:
                print("❌ Please enter a valid number.")
                input("Press Enter to continue...")
            
        elif choice == "3":
            return
        else:
            input("❌ Invalid option. Press Enter to try again...")

def find_duplicate_entries(filepath):
    """Find all duplicate titles in a file"""
    if not os.path.exists(filepath):
        return []
    
    # Dictionary to track title occurrences and their line numbers
    title_occurrences = {}
    
    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            # Extract just the title part (before any "[" or "->")
            title = line.split("->")[0].split("[")[0].strip()
            if title:
                if title in title_occurrences:
                    title_occurrences[title].append({"line_num": i, "full_line": line.strip()})
                else:
                    title_occurrences[title] = [{"line_num": i, "full_line": line.strip()}]
    
    # Filter to only titles with multiple occurrences
    duplicates = {title: occurrences for title, occurrences in title_occurrences.items() 
                 if len(occurrences) > 1}
    
    return duplicates

def manage_duplicates(output_file):
    """Manage duplicate entries in a file"""
    # Use the helper function to get the full file path
    full_output_path = get_output_filepath(output_file)
    
    if not os.path.exists(full_output_path):
        print(f"❌ File '{output_file}' not found.")
        return
    
    # Find all duplicate entries
    duplicates = find_duplicate_entries(full_output_path)
    
    if not duplicates:
        print(f"✅ No duplicate titles found in '{output_file}'.")
        return
    
    print(f"🔍 Found {len(duplicates)} titles with duplicates in '{output_file}'.")
    
    # Read the entire file into memory
    with open(full_output_path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()
    
    # Process each duplicate title
    removed_count = 0
    for i, (title, occurrences) in enumerate(duplicates.items(), 1):
        print(f"\n{i}/{len(duplicates)}: Title: '{title}'")
        print("Occurrences:")
        for j, entry in enumerate(occurrences, 1):
            print(f"  {j}. Line {entry['line_num']}: {entry['full_line']}")
        
        print("\nOptions:")
        print("1. Keep first occurrence, remove others (default - press Enter)")
        print("2. Keep specific occurrence, remove others")
        print("3. Keep all occurrences")
        print("4. Skip this title")
        print("5. Skip all remaining titles")
        
        choice = input("Select an option (1-5): ").strip()
        
        # If user just pressed Enter, default to option 1
        if choice == "":
            choice = "1"
        
        if choice == "1":
            # Keep the first occurrence, remove others
            for entry in occurrences[1:]:
                # Mark lines for removal (we'll use None as a marker)
                # Use 0-based indexing for all_lines
                all_lines[entry["line_num"] - 1] = None
                removed_count += 1
            print(f"✅ Kept first occurrence, removed {len(occurrences)-1} duplicate(s).")
            
        elif choice == "2":
            # Let user choose which occurrence to keep
            keep_idx = input(f"Enter occurrence number to keep (1-{len(occurrences)}): ").strip()
            try:
                keep_idx = int(keep_idx) - 1  # Convert to 0-based
                if 0 <= keep_idx < len(occurrences):
                    for j, entry in enumerate(occurrences):
                        if j != keep_idx:  # Remove all except the selected one
                            all_lines[entry["line_num"] - 1] = None
                            removed_count += 1
                    print(f"✅ Kept occurrence #{keep_idx+1}, removed {len(occurrences)-1} duplicate(s).")
                else:
                    print("❌ Invalid occurrence number. No changes made.")
            except ValueError:
                print("❌ Invalid input. No changes made.")
                
        elif choice == "3":
            # Keep all occurrences
            print("✅ Kept all occurrences of this title.")
            
        elif choice == "4":
            # Skip this title
            print("⏩ Skipping this title.")
            
        elif choice == "5":
            # Skip all remaining titles
            print("⏩ Skipping all remaining titles.")
            break
            
        else:
            print("❌ Invalid option, skipping this title.")
    
    # Remove the None entries (marked for deletion) and write back to file
    if removed_count > 0:
        new_lines = [line for line in all_lines if line is not None]
        with open(full_output_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        print(f"\n✅ Removed {removed_count} duplicate entries from '{output_file}'.")
    else:
        print("\n⚠️ No entries were removed.")

def duplicates_menu():
    """Menu for managing duplicate entries in lists"""
    while True:
        clear_terminal()
        print("🔍 Manage Duplicates in Lists")
        print("1. Select file to check for duplicates")
        print("2. Scan directory for files with duplicates")
        print("3. Back to Main Menu")
        
        choice = input("\nSelect an option (1-3): ").strip()
        
        if choice == "1":
            output_file = input("Enter the name of the file to check: ").strip()
            manage_duplicates(output_file)
            input("\nPress Enter to continue...")
            
        elif choice == "2":
            # Get root directory from settings
            root_dir = get_env_string("OUTPUT_ROOT_DIR", os.getcwd())
            print(f"🔍 Scanning directory: {root_dir}")
            
            # Find all text files
            text_files = []
            for root, dirs, files in os.walk(root_dir):
                for file in files:
                    if file.endswith(".txt"):
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, root_dir)
                        text_files.append(rel_path)
            
            if not text_files:
                print("❌ No text files found.")
                input("Press Enter to continue...")
                continue
                
            # Check each file for duplicates
            files_with_duplicates = []
            for file_path in text_files:
                full_path = os.path.join(root_dir, file_path)
                duplicates = find_duplicate_entries(full_path)
                if duplicates:
                    files_with_duplicates.append({
                        "path": file_path,
                        "dup_count": len(duplicates),
                        "total_dupes": sum(len(occurrences) for occurrences in duplicates.values()) - len(duplicates)
                    })
            
            if not files_with_duplicates:
                print("✅ No files with duplicate entries found.")
                input("Press Enter to continue...")
                continue
                
            # Display files with duplicates
            print(f"\n📋 Found {len(files_with_duplicates)} files with duplicates:")
            for i, file_info in enumerate(files_with_duplicates, 1):
                print(f"{i}. {file_info['path']} " +
                      f"({file_info['dup_count']} titles with {file_info['total_dupes']} duplicate entries)")
            
            file_choice = input("\nEnter file number to manage (or 0 to cancel): ").strip()
            try:
                file_idx = int(file_choice) - 1
                if file_idx == -1:
                    continue
                if 0 <= file_idx < len(files_with_duplicates):
                    file_to_fix = files_with_duplicates[file_idx]['path']
                    manage_duplicates(file_to_fix)
                else:
                    print("❌ Invalid file number.")
                input("\nPress Enter to continue...")
            except ValueError:
                print("❌ Please enter a valid number.")
                input("Press Enter to continue...")
            
        elif choice == "3":
            return
        else:
            input("❌ Invalid option. Press Enter to try again...")

def run_batch_scraper():
    ENABLE_TMDB_MATCHING = get_env_flag("ENABLE_TMDB_MATCHING")
    INCLUDE_YEAR = get_env_flag("INCLUDE_YEAR")
    scan_history = load_scan_history()

    clear_terminal()
    print("📋 Batch Scraping Mode")
    print("Enter URLs (one per line). Type 'done' when finished:")
    
    urls = []
    while True:
        url = input().strip()
        if url.lower() == 'done':
            break
        if url:  # Skip empty lines
            urls.append(url)
    
    if not urls:
        print("❌ No URLs provided. Returning to menu.")
        input("Press Enter to continue...")
        return
    
    # Get concurrency settings
    max_workers = min(os.cpu_count() or 1, len(urls))
    concurrent = input(f"\n⚡ Use parallel processing? (up to {max_workers} URLs at once) (y/n): ").strip().lower() == 'y'
    
    mode = input("\n📁 Output mode:\n1. Single output file for all URLs\n2. Separate output file for each URL\nSelect (1-2): ").strip()
    
    if mode == "1":
        # Single output file mode
        output_file = input("📝 Enter the name of the output file (with .txt extension): ").strip()
        
        total_new = 0
        total_skipped = 0
        
        all_titles = []
        if concurrent and len(urls) > 1:
            print(f"\n⚙️ Scraping {len(urls)} URLs in parallel with {max_workers} workers...")
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(scrape_url_worker, url): url for url in urls}
                    for future in as_completed(futures):
                        url = futures[future]
                        try:
                            titles = future.result()[1]  # Get titles from result tuple
                            print(f"✅ Finished scraping: {url} - found {len(titles)} titles")
                            all_titles.extend(titles)
                        except Exception as e:
                            print(f"❌ Error scraping {url}: {str(e)}")
            except Exception as e:
                print(f"❌ Error during parallel scraping: {str(e)}")
        else:
            for i, url in enumerate(urls, 1):
                print(f"\n🌐 Processing URL {i}/{len(urls)}: {url}")
                try:
                    titles = scrape_all_pages(url)
                    print(f"✅ Finished scraping: {url} - found {len(titles)} titles")
                    all_titles.extend(titles)
                except Exception as e:
                    print(f"❌ Error scraping {url}: {str(e)}")
        
        if all_titles:
            new_count, skipped_count = process_scrape_results(
                all_titles, output_file, scan_history, 
                ENABLE_TMDB_MATCHING, INCLUDE_YEAR
            )
            
            print(f"\n✅ Total: Added {new_count} new titles to '{output_file}'")
            if skipped_count > 0:
                print(f"⏩ Total: Skipped {skipped_count} titles already in '{output_file}'")
        else:
            print("❌ No titles were found. Check for errors above.")
    
    elif mode == "2":
        # Multiple output files mode
        if concurrent and len(urls) > 1:
            # First collect output filenames for each URL
            url_to_output = {}
            for i, url in enumerate(urls, 1):
                output_file = input(f"📝 Enter output file name for URL {i} ({url}) (with .txt extension): ").strip()
                url_to_output[url] = output_file
            
            # Then scrape URLs in parallel
            url_to_titles = {}
            print(f"\n⚙️ Scraping {len(urls)} URLs in parallel with {max_workers} workers...")
            
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(scrape_url_worker, url): url for url in urls}
                    for future in as_completed(futures):
                        url = futures[future]
                        try:
                            _, titles = future.result()
                            url_to_titles[url] = titles
                            print(f"✅ Finished scraping: {url} - found {len(titles)} titles")
                        except Exception as e:
                            print(f"❌ Error scraping {url}: {str(e)}")
                            url_to_titles[url] = []
            except Exception as e:
                print(f"❌ Error during parallel scraping: {str(e)}")
            
            # Process the results for each URL
            for url in urls:
                titles = url_to_titles.get(url, [])
                output_file = url_to_output[url]
                
                if titles:
                    new_count, skipped_count = process_scrape_results(
                        titles, output_file, scan_history, 
                        ENABLE_TMDB_MATCHING, INCLUDE_YEAR
                    )
                    print(f"✅ Added {new_count} new titles to '{output_file}'")
                    if skipped_count > 0:
                        print(f"⏩ Skipped {skipped_count} titles already in '{output_file}'")
                else:
                    print(f"⚠️ No titles found for {url} to add to '{output_file}'")
        else:
            # Process URLs sequentially
            for i, url in enumerate(urls, 1):
                print(f"\n🌐 Processing URL {i}/{len(urls)}: {url}")
                output_file = input(f"📝 Enter output file name for URL {i} ({url}) (with .txt extension): ").strip()
                
                try:
                    titles = scrape_all_pages(url)
                    if titles:
                        new_count, skipped_count = process_scrape_results(
                            titles, output_file, scan_history, 
                            ENABLE_TMDB_MATCHING, INCLUDE_YEAR
                        )
                        print(f"✅ Added {new_count} new titles to '{output_file}'")
                        if skipped_count > 0:
                            print(f"⏩ Skipped {skipped_count} titles already in '{output_file}'")
                    else:
                        print(f"⚠️ No titles found for {url}")
                except Exception as e:
                    print(f"❌ Error scraping {url}: {str(e)}")
    
    else:
        print("❌ Invalid option.")

    input("\nPress Enter to return to the main menu...")

def run_scraper():
    ENABLE_TMDB_MATCHING = get_env_flag("ENABLE_TMDB_MATCHING")
    INCLUDE_YEAR = get_env_flag("INCLUDE_YEAR")
    scan_history = load_scan_history()

    while True:
        clear_terminal()
        url = input("🌐 Enter the MDBList URL to scrape: ").strip()
        output_file = input("📝 Enter the name of the output file (with .txt extension): ").strip()

        titles = scrape_all_pages(url)
        new_count, skipped_count = process_scrape_results(
            titles, output_file, scan_history, 
            ENABLE_TMDB_MATCHING, INCLUDE_YEAR
        )
        
        print(f"\n✅ Saved {new_count} new titles to '{output_file}'")
        if skipped_count > 0:
            print(f"⏩ Skipped {skipped_count} titles already in '{output_file}'")

        again = input("\n🔁 Would you like to scrape another URL? (y/n): ").strip().lower()
        if again != "y":
            break

def show_settings():
    while True:
        clear_terminal()
        tmdb_enabled = get_env_flag("ENABLE_TMDB_MATCHING")
        include_year = get_env_flag("INCLUDE_YEAR")
        parallel_enabled = get_env_flag("ENABLE_PARALLEL_PROCESSING", "true")
        page_delay = float(os.getenv("PAGE_FETCH_DELAY", "0.5"))
        tmdb_workers = int(os.getenv("TMDB_MAX_WORKERS", "16"))
        output_root = get_env_string("OUTPUT_ROOT_DIR", os.getcwd())
        
        print("⚙️ Current Settings:")
        print(f"1. TMDB Matching: {'ON' if tmdb_enabled else 'OFF'}")
        print(f"2. Include Year in Output: {'ON' if include_year else 'OFF'}")
        print(f"3. Parallel Processing: {'ON' if parallel_enabled else 'OFF'}")
        print(f"4. Page Fetch Delay: {page_delay}s")
        print(f"5. TMDB Max Workers: {tmdb_workers}")
        print(f"6. Output Root Directory: {output_root}")
        print("7. Clear history for a specific output file")
        print("8. Clear ALL scan history")
        print("9. Back to Main Menu")
        
        choice = input("Select an option (1-9): ").strip()

        if choice == "1":
            update_env_variable("ENABLE_TMDB_MATCHING", not tmdb_enabled)
        elif choice == "2":
            update_env_variable("INCLUDE_YEAR", not include_year)
        elif choice == "3":
            update_env_variable("ENABLE_PARALLEL_PROCESSING", not parallel_enabled)
        elif choice == "4":
            try:
                new_delay = float(input("Enter page fetch delay in seconds (e.g. 0.5): "))
                if new_delay >= 0:
                    with open(ENV_FILE, "r") as f:
                        lines = f.readlines()
                    found = False
                    for i, line in enumerate(lines):
                        if line.startswith("PAGE_FETCH_DELAY="):
                            lines[i] = f"PAGE_FETCH_DELAY={new_delay}\n"
                            found = True
                            break
                    if not found:
                        lines.append(f"PAGE_FETCH_DELAY={new_delay}\n")
                    with open(ENV_FILE, "w") as f:
                        f.writelines(lines)
                    os.environ["PAGE_FETCH_DELAY"] = str(new_delay)
                    print(f"✅ Page fetch delay set to {new_delay}s")
                else:
                    print("❌ Delay must be non-negative")
            except ValueError:
                print("❌ Invalid input, please enter a number")
            input("Press Enter to continue...")
        elif choice == "5":
            try:
                new_workers = int(input("Enter max worker threads for TMDB API (8-32 recommended): "))
                if new_workers > 0:
                    with open(ENV_FILE, "r") as f:
                        lines = f.readlines()
                    found = False
                    for i, line in enumerate(lines):
                        if line.startswith("TMDB_MAX_WORKERS="):
                            lines[i] = f"TMDB_MAX_WORKERS={new_workers}\n"
                            found = True
                            break
                    if not found:
                        lines.append(f"TMDB_MAX_WORKERS={new_workers}\n")
                    with open(ENV_FILE, "w") as f:
                        f.writelines(lines)
                    os.environ["TMDB_MAX_WORKERS"] = str(new_workers)
                    print(f"✅ TMDB max workers set to {new_workers}")
                else:
                    print("❌ Worker count must be positive")
            except ValueError:
                print("❌ Invalid input, please enter a number")
            input("Press Enter to continue...")
        elif choice == "6":
            current_dir = get_env_string("OUTPUT_ROOT_DIR", os.getcwd())
            print(f"Current output directory: {current_dir}")
            new_dir = input("Enter new output root directory (leave empty to keep current): ").strip()
            
            if new_dir:
                # Check if directory exists, create if not
                if not os.path.exists(new_dir):
                    try:
                        os.makedirs(new_dir)
                        print(f"✅ Created directory: {new_dir}")
                    except Exception as e:
                        print(f"❌ Could not create directory: {str(e)}")
                        input("Press Enter to continue...")
                        continue
                
                # Update the setting
                update_env_string("OUTPUT_ROOT_DIR", new_dir)
                print(f"✅ Output directory set to: {new_dir}")
            
            input("Press Enter to continue...")
        elif choice == "7":
            filename = input("Enter the output filename to clear its history: ").strip()
            clear_history("file", filename)
            print(f"✅ History for '{filename}' cleared.")
            input("Press Enter to return...")
        elif choice == "8":
            confirm = input("⚠️ This will erase ALL scan history. Type 'yes' to confirm: ").strip().lower()
            if confirm == "yes":
                clear_history("all")
                print("✅ All history cleared.")
            else:
                print("❌ Cancelled.")
            input("Press Enter to return...")
        elif choice == "9":
            break
        else:
            input("❌ Invalid option. Press Enter to try again...")

def main_menu():
    while True:
        clear_terminal()
        print("📋 Menu")
        print("1. Run Scraper (Single URL)")
        print("2. Batch Scraper (Multiple URLs)")
        print("3. Monitor Scraper")
        print("4. Fix Errors in Lists")
        print("5. Manage Duplicates in Lists")  # New option
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
        elif choice == "5":  # New option
            duplicates_menu()
        elif choice == "6":
            show_settings()
        elif choice == "7":
            print("👋 Exiting.")
            break
        else:
            input("❌ Invalid option. Press Enter to continue...")

def main():
    """Main function with command line argument support"""
    import sys
    
    # If called with --monitor-check, run monitor check and exit
    if len(sys.argv) > 1 and sys.argv[1] == "--monitor-check":
        run_monitor_check()
        sys.exit(0)
        
    # Otherwise start interactive menu
    main_menu()

if __name__ == "__main__":
    main()

