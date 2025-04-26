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
    file_history = scan_history.get(output_file, {})
    existing_titles = load_titles_from_file(output_file)

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

    with open(output_file, "a", encoding="utf-8") as f:
        for title in titles_to_write:
            if enable_tmdb:
                result = tmdb_results.get(title, "[Error]")

                if isinstance(result, dict):
                    year = f" ({result['year']})" if include_year and result.get("year") else ""
                    f.write(f"{title}{year} [{result['id']}]\n")
                    file_history[title] = {"tmdb_matched": enable_tmdb}
                else:
                    f.write(f"{title} {result}\n")
                    file_history[title] = {"tmdb_matched": enable_tmdb}
            else:
                f.write(title + "\n")
                file_history[title] = {"tmdb_matched": enable_tmdb}

            existing_titles.add(title)
            new_count += 1

    scan_history[output_file] = file_history
    save_scan_history(scan_history)
    return new_count, skipped_count

def scrape_url_worker(url):
    print(f"🌐 Processing: {url}")
    return url, scrape_all_pages(url)

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
        
        print("⚙️ Current Settings:")
        print(f"1. TMDB Matching: {'ON' if tmdb_enabled else 'OFF'}")
        print(f"2. Include Year in Output: {'ON' if include_year else 'OFF'}")
        print(f"3. Parallel Processing: {'ON' if parallel_enabled else 'OFF'}")
        print(f"4. Page Fetch Delay: {page_delay}s")
        print(f"5. TMDB Max Workers: {tmdb_workers}")
        print("6. Clear history for a specific output file")
        print("7. Clear ALL scan history")
        print("8. Back to Main Menu")

        choice = input("Select an option (1-8): ").strip()

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
            filename = input("Enter the output filename to clear its history: ").strip()
            clear_history("file", filename)
            print(f"✅ History for '{filename}' cleared.")
            input("Press Enter to return...")
        elif choice == "7":
            confirm = input("⚠️ This will erase ALL scan history. Type 'yes' to confirm: ").strip().lower()
            if confirm == "yes":
                clear_history("all")
                print("✅ All history cleared.")
            else:
                print("❌ Cancelled.")
            input("Press Enter to return...")
        elif choice == "8":
            break
        else:
            input("❌ Invalid option. Press Enter to try again...")

def run_monitor_scraper():
    """Set up URLs to be monitored for changes every 24 hours"""
    ENABLE_TMDB_MATCHING = get_env_flag("ENABLE_TMDB_MATCHING")
    INCLUDE_YEAR = get_env_flag("INCLUDE_YEAR")
    scan_history = load_scan_history()
    
    # Initialize monitor config if it doesn't exist
    monitor_config_file = "monitor_config.json"
    if os.path.exists(monitor_config_file):
        with open(monitor_config_file, 'r', encoding='utf-8') as f:
            monitor_config = json.load(f)
    else:
        monitor_config = {"urls": [], "last_run": None}
    
    while True:  # Add loop to return to this menu after operations
        clear_terminal()
        print("🔍 Monitor Scraper Mode")
        print("Current monitored URLs:")
        
        if not monitor_config["urls"]:
            print("  No URLs currently monitored.")
        else:
            for i, entry in enumerate(monitor_config["urls"], 1):
                # Show last check time if available
                last_check = "Never" if not entry.get("last_check") else time.strftime(
                    "%Y-%m-%d %H:%M", time.localtime(entry["last_check"]))
                print(f"  {i}. {entry['url']} -> {entry['output_file']} (Last checked: {last_check})")
        
        # Get current cron schedule if available
        try:
            from subprocess import run, PIPE
            crontab = run(["crontab", "-l"], stdout=PIPE, text=True).stdout
            cron_entries = [line for line in crontab.splitlines() if "--monitor-check" in line]
            if cron_entries:
                print("\nCurrent schedule:")
                for entry in cron_entries:
                    schedule = " ".join(entry.split()[:5])
                    print(f"  {schedule}")
            else:
                print("\nNo automatic schedule configured.")
        except:
            print("\nCould not determine current schedule.")
        
        print("\nOptions:")
        print("1. Add URL to monitor")
        print("2. Remove URL from monitor")
        print("3. Run monitor check now")
        print("4. Configure monitor schedule")
        print("5. Back to Main Menu")
        
        choice = input("\nSelect an option (1-5): ").strip()
        
        if choice == "1":
            url = input("🌐 Enter the MDBList URL to monitor: ").strip()
            output_file = input("📝 Enter the name of the output file (with .txt extension): ").strip()
            
            # Add to monitor config
            monitor_config["urls"].append({
                "url": url,
                "output_file": output_file,
                "last_check": None
            })
            
            with open(monitor_config_file, 'w', encoding='utf-8') as f:
                json.dump(monitor_config, f, indent=2)
            
            print(f"✅ URL added to monitor list.")
            input("Press Enter to continue...")
            
        elif choice == "2":
            if not monitor_config["urls"]:
                print("❌ No URLs to remove.")
                input("Press Enter to continue...")
            else:
                try:
                    idx = int(input("Enter the number of the URL to remove: ").strip()) - 1
                    if 0 <= idx < len(monitor_config["urls"]):
                        removed = monitor_config["urls"].pop(idx)
                        with open(monitor_config_file, 'w', encoding='utf-8') as f:
                            json.dump(monitor_config, f, indent=2)
                        print(f"✅ Removed URL: {removed['url']}")
                    else:
                        print("❌ Invalid number.")
                    input("Press Enter to continue...")
                except ValueError:
                    print("❌ Please enter a valid number.")
                    input("Press Enter to continue...")
        
        elif choice == "3":
            if not monitor_config["urls"]:
                print("❌ No URLs to check.")
                input("Press Enter to continue...")
            else:
                check_monitored_urls(monitor_config, scan_history, ENABLE_TMDB_MATCHING, INCLUDE_YEAR)
                # Update last run time
                monitor_config["last_run"] = time.time()
                with open(monitor_config_file, 'w', encoding='utf-8') as f:
                    json.dump(monitor_config, f, indent=2)
                input("Press Enter to continue...")
        
        elif choice == "4":
            configure_monitor_schedule()
        
        elif choice == "5":
            break
        else:
            print("❌ Invalid option.")
            input("Press Enter to try again...")

def configure_monitor_schedule():
    """Configure the automatic monitor schedule"""
    clear_terminal()
    print("📅 Configure Monitor Schedule")
    print("\nCurrent monitoring schedule options:")
    print("1. Daily at midnight")
    print("2. Daily at specific time")
    print("3. Every X hours")
    print("4. Every X days")  # New option
    print("5. Weekly on specific day")
    print("6. Custom cron expression")
    print("7. Remove scheduled monitoring")
    print("8. Back")  # Updated number
    
    choice = input("\nSelect an option (1-8): ").strip()  # Updated number
    
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    script_name = os.path.basename(script_path)
    command = f"cd {script_dir} && python3 '{script_name}' --monitor-check"
    
    # Try to detect system type and scheduling capabilities
    is_windows = os.name == "nt"
    
    # Initialize cron settings
    cron_time = None
    cron_command = ""
    
    if choice == "1":  # Daily at midnight
        cron_time = "0 0 * * *"
        print("✅ Setting up daily monitoring at midnight")
        
    elif choice == "2":  # Daily at specific time
        while True:
            hour = input("Enter hour (0-23): ").strip()
            minute = input("Enter minute (0-59): ").strip()
            try:
                h = int(hour)
                m = int(minute)
                if 0 <= h < 24 and 0 <= m < 60:
                    cron_time = f"{m} {h} * * *"
                    print(f"✅ Setting up daily monitoring at {h:02d}:{m:02d}")
                    break
                else:
                    print("❌ Invalid time. Please try again.")
            except ValueError:
                print("❌ Please enter valid numbers.")
                
    elif choice == "3":  # Every X hours
        while True:
            interval = input("Enter interval in hours (1-24): ").strip()
            try:
                h = int(interval)
                if 1 <= h <= 24 and 24 % h == 0:  # Ensure it divides evenly into a day
                    cron_time = f"0 */{h} * * *"
                    print(f"✅ Setting up monitoring every {h} hours")
                    break
                else:
                    print("❌ Please enter a number between 1-24 that divides evenly into 24.")
            except ValueError:
                print("❌ Please enter a valid number.")
    
    elif choice == "4":  # Every X days (new option)
        while True:
            interval = input("Enter interval in days (1-30): ").strip()
            hour = input("Enter hour of day to run (0-23): ").strip()
            try:
                d = int(interval)
                h = int(hour)
                if 1 <= d <= 30 and 0 <= h < 24:
                    # For "every X days" we use the day of month with modulo
                    cron_time = f"0 {h} */{d} * *"
                    print(f"✅ Setting up monitoring every {d} days at {h:02d}:00")
                    break
                else:
                    print("❌ Invalid input. Day interval must be 1-30 and hour must be 0-23.")
            except ValueError:
                print("❌ Please enter valid numbers.")
                
    elif choice == "5":  # Weekly on specific day (was 4 before)
        days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        print("Days of week:")
        for i, day in enumerate(days):
            print(f"{i}: {day}")
            
        while True:
            day = input("Enter day number (0-6): ").strip()
            hour = input("Enter hour (0-23): ").strip()
            try:
                d = int(day)
                h = int(hour)
                if 0 <= d <= 6 and 0 <= h < 24:
                    cron_time = f"0 {h} * * {d}"
                    print(f"✅ Setting up weekly monitoring on {days[d]} at {h:02d}:00")
                    break
                else:
                    print("❌ Invalid input. Please try again.")
            except ValueError:
                print("❌ Please enter valid numbers.")
                
    elif choice == "6":  # Custom cron expression (was 5 before)
        print("Enter a custom cron expression (minute hour day month weekday):")
        print("Example: '0 3 * * *' runs at 3:00 AM every day")
        cron_time = input("Cron expression: ").strip()
        print(f"✅ Setting up monitoring with custom schedule: {cron_time}")
        
    elif choice == "7":  # Remove scheduled monitoring (was 6 before)
        print("✅ Removing scheduled monitoring")
        # No need to set cron_time, we'll just update the crontab without the monitor line
        
    elif choice == "8":  # Back (was 7 before)
        return
    else:
        print("❌ Invalid option.")
        input("Press Enter to return...")
        return
    
    # Create scheduling instructions based on system type
    if is_windows:
        # Windows scheduling instructions
        print("\n📝 To set up automatic monitoring on Windows:")
        print("1. Open Task Scheduler")
        print("2. Create a Basic Task")
        print(f"3. Set the trigger based on your selection: {cron_time}")
        print("4. For the Action, select 'Start a Program'")
        print(f"5. Program/script: python3 or python")
        print(f"6. Add arguments: \"{script_path}\" --monitor-check")
        print(f"7. Start in: {script_dir}")
    else:
        # Unix/Linux scheduling instructions
        cron_command = f"{cron_time} {command}"
        
        # Try to create a crontab example file for the user
        crontab_example = os.path.join(script_dir, "monitor_crontab.txt")
        try:
            with open(crontab_example, 'w') as f:
                f.write(f"# Monitor schedule for Parsely\n{cron_command}\n")
            print(f"\n✅ Created crontab example file at: {crontab_example}")
        except Exception as e:
            print(f"\n❌ Could not create example file: {str(e)}")
        
        print("\n📝 To set up automatic monitoring on Linux/Unix:")
        print("1. Run 'crontab -e' to edit your crontab")
        print(f"2. Add this line: {cron_command}")
        print("3. Save and exit")
        
        # Try to update crontab directly but handle failures gracefully
        if cron_time:  # Only if we're setting a schedule (not removing)
            try:
                from subprocess import run, PIPE
                
                # First check if crontab is available
                check_result = run(["which", "crontab"], stdout=PIPE, stderr=PIPE, text=True)
                
                if check_result.returncode == 0:
                    # Get current crontab
                    current_crontab = run(["crontab", "-l"], stdout=PIPE, stderr=PIPE, text=True)
                    
                    # Only proceed if getting current crontab was successful
                    if current_crontab.returncode == 0:
                        lines = [line for line in current_crontab.stdout.splitlines() 
                                if "--monitor-check" not in line and line.strip()]
                        
                        # Add our new command
                        if cron_time:
                            lines.append(cron_command)
                        
                        # Write to a temporary file
                        from tempfile import NamedTemporaryFile
                        temp = NamedTemporaryFile(delete=False)
                        try:
                            with open(temp.name, 'w') as f:
                                f.write("\n".join(lines) + "\n")
                            
                            # Update crontab
                            result = run(["crontab", temp.name], stderr=PIPE, text=True)
                            if result.returncode == 0:
                                print("✅ Crontab updated successfully!")
                            else:
                                print(f"⚠️ Could not update crontab automatically: {result.stderr}")
                                
                        except Exception as e:
                            print(f"⚠️ Error creating temporary file: {str(e)}")
                        finally:
                            if os.path.exists(temp.name):
                                os.unlink(temp.name)
                    else:
                        print("⚠️ Could not read current crontab. You'll need to update it manually.")
                else:
                    print("⚠️ Crontab command not found. You'll need to set up scheduling manually.")
                    
            except Exception as e:
                print(f"⚠️ Could not set up automatic scheduling: {str(e)}")
    
    # Always create a monitor-check.sh script as an alternative method
    try:
        script_path = os.path.join(script_dir, "monitor-check.sh")
        with open(script_path, 'w') as f:
            f.write(f"#!/bin/bash\ncd {script_dir}\npython3 '{script_name}' --monitor-check\n")
        os.chmod(script_path, 0o755)  # Make executable
        print(f"\n✅ Created executable script at: {script_path}")
        print("You can run this script manually or add it to your own scheduler")
    except Exception as e:
        print(f"\n❌ Could not create executable script: {str(e)}")
    
    input("\nPress Enter to return to the monitor menu...")

def check_monitored_urls(monitor_config, scan_history, enable_tmdb=True, include_year=True):
    """Check all monitored URLs for new content"""
    print(f"🔍 Checking {len(monitor_config['urls'])} monitored URLs...")
    
    for entry in monitor_config["urls"]:
        url = entry["url"]
        output_file = entry["output_file"]
        
        try:
            print(f"\n🌐 Checking: {url}")
            titles = scrape_all_pages(url)
            
            if titles:
                new_count, skipped_count = process_scrape_results(
                    titles, output_file, scan_history, 
                    enable_tmdb, include_year
                )
                
                # Update last check time
                entry["last_check"] = time.time()
                
                print(f"✅ Added {new_count} new titles to '{output_file}'")
                if skipped_count > 0:
                    print(f"⏩ Skipped {skipped_count} titles already in '{output_file}'")
            else:
                print(f"⚠️ No titles found for {url}")
        except Exception as e:
            print(f"❌ Error checking {url}: {str(e)}")
    
    # Save updated monitor config
    with open("monitor_config.json", 'w', encoding='utf-8') as f:
        json.dump(monitor_config, f, indent=2)

def run_monitor_check():
    """Run monitor check from command line"""
    print("🔄 Running scheduled monitor check...")
    ENABLE_TMDB_MATCHING = get_env_flag("ENABLE_TMDB_MATCHING")
    INCLUDE_YEAR = get_env_flag("INCLUDE_YEAR")
    scan_history = load_scan_history()
    
    monitor_config_file = "monitor_config.json"
    if not os.path.exists(monitor_config_file):
        print("❌ Monitor configuration not found.")
        return
    
    with open(monitor_config_file, 'r', encoding='utf-8') as f:
        monitor_config = json.load(f)
    
    check_monitored_urls(monitor_config, scan_history, ENABLE_TMDB_MATCHING, INCLUDE_YEAR)
    monitor_config["last_run"] = time.time()
    
    with open(monitor_config_file, 'w', encoding='utf-8') as f:
        json.dump(monitor_config, f, indent=2)
    
    print("✅ Monitor check complete.")

def main_menu():
    while True:
        clear_terminal()
        print("📋 Menu")
        print("1. Run Scraper (Single URL)")
        print("2. Batch Scraper (Multiple URLs)")
        print("3. Monitor Scraper")
        print("4. Settings")
        print("5. Exit")

        choice = input("\nChoose an option (1-5): ").strip()

        if choice == "1":
            run_scraper()
        elif choice == "2":
            run_batch_scraper()
        elif choice == "3":
            run_monitor_scraper()
        elif choice == "4":
            show_settings()
        elif choice == "5":
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

