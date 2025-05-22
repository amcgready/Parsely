# Parsely

A powerful tool for scraping and managing movie and TV show lists from popular websites like Trakt, Letterboxd, and MDBList.

<a href="https://wakatime.com/badge/github/amcgready/Parsely"><img src="https://wakatime.com/badge/github/amcgready/Parsely.svg" alt="wakatime"></a>

## 📋 Overview

Parsely helps you collect and organize movie and TV show titles from various online sources, automatically match them with TMDB (The Movie Database) information, and maintain clean, deduplicated lists for your media collection.

## ✨ Features

- **Multi-Site Scraping:** Extract titles from Trakt.tv, Letterboxd, and MDBList
- **TMDB Integration:** Automatically match scraped titles with TMDB IDs
- **Smart Caching:** Reuse previous TMDB lookups to reduce API calls
- **Duplicate Detection:** Find and remove duplicates while preserving the best data
- **Error Fixing:** Automatically fix entries that failed to match properly
- **Batch Processing:** Handle multiple URLs or files at once
- **Parallel Processing:** Multi-threaded design for faster operation

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- TMDB API key (required)
- MDBList API key (optional)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/amcgready/parsely.git
   cd parsely
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your API keys (use `.env.template` as a reference):
   ```env
   TMDB_API_KEY=your_tmdb_key_here
   ENABLE_TMDB_MATCHING=true
   MDBLIST_API_KEY=your_mdblist_key_here
   INCLUDE_YEAR=true
   ```

4. Test your configuration:
   ```bash
   python envtest.py
   ```

### Usage

Run the main script to launch the interactive menu:

```bash
python parsely.py
```

## 📚 Main Features

### 1. Single URL Scraper

Scrape a single URL from Trakt, Letterboxd, or MDBList and save to a file.

### 2. Batch Scraper

Process multiple URLs at once and combine the results into a single list.

### 3. Monitor Scraper (Coming Soon)

Monitor URLs for changes and update your lists automatically.

### 4. Fix Errors

Fix entries that couldn't be matched with TMDB automatically.

### 5. Manage Duplicates

Find and remove duplicate entries across your lists.

### 6. Auto Fix Tool

Comprehensive tool to fix both duplicates and errors across multiple files in one operation.

### 7. Settings

Configure Parsely's behavior including TMDB matching and output format.

## 🛠️ Docker Support

Parsely includes Docker support for easy deployment:

```bash
# Build the Docker image
docker build -t parsely .

# Run with Docker Compose
docker-compose up
```

## 📝 List Format

Lists are stored in text files with the following format:

```
Title (Year) [TMDB_ID]       # For TV shows
Title (Year) [movie:TMDB_ID] # For movies
```

## 🤝 Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgements

- [TMDB API](https://developers.themoviedb.org/3/getting-started/introduction) for metadata
- [Trakt.tv](https://trakt.tv) for watchlist and collection data
- [Letterboxd](https://letterboxd.com) for curated film lists
- [MDBList](https://mdblist.com) for list integration