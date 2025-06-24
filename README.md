# Scraper Profession

This repository contains a simple scraper that collects LinkedIn profile URLs for various professions related to **Santa Clara University** using the Google Custom Search API.

## Requirements

- Python 3.12+
- [`requests`](https://pypi.org/project/requests/) (see `requirements.txt`)
- [`python-dotenv`](https://pypi.org/project/python-dotenv/)
- A Google Custom Search API key stored in the environment variable `GOOGLE_API_KEY`
- A Custom Search Engine ID stored in the environment variable `GOOGLE_CX`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the scraper:

```bash
python google_linkedin_scraper.py
```

The script will generate a `raw_links.csv` file with columns:

- `name` – the person's name as extracted from the search result
- `linkedin_url` – direct link to the LinkedIn profile
- `search_keyword` – the keyword used for the search
- `profession` – the profession category

The scraper cycles through a predefined set of professions and keyword variants, aiming to collect about 40 unique profiles per profession (approximately 600 in total).
