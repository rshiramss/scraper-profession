import os
import csv
import time
from typing import Dict, List, Set

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Mapping of profession to keyword variants
PROFESSIONS: Dict[str, List[str]] = {
    "Software Engineer": [
        "Software Engineer",
        "Backend Developer",
        "Full Stack Engineer",
        "Platform Engineer",
        "SWE",
    ],
    "Data Scientist": [
        "Data Scientist",
        "Machine Learning",
        "AI Research",
        "ML Engineer",
    ],
    "Product Manager": [
        "Product Manager",
        "Product Lead",
        "Product Owner",
    ],
    "UX/UI Designer": [
        "UX Designer",
        "UI/UX",
        "Product Design",
        "Interaction Designer",
    ],
    "Mechanical Engineer": [
        "Mechanical Engineer",
        "Product Development",
        "Manufacturing Engineer",
    ],
    "Electrical Engineer": [
        "Electrical Engineer",
        "Embedded Systems",
        "Hardware Engineer",
    ],
    "Investment Analyst": [
        "Investment Analyst",
        "Equity Research",
        "Portfolio Analyst",
        "Buy-side Analyst",
    ],
    "Consultant": [
        "Consultant",
        "Strategy Consulting",
        "Management Consultant",
        "Business Analyst",
    ],
    "Lawyer": [
        "Attorney",
        "Corporate Law",
        "Legal Counsel",
        "Litigation Associate",
    ],
    "Physician / Med": [
        "Physician",
        "Doctor",
        "Healthcare",
        "Resident MD",
        "Medical Professional",
    ],
    "Research Scientist": [
        "Research Scientist",
        "PhD Candidate",
        "Lab Assistant",
        "Postdoctoral Researcher",
    ],
    "Educator": [
        "Teacher",
        "Professor",
        "Lecturer",
        "Adjunct Instructor",
    ],
    "Journalist": [
        "Journalist",
        "News Reporter",
        "Editor",
        "Columnist",
    ],
    "Marketing / PR": [
        "Marketing",
        "Brand Manager",
        "Public Relations",
        "Content Marketing",
    ],
    "Designer / Creator": [
        "Graphic Designer",
        "Illustrator",
        "Creative Director",
        "Visual Designer",
    ],
}

API_URL = "https://www.googleapis.com/customsearch/v1"
QUERY_TEMPLATE = 'site:linkedin.com/in "Santa Clara University" {}'
RESULTS_PER_PAGE = 10  # Google Custom Search returns up to 10 results per page
MAX_OFFSET = 9
TARGET_RESULTS_PER_PROFESSION = 40


def google_search(query: str, offset: int, api_key: str, cx: str) -> Dict:
    """Perform a single Google Custom Search API request."""
    params = {
        "q": query,
        "key": api_key,
        "cx": cx,
        "num": RESULTS_PER_PAGE,
        "start": offset * RESULTS_PER_PAGE + 1,
    }

    print(f"Making request with query: {query}")
    print(f"Params: {params}")

    response = requests.get(API_URL, params=params, timeout=10)

    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()

    return response.json()


def parse_result(item: Dict) -> Dict:
    """Extract relevant information from a Google search result item."""
    url = item.get("link", "")
    if "/in/" not in url:
        return {}

    # Attempt to extract a human readable name from the title or description
    title = item.get("title", "")
    snippet = item.get("snippet", "")
    name = title.split("-")[0].strip() if title else snippet.split("-")[0].strip()

    return {"name": name, "linkedin_url": url}


def save_csv(records: List[Dict[str, str]], filename: str) -> None:
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["name", "linkedin_url", "search_keyword", "profession"]
        )
        writer.writeheader()
        writer.writerows(records)


def append_to_csv(record: Dict[str, str], filename: str) -> None:
    """Append a single record to the CSV file."""
    # Check if file exists to determine if we need to write header
    file_exists = os.path.exists(filename)
    
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["name", "linkedin_url", "search_keyword", "profession"]
        )
        
        # Write header only if file is new
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(record)


def collect_profiles(api_key: str, cx: str) -> List[Dict[str, str]]:
    profiles: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()
    
    # Create/clear the CSV file at the start
    csv_filename = "raw_links.csv"
    save_csv([], csv_filename)  # This creates the file with just the header
    print(f"Created CSV file: {csv_filename}")

    for profession, keywords in PROFESSIONS.items():
        print(f"\nProcessing profession: {profession}")
        count_for_profession = 0
        keyword_index = 0

        # Continue until we have enough profiles for this profession
        while count_for_profession < TARGET_RESULTS_PER_PROFESSION:
            keyword = keywords[keyword_index % len(keywords)]
            keyword_index += 1
            query = QUERY_TEMPLATE.format(keyword)
            offset = 0

            # Limit offset to MAX_OFFSET (9)
            while count_for_profession < TARGET_RESULTS_PER_PROFESSION and offset <= MAX_OFFSET:
                data = google_search(query, offset=offset, api_key=api_key, cx=cx)
                results = data.get("items", [])
                if not results:
                    break

                for item in results:
                    parsed = parse_result(item)
                    if not parsed:
                        continue

                    url = parsed["linkedin_url"]
                    name = parsed["name"]

                    if url in seen_urls:
                        continue

                    profile_record = {
                        "name": name,
                        "linkedin_url": url,
                        "search_keyword": keyword,
                        "profession": profession,
                    }
                    
                    profiles.append(profile_record)
                    
                    # Immediately append to CSV
                    append_to_csv(profile_record, csv_filename)
                    print(f"Added: {name} ({profession})")

                    seen_urls.add(url)
                    count_for_profession += 1

                    if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                        break

                offset += 1  # Changed from RESULTS_PER_PAGE to 1
                time.sleep(2)  # Increased from 1 to 2 seconds to respect rate limit

    print(f"\nCompleted! Total profiles collected: {len(profiles)}")
    return profiles


def main() -> None:
    api_key = os.getenv("GOOGLE_API_KEY")
    cx = os.getenv("GOOGLE_CX")
    if not api_key or not cx:
        raise EnvironmentError("GOOGLE_API_KEY and GOOGLE_CX environment variables are required")

    profiles = collect_profiles(api_key, cx)
    # CSV is already saved incrementally, so we don't need to save again
    print(f"Results saved to raw_links.csv")


if __name__ == "__main__":
    main()
