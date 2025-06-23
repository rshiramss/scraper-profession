import os
import csv
import time
from typing import Dict, List, Set

import requests

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

API_URL = "https://api.search.brave.com/res/v1/web/search"
QUERY_TEMPLATE = 'site:linkedin.com/in "Santa Clara University" "{}"'
RESULTS_PER_PAGE = 20  # Brave API supports up to 20 results per page
TARGET_RESULTS_PER_PROFESSION = 40


def brave_search(query: str, offset: int, api_key: str) -> Dict:
    """Perform a single Brave Search API request."""
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": api_key,
    }
    params = {
        "q": query,
        "source": "api",
        "count": RESULTS_PER_PAGE,
        "offset": offset,
    }
    response = requests.get(API_URL, headers=headers, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def parse_result(item: Dict) -> Dict:
    """Extract relevant information from a Brave search result item."""
    url = item.get("url", "")
    if "/in/" not in url:
        return {}

    # Attempt to extract a human readable name from the title or description
    title = item.get("title", "")
    snippet = item.get("description", "")
    name = title.split("-")[0].strip() if title else snippet.split("-")[0].strip()

    return {"name": name, "linkedin_url": url}


def collect_profiles(api_key: str) -> List[Dict[str, str]]:
    profiles: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()
    seen_names: Set[str] = set()

    for profession, keywords in PROFESSIONS.items():
        count_for_profession = 0
        keyword_index = 0

        # Continue until we have enough profiles for this profession
        while count_for_profession < TARGET_RESULTS_PER_PROFESSION:
            keyword = keywords[keyword_index % len(keywords)]
            keyword_index += 1
            query = QUERY_TEMPLATE.format(keyword)
            offset = 0

            while count_for_profession < TARGET_RESULTS_PER_PROFESSION:
                data = brave_search(query, offset=offset, api_key=api_key)
                results = data.get("web", {}).get("results", [])
                if not results:
                    break

                for item in results:
                    parsed = parse_result(item)
                    if not parsed:
                        continue

                    url = parsed["linkedin_url"]
                    name = parsed["name"]

                    if url in seen_urls or name.lower() in seen_names:
                        continue

                    profiles.append(
                        {
                            "name": name,
                            "linkedin_url": url,
                            "search_keyword": keyword,
                            "profession": profession,
                        }
                    )

                    seen_urls.add(url)
                    seen_names.add(name.lower())
                    count_for_profession += 1

                    if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                        break

                offset += RESULTS_PER_PAGE
                time.sleep(1)  # avoid hitting rate limits

    return profiles


def save_csv(records: List[Dict[str, str]], filename: str) -> None:
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["name", "linkedin_url", "search_keyword", "profession"]
        )
        writer.writeheader()
        writer.writerows(records)


def main() -> None:
    api_key = os.getenv("BRAVE_API_KEY")
    if not api_key:
        raise EnvironmentError("BRAVE_API_KEY environment variable is required")

    profiles = collect_profiles(api_key)
    save_csv(profiles, "raw_links.csv")


if __name__ == "__main__":
    main()
