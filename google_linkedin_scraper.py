import os
import csv
import time
from typing import Dict, List, Set
import datetime

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

PROFESSION_TO_COMPANIES: Dict[str, List[str]] = {
    "Software Engineer": ["Google", "Microsoft", "Amazon", "Apple", "Meta", "Netflix", "Tesla", "NVIDIA", "Intel", "Oracle"],
    "Data Scientist": ["Google DeepMind", "OpenAI", "Netflix", "Tesla", "Airbnb", "Stripe"],
    "Product Manager": ["Google", "Amazon", "Microsoft", "Salesforce", "Atlassian", "Shopify"],
    "UX/UI Designer": ["IDEO", "Apple", "Airbnb", "Figma", "Adobe", "Google"],
    "Mechanical Engineer": ["SpaceX", "Tesla", "Boeing", "GE Aerospace", "Lockheed Martin", "Boston Dynamics"],
    "Electrical Engineer": ["Intel", "NVIDIA", "Qualcomm", "Texas Instruments", "AMD", "Apple"],
    "Investment Analyst": ["Goldman Sachs", "Morgan Stanley", "BlackRock", "Fidelity Investments", "Bridgewater Associates", "KKR"],
    "Consultant": ["McKinsey & Company", "Boston Consulting Group", "Bain & Company", "Deloitte Consulting", "Accenture Strategy", "Oliver Wyman"],
    "Lawyer": ["Skadden", "Cravath", "Sullivan & Cromwell", "Kirkland & Ellis", "Latham & Watkins", "Wachtell"],
    "Physician / Med": ["Mayo Clinic", "Cleveland Clinic", "Johns Hopkins Hospital", "Mass General", "Stanford Health Care", "UCLA Medical Center"], # Changed key to match PROFESSIONS
    "Research Scientist": ["NASA JPL", "CERN", "Broad Institute", "IBM Research", "Max Planck Society", "Lawrence Berkeley Lab"],
    "Educator": ["MIT", "Stanford", "Harvard", "Oxford", "Cambridge", "UC Berkeley"],
    "Journalist": ["New York Times", "Washington Post", "WSJ", "BBC", "Reuters", "Guardian"],
    "Marketing / PR": ["Procter & Gamble", "Nike", "Unilever", "Coca-Cola", "Ogilvy", "Edelman"],
    "Designer / Creator": ["Pentagram", "IDEO", "Pixar", "Apple Design Studio", "Walt Disney Imagineering", "Nike Design"]
}

API_URL = "https://www.googleapis.com/customsearch/v1"
QUERY_TEMPLATE = 'site:linkedin.com/in "Santa Clara University" {}'
RESULTS_PER_PAGE = 10  # Google Custom Search returns up to 10 results per page
MAX_OFFSET = 9
TARGET_RESULTS_PER_PROFESSION = 40
TARGET_RESULTS_PER_COMPANY = 10 # Optional: Max results per company per profession


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


def collect_profiles(api_key: str, cx: str) -> tuple[list[dict[str, str]], str]:
    """Collect profile data and write it to a timestamped CSV file.

    Returns a tuple of the collected profiles and the CSV filename used.
    """

    profiles: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()
    # seen_names: Set[str] = set() # Add if name uniqueness is strictly required

    # Create/clear the CSV file at the start
    csv_filename = f"raw_links_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    save_csv([], csv_filename)  # This creates the file with just the header
    print(f"Created CSV file: {csv_filename}")

    for profession, keywords in PROFESSIONS.items():
        print(f"\nProcessing profession: {profession}")
        count_for_profession = 0

        # Phase 1: Generic Keyword Search
        print(f"--- Phase 1: Generic Keyword Search for {profession} ---")
        keyword_index_phase1 = 0
        # Continue until we have enough profiles for this profession or run out of keyword/offset combinations
        # Need to ensure keyword_index_phase1 doesn't go into an infinite loop if MAX_OFFSET is always hit before TARGET_RESULTS
        # A better way might be to iterate offsets for each keyword explicitly.

        # Iterate through each keyword once for Phase 1
        for keyword_p1 in keywords:
            if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                break # Already have enough for this profession

            print(f"Searching with keyword: {keyword_p1}")
            query_p1 = QUERY_TEMPLATE.format(f'"{keyword_p1}"') # Ensure keyword is quoted if it contains spaces
            offset_p1 = 0
            while count_for_profession < TARGET_RESULTS_PER_PROFESSION and offset_p1 <= MAX_OFFSET:
                data_p1 = google_search(query_p1, offset=offset_p1, api_key=api_key, cx=cx)
                results_p1 = data_p1.get("items", [])
                if not results_p1:
                    print(f"No more results for keyword '{keyword_p1}' at offset {offset_p1}.")
                    break # No more results for this keyword at this offset

                for item_p1 in results_p1:
                    parsed_p1 = parse_result(item_p1)
                    if not parsed_p1:
                        continue

                    url_p1 = parsed_p1["linkedin_url"]
                    name_p1 = parsed_p1["name"]

                    if url_p1 in seen_urls: # Global check
                        continue
                    # if name_p1 in seen_names: # Optional: if name uniqueness is required
                    #     continue

                    profile_record_p1 = {
                        "name": name_p1,
                        "linkedin_url": url_p1,
                        "search_keyword": keyword_p1, # Store the specific keyword
                        "profession": profession,
                    }
                    
                    profiles.append(profile_record_p1)
                    append_to_csv(profile_record_p1, csv_filename)
                    print(f"Added (Phase 1): {name_p1} ({profession}) - Keyword: {keyword_p1}")

                    seen_urls.add(url_p1)
                    # seen_names.add(name_p1)
                    count_for_profession += 1

                    if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                        break

                if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                    break
                offset_p1 += 1
                time.sleep(2)
            if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                print(f"Target for {profession} reached during Phase 1.")
                break # Break from keyword loop for Phase 1

        # Phase 2: Targeted Company Search
        if count_for_profession < TARGET_RESULTS_PER_PROFESSION:
            print(f"--- Phase 2: Targeted Company Search for {profession} ---")
            target_companies = PROFESSION_TO_COMPANIES.get(profession, [])
            if not target_companies:
                print(f"No target companies defined for {profession}. Skipping Phase 2.")
            else:
                results_by_company_for_profession: Dict[str, int] = {comp: 0 for comp in target_companies}

                for company in target_companies:
                    if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                        break # Overall target for profession met

                    print(f"Targeting company: {company} for {profession}")

                    for keyword_p2 in keywords: # Iterate through the same keywords
                        if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                            break
                        if results_by_company_for_profession.get(company, 0) >= TARGET_RESULTS_PER_COMPANY:
                            print(f"Target for company '{company}' in '{profession}' reached.")
                            break # Target for this specific company met

                        # Ensure company name is quoted if it contains spaces, keyword too
                        query_p2 = QUERY_TEMPLATE.format(f'"{keyword_p2}" "{company}"')
                        offset_p2 = 0

                        while count_for_profession < TARGET_RESULTS_PER_PROFESSION and \
                              results_by_company_for_profession.get(company, 0) < TARGET_RESULTS_PER_COMPANY and \
                              offset_p2 <= MAX_OFFSET:

                            data_p2 = google_search(query_p2, offset=offset_p2, api_key=api_key, cx=cx)
                            results_p2 = data_p2.get("items", [])
                            if not results_p2:
                                print(f"No more results for keyword '{keyword_p2}', company '{company}' at offset {offset_p2}.")
                                break

                            for item_p2 in results_p2:
                                parsed_p2 = parse_result(item_p2)
                                if not parsed_p2:
                                    continue

                                url_p2 = parsed_p2["linkedin_url"]
                                name_p2 = parsed_p2["name"]

                                if url_p2 in seen_urls: # Global check
                                    continue
                                # if name_p2 in seen_names:
                                #    continue

                                profile_record_p2 = {
                                    "name": name_p2,
                                    "linkedin_url": url_p2,
                                    "search_keyword": f"{keyword_p2} @ {company}", # Indicate company search
                                    "profession": profession,
                                }

                                profiles.append(profile_record_p2)
                                append_to_csv(profile_record_p2, csv_filename)
                                print(f"Added (Phase 2): {name_p2} ({profession}) - Keyword: {keyword_p2}, Company: {company}")

                                seen_urls.add(url_p2)
                                # seen_names.add(name_p2)
                                count_for_profession += 1
                                results_by_company_for_profession[company] = results_by_company_for_profession.get(company, 0) + 1

                                if count_for_profession >= TARGET_RESULTS_PER_PROFESSION or \
                                   results_by_company_for_profession.get(company, 0) >= TARGET_RESULTS_PER_COMPANY:
                                    break

                            if count_for_profession >= TARGET_RESULTS_PER_PROFESSION or \
                               results_by_company_for_profession.get(company, 0) >= TARGET_RESULTS_PER_COMPANY:
                                break
                            offset_p2 += 1
                            time.sleep(2)

                        if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                            break # Break from keyword loop for Phase 2

                    if count_for_profession >= TARGET_RESULTS_PER_PROFESSION:
                        print(f"Target for {profession} reached during Phase 2 company searches.")
                        break # Break from company loop for Phase 2

    print(f"\nCompleted! Total profiles collected: {len(profiles)}")
    return profiles, csv_filename


def main() -> None:
    api_key = os.getenv("GOOGLE_API_KEY")
    cx = os.getenv("GOOGLE_CX")
    if not api_key or not cx:
        raise EnvironmentError("GOOGLE_API_KEY and GOOGLE_CX environment variables are required")

    profiles, csv_filename = collect_profiles(api_key, cx)
    # CSV is already saved incrementally, so we just print the filename
    print(f"Results saved to {csv_filename}")


if __name__ == "__main__":
    main()
