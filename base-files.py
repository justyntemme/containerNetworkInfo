import requests
from bs4 import BeautifulSoup
import re
import csv
from datetime import datetime

def parse_api_date(date_str):
    """
    Parses date strings from the Launchpad API, which are in ISO 8601 format.
    Returns a date object or None.
    """
    if not date_str:
        return None
    try:
        # Example format: '2027-04-30T23:59:59+00:00'
        return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
    except (ValueError, TypeError):
        return None

def fetch_ubuntu_release_info_from_api():
    """
    Fetches a mapping of Ubuntu code names to their version, status, and EOL
    dates directly from the Launchpad API.
    """
    series_url = "https://api.launchpad.net/devel/ubuntu/series"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    release_info_map = {}
    
    print(f"\nFetching Ubuntu release info from Launchpad API...")
    
    while series_url:
        try:
            response = requests.get(series_url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for entry in data['entries']:
                try:
                    # Each entry has a link to its own detailed API page
                    detail_response = requests.get(entry['self_link'], headers=headers, timeout=10)
                    detail_response.raise_for_status()
                    detail_data = detail_response.json()

                    # The API returns lowercase names ('jammy'), but Launchpad HTML is capitalized ('Jammy')
                    code_name = detail_data.get("name", "").capitalize()
                    version = detail_data.get("version", "N/A")
                    api_status = detail_data.get("status")

                    # 'standard_support_end_date' is the end of mainstream support
                    support_eol = parse_api_date(detail_data.get("standard_support_end_date"))
                    # 'support_end_date' is the final EOL, including ESM
                    final_eol = parse_api_date(detail_data.get("support_end_date"))
                    
                    if code_name:
                        release_info_map[code_name] = {
                            "version": version,
                            "api_status": api_status,
                            "support_eol": support_eol,
                            "final_eol": final_eol
                        }
                except requests.exceptions.RequestException as e:
                    print(f"Warning: Could not fetch details for {entry.get('name')}. {e}")
                    continue # Try the next entry
            
            # Follow the API's pagination to get all results
            series_url = data.get('next_collection_link', None)
            
        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not fetch release list from Launchpad API. {e}")
            return release_info_map # Return what we have
        except ValueError: # Catches JSON decoding errors
            print("Warning: Could not decode JSON from Launchpad API response.")
            return release_info_map

    print("Successfully built Ubuntu release info map from API.")
    return release_info_map

def determine_status(release_info):
    """
    Determines the support status of a release based on its API status
    and EOL dates.
    """
    current_date = datetime.now().date()
    api_status = release_info.get("api_status")
    support_eol = release_info.get("support_eol")
    final_eol = release_info.get("final_eol")

    # Use the direct API status for clear-cut cases.
    if api_status == "Obsolete":
        return "EOL"
    if api_status in ("Future", "Development"):
        return api_status

    # For a 'Supported' status from the API, we differentiate between
    # standard support and ESM using the dates.
    if api_status == "Supported":
        # If dates are missing for a "Supported" release, we can't be more
        # specific, but we know it's not EOL.
        if not support_eol or not final_eol:
            return "Supported"  # Best guess based on API status

        if current_date < support_eol:
            return "Supported"
        elif current_date < final_eol:
            return "ESM"
        else:
            # If current date is past final_eol, it's EOL, even if the API
            # status hasn't been updated to "Obsolete" yet.
            return "EOL"

    # Fallback for any other case or if API status is missing.
    return "Unknown"

def scrape_ubuntu_base_files():
    """
    Scrapes all pages of the Ubuntu base-files publishing history on Launchpad
    and combines it with version data to create a comprehensive list.
    """
    # Use the new API-based function to get all release info
    release_info_map = fetch_ubuntu_release_info_from_api()

    current_url = "https://launchpad.net/ubuntu/+source/base-files/+publishinghistory"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    releases = []
    
    while current_url:
        print(f"Fetching data from {current_url}...")
        try:
            response = requests.get(current_url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error: Could not retrieve the webpage. {e}")
            break

        soup = BeautifulSoup(response.content, "html.parser")
        history_table = soup.find("table", id="publishing-summary")

        if not history_table or not history_table.tbody:
            print("Warning: Could not find the publishing history table on this page.")
            break

        rows = history_table.tbody.find_all("tr")
        for row in rows:
            if not row.has_attr('id'):
                cells = row.find_all("td")
                if len(cells) == 8:
                    try:
                        code_name = cells[3].a.text.strip()
                        package_version = cells[7].a.text.strip()
                        
                        info = release_info_map.get(code_name, {})
                        release_version = info.get("version", "N/A")
                        status = determine_status(info)
                        
                        releases.append({
                            "package_version": package_version,
                            "code_name": code_name,
                            "release_version": release_version,
                            "status": status
                        })
                    except (AttributeError, IndexError):
                        continue
        
        next_link = soup.select_one('a.next[rel="next"]')
        current_url = next_link['href'] if next_link and next_link.has_attr('href') else None
    
    unique_releases = []
    seen = set()
    for release in releases:
        identifier = (release['package_version'], release['code_name'])
        if identifier not in seen:
            unique_releases.append(release)
            seen.add(identifier)
            
    return unique_releases

def print_release_table(releases):
    """
    Prints the extracted release data in a formatted table.
    """
    if not releases:
        print("No release data to display.")
        return
        
    print("\n" + "-"*95)
    print(f"{'Package Version':<25} | {'Code Name':<20} | {'Version':<15} | {'Status'}")
    print("-" * 95)
    
    for release in releases:
        print(
            f"{release['package_version']:<25} | "
            f"{release['code_name']:<20} | "
            f"{release['release_version']:<15} | "
            f"{release['status']}"
        )
    print("-" * 95)
    print(f"Found {len(releases)} unique package/release combinations.")

def save_to_csv(releases, filename="ubuntu_base_files.csv"):
    """
    Saves the release data to a CSV file.
    """
    if not releases:
        print("\nNo data to save to CSV.")
        return

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as output_file:
            # Manually define field order for clarity in the CSV
            fieldnames = ["package_version", "code_name", "release_version", "status"]
            dict_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            dict_writer.writeheader()
            dict_writer.writerows(releases)
        print(f"\nSuccessfully saved data to {filename}")
    except IOError as e:
        print(f"\nError: Could not write to file {filename}. {e}")
    except KeyError:
         print(f"\nError: A data row was missing an expected key. Cannot write to CSV.")


if __name__ == "__main__":
    release_data = scrape_ubuntu_base_files()
    if release_data:
        print_release_table(release_data)
        save_to_csv(release_data)

