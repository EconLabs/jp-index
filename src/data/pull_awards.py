import time

import requests

URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
HEADERS = {"Content-Type": "application/json"}


def build_payload(start_date: str, end_date: str) -> dict:
    """
    Build the API request payload for a given date range.

    Args:
        start_date (str): Start date in format YYYY-MM-DD.
        end_date (str): End date in format YYYY-MM-DD.

    Returns:
        dict: Payload with filters for the API request.
    """
    return {
        "subawards": False,
        "limit": 100,
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [{"start_date": start_date, "end_date": end_date}],
            "place_of_performance_locations": [{"country": "USA", "state": "PR"}],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Start Date",
            "End Date",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Funding Agency",
            "Funding Sub Agency",
            "Award Type",
        ],
    }


def make_request(payload: dict, retries: int = 5) -> dict:
    """
    Make a POST request to the API with exponential backoff in case of failures.

    Args:
        payload (dict): JSON payload with the filters and fields.
        retries (int): Number of retry attempts before giving up.

    Returns:
        dict: JSON response from the API, or None if the request failed.
    """
    for attempt in range(retries):
        try:
            response = requests.post(URL, json=payload, headers=HEADERS, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            wait_time = 2**attempt
            print(f"Error: {error}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("Error: Request failed after multiple retries.")
    return None


def get_data_for_month(start_date: str, end_date: str, max_pages: int = 1) -> list:
    """
    Retrieve all award data for a specific month.

    Args:
        start_date (str): Start date in format YYYY-MM-DD.
        end_date (str): End date in format YYYY-MM-DD.
        max_pages (int): Maximum number of pages to retrieve.

    Returns:
        list: A list of awards retrieved from the API.
    """
    all_data = []
    for page in range(1, max_pages + 1):
        print(f"Downloading page {page} for range {start_date} - {end_date}...")
        payload = build_payload(start_date, end_date)
        payload["page"] = page

        response = make_request(payload)
        if response is None:
            print(f"Error: No data received for range {start_date} - {end_date}.")
            break

        data = response.get("results", [])
        if not data:
            print(f"No more data available for range {start_date} - {end_date}.")
            break

        all_data.extend(data)

    return all_data
