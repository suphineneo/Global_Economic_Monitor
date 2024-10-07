import requests
import pandas as pd
from datetime import datetime


def fetch_data_from_api(indicator: str, date_range: str) -> pd.DataFrame:
    """
    Fetch data from the World Bank API.

    Parameters:
        indicator (str): The indicator to fetch data for.
        date_range (str): The date range for the data request.

    Returns:
        pd.DataFrame: The fetched data as a DataFrame.
    """
    base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}?"
    params = {"date": date_range, "format": "json", "page": 1}  # Start at page 1

    all_data = []
    current_year = datetime.now().year  # Get the current year

    while True:
        response = requests.get(base_url, params=params)
        response_data = response.json()

        if len(response_data) < 2 or not response_data[1]:  # Check if there's data
            break

        all_data.extend(response_data[1])  # Add current page data to all_data

        # Check if the extracted data contains the current year
        extracted_years = [item["date"] for item in response_data[1]]
        if current_year in extracted_years:
            print(
                f"Stopping extract because current year {current_year} has been reached."
            )
            break

        # Update parameters for the next page
        params["page"] += 1

    df = pd.json_normalize(data=all_data)

    return df
