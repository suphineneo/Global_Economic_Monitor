import pandas as pd
import numpy as np
import os
import requests
import openpyxl
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter, Retry

class WorldBankDataLoader:
    def __init__(self, indicator, start_year, end_year):
        """
        Initialize the WorldBankDataLoader with the indicator, start year, and end year.
        
        Args:
        - indicator (str): The indicator code for the World Bank API (e.g., 'NV.IND.TOTL.KD.ZG').
        - start_year (str): The starting year for the data.
        - end_year (str): The ending year for the data.
        """
        self.indicator = indicator
        self.date_range = f"{start_year}:{end_year}"
        self.base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{self.indicator}?"
        self.params = {
            "date": self.date_range,
            "format": "json",
            "page": 1  # Start at page 1
        }
        self.all_data = []  # To store paginated data

    def fetch_data(self):
        """
        Fetch data from the World Bank API, handling pagination until all data is retrieved.
        
        Returns:
        - pd.DataFrame: A pandas DataFrame containing all the data fetched from the API.
        """
        while True:
            response = requests.get(self.base_url, params=self.params)
            response_data = response.json()

            # Check if valid data is returned
            if len(response_data) < 2 or not response_data[1]:
                break

            # Extend the all_data list with the current page's data
            self.all_data.extend(response_data[1])

            # Increment page number for the next API call
            self.params["page"] += 1

        # Normalize and return the data as a pandas DataFrame
        return pd.json_normalize(data=self.all_data)

# Example usage
# if __name__ == "__main__":
#     # Create an instance of the loader for the specific indicator and date range
#     loader = WorldBankDataLoader(indicator="NV.IND.TOTL.KD.ZG", start_year="1990", end_year="2024")
    
#     # Fetch the data
#     df_data = loader.fetch_data()