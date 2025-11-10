# Import libraries
import pandas as pd
import requests
import json
import os

def extract_data(api_url: str, countries_csv_path: str, json_savepath: str):
    """Extracts JSON data from an API and loads a CSV file."""
    response = requests.get(api_url)
    data = response.json()
    
    # Save raw JSON data
    with open(json_savepath, 'w') as json_file:
        json.dump(data, json_file)
    
    csv_df = pd.read_csv(countries_csv_path)
    return data, csv_df


def transform_data(data: dict, csv_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms JSON and CSV data into a unified, cleaned DataFrame."""
    # Normalize JSON data
    json_df = pd.json_normalize(data[1])

    # Rename columns
    json_df.rename(columns={
        'iso2Code': 'iso2',
        'name': 'country',
        'capitalCity': 'capital',
        'region.value': 'region_value',
        'incomeLevel.value': 'income_level',
        'lendingType.value': 'lending_type'
    }, inplace=True)
        
    # Merge with CSV data on 'iso2'
    merged_df = pd.merge(json_df, csv_df, on='iso2', how='inner')

    # Rename columns from merge
    merged_df.rename(columns={
        'country_x': 'country',
        'capital_x': 'capital'
    }, inplace=True)

    # Select relevant columns
    merged_df = merged_df[[
        'id', 'iso2', 'country', 'capital', 'region_value', 'continents',
        'longitude', 'latitude', 'income_level', 'lending_type', 'area', 'population'
    ]]
    
    return merged_df


def load_data(merge_df: pd.DataFrame, save_path: str):
    """Loads the merged DataFrame to a CSV file, replacing it if it already exists."""
    if os.path.exists(save_path):
        print(f"'{save_path}' already exists. Replacing the file...")
    merge_df.to_csv(save_path, index=False, mode='w')
    print(f"File saved successfully as '{save_path}'.")


# Main ETL pipeline
if __name__ == "__main__":
    api_url = "https://api.worldbank.org/v2/country/?format=json"
    countries_csv_path = 'all_countries.csv'
    json_savepath = 'raw_countries.json'
    save_path = 'merged_countries_data.csv'
    
    # Extract
    data, csv_df = extract_data(api_url, countries_csv_path, json_savepath)
    
    # Transform
    merged_df = transform_data(data, csv_df)
    
    # Load
    load_data(merged_df, save_path)





