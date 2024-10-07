import json
import os

import pandas as pd
import requests
from sqlalchemy import create_engine


def load_config(json_file_path):
    with open(json_file_path, 'r') as file:
        config = json.load(file)
    return config


def download_excel_file(file_url, dest_folder, file_name):
    """Download the Excel file from the URL and save it to the destination folder"""
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    response = requests.get(file_url)

    # Check the content type
    content_type = response.headers.get('Content-Type')
    if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' not in content_type:
        print(f"Unexpected content type: {content_type}. The file might not be an Excel file.")
        return None

    if response.status_code == 200:
        file_path = os.path.join(dest_folder, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully at {file_path}")
        return file_path
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
        return None


def extract_sheet_data(excel_data, sheet_name, skip_rows=1):
    df = pd.read_excel(excel_data, sheet_name=sheet_name, engine='openpyxl', skiprows=1)
    # Replace 'Unnamed: .*' with empty strings and forward fill to propagate non-null values across columns
    df.columns = pd.Series(df.columns).ffill()

    return df


# def process_time_distance_data(excel_file):
#     # Load the Excel file with multiple header rows
#     df = pd.read_excel(excel_file, sheet_name='Provider Time & Distance', header=[0, 1, 2], skiprows=1)
#
#     # Flatten multi-level column names to make them easier to work with
#     # df.columns = ['_'.join(col).strip().replace('Unnamed: ', '').replace('_level_', '') for col in df.columns.values]
#     df.columns =  df.columns.values
#
#     # Prepare a list for holding the final data
#     data_list = []
#
#     # Iterate through the columns to extract specialty code, description, time, and distance
#     for col in df.columns:
#         if 'Time' in col[2] or 'Distance' in col[2]:
#             # Split the column name into relevant parts (description, code, measure)
#             specialty_desc = col[0]  # First element in the tuple
#             specialty_code = col[1]  # Second element in the tuple
#             measure = col[2]  # Third element (either 'Time' or 'Distance')
#         # else:
#         #     countyColumn = col[0]
#         #     specialty_code = col[1]
#         #     measure = col[2]
#             # Add new rows to the list for each measure (Time/Distance)
#             for index, row in df.iterrows():
#                 data_list.append({
#                     'COUNTY': row['COUNTY'],
#                     'ST': row['ST'],
#                     'COUNTY_STATE': row['COUNTY_STATE'],
#                     'SSACD': row['SSACD'],
#                     'COUNTY_DESIGNA-TION': row['COUNTY_DESIGNA-TION'],
#                     'Specialty Description': specialty_desc,
#                     'Specialty Code': specialty_code,
#                     measure: row[col] if pd.notna(row[col]) else None  # Handle missing values
#                 })
#
#     # Convert the list of dicts to a DataFrame
#     processed_df = pd.DataFrame(data_list)
#
#     # Fill missing values or filter rows where both 'Time' and 'Distance' are missing
#     processed_df['Time'].fillna('Missing', inplace=True)
#     processed_df['Distance'].fillna('Missing', inplace=True)
#
#     return processed_df


def extract_county_info(df):
    """
    Extract the county information columns (COUNTY, ST, COUNTY_STATE, etc.) from the DataFrame.
    """
    # Define a dictionary to store the county information column mappings
    county_info_columns = {}

    # Iterate over the multi-index columns
    for col in df.columns:
        if col[0] in ['COUNTY', 'ST', 'COUNTY_STATE', 'SSACD', 'COUNTY DESIGNA-TION']:
            # Map the key (e.g., 'COUNTY') to the full multi-level column name
            county_info_columns[col[0]] = col

    return county_info_columns


# def process_time_distance_data(excel_file):
#     # Load the Excel file with multiple header rows
#     df = pd.read_excel(excel_file, sheet_name='Provider Time & Distance', header=[0, 1, 2], skiprows=1)
#
#     # Convert multi-index columns to a list of tuples for easier manipulation
#     df.columns = df.columns.to_list()
#
#     # Prepare a list for holding the final data∂
#     data_list = []
#
#     county_info_columns = extract_county_info(df)
#
#     # Iterate through the columns to extract specialty code, description, time, and distance
#     for col in df.columns:
#         column = col[0]
#         if 'Time' in col[2] or 'Distance' in col[2]:
#             # Split the column name into relevant parts (description, code, measure)
#             specialty_desc = col[0]  # First element in the tuple (Specialty Description)
#             specialty_code = col[1]  # Second element in the tuple (Specialty Code)
#             measure = col[2]  # Third element (either 'Time' or 'Distance')
#
#             # Add new rows to the list for each measure (Time/Distance)
#             for index, row in df.iterrows():
#                 # row_data = {col_name: row[col_name] for col_name in county_info_columns}
#                 row_data = {col_name: row[county_info_columns[col_name]] for col_name in county_info_columns.keys()}
#                 row_data['specialty_description'] = specialty_desc
#                 row_data['specialty_code'] = specialty_code
#                 row_data[measure] = row[col] if pd.notna(row[col]) else None  # Handle missing values
#
#                 # Append the row data to the final list
#                 data_list.append(row_data)
#
#     # Convert the list of dicts to a DataFrame
#     processed_df = pd.DataFrame(data_list)
#
#     # Optionally, fill missing values for 'Time' and 'Distance' columns
#     if 'Time' in processed_df.columns:
#         processed_df['Time'].fillna('Missing', inplace=True)
#     if 'Distance' in processed_df.columns:
#         processed_df['Distance'].fillna('Missing', inplace=True)
#
#         # Rename columns: convert to lowercase and remove spaces
#     processed_df.columns = processed_df.columns.str.lower().str.replace(" ", "_").str.replace("-", "")
#
#     return processed_df

def process_time_distance_data(excel_file):
    # Load the Excel file with multiple header rows
    df = pd.read_excel(excel_file, sheet_name='Provider Time & Distance', header=[0, 1, 2], skiprows=1)

    # Convert multi-index columns to a list of tuples for easier manipulation
    df.columns = df.columns.to_list()

    # Prepare a list for holding the final data
    data_list = []

    county_info_columns = extract_county_info(df)

    # Iterate through the columns to extract specialty code, description, time, and distance
    for index, row in df.iterrows():
        row_data = {col_name: row[county_info_columns[col_name]] for col_name in county_info_columns.keys()}

        # Now, go through the columns for Time and Distance for this row
        for col in df.columns:
            if 'Time' in col[2]:
                row_data['specialty_description'] = col[0]  # Specialty Description
                row_data['specialty_code'] = col[1]  # Specialty Code
                row_data['time'] = row[col] if pd.notna(row[col]) else None  # Time value
            elif 'Distance' in col[2]:
                row_data['distance'] = row[col] if pd.notna(row[col]) else None  # Distance value

        # Append the complete row with both Time and Distance
        data_list.append(row_data)

    # Convert the list of dicts to a DataFrame
    processed_df = pd.DataFrame(data_list)

    # Optionally, fill missing values for 'Time' and 'Distance' columns
    processed_df['time'].fillna('Missing', inplace=True)
    processed_df['distance'].fillna('Missing', inplace=True)

    # Rename columns: convert to lowercase, replace spaces with underscores, and remove hyphens
    processed_df.rename(columns=lambda x: x.lower().replace(" ", "_").replace("-", ""), inplace=True)

    return processed_df


def save_to_postgres(df, table_name, db_config):
    """Save the DataFrame to PostgreSQL database"""
    try:
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data saved to PostgreSQL table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")


def main():
    # Load the configuration
    config_file_path = 'JsonConfigFile.json'  # Path to your JSON config file
    config_file_path_for_db = 'db_config.json'  # Path to your DB config file
    config = load_config(config_file_path)
    db_config = load_config(config_file_path_for_db)

    # Download the Excel file
    dest_folder = './downloads'
    file_name = config['file_pattern'].replace(' ', '_') + '.xlsx'
    file_path = download_excel_file(config['url'], dest_folder, file_name)

    if file_path:
        print("Proceed with further data processing...")

        # Load the Excel file
        excel_data = pd.ExcelFile(file_path, engine="openpyxl")

        # Process the time-distance sheet
        time_distance_data = extract_sheet_data(excel_data, config['sheets']['provider_time_distance'])
        time_distance_data = process_time_distance_data(file_path)

        # Save the result to PostgreSQL
        save_to_postgres(time_distance_data, "time_distance_data", db_config)


if __name__ == "__main__":
    main()
