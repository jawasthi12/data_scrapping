import os
import json
import requests


def load_config(json_file_path):
    with open(json_file_path, 'r') as file:
        config = json.load(file)
    return config


def download_excel_file(file_url, dest_folder, file_name):
    """ Download the Excel file from the URL and save it to the destination folder """
    # Ensure destination folder exists
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    response = requests.get(file_url)
    if response.status_code == 200:
        file_path = os.path.join(dest_folder, file_name)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully at {file_path}")
        return file_path
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
        return None


def main():
    # Load the configuration
    config_file_path = 'JsonConfigFile.json'  # Path to your JSON config file
    config = load_config(config_file_path)

    # Extract the necessary configuration details
    url = config['url']
    file_pattern = config['file_pattern']
    dest_folder = './downloads'  # You can modify this or take it from the config as well
    file_name = file_pattern.replace(' ', '_') + '.xlsx'  # Modify this as needed

    # Download the file using the URL and save it to the destination folder
    file_path = download_excel_file(url, dest_folder, file_name)

    if file_path:
        print("Proceed with further data processing...")
        # Here you can write the next steps like processing the Excel sheets using pandas.


if __name__ == "__main__":
    main()
