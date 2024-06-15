import pandas as pd
import os

def convert_parquet_to_json(parquet_file_path, json_file_path):
    """
    Convert a Parquet file to a properly formatted JSON file.

    :param parquet_file_path: Path to the input Parquet file.
    :param json_file_path: Path to the output JSON file.
    """
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file_path)
    
    # Convert the DataFrame to a JSON string with a proper array format
    json_str = df.to_json(orient='records', lines=True)
    json_str = '[\n' + json_str.replace('\n', ',\n')[:-2] + '\n]'
    
    # Write the JSON string to a file
    with open(json_file_path, 'w') as json_file:
        json_file.write(json_str)

def batch_convert_parquet_to_json(parquet_files, json_directory):
    """
    Convert multiple Parquet files to JSON files.

    :param parquet_files: List of Parquet file paths.
    :param json_directory: Path to the directory where JSON files will be saved.
    """
    # Ensure the JSON directory exists
    os.makedirs(json_directory, exist_ok=True)
    
    # Iterate over the list of Parquet files
    for parquet_file_path in parquet_files:
        # Extract the base name of the file (e.g., data1(Eng).parquet)
        base_name = os.path.basename(parquet_file_path)
        
        # Replace the .parquet extension with .json
        json_file_name = base_name.replace('.parquet', '.json')
        json_file_path = os.path.join(json_directory, json_file_name)
        
        # Convert the Parquet file to JSON
        convert_parquet_to_json(parquet_file_path, json_file_path)
        print(f"Converted {parquet_file_path} to {json_file_path}")

# Example usage
if __name__ == "__main__":
    parquet_files = [
        r"C:\Users\frank\Desktop\parquet_to_json\data1(Eng).parquet",
        r"C:\Users\frank\Desktop\parquet_to_json\data2(Eng).parquet",
        r"C:\Users\frank\Desktop\parquet_to_json\data3(Eng).parquet",
        r"C:\Users\frank\Desktop\parquet_to_json\data4(Eng).parquet",
        r"C:\Users\frank\Desktop\parquet_to_json\data5(Eng).parquet"
    ]
    json_directory = r"C:\Users\frank\Desktop\parquet_to_json\json_output"
    
    batch_convert_parquet_to_json(parquet_files, json_directory)
