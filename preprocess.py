"""
removes invalid characters from the json file
"""
import json

from tqdm import tqdm

input_path = "twitter.json"
output_path = "twitter_new.json"


def clean_invalid_chars(data):
    if isinstance(data, str):
        return data.replace("\u0000", " ")
    elif isinstance(data, list):
        return [clean_invalid_chars(item) for item in data]
    elif isinstance(data, dict):
        return {k: clean_invalid_chars(v) for k, v in data.items()}
    else:
        return data


with open(input_path, "r", encoding="utf-8") as infile, open(
    output_path, "w", encoding="utf-8"
) as outfile:
    for line in tqdm(infile):
        # Parse each line as JSON, clean it, and write it back out
        try:
            json_data = json.loads(line)
            cleaned_data = clean_invalid_chars(json_data)
            json.dump(cleaned_data, outfile)
            outfile.write("\n")
        except json.JSONDecodeError:
            print("Error processing line:", line)
