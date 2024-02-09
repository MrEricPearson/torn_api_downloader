import requests
import time
import sys
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError

# MongoDB setup
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['torn_data']
faction_attacks_collection = db['faction_attacks']

# User Inputs
api_key = input("Please enter your API key: ")
faction_id = input("Please enter your faction ID: ")
start_date_input = input("Enter a start date in timestamp format (leave blank to start from oldest possible records): ")

# Base URL setup
base_url = f"https://api.torn.com/faction/{faction_id}?selections=attacks&key={api_key}"
fallback_timestamp = 1675467539  # Fallback starting timestamp, used if no start date is provided
cooldown_period = 60  # Cooldown period in seconds

# Function to convert UNIX timestamp to a human-readable format
def format_timestamp(unix_timestamp):
    return datetime.fromtimestamp(int(unix_timestamp), timezone.utc).strftime('%B %d, %Y @ %I:%M:%S %p')

# Function to fetch data
def fetch_data(url, params=None):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json().get('attacks', {})
    else:
        print(f"Failed to fetch data: HTTP {response.status_code}")
        return {}

# Function to insert data into MongoDB
def insert_faction_attacks_data(data):
    saved = 0
    duplicates = 0
    for record in data:
        query = {
            'timestamp_started': record['timestamp_started'],
            'attacker_id': record['attacker_id'],
            'defender_id': record['defender_id']
        }
        existing_record = faction_attacks_collection.find_one(query)
        if existing_record is None:
            record['_id'] = f"{record['timestamp_started']}_{record['attacker_id']}_{record['defender_id']}"
            faction_attacks_collection.insert_one(record)
            saved += 1
        else:
            duplicates += 1
    return duplicates, saved

# Determine the newest timestamp from the Torn API (Exploratory call)
def get_newest_timestamp_from_api():
    response = fetch_data(base_url)
    if response:
        attacks = list(response.values())
        if attacks:
            return max(attacks, key=lambda x: x['timestamp_started'])['timestamp_started']
    return None

# Function to get the start timestamp from the user input or use fallback
def get_start_timestamp():
    if start_date_input:
        return int(start_date_input)
    else:
        return fallback_timestamp

newest_timestamp_in_api = get_newest_timestamp_from_api()
next_fetch_timestamp = get_start_timestamp()
print(f"Newest timestamp from API: {newest_timestamp_in_api}, starting fetch from timestamp: {next_fetch_timestamp}")

# Include your main fetch loop, cooldown management, and final summary logic here


# Function to remove duplicates
def remove_duplicates():
    pipeline = [
        {"$group": {
            "_id": {"timestamp_started": "$timestamp_started", "attacker_id": "$attacker_id", "defender_id": "$defender_id"},
            "docIds": {"$push": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = list(faction_attacks_collection.aggregate(pipeline))
    removal_count = 0

    for duplicate in duplicates:
        # Skip the first document and remove the rest
        for doc_id in duplicate['docIds'][1:]:
            faction_attacks_collection.delete_one({"_id": doc_id})
            removal_count += 1

    return removal_count

# Function to fetch data with retry mechanism
def fetch_data_with_retry(url, params=None, max_retries=3, cooldown=60):
    retry_count = 0
    while retry_count < max_retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('attacks', {})
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}. Retrying...")
            retry_count += 1
            if retry_count < max_retries:
                print(f"Initiating cooldown for {cooldown} seconds before retry #{retry_count + 1}...")
                time.sleep(cooldown)

    print("Max retries exceeded. No more data to fetch or an error occurred.")
    return None

# Main fetch loop modified to use fetch_data_with_retry and include a countdown timer for the rate limiting cooldown
total_requests_made = 1  # Account for the initial exploratory request
requests_since_last_cooldown = 1  # Track requests since the last cooldown
total_new_records = 0
total_duplicate_records = 0
earliest_timestamp = None
latest_timestamp = None

while next_fetch_timestamp <= newest_timestamp_in_api:
    if requests_since_last_cooldown >= 100:
        print("Initiating cooldown for API rate limiting...")
        for remaining in range(cooldown_period, 0, -1):
            sys.stdout.write(f"\rCooldown: {remaining} seconds remaining.")
            sys.stdout.flush()
            time.sleep(1)
        sys.stdout.write("\rCooldown complete. Resuming...\n\n")
        requests_since_last_cooldown = 1  # Reset after cooldown

    data = fetch_data_with_retry(f"{base_url}&from={next_fetch_timestamp}")
    if data:
        fetched_timestamps = [int(record['timestamp_started']) for record in data.values()]
        if fetched_timestamps:
            min_fetched_timestamp = min(fetched_timestamps)
            max_fetched_timestamp = max(fetched_timestamps)
            
            earliest_timestamp = min_fetched_timestamp if earliest_timestamp is None else min(earliest_timestamp, min_fetched_timestamp)
            latest_timestamp = max_fetched_timestamp if latest_timestamp is None else max(latest_timestamp, max_fetched_timestamp)

            # Update next_fetch_timestamp for the next iteration
            next_fetch_timestamp = max_fetched_timestamp + 1

        duplicates, saved = insert_faction_attacks_data(data.values())
        total_new_records += saved
        total_duplicate_records += duplicates

        print(f"API Request #{total_requests_made}: Fetched {len(data)} records. New: {saved}, Duplicates: {duplicates}")

        total_requests_made += 1  # Increment after each successful fetch
        requests_since_last_cooldown += 1  # Increment after each successful fetch

    else:
        break  # Exit the loop if data is None, indicating max retries were exceeded or no more data to fetch

# Remove duplicates after all requests are made
removed_duplicates = remove_duplicates()

# Convert timestamps to human-readable format for the final summary
formatted_earliest_timestamp = format_timestamp(earliest_timestamp) if earliest_timestamp else "Not Available"
formatted_latest_timestamp = format_timestamp(latest_timestamp) if latest_timestamp else "Not Available"

print(f"\nFinal Summary of Requests from {formatted_earliest_timestamp} to {formatted_latest_timestamp}:")
print(f"Total API Requests Made: {total_requests_made}")
print(f"Total New Records Saved: {total_new_records}")
print(f"Total Duplicate Records Encountered: {total_duplicate_records}")