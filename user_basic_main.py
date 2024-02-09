# user_basic_main.py
import requests
import time
from datetime import datetime, timezone
from pymongo import MongoClient

# MongoDB setup
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['torn_data']
user_basic_collection = db['user_basic']
user_statuses_collection = db['user_statuses']

def make_api_call(api_key, user_id=None, max_retries=3):
    """
    Makes an API call to the Torn API.
    Handles retries and rate limits.
    """
    print("Preparing to make API call.")
    url = f"https://api.torn.com/user/{user_id}/?selections=basic&key={api_key}" if user_id else f"https://api.torn.com/user/?selections=basic&key={api_key}"
    print("API URL:", url)

    retries = 0
    backoff = 1

    while retries < max_retries:
        try:
            print(f"Attempt {retries + 1}/{max_retries}.")
            response = requests.get(url)
            print("Received response from API.")
            response.raise_for_status()
            data = response.json()

            if response.status_code == 429:
                wait_time = int(response.headers.get('Retry-After', 60))
                print(f"Rate limit exceeded. Retrying in {wait_time} seconds.")
                time.sleep(wait_time)
                continue

            print("Inserting user status.")
            status_id = insert_user_status(data, user_statuses_collection)
            print("Updating user data.")
            update_user_data(data, status_id, user_basic_collection)

            print("Data processed successfully.")
            return data
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during API call: {e}")
            time.sleep(backoff)
            backoff *= 2
        finally:
            retries += 1
            if retries == max_retries:
                print("Max retries reached. Exiting API call.")

    return None

def update_user_data(data, status_id, user_basic_collection):
    print("Checking for existing user data.")
    timestamp = datetime.now(timezone.utc)
    formatted_timestamp = timestamp.strftime('%B %d %Y %I:%M:%S %p')
    status_count = user_statuses_collection.count_documents({'player_id': data['player_id']})

    user_profile_data = {
        'player_id': data['player_id'],
        'latest_status': status_id,
        'latest_name': data['name'],
        'latest_level': data['level'],
        'last_status_update': formatted_timestamp,
        'status_inventory': status_count
    }
    user_basic_collection.update_one({'player_id': data['player_id']}, {'$set': user_profile_data}, upsert=True)
    print("User data updated or inserted. Last status update:", formatted_timestamp, ", Status count:", status_count)

def insert_user_status(data, user_statuses_collection):
    print("Preparing to insert new user status.")
    user_status = {
        "player_id": data["player_id"],
        "status": data["status"],
        "timestamp": datetime.now(timezone.utc)
    }
    result = user_statuses_collection.insert_one(user_status)
    print("User status inserted with ID:", result.inserted_id)
    return result.inserted_id

if __name__ == "__main__":
    api_key = input("Please enter your Torn API key: ")
    user_id = input("Please enter the user ID (leave blank for all users): ")
    
    make_api_call(api_key, user_id)
