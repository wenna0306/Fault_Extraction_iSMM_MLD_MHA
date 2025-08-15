import pandas as pd
import numpy as np
import datetime
import requests
import os
import pytz
from dotenv import load_dotenv
from supabase import create_client
import time


load_dotenv(dotenv_path="api_key.env")
# Fetch from environment variables
email = os.getenv("email")
password = os.getenv("password")

# Define Singapore timezone
sg_timezone = pytz.timezone('Asia/Singapore')
# Calculate date range
today = datetime.date.today()  # Get today's date
start_date = today - datetime.timedelta(days=90)  # 3 months before today

# Format dates as YYYY-MM-DD
# end_date_str = today.strftime("%Y-%m-%d")
# start_date_str = start_date.strftime("%Y-%m-%d")
# --------------------------------------------------------------
end_date_str = '2025-03-31'
start_date_str = '2025-01-01'
# -------------------------------------------------------------

def get_access_token(email, password):
    url = "https://ismm-midland.sg/app/api/auth/login"
    headers = {'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    data = {'email': email, 'password': password}
    
    response = requests.post(url, headers=headers, json=data)
    # print("Status:", response.status_code)
    # print("Response:", response.text)

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        return None

access_token = get_access_token(email, password)

# Function to fetch paginated fault data within the date range
# # Status filters
# statuses = "work_completed"
statuses = []

# Pagination variables
per_page = 10  # Adjust based on API limits
total_pages = 5000  # Define how many pages you want to fetch (e.g., 5 pages)

site_list = [90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 
             115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 
             138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 
             161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 190]   # 94sites, need to be string

def fetch_faults(access_token, start_date_str, end_date_str, statuses, site_name):
    all_data = []  # List to collect all the fault data

    # Loop through pages and fetch data
    for page in range(1, total_pages + 1):
        # Construct the URL with parameters directly in the query string
        url = f"https://ismm-midland.sg/app/api/faults?site={site_name}&start_date={start_date_str}&end_date={end_date_str}&page={page}&per_page={per_page}&need_extra_column=true"
        #url = f"https://ismm-midland.sg/app/api/faults?site={site_name}&start_date=2025-01-01&end_date=2025-03-31&page={page}&per_page={per_page}&need_extra_column=true"
        
        headers = {"Authorization": f"Bearer {access_token}", "User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}

        # Make GET request
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if not data:  # Stop if no data returned
                print("No more data.")
                break

            # Collect the data
            all_data.extend(data['data'])  # Extend the all_data list with the 'data' part of the response

            # Check if fewer results than per_page (no more pages to fetch)
            if len(data['data']) < per_page:
                print("No more pages.")
                break
        else:
            print(f"Error fetching data: {response.status_code} - {response.json()}")
            break

    return all_data # Return the collected data


# Step 2: Fetch fault data within the date range and status filters

fault_holder = []
for site_name in site_list:
    fault_data = fetch_faults(access_token, start_date_str, end_date_str, statuses, site_name)  # Get the fault data
    df = pd.DataFrame(fault_data)
    fault_holder.append(df)
    time.sleep(5)


df = pd.concat(fault_holder, ignore_index=True)

df = df.loc[:, ['fault_number', 'site_fault_number', 
       'trade_name', 'category_name', 'type_name', 'other_type', 'impact_name', 'site_and_location', 'created_user',
       'responded_date', 'site_visited_date',
       'ra_acknowledged_date', 'work_started_date', 'work_completed_date',
       'latest_status', 'source', 'created_at',
       'response_time', 'recovery_time', 'work_started_user', 'attended_by',
       'action_taken', 'end_user_name', 'end_user_priority']]

import ast
# Convert the string to a Python dictionary
df["site_and_location"] = df["site_and_location"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
# Extract the first dictionary from the 'data' list
df["site_and_location"] = df["site_and_location"].apply(lambda x: x["data"][0] if isinstance(x, dict) and "data" in x and isinstance(x["data"], list) else None)

df = df.reset_index(drop=True)   # must have this step if we concat few dfs above, due to the index is duplicated

# Flatten the extracted dictionary
df_loc = pd.json_normalize(df["site_and_location"])
df_loc = df_loc.loc[:, ["site_name", "building_name", "floor_name", "room_name", "assets"]]
df_loc = df_loc.set_index(df["site_and_location"].index)

# Merge with original DataFrame
df_all = pd.concat([df, df_loc], axis=1)
df_all = df_all.drop(["site_and_location"], axis=1)

df_all.columns = ['Fault Number', 'Site Fault Number', 'Trade', 'Trade Category',
       'Type of Fault', 'other_type', 'Impact', 'Reported By',
       'Fault Acknowledged Date', 'Responded on Site Date', 'RA Conducted Date',
       'Work Started Date', 'Work Completed Date', 'Current Status', 'source',
       'Reported Date', 'Response Time', 'Recovery Time', 'Work Started Company',
       'Attended By', 'Action(s) Taken', 'End User Name', 'End User Priority',
       'Site', 'Building', 'Floor', 'Room', 'Assets']



# Load environment variables
load_dotenv(dotenv_path="myenv.env")


# Fetch from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")



# Create Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Replace NaN/NaT with None
df_all = df_all.replace({np.nan: None})
# Convert DataFrame to list of dicts
data_dic = df_all.to_dict(orient="records")

# Upsert data into Supabase table
supabase.table("fault_MHA").upsert(data_dic, on_conflict=["Fault Number"]).execute() 









