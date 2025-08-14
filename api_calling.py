import pandas as pd
import numpy as np
import datetime
import requests
import os
import pytz
from dotenv import load_dotenv
from supabase import create_client


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
end_date_str = today.strftime("%Y-%m-%d")
start_date_str = start_date.strftime("%Y-%m-%d")

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
def fetch_faults(access_token):

    # # Status filters
    # statuses = "work_completed"

    # Pagination variables
    per_page = 10  # Adjust based on API limits
    total_pages = 5000  # Define how many pages you want to fetch (e.g., 5 pages)

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


site_name = 194
all_data = fetch_faults(access_token)
df_194= pd.DataFrame(all_data)

site_name = 201
all_data = fetch_faults(access_token)
df_201= pd.DataFrame(all_data)

site_name = 202
all_data = fetch_faults(access_token)
df_202= pd.DataFrame(all_data)

site_name = 203
all_data = fetch_faults(access_token)
df_203= pd.DataFrame(all_data)

site_name = 204
all_data = fetch_faults(access_token)
df_204= pd.DataFrame(all_data)

site_name = 205
all_data = fetch_faults(access_token)
df_205= pd.DataFrame(all_data)

site_name = 206
all_data = fetch_faults(access_token)
df_206= pd.DataFrame(all_data)

site_name = 207
all_data = fetch_faults(access_token)
df_207= pd.DataFrame(all_data)

site_name = 208
all_data = fetch_faults(access_token)
df_208= pd.DataFrame(all_data)

site_name = 209
all_data = fetch_faults(access_token)
df_209= pd.DataFrame(all_data)

site_name = 210
all_data = fetch_faults(access_token)
df_210= pd.DataFrame(all_data)

site_name = 211
all_data = fetch_faults(access_token)
df_211= pd.DataFrame(all_data)

site_name = 212
all_data = fetch_faults(access_token)
df_212 = pd.DataFrame(all_data)

site_name = 213
all_data = fetch_faults(access_token)
df_213= pd.DataFrame(all_data)

site_name = 214
all_data = fetch_faults(access_token)
df_214= pd.DataFrame(all_data)

site_name = 215
all_data = fetch_faults(access_token)
df_215= pd.DataFrame(all_data)

site_name = 216
all_data = fetch_faults(access_token)
df_216= pd.DataFrame(all_data)

df_list = [df_194, df_201, df_202, df_203, df_204, df_205, df_206, df_207, df_208, df_209, df_210, df_211, df_212, df_213, df_214, df_215, df_216]
df = pd.concat(df_list)

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
supabase.table("Fault_LTA_Building").upsert(data_dic, on_conflict=["Fault Number"]).execute() 
