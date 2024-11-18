import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import datetime
import json
import time


# Streamlit App Setup
st.title("Astrology Chat Data Processor")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query. Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600, show_spinner=True)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

# User inputs for date range
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

st.sidebar.header("Query Parameters")
start_date = st.sidebar.date_input("Start date", yesterday)
end_date = st.sidebar.date_input("End date", today)

# Format dates to string for BigQuery
start_date_str = start_date.strftime('%Y-%m-%d') + ' 18:30:00'
end_date_str = end_date.strftime('%Y-%m-%d') + ' 18:30:00'

query = f"""
SELECT user_id, device_id, other_data, event_time, event_name, app_id FROM `oneastro-prod.custom_event_tracking.events`
WHERE (app_id = 'com.oneastro' OR app_id = 'com.oneastrologer')
AND event_time >= DATETIME('{start_date_str}')
AND event_time < DATETIME('{end_date_str}')
AND event_name IN ('change_chat_status', 'change_call_status', 'change_multichat_status','chat_call_accept','app_install', 'profile_creation','chat_intake_submit', 'accept_chat', 'open_page', 'chat_msg_send', 'confirm_cancel_waiting_list', 'razorpay_continue_success')
"""

rows = run_query(query)

# Convert data to DataFrame
df = pd.DataFrame(rows)
json_data = []
for item in df['other_data']:
    try:
        data = json.loads(item)
        json_data.append(data)
    except (json.JSONDecodeError, TypeError):
        continue
json_df = pd.json_normalize(json_data)
combined_df = pd.concat([df, json_df], axis=1)
# combined_df

astro_file = pd.read_csv("https://github.com/Jay5973/North-Star-Metrix/blob/main/astro_type.csv?raw=true")

def get_15_minute_interval(hour, minute):
    """
    Given an hour and minute, return the corresponding 15-minute interval.
    """
    if 0 <= minute < 15:
        return f"{hour}:00-15"
    elif 15 <= minute < 30:
        return f"{hour}:15-30"
    elif 30 <= minute < 45:
        return f"{hour}:30-45"
    else:
        return f"{hour}:45-60"


    # Step 3: Process Events to Calculate Unique Users
class UniqueUsersProcessor:
    def __init__(self, raw_df,astro_df):
        self.raw_df = raw_df
        self.astro_df = astro_df

    def process_chat_intake_requests(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['astrologerId', 'date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'chat_intake_requests', 'astrologerId': '_id'}, inplace=True)
        return user_counts

    def process_chat_cancels(self):
        cancel_events = self.raw_df[(self.raw_df['event_name'] == 'confirm_cancel_waiting_list')]
        cancel_events['event_time'] = pd.to_datetime(cancel_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        cancel_events['date'] = cancel_events['event_time'].dt.date
        cancel_events['hour'] = cancel_events['event_time'].dt.hour
        user_counts = cancel_events.groupby(['astrologerId', 'date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'cancelled_requests', 'astrologerId': '_id'}, inplace=True)
        return user_counts

    def cancellation_time(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')].copy()
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        cancel_events = self.raw_df[(self.raw_df['event_name'] == 'confirm_cancel_waiting_list')].copy()
        cancel_events['event_time'] = pd.to_datetime(cancel_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        cancel_events['date'] = cancel_events['event_time'].dt.date
        cancel_events['hour'] = cancel_events['event_time'].dt.hour
        merged_events = pd.merge(intake_events, cancel_events, on=['user_id', 'astrologerId'], suffixes=('_intake', '_cancel'))
        merged_events['time_diff'] = (merged_events['event_time_cancel'] - merged_events['event_time_intake']).dt.total_seconds() / 60.0
        avg_time_diff = merged_events.groupby(['astrologerId', 'date_intake', 'hour_intake'])['time_diff'].mean().reset_index()
        avg_time_diff.rename(columns={'astrologerId': '_id', 'date_intake': 'date', 'hour_intake': 'hour', 'time_diff': 'cancellation_time'}, inplace=True)
        return avg_time_diff

    def overall_accept_time(self):
        # Filter chat_intake_submit events
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')].copy()
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
    
        # Filter accept_chat events
        cancel_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat')].copy()
        cancel_events['event_time'] = pd.to_datetime(cancel_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        cancel_events['date'] = cancel_events['event_time'].dt.date
        cancel_events['hour'] = cancel_events['event_time'].dt.hour
    
        # Merge the intake and cancel events based only on waitingListId
        merged_events = pd.merge(
            intake_events, 
            cancel_events, 
            on='waitingListId',  # Merge only on waitingListId
            suffixes=('_intake', '_cancel')
        )
    
        # Calculate time difference between intake and cancellation
        merged_events['time_diff'] = (merged_events['event_time_cancel'] - merged_events['event_time_intake']).dt.total_seconds() / 60.0
    
        # Group by date and hour of the intake event, and calculate average time difference
        avg_time_diff = merged_events.groupby(['date_intake', 'hour_intake'])['time_diff'].mean().reset_index()
    
        # Rename columns for clarity
        avg_time_diff.rename(columns={'date_intake': 'date', 'hour_intake': 'hour', 'time_diff': 'accept_time'}, inplace=True)
    
        return avg_time_diff

    

    def process_chat_accepted_events(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
        valid_user_ids = intake_events['user_id'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] == 0) & (self.raw_df['clientId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        accept_counts = accept_events.groupby(['user_id', 'date', 'hour'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'chat_accepted', 'user_id': '_id'}, inplace=True)
        return accept_counts
    
    def process_chat_completed_events(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_call_accept') ]
        valid_user_ids = intake_events['chatSessionId'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] == 0) & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        accept_counts = accept_events.groupby(['user_id', 'date', 'hour'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'chat_completed', 'user_id': '_id'}, inplace=True)
        return accept_counts
    
    def process_paid_chat_completed_events(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_call_accept')& (self.raw_df['paid'] == 1)]
        valid_user_ids = intake_events['chatSessionId'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] != 0) & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        accept_counts = accept_events.groupby(['user_id', 'date', 'hour'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'paid_chats_completed', 'user_id': '_id'}, inplace=True)
        return accept_counts

    # def process_chat_completed_events1(self):
    #     completed_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'].isin(['FREE', 'PAID']))]
    #     completed_events['createdAt'] = pd.to_datetime(completed_events['createdAt'], utc=True)
    #     completed_events['date'] = completed_events['createdAt'].dt.date
    #     completed_events['hour'] = completed_events['createdAt'].dt.hour
    #     completed_counts = completed_events.groupby(['astrologerId', 'date', 'hour'])['userId'].nunique().reset_index()
    #     completed_counts.rename(columns={'userId': 'chat_completed', 'astrologerId': '_id'}, inplace=True)
    #     return completed_counts

    # def process_paid_chat_completed_events1(self):
    #     paid_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'] == 'PAID')]
    #     paid_events['createdAt'] = pd.to_datetime(paid_events['createdAt'], utc=True)
    #     paid_events['date'] = paid_events['createdAt'].dt.date
    #     paid_events['hour'] = paid_events['createdAt'].dt.hour
    #     paid_counts = paid_events.groupby(['astrologerId', 'date', 'hour'])['userId'].nunique().reset_index()
    #     paid_counts.rename(columns={'userId': 'paid_chats_completed', 'astrologerId': '_id'}, inplace=True)
    #     return paid_counts

    def merge_with_astro_data(self, final_data):
        merged_data = pd.merge(final_data, self.astro_df, on='_id', how='left')
        columns = ['_id', 'name', 'type', 'date', 'hour', 'chat_intake_requests', 'chat_accepted', 'chat_completed','cancelled_requests','cancellation_time', 'paid_chats_completed']
        return merged_data[columns]

    def merge_with_hour_only(self, final_data):
        columns = ['date', 'hour', 'chat_intake_overall', 'chat_accepted_overall', 'chat_completed_overall','astros_live']
        return merged_data[columns]
    
    # def process_overall_chat_completed_events1(self):
    #     completed_events = self.completed_df[(self.completed_df['status'] == 'COMPLETED') & (self.completed_df['type'].isin(['FREE', 'PAID']))]
    #     completed_events['createdAt'] = pd.to_datetime(completed_events['createdAt'], utc=True)
    #     completed_events['date'] = completed_events['createdAt'].dt.date
    #     completed_events['hour'] = completed_events['createdAt'].dt.hour
    #     completed_counts = completed_events.groupby(['date', 'hour'])['userId'].nunique().reset_index()
    #     completed_counts.rename(columns={'userId': 'chat_completed_overall'}, inplace=True)
    #     return completed_counts
    
    def process_overall_chat_completed_events(self):
        # intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_msg_send']
        # valid_user_ids = intake_events['chatSessionId'].unique()
        # accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
        # accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        # accept_events['date'] = accept_events['event_time'].dt.date
        # accept_events['hour'] = accept_events['event_time'].dt.hour
        # accept_counts = accept_events.groupby(['date', 'hour'])['clientId'].nunique().reset_index()
        # accept_counts.rename(columns={'clientId': 'chat_completed_overall'}, inplace=True)
        # return accept_counts
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_call_accept') ]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'chat_completed_overall'}, inplace=True)
        return user_counts
    
    def process_overall_chat_accepted_events(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
        valid_user_ids = intake_events['user_id'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat')  & (self.raw_df['clientId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        accept_counts = accept_events.groupby(['date', 'hour'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'chat_accepted_overall'}, inplace=True)
        return accept_counts
    
    def process_overall_chat_intake_requests(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'chat_intake_overall'}, inplace=True)
        return user_counts

    def process_overall_profile_creation(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'profile_creation')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'profile_creation'}, inplace=True)
        return user_counts

    def process_overall_app_install(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'app_install')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['device_id'].nunique().reset_index()
        user_counts.rename(columns={'device_id': 'app_installs'}, inplace=True)
        return user_counts

    def process_overall_wallet_recharge_users(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'razorpay_continue_success')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'wallet_recharge_users'}, inplace=True)
        return user_counts

    def process_overall_wallet_recharge_count(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'razorpay_continue_success')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['orderId'].nunique().reset_index()
        user_counts.rename(columns={'orderId': 'wallet_recharge_count'}, inplace=True)
        return user_counts

    def process_overall_wallet_recharge_amount(self):
        # Filter for the relevant events
        intake_events = self.raw_df[self.raw_df['event_name'] == 'razorpay_continue_success']
        
        # Remove duplicate records based on the same orderId
        intake_events = intake_events.drop_duplicates(subset='orderId')
        
        # Ensure the amount column is numeric
        intake_events['amount'] = pd.to_numeric(intake_events['amount'], errors='coerce')
        
        # Convert event_time to datetime and adjust the timezone
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        
        # Extract date and hour
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        
        # Group by date and hour and calculate the sum of amount
        user_counts = intake_events.groupby(['date', 'hour'])['amount'].sum().reset_index()
        
        # Rename the column to wallet_recharge_amount
        user_counts.rename(columns={'amount': 'wallet_recharge_amount'}, inplace=True)
        
        return user_counts





    
    def astros_live(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'open_page') & (self.raw_df['app_id'] == 'com.oneastrologer')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'astros_live'}, inplace=True)
        return user_counts

    def astros_busy(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_msg_send') & (self.raw_df['app_id'] == 'com.oneastrologer')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'astros_busy'}, inplace=True)
        return user_counts

    def users_live(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'open_page') & (self.raw_df['app_id'] == 'com.oneastro')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'users_live'}, inplace=True)
        return user_counts
    
    # Update the methods to group by 15-minute intervals
    
    def process_overall_chat_completed_events_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_call_accept']
        valid_user_ids = intake_events['chatSessionId'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        accept_events['interval'] = accept_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        accept_counts = accept_events.groupby(['date','hour', 'interval'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'chat_completed_overall'}, inplace=True)
        return accept_counts
    
    def process_overall_chat_accepted_events_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_call_accept']
        valid_user_ids = intake_events['user_id'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['clientId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        accept_events['interval'] = accept_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        accept_counts = accept_events.groupby(['date','hour', 'interval'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'chat_accepted_overall'}, inplace=True)
        return accept_counts
    
    def process_overall_chat_intake_requests_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour

        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'chat_intake_overall'}, inplace=True)
        return user_counts
    
    def process_overall_profile_creation_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'profile_creation']
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'profile_creation'}, inplace=True)
        return user_counts
    
    def process_overall_app_install_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'app_install']
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['device_id'].nunique().reset_index()
        user_counts.rename(columns={'device_id': 'app_installs'}, inplace=True)
        return user_counts
    
    def process_overall_wallet_recharge_users_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'razorpay_continue_success']
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'wallet_recharge_users'}, inplace=True)
        return user_counts
    
    def process_overall_wallet_recharge_count_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'razorpay_continue_success']
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['orderId'].nunique().reset_index()
        user_counts.rename(columns={'orderId': 'wallet_recharge_count'}, inplace=True)
        return user_counts
    
    def process_overall_wallet_recharge_amount_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'razorpay_continue_success']
        intake_events = intake_events.drop_duplicates(subset='orderId')
        intake_events['amount'] = pd.to_numeric(intake_events['amount'], errors='coerce')
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['amount'].sum().reset_index()
        user_counts.rename(columns={'amount': 'wallet_recharge_amount'}, inplace=True)
        return user_counts
    
    def astros_live_15(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'open_page') & (self.raw_df['app_id'] == "com.oneastrologer")]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'astros_live'}, inplace=True)
        return user_counts

    def astros_busy_15(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_msg_send') & (self.raw_df['app_id'] == "com.oneastrologer")]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'astros_busy'}, inplace=True)
        return user_counts

    def astros_busy_1(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_msg_send') & (self.raw_df['app_id'] == "com.oneastrologer")]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        intake_events['minute'] = intake_events['event_time'].dt.minute
        # Create a new column for 15-minute intervals
        # intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'minute'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'astros_busy_live'}, inplace=True)
        return user_counts
    
    def users_live_15(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'open_page']
        intake_events = intake_events[(self.raw_df['app_id'] == 'com.oneastro')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        # Create a new column for 15-minute intervals
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'interval'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'users_live'}, inplace=True)
        return user_counts

    def users_live_1(self):
        intake_events = self.raw_df[self.raw_df['event_name'] == 'open_page']
        intake_events = intake_events[(self.raw_df['app_id'] == 'com.oneastro')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        intake_events['minute'] = intake_events['event_time'].dt.minute
        # Create a new column for 15-minute intervals
        # intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        user_counts = intake_events.groupby(['date','hour', 'minute'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'users_live_live'}, inplace=True)
        return user_counts

    def overall_accept_time_15(self):
        # Filter chat_intake_submit events
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'chat_intake_submit')].copy()
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        intake_events['interval'] = intake_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        # Filter accept_chat events
        cancel_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat')].copy()
        cancel_events['event_time'] = pd.to_datetime(cancel_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        cancel_events['date'] = cancel_events['event_time'].dt.date
        cancel_events['hour'] = cancel_events['event_time'].dt.hour
        cancel_events['interval'] = cancel_events['event_time'].apply(lambda x: get_15_minute_interval(x.hour, x.minute))
        
        # Merge the intake and cancel events based only on waitingListId
        merged_events = pd.merge(
            intake_events, 
            cancel_events, 
            on='waitingListId',  # Merge only on waitingListId
            suffixes=('_intake', '_cancel')
        )
        
        # Calculate time difference between intake and cancellation
        merged_events['time_diff'] = (merged_events['event_time_cancel'] - merged_events['event_time_intake']).dt.total_seconds() / 60.0
        
        # Group by date, hour, and interval of the intake event, and calculate average time difference
        avg_time_diff = merged_events.groupby(['date_intake', 'hour_intake', 'interval_intake'])['time_diff'].mean().reset_index()
        
        # Rename columns for clarity
        avg_time_diff.rename(columns={'date_intake': 'date', 'hour_intake': 'hour', 'interval_intake': 'interval', 'time_diff': 'accept_time'}, inplace=True)
        
        return avg_time_diff

    def astros_live_1(self):
        # Filter for status-change events (change_chat_status, change_call_status, change_multichat_status)
        status_events = self.raw_df[(self.raw_df['event_name'].isin(['change_chat_status', 'change_call_status', 'change_multichat_status'])) & (self.raw_df['app_id'] == "com.oneastrologer")]
        
        active_status_events = status_events[status_events['status'] == True]
    
        # Convert event_time to datetime
        active_status_events['event_time'] = pd.to_datetime(active_status_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        
        # Sort by user_id and event_time (latest first)
        active_status_events = active_status_events.sort_values(by=['user_id', 'event_time'], ascending=[True, False])
        
        # Drop duplicates, keeping the latest event for each user
        latest_active_events = active_status_events.drop_duplicates(subset=['user_id'], keep='first')
        
        # Extract date, hour, minute from event_time
        latest_active_events['date'] = latest_active_events['event_time'].dt.date
        latest_active_events['hour'] = latest_active_events['event_time'].dt.hour
        latest_active_events['minute'] = latest_active_events['event_time'].dt.minute
        
        # Group by date, hour, and minute to count active users
        active_astros = latest_active_events.groupby(['date', 'hour', 'minute']).size().reset_index(name='astros_live_1')
        
        return active_astros




astro_df = pd.read_csv('https://github.com/Jay5973/North-Star-Metrix/blob/main/astro_type.csv?raw=true')

# Step 4: Process Data
processor = UniqueUsersProcessor(combined_df, astro_df)


live_astros_busy = processor.astros_busy_1()

from streamlit_card import card

# Get the last value of the 'astros_busy_live' column
live_astros_busy_str = str(live_astros_busy['astros_busy_live'].tail(1).values[0])

hasClicked = card(
  title=live_astros_busy_str,  # Now passing a string to the title
  text="Astrologers Busy Currently"
)

live_users_live = processor.users_live_1()

# Get the last value of the 'astros_busy_live' column
live_users_live_str = str(live_users_live['users_live_live'].tail(1).values[0])

hasClicked = card(
  title=live_users_live_str,  # Now passing a string to the title
  text="Users Live Currently"
)

astros_live_1 = processor.astros_live_1()
astros_live_1_str = str(live_users_live['astros_live_1'].tail(1).values[0])
hasClicked = card(
  title=astros_live_1,  # Now passing a string to the title
  text="Astrologers Live Currently"
)


# Process each event type
intake_data = processor.process_chat_intake_requests()
accepted_data = processor.process_chat_accepted_events()
completed_data = processor.process_chat_completed_events()
paid_completed_data = processor.process_paid_chat_completed_events()
cancelled = processor.process_chat_cancels()
cancel_time = processor.cancellation_time()
overall_chat_completed = processor.process_overall_chat_completed_events()
overall_chat_intakes = processor.process_overall_chat_intake_requests()
overall_chat_accepts = processor.process_overall_chat_accepted_events()
astro_live = processor.astros_live()
users_live = processor.users_live()
profile_creation = processor.process_overall_profile_creation()
app_installs = processor.process_overall_app_install()
wallet_recharge_users = processor.process_overall_wallet_recharge_users()
wallet_recharge_count = processor.process_overall_wallet_recharge_count()
wallet_recharge_amount = processor.process_overall_wallet_recharge_amount()
accept_time = processor.overall_accept_time()
overall_chat_completed_15 = processor.process_overall_chat_completed_events_15()
overall_chat_intakes_15 = processor.process_overall_chat_intake_requests_15()
overall_chat_accepts_15 = processor.process_overall_chat_accepted_events_15()
astro_live_15 = processor.astros_live_15()
users_live_15 = processor.users_live_15()
profile_creation_15 = processor.process_overall_profile_creation_15()
app_installs_15 = processor.process_overall_app_install_15()
wallet_recharge_users_15 = processor.process_overall_wallet_recharge_users_15()
wallet_recharge_count_15 = processor.process_overall_wallet_recharge_count_15()
wallet_recharge_amount_15 = processor.process_overall_wallet_recharge_amount_15()
astros_busy_15 = processor.astros_busy_15()
accept_time_15 = processor.overall_accept_time_15()
astros_busy = processor.astros_busy()

# accept_time_15 = processor.overall_accept_time()

# Combine results
final_results = intake_data
final_results = pd.merge(final_results, accepted_data, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, completed_data, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, paid_completed_data, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, cancelled, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, cancel_time, on=['_id', 'date', 'hour'], how='outer')

final_overall = users_live
final_overall = pd.merge(final_overall, astro_live, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, astros_busy, on = ['date','hour'],how = 'outer')
final_overall = pd.merge(final_overall, app_installs, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, profile_creation, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, overall_chat_intakes, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, overall_chat_accepts, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, overall_chat_completed, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, wallet_recharge_users, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, wallet_recharge_count, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, wallet_recharge_amount, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, accept_time, on = ['date','hour'],how = 'outer')


fifteen_overall = users_live_15
fifteen_overall = pd.merge(fifteen_overall, astro_live_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, astros_busy_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, app_installs_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, profile_creation_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, overall_chat_intakes_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, overall_chat_accepts_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, overall_chat_completed_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, wallet_recharge_count_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, wallet_recharge_users_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, wallet_recharge_amount_15, on=['date', 'hour', 'interval'], how='outer')
fifteen_overall = pd.merge(fifteen_overall, accept_time_15, on = ['date', 'hour','interval'],how = 'outer')

# Merge with astro data and display final data
merged_data = processor.merge_with_astro_data(final_results)
merged_overall = final_overall


# import streamlit as st
# import pandas as pd
# import numpy as np

# # Assuming final_overall is your DataFrame
# # Get the last row of the DataFrame
# import pandas as pd
# import streamlit as st
# from datetime import datetime, timedelta

# # Assuming `final_overall` is already defined

# # Get the last 4 rows
# last_rows = final_overall.tail(4)

# # Drop the columns for date, hour, and interval
# # columns_to_drop = ['date', 'hour', 'interval']  # Adjust these column names based on your DataFrame
# # last_rows = last_rows.drop(columns=columns_to_drop)

# # Convert all numerical values to integers, ensuring that date and datetime fields are preserved
# def convert_to_int(value):
#     if pd.notnull(value) and isinstance(value, (int, float)):
#         return int(value)
#     return value

# # Apply the conversion function to each column (without transpose)
# last_rows = last_rows.apply(lambda col: col.map(convert_to_int))

# # Define time range labels
# now = datetime.now()
# time_ranges = [
#     f"{(now - timedelta(hours=1, minutes=0)).strftime('%H:%M')} - {(now - timedelta(minutes=45)).strftime('%H:%M')}",
#     f"{(now - timedelta(minutes=45)).strftime('%H:%M')} - {(now - timedelta(minutes=30)).strftime('%H:%M')}",
#     f"{(now - timedelta(minutes=30)).strftime('%H:%M')} - {(now - timedelta(minutes=15)).strftime('%H:%M')}",
#     f"{(now - timedelta(minutes=15)).strftime('%H:%M')} - {now.strftime('%H:%M')}"
# ]

# # Replace the index with the time range labels
# last_rows.index = time_ranges

# # Convert the last 4 rows to text format
# last_rows_text = last_rows.astype(str)

# # Transpose the rows to display them vertically
# last_rows_transposed = last_rows_text.T

# # Display the last 4 rows in Streamlit (as rows, not transposed)
# st.write("Live Data")
# st.table(last_rows_transposed)

import pandas as pd
import streamlit as st

# Assume 'fifteen_overall' is your DataFrame

# Extract the last 4 rows
last_rows = fifteen_overall.tail(4)

# Convert all numerical values to integers, ensuring that date and datetime fields are preserved
def convert_to_int(value):
    if pd.notnull(value) and isinstance(value, (int, float)):
        return int(value)
    return value

# Apply the conversion function to each column
last_rows = last_rows.applymap(convert_to_int)

# Drop the 'hour' and 'date' columns
columns_to_drop = ['hour', 'date']
last_rows = last_rows.drop(columns=columns_to_drop, errors='ignore')

# Convert the last 4 rows to text format
last_rows_text = last_rows.astype(str)

# Transpose the rows to display them vertically
last_rows_transposed = last_rows_text.T

# Display the last 4 rows in Streamlit
st.write("Live Data")
st.table(last_rows_transposed)

merged_data_overall = merged_overall

# Apply the conversion function to each column
merged_data_overall = merged_data_overall.applymap(convert_to_int)

# Drop the 'hour' and 'date' columns
# columns_to_drop = ['hour', 'date']
# last_rows = last_rows.drop(columns=columns_to_drop, errors='ignore')

# Convert the last 4 rows to text format
merged_overall_text = merged_data_overall.astype(str)

# Transpose the rows to display them vertically
merged_overall_transpose = merged_overall_text.T


st.write('### Overall-Hour Wise Data')
st.dataframe(merged_overall_transpose)

# Display final output
st.write("### Astro-Hour Wise Data Data")
st.dataframe(merged_data)


import plotly.express as px

fig4 = px.line(merged_overall, x='hour', y=['app_installs','profile_creation','chat_intake_overall', 'chat_accepted_overall', 'chat_completed_overall', 'astros_live', 'users_live', 'wallet_recharge_count'], 
                title="Overall Metrics",
                labels={
                    'app_installs' : 'App Installs',
                    'profile_creation' : 'Profile Creations',
                    'chat_intake_overall': 'Chat Intakes',
                    'chat_accepted_overall': 'Chat Accepts',
                    'chat_completed_overall': 'Chat Completes',
                    'astros_live': 'Astrologers Live',
                    'users_live': 'Users Live',
                    'wallet_recharge_amount' : 'Wallet Recharge Total in INR'
                })
fig4.update_layout(xaxis_title="Hour", yaxis_title="Count")
fig4.update_traces(connectgaps=False)
st.plotly_chart(fig4)

# st.write('### Live Data')
# st.dataframe(fifteen_overall)



# Plot the graph for Chat Intake Requests - Hour-wise and Astrologer-wise
fig1 = px.line(merged_data, x='hour', y='chat_intake_requests', color='name', line_group='name', title="Chat Intake Requests Hour-wise Astrologer-wise")
fig1.update_layout(xaxis_title="Hour", yaxis_title="Chat Intake Requests")
fig1.update_traces(connectgaps=False)
st.plotly_chart(fig1)

# Plot the graph for Chat Accept - Hour-wise and Astrologer-wise
fig2 = px.line(merged_data, x='hour', y='chat_accepted', color='name', line_group='name', title="Chat Accept Hour-wise Astrologer-wise")
fig2.update_layout(xaxis_title="Hour", yaxis_title="Chat Accepted")
fig2.update_traces(connectgaps=False)
st.plotly_chart(fig2)

# Plot the graph for Chat Completed - Hour-wise and Astrologer-wise
fig3 = px.line(merged_data, x='hour', y='chat_completed', color='name', line_group='name', title="Chat Completed Hour-wise Astrologer-wise")
fig3.update_layout(xaxis_title="Hour", yaxis_title="Chat Completed")
fig3.update_traces(connectgaps=False)
st.plotly_chart(fig3)

print(merged_overall.columns)

# Plot the graph for Overall Metrics


# Option to download final data
csv = merged_data.to_csv(index=False)
st.download_button("Download Final Data as CSV", data=csv, file_name="combined_data_final_hour_wise.csv", mime="text/csv")

time.sleep(900)
st.rerun()
