import streamlit as st
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery

# Streamlit App Setup
st.title("Astrology Chat Data Processor")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# retry_policy = Retry(
#     initial=1.0,  # Start with 1 second
#     maximum=10.0,  # Maximum retry delay
#     multiplier=2.0,  # Exponential backoff multiplier
#     deadline=30.0,  # Retry for up to 30 seconds
# ), 

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    timeout_ms=600000,
    # retry=retry_policy,
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

query = """
SELECT * FROM `oneastro---stage.custom_event_tracking.events`
WHERE (app_id = 'com.oneastro' OR app_id = 'com.oneastrologer')
AND event_time >= DATETIME('2024-11-10 18:30:00')
AND event_time < DATETIME('2024-11-11 18:30:00')
AND event_name IN ('chat_intake_submit', 'accept_chat', 'open_page', 'chat_msg_send', 'confirm_cancel_waiting_list')
"""

rows = run_query(query)

# Convert query results to DataFrame
raw_file = pd.DataFrame(rows)
astro_file = pd.read_csv("https://github.com/Jay5973/North-Star-Metrix/blob/main/astro_type.csv?raw=true")

if not raw_file.empty:
    # Step 2: Extract JSON Data from raw_data.csv and Save to a DataFrame
    def extract_json(raw_df, json_column):
        json_data = []
        for item in raw_df[json_column]:
            try:
                data = json.loads(item)
                json_data.append(data)
            except (json.JSONDecodeError, TypeError):
                continue
        json_df = pd.json_normalize(json_data)
        combined_df = pd.concat([raw_df, json_df], axis=1)
        return combined_df

    # Step 3: Process Events to Calculate Unique Users
    class UniqueUsersProcessor:
        def __init__(self, raw_df, astro_df):
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
            avg_time_diff.rename(columns={'astrologerId': '_id', 'date_intake': 'date', 'hour_intake': 'hour', 'time_diff': 'avg_time_diff_minutes'}, inplace=True)
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
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_msg_send']
            valid_user_ids = intake_events['chatSessionId'].unique()
            accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] == 0) & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
            accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            accept_events['date'] = accept_events['event_time'].dt.date
            accept_events['hour'] = accept_events['event_time'].dt.hour
            accept_counts = accept_events.groupby(['user_id', 'date', 'hour'])['clientId'].nunique().reset_index()
            accept_counts.rename(columns={'clientId': 'chat_completed', 'user_id': '_id'}, inplace=True)
            return accept_counts
        
        def process_paid_chat_completed_events(self):
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_msg_send']
            valid_user_ids = intake_events['chatSessionId'].unique()
            accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] != 0) & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
            accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            accept_events['date'] = accept_events['event_time'].dt.date
            accept_events['hour'] = accept_events['event_time'].dt.hour
            accept_counts = accept_events.groupby(['user_id', 'date', 'hour'])['clientId'].nunique().reset_index()
            accept_counts.rename(columns={'clientId': 'paid_chats_completed', 'user_id': '_id'}, inplace=True)
            return accept_counts

        def merge_with_astro_data(self, final_data):
            merged_data = pd.merge(final_data, self.astro_df, on='_id', how='left')
            columns = ['_id', 'name', 'type', 'date', 'hour', 'chat_intake_requests', 'chat_accepted', 'chat_completed', 'cancelled_requests', 'avg_time_diff_minutes', 'paid_chats_completed']
            return merged_data[columns]

        def merge_with_hour_only(self, final_data):
            columns = ['date', 'hour', 'chat_intake_overall', 'chat_accepted_overall', 'chat_completed_overall', 'astros_live']
            return final_data[columns]
        
        def process_overall_chat_completed_events(self):
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_msg_send']
            valid_user_ids = intake_events['chatSessionId'].unique()
            accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
            accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            accept_events['date'] = accept_events['event_time'].dt.date
            accept_events['hour'] = accept_events['event_time'].dt.hour
            accept_counts = accept_events.groupby(['date', 'hour'])['clientId'].nunique().reset_index()
            accept_counts.rename(columns={'clientId': 'chat_completed_overall'}, inplace=True)
            return accept_counts

        def process_overall_chat_intake_events(self):
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
            intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            intake_events['date'] = intake_events['event_time'].dt.date
            intake_events['hour'] = intake_events['event_time'].dt.hour
            user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
            user_counts.rename(columns={'user_id': 'chat_intake_overall'}, inplace=True)
            return user_counts
        
        def process_overall_chat_accepted_events(self):
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
            valid_user_ids = intake_events['user_id'].unique()
            accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] == 0) & (self.raw_df['clientId'].isin(valid_user_ids))]
            accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            accept_events['date'] = accept_events['event_time'].dt.date
            accept_events['hour'] = accept_events['event_time'].dt.hour
            accept_counts = accept_events.groupby(['date', 'hour'])['clientId'].nunique().reset_index()
            accept_counts.rename(columns={'clientId': 'chat_accepted_overall'}, inplace=True)
            return accept_counts

        def astros_live(self):
            intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_intake_submit']
            valid_user_ids = intake_events['user_id'].unique()
            accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['paid'] == 0) & (self.raw_df['clientId'].isin(valid_user_ids))]
            accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
            accept_events['date'] = accept_events['event_time'].dt.date
            accept_events['hour'] = accept_events['event_time'].dt.hour
            astro_counts = accept_events.groupby(['date', 'hour'])['astrologerId'].nunique().reset_index()
            astro_counts.rename(columns={'astrologerId': 'astros_live'}, inplace=True)
            return astro_counts
        
    processor = UniqueUsersProcessor(raw_file, astro_file)
    chat_intake_requests = processor.process_chat_intake_requests()
    chat_cancels = processor.process_chat_cancels()
    chat_cancel_time = processor.cancellation_time()
    chat_accepted = processor.process_chat_accepted_events()
    chat_completed = processor.process_chat_completed_events()
    paid_chat_completed = processor.process_paid_chat_completed_events()
    overall_chat_completed = processor.process_overall_chat_completed_events()
    overall_chat_accepted = processor.process_overall_chat_accepted_events()
    overall_chat_intake = processor.process_overall_chat_intake_events()
    astros_live = processor.astros_live()

    final_data = chat_intake_requests.merge(chat_accepted, on=['_id', 'date', 'hour'], how='left')
    final_data = final_data.merge(chat_completed, on=['_id', 'date', 'hour'], how='left')
    final_data = final_data.merge(chat_cancels, on=['_id', 'date', 'hour'], how='left')
    final_data = final_data.merge(chat_cancel_time, on=['_id', 'date', 'hour'], how='left')
    final_data = final_data.merge(paid_chat_completed, on=['_id', 'date', 'hour'], how='left')
    final_data = processor.merge_with_astro_data(final_data)
    
    final_data.fillna(0, inplace=True)

    hour_data = overall_chat_completed.merge(overall_chat_accepted, on=['date', 'hour'], how='left')
    hour_data = hour_data.merge(overall_chat_intake, on=['date', 'hour'], how='left')
    hour_data = hour_data.merge(astros_live, on=['date', 'hour'], how='left')
    hour_data = processor.merge_with_hour_only(hour_data)
    
    hour_data.fillna(0, inplace=True)

    st.write("Astrologer Data:")
    st.dataframe(final_data)
    
    st.write("Overall Hour Data:")
    st.dataframe(hour_data)

    csv = final_data.to_csv(index=False)
    st.download_button("Download Final Data as CSV", csv, "final_data.csv", "text/csv")

    csv2 = hour_data.to_csv(index=False)
    st.download_button("Download Hour Data as CSV", csv2, "hour_data.csv", "text/csv")
else:
    st.write("No data available for the specified date range and app ID.")
