import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import datetime
import json


# Streamlit App Setup
st.title("Astrology Chat Data Processor")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query. Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=30)
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
SELECT user_id, device_id, other_data, event_time, event_name FROM `oneastro-prod.custom_event_tracking.events`
WHERE (app_id = 'com.oneastro' OR app_id = 'com.oneastrologer')
AND event_time >= DATETIME('{start_date_str}')
AND event_time < DATETIME('{end_date_str}')
AND event_name IN ('app_install', 'profile_creation','chat_intake_submit', 'accept_chat', 'open_page', 'chat_msg_send', 'confirm_cancel_waiting_list', 'razorpay_continue_success')
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
combined_df

astro_file = pd.read_csv("https://github.com/Jay5973/North-Star-Metrix/blob/main/astro_type.csv?raw=true")


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
        columns = ['_id', 'name', 'type', 'date', 'hour', 'chat_intake_requests', 'chat_accepted', 'chat_completed','cancelled_requests','avg_time_diff_minutes', 'paid_chats_completed']
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
        intake_events = self.raw_df[self.raw_df['event_name'] == 'chat_msg_send']
        valid_user_ids = intake_events['chatSessionId'].unique()
        accept_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat') & (self.raw_df['chatSessionId'].isin(valid_user_ids))]
        accept_events['event_time'] = pd.to_datetime(accept_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        accept_events['date'] = accept_events['event_time'].dt.date
        accept_events['hour'] = accept_events['event_time'].dt.hour
        accept_counts = accept_events.groupby(['date', 'hour'])['clientId'].nunique().reset_index()
        accept_counts.rename(columns={'clientId': 'chat_completed_overall'}, inplace=True)
        return accept_counts
    
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
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'accept_chat')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'astros_live'}, inplace=True)
        return user_counts

    def users_live(self):
        intake_events = self.raw_df[(self.raw_df['event_name'] == 'open_page')]
        intake_events['event_time'] = pd.to_datetime(intake_events['event_time'], utc=True) + pd.DateOffset(hours=5, minutes=30)
        intake_events['date'] = intake_events['event_time'].dt.date
        intake_events['hour'] = intake_events['event_time'].dt.hour
        user_counts = intake_events.groupby(['date', 'hour'])['user_id'].nunique().reset_index()
        user_counts.rename(columns={'user_id': 'users_live'}, inplace=True)
        return user_counts

astro_df = pd.read_csv('https://github.com/Jay5973/North-Star-Metrix/blob/main/astro_type.csv?raw=true')

# Step 4: Process Data
processor = UniqueUsersProcessor(combined_df, astro_df)

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

# Combine results
final_results = intake_data
final_results = pd.merge(final_results, accepted_data, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, completed_data, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, paid_completed_data, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, cancelled, on=['_id', 'date', 'hour'], how='outer')
final_results = pd.merge(final_results, cancel_time, on=['_id', 'date', 'hour'], how='outer')

final_overall = overall_chat_intakes
final_overall = pd.merge(final_overall, overall_chat_accepts, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, overall_chat_completed, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, astro_live, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, users_live, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, app_installs, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, profile_creation, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, wallet_recharge_users, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, wallet_recharge_count, on=['date', 'hour'], how='outer')
final_overall = pd.merge(final_overall, wallet_recharge_amount, on=['date', 'hour'], how='outer')

# Merge with astro data and display final data
merged_data = processor.merge_with_astro_data(final_results)
merged_overall = final_overall

import streamlit as st
import pandas as pd

# Assuming final_overall is your DataFrame
# Get the last row of the DataFrame
last_row1 = final_overall.tail(1)

# Convert all numerical values to integers, ensuring that date and datetime fields are preserved
last_row1 = last_row1.apply(lambda col: col.map(lambda x: int(x) if isinstance(x, (int, float)) else x))

# Transpose the DataFrame
last_row = last_row1.transpose()

# Display the last row in Streamlit
st.write("Live Data")
st.table(last_row)


# Display final output
st.write("### Astro-Hour Wise Data Data")
st.dataframe(merged_data)

st.write('### Overall-Hour Wise Data')
st.dataframe(merged_overall)

import plotly.express as px

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


# Option to download final data
csv = merged_data.to_csv(index=False)
st.download_button("Download Final Data as CSV", data=csv, file_name="combined_data_final_hour_wise.csv", mime="text/csv")
