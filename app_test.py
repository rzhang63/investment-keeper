# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery



# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

st.write(client.project)


schema = [
    bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("password", "INTEGER", mode="REQUIRED"),
]

table = bigquery.Table('pure-fold-324517.investment_keeper.userstable', schema=schema)
table = client.create_table(table)  # Make an API request.


# Perform query.
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

rows = run_query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")

# Print results.
st.write("Some wise words from Shakespeare:")
for row in rows:
    st.write("✍️ " + row['word'])