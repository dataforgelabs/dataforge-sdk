# ----- Install the Stripe Python SDK and DataForge SDK into the Snowflake notebook environment
!pip install stripe
!pip install --extra-index-url https://test.pypi.org/simple dataforge-sdk[psycopg2]==10.0.0b108

# ----- Import DataForge IngestionSession to run a custom ingestion in this notebook along with other standard packages
from dataforge import IngestionSession
from snowflake.snowpark.context import get_active_session
import stripe
import json
from datetime import datetime, timezone
from snowflake.snowpark.types import StructType, StructField, StringType

# ----- Get the current Snowpark session bound to this Snowflake notebook and set role to ACCOUNTADMIN for custom notebook runs
session = get_active_session()
session.sql("USE ROLE ACCOUNTADMIN").collect()

# ----- Start new ingestion session in DataForge. Creates a new input and starts a new ingestion process in DataForge
df_session = IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")

# Optional logic to show how to access different parameters
# session.custom_parameters() = Custom parameters you can set in Source Settings Ingestion Parameters. Parameters are referenced by the Key name from the key:value pair in DataForge
custom_params = df_session.custom_parameters() or {}
limit_val = custom_params.get("limit")

# Latest Tracking Fields can be referenced to pull metadata from DataForge with information about what data has been pulled into the Source. Commonly used to generate incremental data pulls
# df_session.latest_tracking_fields() = All available tracking fields. Not every field is populated for every source and depend on the Source Refresh Type
latest_tracking_fields = df_session.latest_tracking_fields()
# input_id references the DataForge input id that is generated as part of this Ingestion Session
current_input_id = latest_tracking_fields.get('input_id')
# s_sequence = Max value from the Range Column defined in Source Settings for Sequence Refresh type
max_sequence_id = latest_tracking_fields.get('s_sequence') or '0'
# s_timestamp = Max value from the Date Column for Timestamp Refresh type or CDC Parameters date column for other Refresh types
max_timestamp = latest_tracking_fields.get('s_timestamp') or '1900-01-01 00:00:00'
# extract_datetime = UTC timestamp of the last time data was ingested in the DataForge source
last_pull_timestamp = latest_tracking_fields.get('extract_datetime') or '1900-01-01 00:00:00'

# Convert last_pull_timestamp to unix seconds for Stripe request
last_pull_dt = datetime.strptime(last_pull_timestamp, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
last_pull_unix = int(last_pull_dt.timestamp())

# Build Stripe params for incremental pull
stripe_params = {
    # Always filter by created >= last_pull_timestamp
    "created": {"gte": last_pull_unix}
}

# Check if optional limit param is set in custom parameters. If so, add to Stripe params
if limit_val is not None:
    stripe_params["limit"] = int(limit_val)

# Optional logging can be passed to DataForge logs using df_session.log()
df_session.log("Stripe params being sent: " + str(stripe_params))

# Initialize Stripe client using the API key stored in the DataForge custom connection private parameters
client = stripe.StripeClient(df_session.connection_parameters()['private_connection_parameters']['stripe_api_key'])

# ----- Create a function that returns a Snowpark DataFrame to pass back to DataForge
def ingest_df():
    # Call Stripe Customers API with our incremental + optional limit params
    first_page = client.v1.customers.list(stripe_params)
    
    # Iterate over all pages
    rows = []
    for cust in first_page.auto_paging_iter():
        rows.append(dict(cust))  

    # If there are no rows, Snowpark can't infer schema and fails so pass a dummy schema in
    if not rows:
        # No new customers since last pull: return an empty DF with a fixed schema
        schema = StructType([StructField("id", StringType())])
        return session.create_dataframe([], schema=schema)
        
    # ----- Create a Spark DataFrame to return in the function
    df = session.create_dataframe(rows)
    return df

# ----- Complete the Ingestion process by calling your function which returns a DataFrame inside df_session.ingest()
# Marks ingestion process complete and successful or failed in DataForge. If successful, the input will move on to the next process needed such as Capture Data Changes
df_session.ingest(ingest_df)
