# ----- Import DataForge IngestionSession to run a custom ingestion in this notebook along with other standard packages
from dataforge import IngestionSession
import stripe
import json
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType

# ----- Start new ingestion session in DataForge. Creates a new input and starts a new ingestion process in DataForge
df_session = IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")

# Optional logic to show how to access different parameters
# df_session.custom_parameters() = Custom parameters you can set in Source Settings Ingestion Parameters.
# Parameters are referenced by the Key name from the key:value pair in DataForge.
custom_params = df_session.custom_parameters() or {}
limit_val = custom_params.get("limit")

# Latest Tracking Fields can be referenced to pull metadata from DataForge with information about what data has been pulled into the Source. Commonly used to generate incremental data pulls.
# df_session.latest_tracking_fields() = All available tracking fields. Not every field is populated for every source and depends on the Source Refresh Type.
latest_tracking_fields = df_session.latest_tracking_fields()
# input_id references the DataForge input id that is generated as part of this Ingestion Session
current_input_id = latest_tracking_fields.get("input_id")
# s_sequence = Max value from the Range Column defined in Source Settings for Sequence Refresh type
max_sequence_id = latest_tracking_fields.get("s_sequence") or "0"
# s_timestamp = Max value from the Date Column for Timestamp Refresh type or CDC Parameters date column for other Refresh types
max_timestamp = latest_tracking_fields.get("s_timestamp") or "1900-01-01 00:00:00"
# extract_datetime = UTC timestamp of the last time data was ingested in the DataForge source
last_pull_timestamp = latest_tracking_fields.get("extract_datetime") or "1900-01-01 00:00:00"

# Convert last_pull_timestamp to Unix seconds for Stripe request
last_pull_dt = datetime.strptime(last_pull_timestamp, "%Y-%m-%d %H:%M:%S.%f")
last_pull_unix = int(last_pull_dt.timestamp())

# Build Stripe params for incremental pull
stripe_params = {
    "created": {"gte": last_pull_unix}
}

# Check if optional limit param is set in custom parameters. If so, add to Stripe params.
if limit_val is not None:
    stripe_params["limit"] = int(limit_val)

# Optional logging can be passed to DataForge logs using df_session.log()
df_session.log("Stripe params being sent: " + str(stripe_params))

# Initialize Stripe client using the API key stored in the DataForge custom connection private parameters
client = stripe.StripeClient(
    df_session.connection_parameters()["private_connection_parameters"]["stripe_api_key"]
)

# ----- Create a function that returns a Spark DataFrame to pass back to DataForge
def ingest_df():
    # Call Stripe Customers API with our incremental + optional limit params
    first_page = client.v1.customers.list(stripe_params)

    # Collect each customer as a JSON string
    rows_json = []
    for cust in first_page.auto_paging_iter():
        # Stripe object -> plain Python dict -> JSON string
        rows_json.append(json.dumps(dict(cust)))

    # If there are no rows, Spark can't infer schema and fails so pass a dummy schema in
    if not rows_json:
        # No new customers since last pull: return an empty DF with a fixed schema
        schema = StructType([StructField("id", StringType(), True)])
        return spark.createDataFrame([], schema=schema)

    # Let Spark's JSON reader infer schema from the JSON strings
    rdd = spark.sparkContext.parallelize(rows_json)

    # ----- Create a Spark DataFrame to return in the function
    df = spark.read.json(rdd)
    return df

# ----- Complete the Ingestion process by calling your function which returns a DataFrame inside df_session.ingest()
# Marks ingestion process complete and successful or failed in DataForge. If successful, the input will move on to the next process needed such as Capture Data Changes
df_session.ingest(ingest_df)
