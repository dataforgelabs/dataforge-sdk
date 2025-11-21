# ----- Install the Stripe Python SDK and DataForge SDK into the Snowflake notebook environment
!pip install --extra-index-url https://test.pypi.org/simple dataforge-sdk[psycopg2]==10.0.0b108

# ----- Import standard packages and DataForge SDK PostOutputSession and IngestionSession to run a custom loobpack process using this notebook
from dataforge import PostOutputSession, IngestionSession
from snowflake.snowpark.functions import current_timestamp

# ----- Get the current Snowpark session bound to this Snowflake notebook and set role to ACCOUNTADMIN for custom notebook runs
session = get_active_session()
session.sql("USE ROLE ACCOUNTADMIN").collect()

# ----- Start new post output session in DataForge for a specific output channel. Starts a new process in DataForge
post_output_session = PostOutputSession("<DataForge Output Name>","<DataForge Output Source Name>", "<DataForge Project Name>")

# ----- Reference process parameters to get the database and name of the view that was created during the previous output process
view_database = post_output_session.process.parameters['view_database']
view_schema = post_output_session.process.parameters['view_schema']
view_name = post_output_session.process.parameters['view_name']
# Get project name to use in downstream custom ingestion session rather than hardcoding project name
project_name = post_output_session.process.parameters['project_name']

# ----- User defined parameter set in Output Settings Output Parameters for which source to run loopback ingestion into
# post_output_session.custom_parameters() contains all custom parameters set in the Output settings or Output Channel settings. Use ['output']['<key>'] or ['output_channel']['<key>'] to get key value
destination_source_name = post_output_session.custom_parameters()['output']['destination_source_name']

# ----- Create a function within the PostOutputSession that will encapsulate a new IngestionSession and any custom logic
def post_output():

  # ----- Start a new IngestionSession within the PostOutputSession to start a new custom ingestion in the destination source
  ingest_session = IngestionSession(destination_source_name, project_name)

  # ----- Create a function that returns a Spark DataFrame to pass back to DataForge
  def ingestion_code():
    # Your custom code goes here
    return (session.table(f"{view_database}.{view_schema}.{view_name}")
            .withColumn("current_datetime", current_timestamp()))
  
  # ----- Pass the function returning a Spark DataFrame inside of ingest_session.ingest() to complete the process and mark it successful or failed in DataForge
  ingest_session.ingest(ingestion_code)

# ----- Complete the post output process by calling your function inside session.run()
# Marks process complete and successful in DataForge
post_output_session.run(post_output)
