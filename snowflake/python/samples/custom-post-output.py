# ----- Install the Stripe Python SDK and DataForge SDK into the Snowflake notebook environment
!pip install --extra-index-url https://test.pypi.org/simple dataforge-sdk[psycopg2]==10.0.0b108

# ----- Import DataForge IngestionSession to run a custom ingestion in this notebook along with other standard packages
from dataforge import PostOutputSession
from snowflake.snowpark.context import get_active_session

# ----- Get the current Snowpark session bound to this Snowflake notebook and set role to ACCOUNTADMIN for custom notebook runs
session = get_active_session()
session.sql("USE ROLE ACCOUNTADMIN").collect()

# ----- Start new post output session in DataForge for a specific channel. Starts a new process in DataForge
df_session = PostOutputSession("<DataForge Output Name>","<DataForge Output Source Name>", "<DataForge Project Name>")

# Optional logic to show how to access different parameters
# df_session.custom_parameters() = Custom parameters you can set in your Output settings or Output Channel settings. Parameters are referenced by the Key name from the key:value pair in DataForge
# df_session.custom_parameters()['output'] = Custom parameters set in Output Settings Output Parameters
custom_output_param = df_session.custom_parameters()['output']['customoutputparam']
# df_session.custom_parameters()['output_channel'] = Custom parameters set in Output Channel output_source Parameters
custom_output_source_param = df_session.custom_parameters()['output_channel']['customoutputsourceparam']
# df_session.process.parameters = DataForge generated parameters for the post output process that is now running in this session
view_database = df_session.process.parameters['view_database']
view_schema = df_session.process.parameters['view_schema']
view_name = df_session.process.parameters['view_name']
# Optional logging can be passed to DataForge logs using df_session.log()
df_session.log(f"View location is {view_database}.{view_schema}.{view_name}")

# ----- Define a function with your custom code
def post_output():
    # Your custom code goes here
    df_session.log(f"custom output parameter: {custom_output_param}")
    df_session.log(f"custom output source/channel parameter: {custom_output_source_param}")

# ----- Complete the post output process by calling your function inside df_session.run()
# Marks process complete and successful in DataForge
df_session.run(post_output)