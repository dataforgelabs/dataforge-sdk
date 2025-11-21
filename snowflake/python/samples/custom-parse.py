# ----- Install the DataForge SDK into the Snowflake notebook environment
!pip install --extra-index-url https://test.pypi.org/simple dataforge-sdk[psycopg2]==10.0.0b107

# ----- Import standard packages and DataForge SDK ParsingSession to run a custom process using this notebook
from dataforge import ParsingSession
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import current_timestamp
import posixpath
import os

# ----- Get the current Snowpark session bound to this Snowflake notebook and set role to ACCOUNTADMIN for custom notebook runs
session = get_active_session()
session.sql("USE ROLE ACCOUNTADMIN").collect()

# ----- Start new parsing session in DataForge for a specific input. Starts a new process in DataForge
df_session = ParsingSession(<DataForge Input ID>)

# Optional logic to show how to access different parameters
# df_session.custom_parameters() = Custom parameters you can set in Source Settings Parsing Parameters. Parameters are referenced by the Key name from the key:value pair in DataForge
custom_column_name = df_session.custom_parameters()['new_column_name']
# df_session.process.parameters = DataForge generated parameters for the parse process that is now running in this session
input_id = df_session.process.parameters['input_ids'][0]

# df_session.file_path = The resulting stage raw file path of the completed previous ingestion process
stage_file_path = df_session.file_path
# Need to strip the extension off file_path to make it @STAGE/folder_path
stage_folder_path = posixpath.splitext(stage_file_path)[0]

# ----- Create a function that returns a Spark DataFrame to pass back to DataForge
def parse_df() -> DataFrame:
    # Optional logging can be passed to DataForge logs using df_session.log()
    df_session.log(f"Parsing file from path {stage_folder_path}")
    
    # Read the file moved from the Ingestion Process as a starting point, which is done in three steps to infer headers and schema
    # 1. Create temp file format to use
    session.sql("""
    CREATE OR REPLACE TEMPORARY FILE FORMAT tmp_file_format
      TYPE = CSV
      FIELD_DELIMITER = ','
      PARSE_HEADER = TRUE
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    """).collect()

    # 2. Create new temp table inferring schema from the stage folder path
    session.sql(f"""
    CREATE OR REPLACE TEMPORARY TABLE tmp_inferred_{input_id}
    USING TEMPLATE (
      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION => '{stage_folder_path}',
          FILE_FORMAT => 'TMP_FILE_FORMAT'
        )
      )
    )
    """).collect()

    # 3. Copy the data into the temp table matching on column name
    session.sql(f"""
    COPY INTO tmp_inferred_{input_id}
    FROM {stage_folder_path}
    FILE_FORMAT = (FORMAT_NAME = TMP_FILE_FORMAT)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """).collect()
    
    # Create a Snowpark DataFrame to return in the function
    df = session.table(f"tmp_inferred_{input_id}")
    # Optional logic to transform and add a column to the DataFrame of the file data
    df = df.with_column(custom_column_name, current_timestamp())
    df_session.log(f"Added column {custom_column_name}")
    
    # ----- Return the DataFrame to close the function
    return df

# ----- Complete the parse process by calling your function inside session.run()
# Marks process complete and successful or failed in DataForge
df_session.run(parse_df)