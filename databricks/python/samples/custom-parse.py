# ----- Import standard packages and DataForge SDK ParsingSession to run a custom process using this notebook
from dataforge import ParsingSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# ----- Start new parsing session in DataForge for a specific input. Starts a new process in DataForge
session = ParsingSession(<DataForge Input ID>)

# Optional logic to show how to access different parameters
# session.custom_parameters() = Custom parameters you can set in Source Settings Parsing Parameters. Parameters are referenced by the Key name from the key:value pair in DataForge
custom_column_name = session.custom_parameters()['new_column_name']
# session.process.parameters = DataForge generated parameters for the parse process that is now running in this session
input_id = session.process.parameters['input_ids'][0]
# You can query optional metadata from the DataForge metastore
original_file_name = spark.sql(f"SELECT source_file_name FROM meta.input WHERE input_id = {input_id}").head()[0]
# session.file_path = The resulting raw file path of the completed previous ingestion process
file_path = session.file_path

# ----- Create a function that returns a Spark DataFrame to pass back to DataForge
def parse_df() -> DataFrame:
    # Read the raw file path into a Spark DataFrame to transform
    df = spark.read.option("header", True).csv(file_path)
    # Optional logging can be passed to DataForge logs using session.log()
    session.log(f"Parsing file from path {file_path}")
    # Optional logic to transform and add two columns to the DataFrame of the file data
    df = df.withColumn(custom_column_name, F.now()) \
        .withColumn("original_file_name", F.lit(original_file_name))
    
    # ----- Return the DataFrame to close the function
    return df

# ----- Complete the parse process by calling your function inside session.run()
# Marks process complete and successful or failed in DataForge
session.run(parse_df)