# ----- Import standard packages and DataForge SDK PostOutputSession to run a custom process using this notebook
from dataforge import PostOutputSession

# ----- Start new post output session in DataForge for a specific channel. Starts a new process in DataForge
session = PostOutputSession("<DataForge Output Name>","<DataForge Output Source Name>", "<DataForge Project Name>")

# Optional logic to show how to access different parameters
# session.custom_parameters() = Custom parameters you can set in your Output settings or Output Channel settings. Parameters are referenced by the Key name from the key:value pair in DataForge
# session.custom_parameters()['output'] = Custom parameters set in Output Settings Output Parameters
custom_output_param = session.custom_parameters()['output']['customoutputparam']
# session.custom_parameters()['output_channel'] = Custom parameters set in Output Channel output_source Parameters
custom_output_source_param = session.custom_parameters()['output_channel']['customoutputsourceparam']
# session.process.parameters = DataForge generated parameters for the post output process that is now running in this session
view_database = session.process.parameters['view_database']
view_name = session.process.parameters['view_name']
view_catalog = "hive_metastore"
# Optional logging can be passed to DataForge logs using session.log()
session.log(f"View location is {view_catalog}.{view_database}.{view_name}")

# ----- Define a function with your custom code
def post_output():
    # Your custom code goes here
    session.log(f"custom output parameter: {custom_output_param}")
    session.log(f"custom output source/channel parameter: {custom_output_source_param}")

# ----- Complete the post output process by calling your function inside session.run()
# Marks process complete and successful in DataForge
session.run(post_output)