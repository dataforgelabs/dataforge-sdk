// ----- Import standard packages and DataForge SDK PostOutputSession to run a custom process using this notebook
import com.dataforgelabs.sdk._

// ----- Start new post output session in DataForge for a specific channel. Starts a new process in DataForge
val session = new PostOutputSession("<DataForge Output Name>","<DataForge Output Source Name>", "<DataForge Project Name>")

// Optional logic to show how to access different parameters
// session.customParameters = Custom parameters you can set in your Output settings or Output Channel settings. Parameters are referenced by the Key name from the key:value pair in DataForge
// (session.customParameters \ "output") = Custom parameters set in Output Settings Output Parameters
val customOutputParam = (session.customParameters \ "output" \ "customoutputparam").as[String]
// (session.customParameters \ "output_channel") = Custom parameters set in Output Channel output_source Parameters
val customOutputSourceParam = (session.customParameters \ "output_channel" \ "customoutputsourceparam").as[String]
// session.processParameters = DataForge generated parameters for the post output process that is now running in this session
val viewDatabase = (session.processParameters \ "view_database").as[String]
val viewName = (session.processParameters \ "view_name").as[String]
val viewCatalog = "hive_metastore"
// Optional logging can be passed to DataForge logs using session.log()
session.log(s"View location is $viewCatalog.$viewDatabase.$viewName")

// ----- Define a function with your custom code
def postOutput(): Unit = {
    // Your custom code goes here
    session.log(s"custom output parameter: $customOutputParam")
    session.log(s"custom output source/channel parameter: $customOutputSourceParam")
}

// ----- Complete the post output process by calling your function inside session.run()
// Marks process complete and successful in DataForge
session.run(postOutput)