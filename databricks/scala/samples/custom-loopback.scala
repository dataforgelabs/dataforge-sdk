// ----- Import standard packages and DataForge SDK PostOutputSession and IngestionSession to run a custom loobpack process using this notebook
import com.dataforgelabs.sdk._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// ----- Start new post output session in DataForge for a specific output channel. Starts a new process in DataForge
val postOutputSession = new PostOutputSession("<DataForge Output Name>","<DataForge Output Source Name>", "<DataForge Project Name>")

// ----- Reference process parameters to get the database and name of the view that was created during the previous output process
val viewDatabase = (postOutputSession.processParameters \ "view_database").as[String]
val viewName = (postOutputSession.processParameters \ "view_name").as[String]
// Get project name to use in downstream custom ingestion session rather than hardcoding project name
val projectName = (postOutputSession.processParameters \ "project_name").as[String]
// Query DataForge metadata to read the catalog the view was created in
// val view_catalog = spark.sql("SELECT value FROM meta.system_configuration WHERE name = 'datalake-db-name'").as[String].head()
val viewCatalog = "hive_metastore"

// ----- User defined parameter set in Output Settings Output Parameters for which source to run loopback ingestion into
// post_output_session.custom_parameters() contains all custom parameters set in the Output settings or Output Channel settings. Use ['output']['<key>'] or ['output_channel']['<key>'] to get key value
val destinationSourceName = (postOutputSession.customParameters \ "output" \ "destination_source_name").as[String]

// ----- Create a function within the PostOutputSession that will encapsulate a new IngestionSession and any custom logic
def postOutput(): Unit = {

  // ----- Start a new IngestionSession within the PostOutputSession to start a new custom ingestion in the destination source
  val ingestSession = new IngestionSession(destinationSourceName, projectName)

  // ----- Create a function that returns a Spark DataFrame to pass back to DataForge
  def ingestionCode(): DataFrame = {
    // Your custom code goes here
    return (spark.table(s"$viewCatalog.$viewDatabase.$viewName")
            .withColumn("current_datetime", current_timestamp()))
  }

  // ----- Pass the function returning a Spark DataFrame inside of ingest_session.ingest() to complete the process and mark it successful or failed in DataForge
  ingestSession.ingest(ingestionCode)
}

// ----- Complete the post output process by calling your function inside session.run()
// Marks process complete and successful in DataForge
postOutputSession.run(postOutput)