// ----- Import standard packages and DataForge SDK ParsingSession to run a custom process using this notebook
import com.dataforgelabs.sdk._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// ----- Start new parsing session in DataForge for a specific input. Starts a new process in DataForge
val session = new ParsingSession(<DataForge Input ID>)

// Optional logic to show how to access different parameters
// session.customParameters = Custom parameters you can set in Source Settings Parsing Parameters. Parameters are referenced by the Key name from the key:value pair in DataForge
val customColumnName = (session.customParameters \ "new_column_name").as[String]
//session.processParameters = DataForge generated parameters for the parse process that is now running in this session
val inputId = (session.processParameters \ "input_ids").as[Vector[Int]].head
// You can query optional metadata from the DataForge metastore
val originalFileName = spark.sql(s"SELECT source_file_name FROM meta.input WHERE input_id = $inputId").head().getString(0)
// session.filePath = The resulting raw file path of the completed previous ingestion process
val filePath = session.filePath

// ----- Create a function that returns a Spark DataFrame to pass back to DataForge
def parseDf(): DataFrame = {
  // Read the raw file path into a Spark DataFrame to transform
  val df = spark.read.option("header", true).csv(filePath)
  // Optional logging can be passed to DataForge logs using session.log()
  session.log(s"Parsing file from path $filePath")
  // Optional logic to transform and add two columns to the DataFrame of the file data
  val df2 = df.withColumn(customColumnName, now())
  .withColumn("original_file_name", lit(originalFileName))

  // ----- Return the DataFrame to close the function
  return df2
}

// ----- Complete the parse process by calling your function inside session.run()
// Marks process complete and successful or failed in DataForge
session.run(parseDf)