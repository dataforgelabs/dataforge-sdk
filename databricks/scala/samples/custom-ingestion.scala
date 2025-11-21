// ----- Import DataForge IngestionSession and other standard packages
import com.dataforgelabs.sdk._
import org.apache.spark.sql.{DataFrame, Row}
import com.stripe.Stripe
import com.stripe.model.Customer
import com.stripe.param.CustomerListParams
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `iterable AsScalaIterable`}
import org.apache.spark.sql.Encoders
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import org.apache.spark.sql.types.{StructField, StructType, StringType}

// ----- Start new ingestion session in DataForge. Creates a new input and starts a new ingestion process in DataForge
val dfSession = new IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")

// Optional logic to show how to access different parameters
// dfSession.customParameters() = Custom parameters you can set in Source Settings Ingestion Parameters. Parameters are referenced by the Key name from the key:value pair in DataForge
val customParams = dfSession.customParameters
val limitVal = (customParams \ "limit").asOpt[Long]

// Latest Tracking Fields can be referenced to pull metadata from DataForge with information about what data has been pulled into the Source. Commonly used to generate incremental data pulls
// dfSession.latestTrackingFields = All available tracking fields. Not every field is populated for every source and depend on the Source Refresh Type
val trackingFields = dfSession.latestTrackingFields
// inputId references the DataForge input id that is generated as part of this Ingestion Session
val inputId = trackingFields.inputId
// sSequence = Max value from the Range Column defined in Source Settings for Sequence Refresh type
val maxSequenceId = trackingFields.sSequence.getOrElse("0")
// sTimestamp = Max value from the Date Column for Timestamp Refresh type or CDC Parameters date column for other Refresh types
val maxTimestamp = trackingFields.sTimestamp.getOrElse("1900-01-01 00:00:00")
// extract_datetime = UTC timestamp of the last time data was ingested in the DataForge source
val lastPullTimestamp = trackingFields.extractDatetime.getOrElse("1900-01-01 00:00:00.000000")

// Convert last_pull_timestamp to unix for Stripe request
val lastPullDt = LocalDateTime.parse(lastPullTimestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
val lastPullUnix = lastPullDt.toEpochSecond(ZoneOffset.UTC)

// Initialize Stripe client using the API key stored in the DataForge custom connection private parameters
Stripe.apiKey = (dfSession.connectionParameters \ "stripe_api_key").as[String]

// ----- Create a function that returns a Spark DataFrame to pass back to DataForge
def ingestDf(): DataFrame = {
  // Call Stripe Customer List Params API with our incremental + optional limit params
  val customerParamsBuilder = CustomerListParams.builder()

  // This notebook always filter by created >= lastPullUnix
  customerParamsBuilder.setCreated(
    CustomerListParams.Created
      .builder()
      .setGte(Long.box(lastPullUnix))
      .build()
  )

  // Optional limit from DataForge custom parameters
  limitVal.foreach { limit => customerParamsBuilder.setLimit(limit)}

  // Build Customer Params before sending the request
  val customerParams = customerParamsBuilder.build()

  // Optional logging can be passed to DataForge logs using dfSession.log()
  dfSession.log(s"Stripe params being sent: Limit: $limitVal, created.gte >= $lastPullUnix")

  // Request data from Stripe
  val customers = Customer.list(customerParams)
    .autoPagingIterable() // Auto-iterate through all pages
    .asScala
    .map(_.toJson) // Convert Stripe customer object to JSON string
    .toSeq // Arrange json strings into list/sequence

  // Check if Stripe request returned any records. If not, create a dummy schema in an empty DataFrame to return
  if (customers.isEmpty) {
      val schema = StructType(
        Array(StructField("value", StringType, nullable = true))
      )
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

  // Create dataset in Spark to convert to a DataFrame
  val dataset = spark.createDataset(customers)

  // ----- Create a Spark DataFrame to return to the DataForge Custom Ingestion
  val df = spark.read.json(dataset)
  return df
}

// ----- Complete the Ingestion process by calling your function which returns a DataFrame inside dfSession.ingest()
// Marks ingestion process complete and successful or failed in DataForge. If successful, the input will move on to the next process needed such as Capture Data Changes
dfSession.ingest(ingestDf)
