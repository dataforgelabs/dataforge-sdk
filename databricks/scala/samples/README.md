# DataForge SDK – Scala Examples

This folder contains runnable examples (Databricks notebooks written in Scala) that demonstrate how to build custom Ingestion, Parsing, and Post-Output processes using the DataForge SDK.

- Maven: DataForge SDK — https://mvnrepository.com/artifact/com.dataforgelabs/dataforge-sdk
- These examples assume a Spark runtime (e.g., Databricks or a Spark cluster) available in the DataForge execution environment.

## Getting started

1) Make the DataForge SDK available on your Databricks cluster:
- Install the DataForge SDK library on the cluster so all notebook runs on that cluster have the SDK available (e.g., via Libraries > Install New > Maven coordinates, or by attaching a workspace-installed JAR, or via an init script/cluster policy as provided by your admin).

Example Maven coordinates:
```xml
<dependency>
  <groupId>com.dataforgelabs</groupId>
  <artifactId>dataforge-sdk</artifactId>
  <version>RELEASE</version>
</dependency>
```

Note for Databricks:
- Use Libraries > Install New > Maven and paste the coordinates above (replace VERSION/RELEASE with the specific version you want from the Maven page). Alternatively, include it via an init script or cluster policy.

2) Run the examples from within DataForge or interactively in Databricks. For interactive development in Databricks, you will provide identifiers to give the notebook context (see the section below). For production runs triggered by DataForge (including schedules), the platform injects runtime context for you.

## Example scripts

- `samples/custom-ingestion.scala`
  - Demonstrates creating an `IngestionSession` and returning a DataFrame to DataForge via `dfSession.ingest(ingestDf)`.
  - Shows how to read Source-level custom parameters, connection parameters (for example, a Stripe API key), and latest tracking fields to build an incremental API pull.

- `samples/custom-parse.scala`
  - Demonstrates creating a `ParsingSession` and returning a transformed DataFrame via `session.run(parseDf)`.
  - Shows how to access Parsing custom parameters, process parameters (e.g., input IDs), the source file path produced by ingestion, and how to log into DataForge.

- `samples/custom-post-output.scala`
  - Demonstrates creating a `PostOutputSession` and running custom code via `session.run(postOutput)`.
  - Shows how to access Output-level and Output Channel-level custom parameters as well as process parameters (e.g., `view_database`, `view_name`).

- `samples/custom-loopback.scala`
  - Demonstrates a loopback pattern where a `PostOutputSession` triggers a downstream `IngestionSession` to ingest from a view produced by the output.
  - Shows how to read Output custom parameters and process parameters, then start a nested ingestion and return a DataFrame.

## Running from Databricks vs. Running from DataForge

These examples are authored as Databricks notebooks. How you create the session depends on where the notebook is initiated from:

- When you run interactively in Databricks (for development/testing), pass identifiers so the notebook has context about which DataForge configuration to use.
- When the notebook is triggered by DataForge (for production runs or schedules), do not pass identifiers—DataForge injects the proper runtime context.

Decision guide:

| Where you're running                          | IngestionSession                                                                 | ParsingSession                         | PostOutputSession                                                                                          |
|-----------------------------------------------|----------------------------------------------------------------------------------|----------------------------------------|------------------------------------------------------------------------------------------------------------|
| Databricks interactive development/testing    | `new IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")` | `new ParsingSession(<DataForge Input ID>)` | `new PostOutputSession("<DataForge Output Name>", "<DataForge Output Channel Name>", "<DataForge Project Name>")` |
| Triggered by DataForge (prod/schedule/MLOps)  | `new IngestionSession()`                                                         | `new ParsingSession()`                 | `new PostOutputSession()`                                                                                  |

Examples:

- IngestionSession
  - Interactive (Databricks):
    ```scala
    // Provide context so the session knows which Source and Project to use
    val session = new IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")
    ```
  - Production (triggered by DataForge):
    ```scala
    // DataForge provides Source/Project context automatically
    val session = new IngestionSession()
    ```

- ParsingSession
  - Interactive (Databricks):
    ```scala
    // Provide the specific input (from a prior ingestion) to parse
    val session = new ParsingSession(<DataForge Input ID>)
    ```
  - Production (triggered by DataForge):
    ```scala
    // DataForge provides the input context for the running parse process
    val session = new ParsingSession()
    ```

- PostOutputSession
  - Interactive (Databricks):
    ```scala
    // Provide the Output name, Output Channel name, and Project name
    val session = new PostOutputSession("<DataForge Output Name>", "<DataForge Output Channel Name>", "<DataForge Project Name>")
    ```
  - Production (triggered by DataForge):
    ```scala
    // DataForge provides Output and Channel context automatically
    val session = new PostOutputSession()
    ```

Reusability tips:
- You can leave the identifiers in place even for production runs; DataForge will still provide context, and your explicit identifiers will work for that single configuration.
- However, removing the identifiers in production (`new IngestionSession()`, `new ParsingSession()`, `new PostOutputSession()`) makes the notebook reusable across multiple DataForge Sources/Outputs. Combine this with custom parameters (Source/Parsing/Output/Channel) to keep your code generic and configuration-driven.

## Parameter access patterns

The examples include end-to-end demonstrations of how to access the different parameter types available in DataForge at runtime. Below is a quick reference tying directly to the Scala code in this folder.

### 1) Custom parameters

- Source (Ingestion): `dfSession.customParameters`
  - Example (`samples/custom-ingestion.scala`):
    ```scala
    val customParams = dfSession.customParameters
    val limitVal = (customParams \\ "limit").asOpt[Long]
    ```

- Parsing: `session.customParameters`
  - Example (`samples/custom-parse.scala`):
    ```scala
    val customColumnName = (session.customParameters \ "new_column_name").as[String]
    ```

- Output / Output Channel (Post-Output): `session.customParameters`
  - Example (`samples/custom-post-output.scala`):
    ```scala
    val customOutputParam = (session.customParameters \ "output" \ "customoutputparam").as[String]
    val customOutputSourceParam = (session.customParameters \ "output_channel" \ "customoutputsourceparam").as[String]
    ```

- Output (Loopback): `postOutputSession.customParameters`
  - Example (`samples/custom-loopback.scala`):
    ```scala
    val destinationSourceName = (postOutputSession.customParameters \ "output" \ "destination_source_name").as[String]
    ```

Notes:
- Keys correspond to the names you define in DataForge settings (Source Settings -> Ingestion/Parsing Parameters; Output Settings -> Output Parameters; Output Channel settings -> Output Channel Parameters).

### 2) Connection parameters (Custom Source Connection)

- Ingestion sources can attach a Custom Source Connection whose parameters are available at runtime. In the Stripe example, the API key is stored as a secure parameter:

```scala
val connectionParameters = dfSession.connectionParameters
val stripeApiKey = (connectionParameters \ "stripe_api_key").as[String]
```

- Example usage (`samples/custom-ingestion.scala`) initializes the Stripe client with this API key and then performs an incremental pull.

Security tip: keep sensitive values (e.g., API keys) out of logs and do not persist them.

### 3) Process parameters (system-provided by DataForge)

- Parsing (`samples/custom-parse.scala`):
  ```scala
  val inputId = (session.processParameters \ "input_ids").as[Vector[Int]].head
  ```

- Post-Output (`custom-post-output.scala`):
  ```scala
  val viewDatabase = (session.processParameters \ "view_database").as[String]
  val viewName     = (session.processParameters \ "view_name").as[String]
  ```

- Loopback (`custom-loopback.scala`):
  ```scala
  val projectName = (postOutputSession.processParameters \ "project_name").as[String]
  ```

These are set by DataForge for the current running process and are useful for chaining operations.

### 4) Latest tracking fields (Ingestion)

- Ingestion (`custom-ingestion.scala`):

```scala
val trackingFields    = session.latestTrackingFields
val currentInputId    = trackingFields.inputId
val maxSequenceId     = trackingFields.sSequence.getOrElse("0")
val maxTimestamp      = trackingFields.sTimestamp.getOrElse("1900-01-01 00:00:00")
val lastPullTimestamp = trackingFields.extractDatetime.getOrElse("1900-01-01 00:00:00")
```

These help you implement incremental logic (e.g., range, timestamp, CDC).

### 5) File paths and metadata (Parsing)

- Parsing (`custom-parse.scala`):
  - `val filePath = session.filePath` — path to the raw file emitted by ingestion.
  - Optional metastore query example:
    ```scala
    val originalFileName = spark.sql(s"SELECT source_file_name FROM meta.input WHERE input_id = $inputId").head().getString(0)
    ```

### 6) Logging

All sessions support logging back to DataForge:

```scala
session.log("message or SQL snippet here")
```

Examples:
- Ingestion logs the generated SQL statement (`custom-ingestion.scala`).
- Parsing logs the file path being parsed (`custom-parse.scala`).
- Post-Output logs view location and custom parameter values (`custom-post-output.scala`).

## Spark usage patterns

- The examples assume a SparkSession named `spark` is available in the runtime.
- Ingestion (JDBC): demonstrates `spark.read.format("jdbc").option(...).load()` using connection parameters and incremental predicates.
- Parsing (CSV): demonstrates `spark.read.option("header", true).csv(filePath)` and simple column additions with `org.apache.spark.sql.functions`.
- Loopback: reads a view created by the output process, augments it (e.g., `current_timestamp()`), and re-ingests it via a nested `IngestionSession`.

## Running in DataForge

- Ingestion: call `session.ingest(yourFnReturningDf)`.
- Parsing: call `session.run(yourFnReturningDf)`.
- Post-Output: call `session.run(yourFn)`.
- Loopback: inside a `PostOutputSession`, create an `IngestionSession` and call `ingest()` with a function that returns a DataFrame.

On success, DataForge marks the process complete and advances downstream steps (e.g., CDC, outputs).

## Notes and best practices

- Parameter keys/names must exactly match your DataForge configuration.
- Keep secrets in secure parameters; avoid logging credentials.
- Use tracking fields to implement robust incremental logic.
- Prefer referencing project/source/output names and IDs from `processParameters` rather than hardcoding them.
- When developing locally, stub `spark` and parameter providers or run against a development DataForge project.