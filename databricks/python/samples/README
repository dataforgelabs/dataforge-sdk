# DataForge SDK – Python Examples

This folder contains runnable examples that demonstrate how to build custom Ingestion, Parsing, and Post-Output processes using the DataForge SDK.

- PyPI: dataforge-sdk — https://pypi.org/project/dataforge-sdk/#description
- These examples assume a Spark runtime (e.g., Databricks or a Spark cluster) available in the DataForge execution environment.

## Getting started

1) Make the DataForge SDK available on your Databricks cluster:
- Install `dataforge-sdk` as a Cluster Library (via Libraries > Install New > PyPI) so all notebook runs on that cluster have the SDK available. Alternatively, include it via a cluster init script or cluster policy.

2) Run the examples from within DataForge or interactively in Databricks. For interactive development in Databricks, provide identifiers to give the notebook context (see the section below). For production runs triggered by DataForge (including schedules), the platform injects runtime context for you.

## Example scripts

- `samples/custom-ingestion.py`
  - Demonstrates creating an `IngestionSession` and returning a DataFrame to DataForge via `df_session.ingest(ingest_df)`.
  - Shows how to read Source-level custom parameters, connection parameters (for example, a Stripe API key), and latest tracking fields to build an incremental API pull.

- `samples/custom-parse.py`
  - Demonstrates creating a `ParsingSession` and returning a transformed DataFrame via `session.run(parse_df)`.
  - Shows how to access Parsing custom parameters, process parameters (e.g., input IDs), the source file path produced by ingestion, and how to log into DataForge.

- `samples/custom-post-output.py`
  - Demonstrates creating a `PostOutputSession` and running custom code via `session.run(post_output)`.
  - Shows how to access Output-level and Output Channel-level custom parameters as well as process parameters (e.g., `view_database`, `view_name`).

- `samples/custom-loopback.py`
  - Demonstrates a loopback pattern where a `PostOutputSession` triggers a downstream `IngestionSession` to ingest from a view produced by the output.
  - Shows how to read Output custom parameters and process parameters, then start a nested ingestion and return a DataFrame.

## Running from Databricks vs. Running from DataForge

These examples are authored as Databricks notebooks. How you create the session depends on where the notebook is initiated from:

- When you run interactively in Databricks (for development/testing), pass identifiers so the notebook has context about which DataForge configuration to use.
- When the notebook is triggered by DataForge (for production runs or schedules), do not pass identifiers—DataForge injects the proper runtime context.

Decision guide:

| Where you're running                          | IngestionSession                               | ParsingSession                 | PostOutputSession                                                   |
|-----------------------------------------------|------------------------------------------------|--------------------------------|----------------------------------------------------------------------|
| Databricks interactive development/testing    | `IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")` | `ParsingSession(<DataForge Input ID>)`  | `PostOutputSession("<DataForge Output Name>", "<DataForge Output Channel Name>", "<DataForge Project Name>")`        |
| Triggered by DataForge (prod/schedule/MLOps)  | `IngestionSession()`                            | `ParsingSession()`             | `PostOutputSession()`                                               |

Examples:

- IngestionSession
  - Interactive (Databricks):
    ```python
    # Provide context so the session knows which Source and Project to use
    session = IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")
    ```
  - Production (triggered by DataForge):
    ```python
    # DataForge provides Source/Project context automatically
    session = IngestionSession()
    ```

- ParsingSession
  - Interactive (Databricks):
    ```python
    # Provide the specific input (from a prior ingestion) to parse
    session = ParsingSession(<DataForge Input ID>)
    ```
  - Production (triggered by DataForge):
    ```python
    # DataForge provides the input context for the running parse process
    session = ParsingSession()
    ```

- PostOutputSession
  - Interactive (Databricks):
    ```python
    # Provide the Output name, Output Channel name, and Project name
    session = PostOutputSession("<DataForge Output Name>", "<DataForge Output Channel Name>", "<DataForge Project Name>")
    ```
  - Production (triggered by DataForge):
    ```python
    # DataForge provides Output and Channel context automatically
    session = PostOutputSession()
    ```

Reusability tips:
- You can leave the identifiers in place even for production runs; DataForge will still provide context, and your explicit identifiers will work for that single configuration.
- However, removing the identifiers in production (`IngestionSession()`, `ParsingSession()`, `PostOutputSession()`) makes the notebook reusable across multiple DataForge Sources/Outputs. Combine this with custom parameters (Source/Parsing/Output/Channel) to keep your code generic and configuration-driven.

## Parameter access patterns

The examples include end-to-end demonstrations of how to access the different parameter types available in DataForge at runtime. Below is a quick reference tying directly to the code in this folder.

### 1) Custom parameters

- Source (Ingestion): `df_session.custom_parameters()`
  - Example (`samples/custom-ingestion.py`):
    - `custom_params = df_session.custom_parameters() or {}`
    - `limit_val = custom_params.get("limit")`

- Parsing: `session.custom_parameters()`
  - Example (`samples/custom-parse.py`):
    - `custom_column_name = session.custom_parameters()['new_column_name']`

- Output / Output Channel (Post-Output): `session.custom_parameters()`
  - Example (`samples/custom-post-output.py`):
    - `custom_output_param = session.custom_parameters()['output']['customoutputparam']`
    - `custom_output_source_param = session.custom_parameters()['output_channel']['customoutputsourceparam']`

- Output (Loopback): `post_output_session.custom_parameters()`
  - Example (`samples/custom-loopback.py`):
    - `destination_source_name = post_output_session.custom_parameters()['output']['destination_source_name']`

Notes:
- Keys correspond to the names you define in DataForge settings (Source Settings -> Ingestion/Parsing Parameters; Output Settings -> Output Parameters; Output Channel settings -> Output Channel Parameters).

### 2) Connection parameters (Custom Source Connection)

- Ingestion sources can attach a Custom Source Connection whose parameters are available at runtime. In the Stripe example, the API key is stored as a private connection parameter:

```python
connection_parameters = df_session.connection_parameters()
stripe_api_key = connection_parameters["private_connection_parameters"]["stripe_api_key"]
```

- Example usage (`samples/custom-ingestion.py`) initializes the Stripe client with this API key and then performs an incremental pull.

Security tip: keep sensitive values in private parameters—do not log or persist them.

### 3) Process parameters (system-provided by DataForge)

- Parsing (`samples/custom-parse.py`):
  - `input_id = session.process.parameters['input_ids'][0]`

- Post-Output (`samples/custom-post-output.py`):
  - `view_database = session.process.parameters['view_database']`
  - `view_name = session.process.parameters['view_name']`

- Loopback (`samples/custom-loopback.py`):
  - `project_name = post_output_session.process.parameters['project_name']`

These are set by DataForge for the current running process and are useful for chaining operations.

### 4) Latest tracking fields (Ingestion)

- Ingestion (`samples/custom-ingestion.py`):

```python
latest_tracking_fields = session.latest_tracking_fields()
current_input_id = latest_tracking_fields['input_id']
max_sequence_id = latest_tracking_fields.get('s_sequence') or '0'
max_timestamp = latest_tracking_fields.get('s_timestamp') or '1900-01-01 00:00:00'
last_pull_timestamp = latest_tracking_fields.get('extract_datetime') or '1900-01-01 00:00:00'
```

These help you implement incremental logic (e.g., range, timestamp, CDC).

### 5) File paths and metadata (Parsing)

- Parsing (`samples/custom-parse.py`):
  - `file_path = session.file_path` — path to the raw file emitted by ingestion.
  - Optional metastore query example:
    - `original_file_name = spark.sql("SELECT source_file_name FROM meta.input WHERE input_id = {input_id}").head()[0]`

### 6) Logging

All sessions support logging back to DataForge:

```python
session.log("message or SQL snippet here")
```

Examples:
- Ingestion logs the generated SQL statement (`samples/custom-ingestion.py`).
- Parsing logs the file path being parsed (`samples/custom-parse.py`).
- Post-Output logs view location and custom parameter values (`samples/custom-post-output.py`).

## Spark usage patterns

- The examples assume a SparkSession named `spark` is available in the runtime.
- Ingestion (API): demonstrates using a Stripe API key stored as a private connection parameter.
- Parsing (CSV): demonstrates `spark.read.option("header", True).csv(file_path)` and simple column additions with `pyspark.sql.functions`.
- Loopback: reads a view created by the output process, augments it (e.g., `current_timestamp()`), and re-ingests it via a nested `IngestionSession`.

## Running in DataForge

- Ingestion: call `session.ingest(your_fn_returning_df)`.
- Parsing: call `session.run(your_fn_returning_df)`.
- Post-Output: call `session.run(your_fn)`.
- Loopback: inside a `PostOutputSession`, create an `IngestionSession` and call `ingest()` with a function that returns a DataFrame.

On success, DataForge marks the process complete and advances downstream steps (e.g., CDC, outputs).

## Notes and best practices

- Parameter keys/names must exactly match your DataForge configuration.
- Keep secrets in private parameters; avoid logging credentials.
- Use tracking fields to implement robust incremental logic.
- Prefer referencing project/source/output names and IDs from `process.parameters` rather than hardcoding them.
- When developing locally, stub `spark` and parameter providers or run against a development DataForge project.

## Links

- PyPI: dataforge-sdk — https://pypi.org/project/dataforge-sdk/#description
- This folder: `dataforge-sdk/databricks/python/samples`