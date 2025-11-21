# DataForge SDK – Snowflake Python Examples

This folder contains runnable Snowflake Notebook examples that demonstrate how to build custom Ingestion, Parsing, and Post-Output processes using the DataForge SDK with Snowpark.

- PyPI: dataforge-sdk — https://pypi.org/project/dataforge-sdk/#description
- These examples assume a Snowflake Notebook runtime with Snowpark enabled and access to install `dataforge-sdk` and `stripe`.

## Getting started

1) Make the DataForge SDK available in your Snowflake Notebook:
- Install `dataforge-sdk` (and any extras such as `psycopg2`) using `pip` from within the notebook, as shown in the samples.

2) Run the examples from within DataForge or interactively in Snowflake. For interactive development in Snowflake, provide identifiers to give the notebook context (see the section below). For production runs triggered by DataForge (including schedules), the platform injects runtime context for you.

## Example scripts

- `custom-ingestion.py`
  - Demonstrates creating an `IngestionSession` and returning a Snowpark `DataFrame` to DataForge via `df_session.ingest(ingest_df)`.
  - Shows how to read Source-level custom parameters (for example, a `limit` value), connection parameters (Stripe API key), and latest tracking fields to build an incremental API pull.

- `custom-parse.py`
  - Demonstrates creating a `ParsingSession` and returning a transformed Snowpark `DataFrame` via `df_session.run(parse_df)`.
  - Shows how to access Parsing custom parameters, process parameters (e.g., input IDs), the stage file path produced by ingestion, and how to log into DataForge.

- `custom-post-output.py`
  - Demonstrates creating a `PostOutputSession` and running custom code via `df_session.run(post_output)`.
  - Shows how to access Output-level and Output Channel-level custom parameters as well as process parameters (e.g., `view_database`, `view_schema`, `view_name`).

- `custom-loopback.py`
  - Demonstrates a loopback pattern where a `PostOutputSession` triggers a downstream `IngestionSession` to ingest from a view produced by the output.
  - Shows how to read Output custom parameters and process parameters, then start a nested ingestion and return a Snowpark `DataFrame`.

## Running from Snowflake vs. Running from DataForge

These examples are authored as Snowflake notebooks. How you create the session depends on where the notebook is initiated from:

- When you run interactively in Snowflake (for development/testing), pass identifiers so the notebook has context about which DataForge configuration to use.
- When the notebook is triggered by DataForge (for production runs or schedules), do not pass identifiers—DataForge injects the proper runtime context.

Decision guide:

| Where you're running                          | IngestionSession                                               | ParsingSession                     | PostOutputSession                                                                       |
|-----------------------------------------------|----------------------------------------------------------------|------------------------------------|-----------------------------------------------------------------------------------------|
| Snowflake interactive development/testing      | `IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")` | `ParsingSession(<DataForge Input ID>)` | `PostOutputSession("<DataForge Output Name>", "<DataForge Output Source Name>", "<DataForge Project Name>")` |
| Triggered by DataForge (prod/schedule/MLOps)  | `IngestionSession()`                                           | `ParsingSession()`                 | `PostOutputSession()`                                                                   |

Examples:

- IngestionSession
  - Interactive (Snowflake):
    ```python
    # Provide context so the session knows which Source and Project to use
    df_session = IngestionSession("<DataForge Source Name>", "<DataForge Project Name>")
    ```
  - Production (triggered by DataForge):
    ```python
    # DataForge provides Source/Project context automatically
    df_session = IngestionSession()
    ```

- ParsingSession
  - Interactive (Snowflake):
    ```python
    # Provide the specific input (from a prior ingestion) to parse
    df_session = ParsingSession(<DataForge Input ID>)
    ```
  - Production (triggered by DataForge):
    ```python
    # DataForge provides the input context for the running parse process
    df_session = ParsingSession()
    ```

- PostOutputSession
  - Interactive (Snowflake):
    ```python
    # Provide the Output name, Output Source name, and Project name
    df_session = PostOutputSession("<DataForge Output Name>", "<DataForge Output Source Name>", "<DataForge Project Name>")
    ```
  - Production (triggered by DataForge):
    ```python
    # DataForge provides Output and Channel context automatically
    df_session = PostOutputSession()
    ```

Reusability tips:
- You can leave the identifiers in place even for production runs; DataForge will still provide context, and your explicit identifiers will work for that single configuration.
- However, removing the identifiers in production (`IngestionSession()`, `ParsingSession()`, `PostOutputSession()`) makes the notebook reusable across multiple DataForge Sources/Outputs. Combine this with custom parameters (Source/Parsing/Output/Channel) to keep your code generic and configuration-driven.

## Parameter access patterns

The examples include end-to-end demonstrations of how to access the different parameter types available in DataForge at runtime. Below is a quick reference tying directly to the code in this folder.

### 1) Custom parameters

- Source (Ingestion): `df_session.custom_parameters()`
  - Example (`custom-ingestion.py`):
    - `custom_params = df_session.custom_parameters() or {}`
    - `limit_val = custom_params.get("limit")`

- Parsing: `df_session.custom_parameters()`
  - Example (`custom-parse.py`):
    - `custom_column_name = df_session.custom_parameters()['new_column_name']`

- Output / Output Channel (Post-Output): `df_session.custom_parameters()`
  - Example (`custom-post-output.py`):
    - `custom_output_param = df_session.custom_parameters()['output']['customoutputparam']`
    - `custom_output_source_param = df_session.custom_parameters()['output_channel']['customoutputsourceparam']`

- Output (Loopback): `post_output_session.custom_parameters()`
  - Example (`custom-loopback.py`):
    - `destination_source_name = post_output_session.custom_parameters()['output']['destination_source_name']`

Notes:
- Keys correspond to the names you define in DataForge settings (Source Settings -> Ingestion/Parsing Parameters; Output Settings -> Output Parameters; Output Channel settings -> Output Channel Parameters).

### 2) Connection parameters (Custom Source Connection)

- Ingestion sources can attach a Custom Source Connection whose parameters are available at runtime. In the Stripe example, the API key is stored as a private connection parameter:

```python
df_connection_parameters = df_session.connection_parameters()
stripe_api_key = df_connection_parameters["private_connection_parameters"]["stripe_api_key"]
```

- Example usage (`custom-ingestion.py`) initializes the Stripe client with this API key and then performs an incremental pull.

Security tip: keep sensitive values in private parameters—do not log or persist them.

### 3) Process parameters (system-provided by DataForge)

- Parsing (`custom-parse.py`):
  - `input_id = df_session.process.parameters['input_ids'][0]`

- Post-Output (`custom-post-output.py`):
  - `view_database = df_session.process.parameters['view_database']`
  - `view_schema = df_session.process.parameters['view_schema']`
  - `view_name = df_session.process.parameters['view_name']`

- Loopback (`custom-loopback.py`):
  - `project_name = post_output_session.process.parameters['project_name']`

These are set by DataForge for the current running process and are useful for chaining operations.

### 4) Latest tracking fields (Ingestion)

- Ingestion (`custom-ingestion.py`):

```python
latest_tracking_fields = df_session.latest_tracking_fields()
current_input_id = latest_tracking_fields.get('input_id')
max_sequence_id = latest_tracking_fields.get('s_sequence') or '0'
max_timestamp = latest_tracking_fields.get('s_timestamp') or '1900-01-01 00:00:00'
last_pull_timestamp = latest_tracking_fields.get('extract_datetime') or '1900-01-01 00:00:00'
```

These help you implement incremental logic (e.g., range, timestamp, CDC) for your API-based ingestions.

### 5) File paths and metadata (Parsing)

- Parsing (`custom-parse.py`):
  - `stage_file_path = df_session.file_path` — stage path to the raw file emitted by ingestion.
  - `stage_folder_path = posixpath.splitext(stage_file_path)[0]` — folder path used for `INFER_SCHEMA` and `COPY INTO`.

### 6) Logging

All sessions support logging back to DataForge:

```python
df_session.log("message or SQL snippet here")
```

Examples:
- Ingestion logs the generated Stripe parameters (`custom-ingestion.py`).
- Parsing logs the stage path being parsed (`custom-parse.py`).
- Post-Output logs view location and custom parameter values (`custom-post-output.py`).

## Snowpark usage patterns

- The examples assume an active Snowpark `Session` obtained via `get_active_session()`.
- Ingestion (Stripe API): uses the Stripe Python SDK to pull data and then creates a Snowpark `DataFrame` with `session.create_dataframe(...)`.
- Parsing: uses Snowflake SQL (`CREATE FILE FORMAT`, `INFER_SCHEMA`, `COPY INTO`) plus Snowpark to build a typed `DataFrame`.
- Loopback: reads a view created by the output process, augments it (e.g., `current_timestamp()`), and re-ingests it via a nested `IngestionSession`.

## Running in DataForge

- Ingestion: call `df_session.ingest(your_fn_returning_df)`.
- Parsing: call `df_session.run(your_fn_returning_df)`.
- Post-Output: call `df_session.run(your_fn)`.
- Loopback: inside a `PostOutputSession`, create an `IngestionSession` and call `ingest()` with a function that returns a `DataFrame`.

On success, DataForge marks the process complete and advances downstream steps (e.g., CDC, outputs).

## Notes and best practices

- Parameter keys/names must exactly match your DataForge configuration.
- Keep secrets in private connection parameters; avoid logging credentials.
- Use tracking fields to implement robust incremental logic.
- Prefer referencing project/source/output names and IDs from `process.parameters` rather than hardcoding them.
- When developing, test initially against a development DataForge project and Snowflake environment.

## Links

- PyPI: dataforge-sdk — https://pypi.org/project/dataforge-sdk/#description
- This folder: `dataforge-sdk/snowflake/python/samples`
