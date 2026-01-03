"""
Utility functions to initialize Spark sessions, read from Kafka topics,
and write streaming DataFrames to various sinks such as memory, console,
Parquet, and Delta Lake.

This module supports:
- Local Spark sessions with Delta & Kafka connectors
- Streaming ingestion from Kafka
- Streaming writes including foreachBatch Delta MERGE
- Helper decorators to inject Kafka environment variables

Intended for real-time data engineering experimentation.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, lit
from delta.tables import DeltaTable
from dotenv import load_dotenv
from functools import wraps,lru_cache
from types import SimpleNamespace
import os, uuid


def _get_spark_vars():
    """
    Load Spark-related configuration from environment variables.

    Currently only retrieves:
    - KAFKA_BOOTSTRAP_SERVERS

    Returns
    -------
    SimpleNamespace
        Contains resolved environment values as attributes.
    """
    load_dotenv()
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

    return SimpleNamespace(
        kafka_bootstrap_servers=kafka_bootstrap_servers
    )


def with_spark_vars(func):
    """
    Decorator that injects runtime Spark configuration into the function.

    The wrapped function receives a `sv` namespace instance with values
    retrieved from `_get_spark_vars()`.

    Example
    -------
    >>> @with_spark_vars
    ... def foo(sv):
    ...     print(sv.kafka_bootstrap_servers)
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        sv = _get_spark_vars()
        return func(sv, *args, **kwargs)
    return wrapper

@lru_cache(maxsize=1)
def start_spark_session() -> SparkSession:
    """
    Create and start a local Spark session configured for:
    - Structured Streaming
    - Kafka Source (spark-sql-kafka)
    - Delta Lake support
    - Reduced shuffle partitions for local workloads

    Returns
    -------
    SparkSession
        A fully initialized Spark session.

    Notes
    -----
    - This config uses local[*] and should not be used in production.
    - Automatically injects Maven coordinates using `spark.jars.packages`.
    """
    app_name = f"spark_session_{str(uuid.uuid4())[:8]}"
    print(f"Starting Spark App: {app_name}")

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.12:3.2.0",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "org.apache.kafka:kafka-clients:3.5.0",
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0"
            ])
        )
        # Enable Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Performance tuning for local use
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.schemaInference", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Spark session initialized successfully with Delta support.")
    return spark


@with_spark_vars
def read_kafka(
        sv,
        spark: SparkSession,
        topic: str,
        schema,
        startingOffsets: str = "earliest"
    ) -> DataFrame:
    """
    Create a Structured Streaming DataFrame from a Kafka topic and parse JSON payloads.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    topic : str
        Kafka topic to subscribe to
    schema : StructType
        Schema to apply when parsing JSON records
    startingOffsets : str, optional
        Where to start reading data: earliest, latest, or specific offsets

    Returns
    -------
    DataFrame
        A streaming DataFrame with parsed JSON fields.

    Notes
    -----
    Returned DataFrame is a STREAM — not a static DataFrame — and must be
    consumed via `.writeStream`.
    """
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", sv.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", startingOffsets)
        .load()
    )

    df = (
        raw_df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    return df

def write_spark_stream(
    df: DataFrame,
    spark: SparkSession,
    topic: str,
    write_to: str = "memory",
    trigger_mode: str = "once",
    interval_seconds: int = 5,
    output_path: str = None,
    output_mode: str = "append",
    merge_keys: list = None
):
    """
    Write a streaming DataFrame to a sink.

    Parameters
    ----------
    df : DataFrame
        Streaming DataFrame
    spark : SparkSession
        Active Spark session
    topic : str
        Name to register for memory sink OR logical stream ID
    write_to : str
        Output sink: 'memory', 'console', 'parquet', or 'delta'
    trigger_mode : str
        'once', 'processingTime', or 'continuous'
    interval_seconds : int
        Frequency when using processingTime trigger
    output_path : str
        Path to write output when using parquet or delta
    output_mode : str
        append | update | complete
    merge_keys : list
        Keys used to merge into an existing Delta table

    Returns
    -------
    StreamingQuery
        Active Structured Streaming query handle.

    Notes
    -----
    When writing to Delta w/ merge_keys, foreachBatch is used to execute MERGE.
    """
    # Determine trigger type
    if trigger_mode == "once":
        trigger = {"once": True}
    elif trigger_mode == "processingTime":
        trigger = {f"processingTime": f"{interval_seconds} seconds"}
    elif trigger_mode == "continuous":
        trigger = {"continuous": "1 second"}
    else:
        raise ValueError(f"Invalid trigger_mode: {trigger_mode}")

    # Console/memory sinks (including continuous mode)
    if write_to in ["memory", "console"] or trigger_mode == "continuous":
        write_to = "console" if trigger_mode == "continuous" else write_to

        return (
            df.writeStream
            .format(write_to)
            .queryName(topic)
            .outputMode("append")
            .trigger(**trigger)
            .start()
        )

    # Parquet / Delta sinks
    elif write_to in ["parquet", "delta"]:
        if output_path is None:
            raise ValueError("output_path must be specified for parquet and delta sinks.")

        checkpoint_location = os.path.join(output_path, "checkpoints")

        # Delta merge mode using foreachBatch
        if write_to == "delta" and merge_keys:
            delta_table = DeltaTable.forPath(spark, os.path.abspath(output_path))
            table_cols = spark.read.format("delta").load(output_path).columns
            df_cols = df.columns

            # Add missing columns so merge doesn't fail
            missing_cols = set(table_cols) - set(df_cols)
            for col_name in missing_cols:
                df = df.withColumn(col_name, lit(None))

            def merge_batch(df: DataFrame, epoch_id: int):
                
                # (
                # df.writeStream
                # .format("console")
                # # .outputMode("append") # or "update" or "complete"
                # .start()
                # # .awaitTermination()
                # # .foreachBatch(merge_batch)
                # # .option("checkpointLocation", checkpoint_location)
                # # .trigger(**trigger)
                # # .start()
                # )
                print(f"Number of records passed to foreachbatch - {df.count()}")
                try:
                    merge_condition = " AND ".join([f"target.{k}=source.{k}" for k in merge_keys])

                    (
                        delta_table.alias("target")
                        .merge(df.alias("source"), merge_condition)
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    print(f"MERGED BATCH {epoch_id}. ROWS: {df.count()}")

                except Exception as e:
                    print("foreachBatch FAILED:", e)
                    raise e  # <-- avoid silent errors

            query = (
                df.writeStream
                .foreachBatch(merge_batch)
                .option("checkpointLocation", checkpoint_location)
                .trigger(**trigger)
                .start()
            )
            print("Started merge stream with foreachBatch.")
            return query

        # Normal parquet/delta write
        return (
            df.writeStream
            .format(write_to)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode)
            .option("path", output_path)
            .trigger(**trigger)
            .start()
        )


def read_spark_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a Parquet dataset into a static Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    path : str
        Path to a Parquet directory

    Returns
    -------
    DataFrame
    """
    return spark.read.parquet(path)


def run_vacuum(spark: SparkSession, delta_path: str, retention_hours: int = 0):
    """
    Run VACUUM to clean up old files in a Delta table.

    Parameters
    ----------
    delta_path : str
        Path to the Delta table
    retention_hours : int, default 0
        Retention period for deleted files (0 = remove everything)

    Notes
    -----
    - Disables retention safety check intentionally for local testing.
    """
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    print(f"Running VACUUM on Delta table at: {delta_path}")

    deltaTable = DeltaTable.forPath(spark, delta_path)
    deltaTable.vacuum(retention_hours)


if __name__ == "__main__":
    spark = start_spark_session()
    raw_df = read_spark_parquet(spark, "data/nyctaxi/fhvhv_tripdata_2025-01.parquet")
    raw_df.show(5, truncate=False)
    print("\nRecord Count:")
    spark.sql("SELECT COUNT(*) FROM nyctaxistream").show()
