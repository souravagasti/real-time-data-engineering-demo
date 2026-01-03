class Startup:
    def __init__(self):
        import time, os
        from datetime import datetime, timedelta
        from utils.generic import load_environment_variables
        from utils.generate_data import generate_nyctaxistream_async,generate_locationid_temperature_async
        from utils.kafka import write_to_kafka_async,list_kafka_topics,delete_kafka_topic,create_kafka_topic,close_kafka_admin,clean_reset_kafka_topics
        from utils.spark import start_spark_session,read_kafka,read_spark_parquet,write_spark_stream
        import asyncio
        from delta.tables import DeltaTable
        from dotenv import load_dotenv
        from pyspark.sql.functions import to_timestamp

        # load env vars
        self.config = load_environment_variables()
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.delta_path = os.getenv("DELTA_PATH_NYCTAXI")
        self.write_folder_path = os.getenv("WRITE_FOLDER_PATH_NYCTAXI")
        self.schema = os.getenv("SCHEMA_NYCTAXI")
        self.merge_keys = os.getenv("MERGE_KEYS_NYCTAXI").split(",")
        self.raw_file_path = os.path.join(os.getenv("RAW_PATH_NYCTAXI"),os.getenv("RAW_FILE_NAME"))

        clean_reset_kafka_topics()
        self.spark = start_spark_session()
        ...