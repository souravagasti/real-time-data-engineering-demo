# Real-Time NYC Taxi Streaming with Kafka & Spark

This repository is a hands-on playground for learning real-time data engineering using:

- Apache Kafka for event ingestion  
- Apache Spark Structured Streaming for processing  
- Delta Lake for storage and replay  
- Python utilities and Jupyter notebooks for an interactive workflow  

The example data is inspired by NYC Taxi trips, but the patterns apply to any real-time pipeline.

---

## Features

### Implemented

- Kafka producer for NYC taxi–like events  
- Spark Structured Streaming consumer  
- Multiple sinks: memory and Delta  
- Checkpointing and trigger options  
- JSON parsing and schema utilities  
- Notebook-first workflow

### Planned (Phase 2)

- Watermarking & late data handling  
- Streaming aggregations  
- Windowed aggregations  
- Stream–stream joins  
- Stream–batch joins  
- Schema evolution (JSON + Delta)  
- Common errors & troubleshooting

---

## Repository Structure

```text
.
├── notebooks/
│   ├── nyctaxistream/
│   │   ├── NB_produce_kafka_nyctaxistream.ipynb
│   │   ├── NB_consume_spark_nyctaxistream.ipynb
│   │   └── NB_write_nyctaxi_raw_to_delta.ipynb
│   └── clickstream/
│       └── NB_consume_clickstream.ipynb
│
├── utils/
│   ├── __init__.py
│   ├── generate_data.py
│   ├── utils_generic.py
│   ├── utils_kafka.py
│   ├── utils_kafka_admin.py
│   └── utils_spark.py
│
├── docker-compose.yml
├── Dockerfile.spark
├── pyproject.toml
├── LICENSE
└── README.md
```

---

## Quickstart

### Prerequisites

Install:

- Docker + Docker Compose  
- Python 3.10+  
- Java 11+ and Spark 3.5+

Create `.env` (not committed) based on `.env.example`:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DELTA_PATH_NYCTAXI=/tmp/delta/nyctaxi
RAW_PATH_NYCTAXI=/tmp/raw/nyctaxi
SCHEMA_NYCTAXI=/path/to/schema.json
WRITE_FOLDER_PATH_NYCTAXI=/tmp/nyctaxi_write
MERGE_KEYS_NYCTAXI=vendor_id,pickup_datetime
RAW_FILE_NAME=nyctaxi_raw.json
```

---

### Start Kafka services

```bash
docker compose up -d
```

---

### Produce Events (Notebook)

Open:

notebooks/nyctaxistream/NB_produce_kafka_nyctaxistream.ipynb

---

### Consume Events with Spark

Open:

notebooks/nyctaxistream/NB_consume_spark_nyctaxistream.ipynb

---

## Stop docker services

```bash
	docker compose down
```

---

## License

MIT
