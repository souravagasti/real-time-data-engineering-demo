from kafka import KafkaProducer
from utils.kafka_admin import KafkaAdmin
import asyncio
import json
from dotenv import load_dotenv
import os
from functools import wraps
from types import SimpleNamespace


def _get_kafka_vars():
    """
    Load Kafka configuration from environment variables and construct
    helper objects for producing and admin operations.

    This function:
    1. Loads environment variables from `.env` using `load_dotenv()`
    2. Retrieves the Kafka broker URL (KAFKA_BOOTSTRAP_SERVERS)
    3. Instantiates:
       - KafkaAdmin (for topic-level admin operations)
       - KafkaProducer (for producing JSON messages)

    Returns
    -------
    SimpleNamespace
        An object with attributes:
        - kafka_bootstrap_servers : str
        - kafka : KafkaAdmin
        - producer : KafkaProducer

    Notes
    -----
    This function is intended to be used internally via the decorator
    `@with_kafka_vars` to inject Kafka dependencies into functions.

    Environment Variables Required
    ------------------------------
    KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
    """
    load_dotenv()
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka = KafkaAdmin(bootstrap_servers=kafka_bootstrap_servers)
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return SimpleNamespace(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka=kafka,
        producer=producer
    )


def with_kafka_vars(func):
    """
    Decorator that injects Kafka-related helper objects into a function.

    The wrapped function will receive a `kv` argument (SimpleNamespace)
    containing:
    - kafka_bootstrap_servers
    - kafka (KafkaAdmin)
    - producer (KafkaProducer)

    This avoids repeated initialization across multiple functions.

    Example
    -------
    >>> @with_kafka_vars
    ... def list_topics(kv):
    ...     return kv.kafka.list_topics()
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        kv = _get_kafka_vars()
        return func(kv, *args, **kwargs)
    return wrapper


@with_kafka_vars
async def write_to_kafka_async(kv, topic: str, message_generator: iter):
    """
    Asynchronously write messages to a Kafka topic using a generator.

    Parameters
    ----------
    kv : SimpleNamespace
        Injected via decorator; contains Kafka producer/admin instances.
    topic : str
        Kafka topic to publish messages to.
    message_generator : async iterator
        Yields event dictionaries to publish to Kafka.

    Notes
    -----
    - Messages are serialized as JSON
    - Producer is flushed and closed automatically
    - `await asyncio.sleep(0)` yields control back to the event loop

    Example
    -------
    >>> async for event in generate_clickstream_async(100):
    ...     await write_to_kafka_async("clickstream", event)
    """
    try:
        async for message in message_generator:
            kv.producer.send(topic, value=message)
            print("Generated:", message)
            await asyncio.sleep(0)
    finally:
        kv.producer.flush()
        kv.producer.close()


@with_kafka_vars
def list_kafka_topics(kv):
    """
    List all Kafka topics in the cluster.

    Returns
    -------
    list[str]
        A list of topic names.
    """
    topics = kv.kafka.list_topics()
    print(__name__, topics)
    return topics


@with_kafka_vars
def delete_kafka_topic(kv, topic: str):
    """
    Delete a Kafka topic if it exists.

    Parameters
    ----------
    topic : str
        Name of the topic to delete.
    """
    kv.kafka.delete_topic(topic)


@with_kafka_vars
def create_kafka_topic(kv, topic: str, num_partitions: int = 1, replication_factor: int = 1):
    """
    Create a Kafka topic.

    Parameters
    ----------
    topic : str
        Topic name.
    num_partitions : int, optional
        Number of partitions.
    replication_factor : int, optional
        Replication factor across brokers.
    """
    kv.kafka.create_topic(topic, num_partitions=num_partitions, replication_factor=replication_factor)


@with_kafka_vars
def close_kafka_admin(kv):
    """
    Close the underlying KafkaAdmin client connection.

    Useful for manual teardown when not relying on async finalization.
    """
    kv.kafka.close()
