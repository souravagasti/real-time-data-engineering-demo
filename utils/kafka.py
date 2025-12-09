import asyncio,json,time,os
from kafka import KafkaProducer
from utils.kafka_admin import KafkaAdmin
from dotenv import load_dotenv
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
    Publish messages to a Kafka topic using an asynchronous message generator.

    Parameters
    ----------
    kv : SimpleNamespace
        Injected by @with_kafka_vars. Contains:
        - kafka_bootstrap_servers : str
        - kafka : KafkaAdmin
        - producer : KafkaProducer
    topic : str
        Name of the Kafka topic to write to.
    message_generator : AsyncIterator[dict]
        Asynchronous iterator that yields message dictionaries.

    Notes
    -----
    - Messages are serialized as JSON using the configured `KafkaProducer`.
    - Each iteration awaits briefly (sleep(0)) to yield control to the event loop.
    - The Kafka producer is flushed and closed when the coroutine completes.
      (Avoid using the same producer instance across multiple concurrent writers.)
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

@with_kafka_vars
def clean_reset_kafka_topics(kv,topicnames: list[str]|None=None):
    """
    Delete and recreate specified Kafka topics.
    Deletes ALL topics if topicnames is empty

    Parameters
    ----------
    topics : list[str]
        List of topic names to reset.
    """
    if not topicnames:
        topics = list_kafka_topics()
    else:
        topics = topicnames

    if len(topics) > 0:
        print("Resetting kafka topics...")
        for topic in topics:
            print(f"Deleting topic {topic}")
            delete_kafka_topic(topic)

        for _ in range(10):  # up to ~10 seconds
            time.sleep(1)
            remaining = list_kafka_topics()
            if not any(t in topics for t in remaining):
                break
            else:
                print(f"Topics still pending deletion, continuing anyway ({_})...")

    if topicnames:
        for topicname in topicnames:
            print(f"Creating Kafka topic {topicname}...")  
            create_kafka_topic(topicname)
        close_kafka_admin()
        print("Kafka topics reset complete.")