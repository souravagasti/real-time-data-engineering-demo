from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError


class KafkaAdmin:
    """
    A convenience wrapper around KafkaAdminClient to manage Kafka topics.

    This class provides high-level helper methods to create, delete,
    and list topics in a Kafka cluster. Useful for local development,
    streaming demos, and automated setup scripts.

    Parameters
    ----------
    bootstrap_servers : str
        Kafka broker address (e.g., "localhost:9092").

    Examples
    --------
    >>> admin = KafkaAdmin("localhost:9092")
    >>> admin.create_topic("events")
    >>> admin.list_topics()
    ['events', 'nyc_taxi_trips']
    """

    def __init__(self, bootstrap_servers: str):
        self.BOOTSTRAP_SERVERS = bootstrap_servers
        self.admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1) -> None:
        """
        Create a Kafka topic if it does not already exist.

        Parameters
        ----------
        topic_name : str
            Name of the Kafka topic.
        num_partitions : int, optional
            Number of partitions to create, defaults to 1.
        replication_factor : int, optional
            Replication factor across brokers, defaults to 1.

        Notes
        -----
        - No error is raised if the topic already exists; a message is printed.
        - Partition count cannot be reduced once created.

        """
        try:
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            self.admin.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{topic_name}' created")
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' already exists")

    def delete_topic(self, topic_name: str) -> None:
        """
        Delete a Kafka topic if it exists.

        Parameters
        ----------
        topic_name : str
            Name of the Kafka topic to delete.

        Notes
        -----
        - Deletion must be enabled on the Kafka broker (`delete.topic.enable=true`).
        """
        try:
            self.admin.delete_topics(topics=[topic_name])
            print(f"Topic '{topic_name}' deleted")
        except UnknownTopicOrPartitionError:
            print(f"Topic '{topic_name}' does not exist")

    def list_topics(self) -> list[str]:
        """
        List all Kafka topics in the cluster.

        Returns
        -------
        list[str]
            A list of topic names.
        """
        return self.admin.list_topics()

    def close(self) -> None:
        """
        Close the underlying Kafka admin client connection.
        """
        self.admin.close()


if __name__ == "__main__":
    kafka = KafkaAdmin("localhost:9092")
    kafka.delete_topic("clickstream")
    kafka.create_topic("clickstream")
    print(kafka.list_topics())
    kafka.close()
