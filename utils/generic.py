from dotenv import load_dotenv, dotenv_values


def load_environment_variables():
    """
    Load environment variables from a .env file and return them as a dictionary.

    This does two things:
    1. Reads key-value pairs directly from the `.env` file using `dotenv_values()`,
       allowing Python code to access variables programmatically as a dict.
    2. Loads the same variables into the process environment using `load_dotenv()`,
       which makes them available via `os.getenv()` and libraries that rely on
       environment variables (e.g., Spark, cloud SDKs, auth libraries).

    Returns
    -------
    dict
        A dictionary of environment variables loaded from `.env`.

    Notes
    -----
    - Values in the actual OS environment take precedence over `.env` entries.
    - Use `dotenv_values()` when you want the raw values inside Python.
    - Use `os.getenv()` when passing values to external tools (Kafka, Spark configs).

    Example
    -------
    >>> config = load_environment_variables()
    >>> print(config["KAFKA_BOOTSTRAP_SERVERS"])
    >>> import os
    >>> os.getenv("KAFKA_BOOTSTRAP_SERVERS")  # also available here after load_dotenv()

    """
    config = dotenv_values(".env")  # returns dict
    load_dotenv()  # Load into environment for os.getenv()
    return config


if __name__ == "__main__":
    load_environment_variables()
    