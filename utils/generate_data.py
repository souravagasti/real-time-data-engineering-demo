async def generate_clickstream_async(num_events: int, latency: float = 0.1):
    """
    Asynchronously generate a stream of synthetic clickstream events.

    This simulates web/app user interactions and emits events one at a time,
    mimicking real-time behavior using asyncio.sleep(latency).

    Event fields include:
    - user_id: Identifier for the user/session
    - timestamp: ISO-formatted timestamp of event occurrence
    - event_type: Type of interaction (page_view, click, search, add_to_cart, purchase)
    - page: The page the user is viewing (for page_view & click)
    - element: UI element clicked, e.g. a button (for click events)
    - query: Search query text (for search events)
    - product: Product interacted with (for add_to_cart or purchase events)
    - price: Price associated with product interactions
    - session/behavior elements can be extended for scroll depth, time spent, etc.

    Parameters
    ----------
    num_events : int
        Number of events to generate.
    latency : float, optional
        Seconds to wait between generated events to simulate streaming.

    Yields
    ------
    dict
        A single synthetic clickstream event.

    Notes
    -----
    This function is intended for streaming demos with Kafka producers or
    async pipelines. For batch generation, wrap with `list(...)`.
    """

    # mimicking clickstream data generation using the below lists
    userids = [1,2,3,4,5]
    event_types = ['page_view', 'click', 'search', 'add_to_cart', 'purchase']
    pages = ['/home', '/product', '/search', '/cart', '/checkout']
    products = ['widget', 'gadget', 'thingamajig', 'doodad', 'whatchamacallit']

    import random
    from datetime import datetime, timedelta
    import asyncio
    
    current_time = datetime.now()
    
    for _ in range(num_events):
        event = {}
        event['user_id'] = random.choice(userids)
        event['timestamp'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        event['event_type'] = random.choice(event_types)
        
        if event['event_type'] == 'page_view':
            event['page'] = random.choice(pages)
        elif event['event_type'] == 'click':
            event['page'] = random.choice(pages)
            event['element'] = f'button{random.randint(1,50)}'
        elif event['event_type'] == 'search':
            event['query'] = f'example search term{random.randint(1,10)}'
        elif event['event_type'] == 'add_to_cart':
            event['product'] = random.choice(products)
            event['price'] = round(random.uniform(10.0, 100.0), 2)
        elif event['event_type'] == 'purchase':
            event['product'] = random.choice(products)
            event['price'] = round(random.uniform(10.0, 100.0), 2)
        
        yield event
        await asyncio.sleep(latency)  # Wait for "latency" seconds before generating the next event
        current_time += timedelta(seconds=random.randint(1, 300))

async def generate_nyctaxistream_async(num_events: int = 1, latency: float = 0.1, **vars):
    """
    Asynchronously generate synthetic NYC taxi trip events.

    Event fields include:
    - hvfhs_license_num: Provider license identifier
    - dispatching_base_num: Fleet/base that dispatched the trip
    - pickup_datetime: Actual pickup timestamp
    - dropoff_datetime: Actual dropoff timestamp
    - PULocationID / DOLocationID: Zone identifiers (1--264)

    Parameters
    ----------
    num_events : int
        Number of events to stream.
    latency : float
        Seconds to wait between generated events.
    **vars :
        Optional overrides for field lists, useful for custom workloads.

    Yields
    ------
    dict
        A single structured taxi trip event.

    Notes
    -----
    - Timestamps are intentionally non-sequential to support tests for
      late-arriving data, watermark logic, and time-based windowing.
    - Use with Kafka producers for streaming demos, or collect into lists
      for batch tests / Delta ingestion.
    """

    import random, asyncio
    from datetime import datetime, timedelta
    from pyspark.sql.functions import to_timestamp

    try:
        # print(f"number of events: {num_events} and latency: {latency}")
        for _ in range(num_events):
            hvfhs_license_num = vars.get("hvfhs_license_num", "HV" + str(random.randint(1,9)).zfill(5))
            dispatching_base_num = vars.get("dispatching_base_num", random.choice(['B03404', 'B03406', 'B02510', 'B02764', 'B03288']))
            PULocationID = vars.get("PULocationID", random.randint(1,265))
            DOLocationID = vars.get("DOLocationID", random.randint(1,265))
            pickup_datetime_random_dt = datetime(2025, 1, 1) + timedelta(seconds=random.randint(0, 1800))
            pickup_datetime = vars.get("pickup_datetime",pickup_datetime_random_dt.strftime("%Y-%m-%d %H:%M:%S"))
            dropoff_datetime_random_dt = pickup_datetime_random_dt + timedelta(seconds=random.randint(0, 1800))
            dropoff_datetime = vars.get("dropoff_datetime",dropoff_datetime_random_dt.strftime("%Y-%m-%d %H:%M:%S"))
            event = {
                "hvfhs_license_num":hvfhs_license_num,
                "dispatching_base_num":dispatching_base_num,
                "PULocationID":PULocationID,
                "DOLocationID":DOLocationID,
                "pickup_datetime":pickup_datetime,
                "dropoff_datetime":dropoff_datetime,

            }
            yield event
            await asyncio.sleep(latency)
    
    except Exception as e:
        raise e

async def generate_locationid_temperature_async(num_events: int = 1, latency: float = 0.5, **vars):

    # for the location id passed, generate an event every minute of the days passed (1 per minute)

    import random, asyncio
    from datetime import datetime, timedelta

    try:
        for _ in range(1000):
            event = {
                "locationId":vars.get("locationId",random.randint(1,265)),
                "temperature": vars.get("temperature",round(random.uniform(10,30),2)),
                "record_datetime": vars.get("record_datetime",datetime(2025,1,1) + timedelta(seconds = random.randint(0,86400)))
            }

            yield event
            await asyncio.sleep(latency)
    except Exception as e:
        raise e

        