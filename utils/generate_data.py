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

async def generate_nyctaxistream_async(num_events: int, latency: float = 0.1, **vars):
    """
    Asynchronously generate synthetic NYC taxi trip events.

    Each event mimics real NYC TLC trip records including dispatch, pickup,
    dropoff times, locations, and trip metrics. Useful for streaming pipelines,
    join demos, watermarking tests, and schema evolution workloads.

    Event fields include:
    - hvfhs_license_num: Provider license identifier
    - dispatching_base_num: Fleet/base that dispatched the trip
    - originating_base_num: Base where trip originated
    - request_datetime: Time passenger requested a ride
    - on_scene_datetime: Time driver reached pickup
    - pickup_datetime: Actual pickup timestamp
    - dropoff_datetime: Actual dropoff timestamp
    - PULocationID / DOLocationID: Zone identifiers (1–264)
    - trip_miles: Distance traveled
    - trip_time: Duration in seconds

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

    import random
    from datetime import datetime, timedelta
    import asyncio
    
    # NYC Taxi Stream Data Generation Placeholders
    hvfhs_license_num = vars.get("hvfhs_license_num", ["HV" + str(i).zfill(5) for i in range(10)])
    dispatching_base_num = vars.get("dispatching_base_num", ['B03404', 'B03406', 'B02510', 'B02764', 'B03288'])
    originating_base_num = vars.get("originating_base_num", ['B03404', 'B03406', 'B02510', 'B02764', None])
    PULocationID = vars.get("PULocationID", list(range(1, 265)))
    DOLocationID = vars.get("DOLocationID", list(range(1, 265)))
    trip_miles = vars.get("trip_miles", [round(random.uniform(0.1, 30.0), 2) for _ in range(10)])
    trip_time = vars.get("trip_time", [random.randint(60, 7200) for _ in range(10)])
    request_datetime = vars.get("request_datetime", [datetime(2025, 1, 1) + timedelta(seconds=random.randint(0, 86400)) for _ in range(10)])
    on_scene_datetime = vars.get("on_scene_datetime", [req + timedelta(seconds=random.randint(0, 1800)) for req in request_datetime])
    pickup_datetime = vars.get("pickup_datetime", [onscene + timedelta(seconds=random.randint(0, 1800)) for onscene in on_scene_datetime])
    dropoff_datetime = vars.get("dropoff_datetime", [pickup + timedelta(seconds=random.randint(60, 7200)) for pickup in pickup_datetime])

    for _ in range(num_events):
        event = {
            "hvfhs_license_num": random.choice(hvfhs_license_num),
            "dispatching_base_num": random.choice(dispatching_base_num),
            "originating_base_num": random.choice(originating_base_num),
            "request_datetime": random.choice(request_datetime).strftime('%Y-%m-%d %H:%M:%S'),
            "on_scene_datetime": random.choice(on_scene_datetime).strftime('%Y-%m-%d %H:%M:%S'),
            "pickup_datetime": random.choice(pickup_datetime).strftime('%Y-%m-%d %H:%M:%S'),
            "dropoff_datetime": random.choice(dropoff_datetime).strftime('%Y-%m-%d %H:%M:%S'),
            "PULocationID": random.choice(PULocationID),
            "DOLocationID": random.choice(DOLocationID),
            "trip_miles": round(random.choice(trip_miles), 2),
            "trip_time": random.choice(trip_time)
        }
        yield event
        await asyncio.sleep(latency)

