import json
import random
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
from pydantic import ValidationError
from ingestion.models import TheatreSeed, ShowSeatSnapshotEvent, SeatCategorySnapshot

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "raw.cinema.seat_snapshots"

def delivery_report(err, msg):
    """Callback function to confirm message delivery to Kafka."""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Produced event to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

def create_kafka_producer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'linger.ms': 50,
        'batch.num.messages': 1000
    }
    return Producer(conf)

def load_theatres():
    seed_path = "ingestion/seeds/theatres_seed.json"
    with open(seed_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    # Validate via Pydantic model
    theatres = []
    for item in raw_data:
        try:
            theatres.append(TheatreSeed(**item)) ##The ** operator unpacks a dictionary into keyword arguments.
        except ValidationError as e:
            print(f"❌ Invalid theatre seed found: {e}")
    return theatres

def simulate_streaming():
    producer = create_kafka_producer()
    theatres = load_theatres()
    
    if not theatres:
        print("❌ No valid theatres loaded. Exiting simulator.")
        return

    movies = [
        {"movie_id": "mov_kantara_prequel", "title": "Kantara: Legend Chapter 1"},
        {"movie_id": "mov_war_2", "title": "War 2"},
        {"movie_id": "mov_ram_ayana", "title": "Ramayana Part 1"}
    ]

    snapshot_windows = ["T-24h", "T-6h", "T-2h", "T-30m", "T-10m"]
    
    print("🚀 Starting Box Office Telemetry Simulator...")
    print(f"Streaming to Kafka broker at {KAFKA_BOOTSTRAP_SERVERS} | Topic: {TOPIC_NAME}\n")

    try:
        while True:
            theatre = random.choice(theatres)
            screen = random.choice(theatre.screens)
            movie = random.choice(movies)
            
            show_start_time = datetime.utcnow() + timedelta(hours=random.randint(2, 24))
            show_id = f"sh_{theatre.theatre_id}_{show_start_time.strftime('%Y%m%d_%H%M')}"
            
            # Simulate progression across snapshot windows
            for window in snapshot_windows:
                # Stochastic booking velocity based on window proximity
                fill_multiplier = {
                    "T-24h": random.uniform(0.1, 0.3),
                    "T-6h": random.uniform(0.3, 0.5),
                    "T-2h": random.uniform(0.5, 0.75),
                    "T-30m": random.uniform(0.75, 0.90),
                    "T-10m": random.uniform(0.85, 0.98)
                }[window]

                category_snapshots = []
                for cat in screen.seat_categories:
                    booked = int(cat.total_seats * fill_multiplier)
                    blocked = random.randint(0, 2)
                    if booked + blocked > cat.total_seats:
                        booked = cat.total_seats - blocked

                    # Generate dummy bitmask representing individual seat states
                    bitmask = "".join(random.choice(["0", "1", "2"]) for _ in range(min(cat.total_seats, 30)))

                    category_snapshots.append(
                        SeatCategorySnapshot(
                            category_name=cat.category_name,
                            base_price_inr=cat.base_price_inr,
                            total_seats=cat.total_seats,
                            booked_seats=booked,
                            blocked_seats=blocked,
                            seat_states_bitmask=bitmask
                        )
                    )

                event = ShowSeatSnapshotEvent(
                    event_id=f"evt_{random.randint(100000, 999999)}",
                    show_id=show_id,
                    theatre_id=theatre.theatre_id,
                    city_code=theatre.city_code,
                    snapshot_window=window,
                    snapshot_timestamp=datetime.utcnow(),
                    show_start_timestamp=show_start_time,
                    movie_id=movie["movie_id"],
                    screen_id=screen.screen_id,
                    seat_categories=category_snapshots
                )

                # Serialize and produce to Kafka with show_id as partition key
                payload = event.model_dump_json()
                producer.produce(
                    TOPIC_NAME,
                    key=show_id.encode('utf-8'),
                    value=payload.encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)

                # Sleep briefly between windows to simulate realistic streaming intervals
                time.sleep(0.5)

            print(f"--- Completed simulation cycle for Show: {show_id} at {theatre.theatre_name} ---\n")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n⏹️ Simulator stopped by user. Flushing remaining messages...")
        producer.flush()

if __name__ == "__main__":
    simulate_streaming()