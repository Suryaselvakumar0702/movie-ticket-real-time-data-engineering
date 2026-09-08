import json
import os
import random
import socket
import time
from datetime import datetime

from kafka import KafkaProducer


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

BOOKING_TOPIC = os.getenv(
    "BOOKING_TOPIC",
    "booking_events"
)


# ============================================================
# CHECK KAFKA AVAILABILITY
# ============================================================

def kafka_available(servers: str) -> bool:
    """
    Check whether at least one Kafka broker is reachable.
    """

    for server in [s.strip() for s in servers.split(",") if s.strip()]:

        try:
            host, port = server.rsplit(":", 1)

            with socket.create_connection(
                (host, int(port)),
                timeout=5
            ):
                return True

        except (OSError, ValueError):
            continue

    return False


if not kafka_available(KAFKA_BOOTSTRAP_SERVERS):

    raise RuntimeError(
        f"Kafka broker is not reachable at "
        f"{KAFKA_BOOTSTRAP_SERVERS}. "
        "Start Kafka first with "
        "'docker compose up -d kafka'."
    )


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ============================================================
# MOVIE MASTER DATA
# ============================================================

movies = [

    {
        "movie_id": 1311031,
        "movie_name": "Spider-Man: Brand New Day"
    },

    {
        "movie_id": 1061474,
        "movie_name": "The Odyssey"
    },

    {
        "movie_id": 1299651,
        "movie_name": "Hoppers"
    },

    {
        "movie_id": 1242898,
        "movie_name": "Predator: Badlands"
    },

    {
        "movie_id": 1234821,
        "movie_name": "Jurassic World Rebirth"
    }

]


# ============================================================
# CITY MASTER DATA
# ============================================================

cities = [

    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi"

]


# ============================================================
# THEATER MASTER DATA
# ============================================================

theaters = [

    "PVR Cinemas",
    "INOX",
    "AGS Cinemas",
    "Cinepolis",
    "Luxe Cinemas"

]


# ============================================================
# BOOKING STATUS
# ============================================================
# Approximately:
# 75% CONFIRMED
# 25% CANCELLED

booking_status = [

    "CONFIRMED",
    "CONFIRMED",
    "CONFIRMED",
    "CANCELLED"

]


# ============================================================
# UNIQUE BOOKING ID
# ============================================================
# Using current timestamp prevents duplicate IDs when
# the producer is restarted.

booking_id = int(datetime.now().timestamp())


# ============================================================
# REALISTIC BOOKING HOUR DISTRIBUTION
# ============================================================
# Movie bookings are more common during evening/night hours.

hour_weights = {

    9: 2,
    10: 3,
    11: 4,
    12: 5,
    13: 5,
    14: 4,
    15: 3,
    16: 3,
    17: 5,
    18: 7,
    19: 9,
    20: 10,
    21: 9,
    22: 6,
    23: 3

}


# ============================================================
# START PRODUCER
# ============================================================

print("=" * 60)
print("MOVIE TICKET KAFKA PRODUCER")
print("=" * 60)

print(f"Kafka Server : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic  : {BOOKING_TOPIC}")
print("Status       : RUNNING")
print("")

print("Generating realistic booking events...")
print("Booking hours: 09:00 AM - 11:59 PM")
print("Peak hours   : 06:00 PM - 09:00 PM")
print("")

print("Press CTRL + C to stop.")
print("=" * 60)


# ============================================================
# CONTINUOUS EVENT GENERATION
# ============================================================

try:

    while True:

        # ----------------------------------------------------
        # GENERATE UNIQUE BOOKING ID
        # ----------------------------------------------------

        booking_id += 1


        # ----------------------------------------------------
        # SELECT RANDOM MOVIE
        # ----------------------------------------------------

        movie = random.choice(movies)


        # ----------------------------------------------------
        # SELECT CITY
        # ----------------------------------------------------

        city = random.choice(cities)


        # ----------------------------------------------------
        # SELECT THEATER
        # ----------------------------------------------------

        theater = random.choice(theaters)


        # ----------------------------------------------------
        # GENERATE TICKET COUNT
        # ----------------------------------------------------

        tickets = random.randint(1, 5)


        # ----------------------------------------------------
        # GENERATE TICKET PRICE
        # ----------------------------------------------------

        ticket_price = random.choice(
            [
                150,
                180,
                200,
                250,
                300
            ]
        )


        # ----------------------------------------------------
        # CALCULATE TOTAL AMOUNT
        # ----------------------------------------------------

        total_amount = tickets * ticket_price


        # ----------------------------------------------------
        # SELECT REALISTIC BOOKING HOUR
        # ----------------------------------------------------

        booking_hour = random.choices(

            list(hour_weights.keys()),

            weights=list(hour_weights.values()),

            k=1

        )[0]


        # ----------------------------------------------------
        # GENERATE RANDOM MINUTE AND SECOND
        # ----------------------------------------------------

        booking_minute = random.randint(0, 59)

        booking_second = random.randint(0, 59)

        booking_microsecond = random.randint(
            0,
            999999
        )


        # ----------------------------------------------------
        # CREATE BOOKING TIMESTAMP
        # ----------------------------------------------------

        now = datetime.now()

        booking_time = now.replace(

            hour=booking_hour,

            minute=booking_minute,

            second=booking_second,

            microsecond=booking_microsecond

        )


        # ----------------------------------------------------
        # SELECT BOOKING STATUS
        # ----------------------------------------------------

        status = random.choice(booking_status)


        # ----------------------------------------------------
        # CREATE BOOKING EVENT
        # ----------------------------------------------------

        event = {

            "booking_id":
                f"B{booking_id}",

            "movie_id":
                movie["movie_id"],

            "movie_name":
                movie["movie_name"],

            "city":
                city,

            "theater":
                theater,

            "tickets":
                tickets,

            "ticket_price":
                ticket_price,

            "total_amount":
                total_amount,

            "booking_status":
                status,

            "booking_time":
                booking_time.isoformat()

        }


        # ----------------------------------------------------
        # SEND EVENT TO KAFKA
        # ----------------------------------------------------

        producer.send(

            BOOKING_TOPIC,

            value=event

        )


        # ----------------------------------------------------
        # WAIT FOR KAFKA CONFIRMATION
        # ----------------------------------------------------

        producer.flush()


        # ----------------------------------------------------
        # DISPLAY EVENT
        # ----------------------------------------------------

        print(
            f"{event['booking_id']} | "
            f"{event['movie_name']} | "
            f"{event['city']} | "
            f"{event['tickets']} tickets | "
            f"₹{event['total_amount']} | "
            f"{event['booking_status']} | "
            f"{event['booking_time']}"
        )


        # ----------------------------------------------------
        # GENERATE ONE EVENT EVERY 2 SECONDS
        # ----------------------------------------------------

        time.sleep(2)


# ============================================================
# STOP PRODUCER SAFELY
# ============================================================

except KeyboardInterrupt:

    print("\n")
    print("Stopping Kafka producer...")


finally:

    producer.close()

    print("Kafka producer stopped.")