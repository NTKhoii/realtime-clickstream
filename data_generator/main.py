import os
import json
import time
import random
import uuid
from datetime import datetime, timezone, timedelta
from faker import Faker
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
load_dotenv()
fake = Faker()

CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# ==============================
# 2. STATIC DATA
# ==============================

PRODUCT_CATALOG = {
    "Electronics": ["P_101", "P_102", "P_103"],
    "Clothing": ["P_201", "P_202"],
    "Home & Garden": ["P_301", "P_302"],
    "Sports": ["P_401", "P_402"],
    "Books": ["P_501", "P_502"]
}

SOURCES = ["organic", "google_ads", "facebook_ads", "email_campaign", "direct"]
OS_MAP = {"Mobile": ["iOS", "Android"], "Desktop": ["Windows", "MacOS"]}

ACTIONS = ["view_item", "add_to_cart", "purchase"]
ACTION_WEIGHTS = [0.8, 0.15, 0.05]

# ==============================
# 3. SESSION STATE
# ==============================

active_sessions = {}
MAX_SESSIONS = 5000 # Giới hạn RAM

def get_or_create_session():
    """Maintain user-session state without leaking memory"""
    if random.random() < 0.7 and active_sessions:
        return random.choice(list(active_sessions.values()))

    # Cơ chế dọn rác (Garbage Collection): Xóa session cũ nhất nếu quá giới hạn
    if len(active_sessions) >= MAX_SESSIONS:
        # Trong Python 3.7+, dict duy trì thứ tự chèn. Lấy key đầu tiên (cũ nhất) và xóa.
        oldest_session_id = next(iter(active_sessions))
        del active_sessions[oldest_session_id]

    user_id = f"U_{random.randint(1000, 2000)}"
    session_id = f"S_{uuid.uuid4().hex[:8]}"

    session = {
        "user_id": user_id,
        "session_id": session_id,
        "last_action": None,
        "device_type": random.choice(["Mobile", "Desktop"]),
        "start_time": datetime.now(timezone.utc)
    }

    active_sessions[session_id] = session
    return session

# ==============================
# 4. BEHAVIOR LOGIC
# ==============================

def next_action(prev_action):
    if prev_action == "view_item":
        return random.choices(
            ["view_item", "add_to_cart"],
            weights=[0.7, 0.3]
        )[0]
    elif prev_action == "add_to_cart":
        return random.choices(
            ["purchase", "view_item"],
            weights=[0.4, 0.6]
        )[0]
    return random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0]

# ==============================
# 5. TIMESTAMP (LATE EVENT)
# ==============================

def generate_timestamp():
    now = datetime.now(timezone.utc)
    delay = random.choice([0, 0, 0, 5, 10, 30])
    return (now - timedelta(seconds=delay)).isoformat()

# ==============================
# 6. EVENT GENERATOR
# ==============================

def generate_clickstream_event():
    session = get_or_create_session()

    action = next_action(session["last_action"])
    session["last_action"] = action

    category = random.choice(list(PRODUCT_CATALOG.keys()))
    product_id = random.choice(PRODUCT_CATALOG[category])

    device_type = session["device_type"]

    event = {
        "schema_version": "v1",
        "event_id": str(uuid.uuid4()),
        "timestamp": generate_timestamp(),

        "user_id": session["user_id"],
        "session_id": session["session_id"],

        "device_type": device_type,
        "os": random.choice(OS_MAP[device_type]),

        "action": action,

        "product_id": product_id,
        "category": category,
        "price": round(random.uniform(10.0, 2500.0), 2),
        "quantity": random.randint(1, 3),
        "discount": random.choice([0, 0, 10, 20]),

        "utm_source": random.choice(SOURCES),

        "ip_address": fake.ipv4(),
        "country": fake.country_code(),
        "city": fake.city(),

        "is_bot": random.random() < 0.02
    }

    # ==============================
    # 7. DIRTY DATA INJECTION
    # ==============================

    error_chance = random.random()

    if error_chance < 0.03:
        event["user_id"] = None  # missing critical field

    elif error_chance < 0.05:
        event["price"] = -100  # invalid business logic

    elif error_chance < 0.07:
        event["price"] = "free"  # wrong datatype

    elif error_chance < 0.09:
        event["new_field_unexpected"] = "schema_drift"

    elif error_chance < 0.10:
        return None  # null burst

    elif error_chance < 0.12:
        return str(event).replace("'", "")  # broken JSON

    return event

# ==============================
# 8. PRODUCER LOOP
# ==============================

def run_producer():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    print("🚀 Start streaming data to Azure Event Hubs... (Ctrl+C to stop)")

    try:
        with producer:
            while True:
                event_data_batch = producer.create_batch()

                # traffic spike simulation
                base = random.randint(50, 100)
                spike = random.choice([1, 1, 1, 3, 5])
                events_per_second = base * spike

                sent_count = 0

                for _ in range(events_per_second):
                    event = generate_clickstream_event()

                    if event is None:
                        continue  # simulate dropped events

                    if isinstance(event, dict):
                        payload = json.dumps(event)
                        partition_key = event.get("user_id", None)
                    else:
                        payload = event
                        partition_key = None

                    try:
                        event_data_batch.add(
                            EventData(payload, partition_key=partition_key)
                        )
                        sent_count += 1
                    except Exception:
                        # batch full → gửi rồi tạo batch mới
                        producer.send_batch(event_data_batch)
                        event_data_batch = producer.create_batch()
                        event_data_batch.add(
                            EventData(payload, partition_key=partition_key)
                        )
                        sent_count += 1

                producer.send_batch(event_data_batch)

                print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {sent_count} events")

                time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Stopped streaming.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_producer()