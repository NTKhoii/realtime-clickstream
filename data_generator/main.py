import os
import json
import time
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# 1. Khởi tạo môi trường và cấu hình
load_dotenv()  # Đọc các biến môi trường từ file .env
fake = Faker()

# Lấy "chìa khóa" từ file .env
CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
ACTIONS = ["view_item", "view_item", "view_item", "add_to_cart", "purchase"] 
SOURCES = ["organic", "google_ads", "facebook_ads", "email_campaign", "direct"]
OS_MAP = {"Mobile": ["iOS", "Android"], "Desktop": ["Windows", "MacOS"]}

def generate_clickstream_event():
    """Hàm tạo ra 1 dòng sự kiện Clickstream ngẫu nhiên"""
    device_type = random.choice(["Mobile", "Desktop"])
    
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(), 
        "user_id": f"U_{random.randint(1000, 9999)}",
        "session_id": f"S_{random.randint(10000, 99999)}",
        "device_type": device_type,
        "os": random.choice(OS_MAP[device_type]),
        "action": random.choice(ACTIONS),
        "product_id": f"P_{random.randint(100, 500)}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(10.0, 2500.0), 2),
        "utm_source": random.choice(SOURCES),
        "ip_address": fake.ipv4() 
    }
    return event

def run_producer():
    """Hàm chạy vòng lặp gửi dữ liệu lên Azure Event Hubs"""
    # Khởi tạo "đường ống" kết nối lên Azure
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )
    
    print("Bắt đầu gửi dữ liệu lên Azure Event Hubs... (Nhấn Ctrl+C để dừng)")
    
    try:
        with producer:
            while True:
                event_data_batch = producer.create_batch()
                events_per_second = random.randint(50, 100)
                
                for _ in range(events_per_second):
                    event = generate_clickstream_event()
                    # Chuyển Dict thành chuỗi JSON và thêm vào Batch
                    event_data_batch.add(EventData(json.dumps(event)))
                
                # Bắn nguyên cục Batch đó lên Event Hub
                producer.send_batch(event_data_batch)
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã gửi {events_per_second} sự kiện lên Event Hubs.")
                
                # Nghỉ 1 giây rồi bắn tiếp
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\n Đã dừng tiến trình sinh dữ liệu.")
    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    # Nhớ đảm bảo trong file .env em đã dán đúng Connection String nhé
    run_producer()