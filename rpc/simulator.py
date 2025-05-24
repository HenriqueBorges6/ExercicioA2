
import grpc
import event_pb2
import event_pb2_grpc
import random
import json
import time

channel = grpc.insecure_channel("localhost:50051")
stub = event_pb2_grpc.EventServiceStub(channel)

def simulate_rating_event():
    event = {
        "user_id": random.randint(1, 100),
        "content_id": random.randint(1, 50),
        "rating": random.randint(1, 5),
        "timestamp": int(time.time())
    }
    response = stub.SendEvent(event_pb2.Event(
        type="rating",
        payload=json.dumps(event)
    ))
    print(f"Sent event: {event} | Success: {response.success}")

if __name__ == "__main__":
    print("[Simulator] Starting gRPC event generation...")
    try:
        while True:
            simulate_rating_event()
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[Simulator] Stopped.")
