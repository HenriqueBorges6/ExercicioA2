import os
import grpc
from concurrent import futures
import event_pb2
import event_pb2_grpc
import sqlite3
import json

DB_FILE = os.path.join(os.path.dirname(__file__), '..', 'streaming_mock.db')


class EventServiceServicer(event_pb2_grpc.EventServiceServicer):
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        # here maybe call create_schema(self.conn)

    def SendEvent(self, request, context):
        try:
            event_type = request.type
            payload = json.loads(request.payload)

            if event_type == "rating":
                self.conn.execute(
                    "INSERT INTO Rating(user_id, content_id, rating, timestamp) VALUES (?, ?, ?, ?)",
                    (payload["user_id"], payload["content_id"], payload["rating"], payload["timestamp"])
                )
            elif event_type == "view":
                # Handle view history similarly
                pass
            # Add more handlers as needed

            self.conn.commit()
            return event_pb2.Ack(success=True)

        except Exception as e:
            print(f"[ERROR] Failed to insert event: {e}")
            return event_pb2.Ack(success=False)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    event_pb2_grpc.add_EventServiceServicer_to_server(EventServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[INFO] gRPC server started on port 50051.")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()