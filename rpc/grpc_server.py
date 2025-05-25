import os
import grpc
from concurrent import futures
import event_pb2
import event_pb2_grpc
import sqlite3
import json
from google.protobuf.json_format import MessageToJson

DB_FILE = os.path.join(os.path.dirname(__file__), '..', 'streaming_mock.db')

field_map = {
    "user_event": ["user_id", "user_name", "user_email", "user_birthdate", "user_service_plan", "user_signup_date"],
    "content_event": ["content_id", "content_title", "content_type", "content_genre"],
    "episode_event": ["episode_id", "content_id"],
    "rating_event": ["rating_id", "grade", "rating_date", "user_id", "content_id"],
    "device_event": ["device_id", "device_type", "user_id"],
    "view_history_event": ["view_id", "start_date", "end_date", "device_id", "user_id", "content_id", "episode_id"],
    "plan_event": ["plan_id", "plan_name", "plan_price", "num_screens"],
    "subscription_transactions_event": ["transaction_id", "transaction_date", "payment_method", "user_id", "plan_id"],
    "revenue_event": ["revenue_id", "date", "value"]
}


class EventServiceServicer(event_pb2_grpc.EventServiceServicer):
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.cursor = self.conn.cursor()

    def SendEvent(self, request_iterator, context):
        try:
            for request in request_iterator:
                event_type = request.WhichOneof("event_type")
                
                if event_type == "user_event":
                    u = request.user_event
                    self.conn.execute(
                        "INSERT INTO User VALUES (?, ?, ?, ?, ?, ?)",
                        (u.user_id, u.user_name, u.user_email, u.user_birthdate,
                         u.user_service_plan, u.user_signup_date)
                    )

                elif event_type == "content_event":
                    c = request.content_event
                    self.conn.execute(
                        "INSERT INTO Content VALUES (?, ?, ?, ?)",
                        (c.content_id, c.content_title, c.content_type, c.content_genre)
                    )

                elif event_type == "episode_event":
                    e = request.episode_event
                    self.conn.execute(
                        "INSERT INTO Episode VALUES (?, ?)",
                        (e.episode_id, e.content_id)
                    )

                elif event_type == "device_event":
                    d = request.device_event
                    self.conn.execute(
                        "INSERT INTO Device VALUES (?, ?, ?)",
                        (d.device_id, d.device_type, d.user_id)
                    )

                elif event_type == "rating_event":
                    r = request.rating_event
                    self.conn.execute(
                        "INSERT INTO Rating VALUES (?, ?, ?, ?, ?)",
                        (r.rating_id, r.grade, r.rating_date, r.user_id, r.content_id)
                    )

                elif event_type == "view_event":
                    v = request.view_event
                    self.conn.execute(
                        "INSERT INTO ViewHistory VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (v.view_id, v.start_date, v.end_date, v.device_id, v.user_id,
                         v.content_id, v.episode_id)
                    )

                elif event_type == "plan_event":
                    p = request.plan_event
                    self.conn.execute(
                        "INSERT INTO Plan VALUES (?, ?, ?, ?)",
                        (p.plan_id, p.plan_name, p.plan_price, p.num_screens)
                    )

                elif event_type == "transaction_event":
                    t = request.transaction_event
                    self.conn.execute(
                        "INSERT INTO SubscriptionTransactions VALUES (?, ?, ?, ?, ?)",
                        (t.transaction_id, t.transaction_date, t.payment_method, t.user_id, t.plan_id)
                    )

                elif event_type == "revenue_event":
                    r = request.revenue_event
                    self.conn.execute(
                        "INSERT INTO Revenue VALUES (?, ?, ?)",
                        (r.revenue_id, r.date, r.value)
                    )

                else:
                    print(f"[gRPC Server] Unknown event type: {event_type}")
                    continue

                self.conn.commit()
                print("Sent", event_type)

            return event_pb2.Ack(status="OK")

        except Exception as e:
            print(f"[gRPC Server] Error handling event: {e}")
            return event_pb2.Ack(status='Error')
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    event_pb2_grpc.add_EventServiceServicer_to_server(EventServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[INFO] gRPC server started on port 50051.")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()