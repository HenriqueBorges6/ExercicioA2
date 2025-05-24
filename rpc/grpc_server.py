import os
import grpc
from concurrent import futures
import event_pb2
import event_pb2_grpc
import sqlite3
import json
from google.protobuf.json_format import MessageToJson

DB_FILE = os.path.join(os.path.dirname(__file__), '..', 'streaming_mock.db')



class EventServiceServicer(event_pb2_grpc.EventServiceServicer):
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.cursor = self.conn.cursor()
    '''
    def SendEvent(self, request, context):
        try:
            event_type = request.type
            payload = json.loads(request.payload)

            if event_type in FIELD_MAP:
                fields = FIELD_MAP[event_type]
                placeholders = ", ".join("?" for _ in fields)
                sql = f"INSERT OR REPLACE INTO {event_type}({', '.join(fields)}) VALUES ({placeholders})"
                self.conn.execute(sql, tuple(payload[i] for i in fields))
            else:
                print(f"[gRPC Server] Unknown event type: {event_type}")
                return event_pb2.Ack(status='UnknownType')

            self.conn.commit()
            return event_pb2.Ack(status='OK')

        except Exception as e:
            print(f"[gRPC Server] Error processing {request.type}: {e}")
            return event_pb2.Ack(status='Error')
    '''

    def SendEvent(self, request, context):
        try:
            event_type = request.WhichOneof("event")
            if event_type is None:
                raise ValueError("Event type is missing in the request.")

            event_message = getattr(request, event_type)
            payload = json.loads(MessageToJson(event_message))

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

            table_name = event_type.replace("_event", "").capitalize()
            fields = field_map[event_type]
            placeholders = ", ".join("?" for _ in fields)
            sql = f"INSERT OR REPLACE INTO {table_name}({', '.join(fields)}) VALUES ({placeholders})"
            self.conn.execute(sql, tuple(payload[k] for k in fields))
            self.conn.commit()

            print(f"[gRPC Server] Inserted {event_type} -> {payload}")
            return event_pb2.Ack(status='OK')

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