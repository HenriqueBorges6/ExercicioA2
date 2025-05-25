# server.py  ─────────────────────────────────────────────────────────
import os
import sqlite3
import time
import grpc
from concurrent import futures

import event_pb2
import event_pb2_grpc
from google.protobuf import empty_pb2          # <-- faltava este import

from pipeline_manager import PipelineManager   # já criado antes

DB_FILE = os.path.join(os.path.dirname(__file__), '..', 'streaming_mock.db')

SCHEMA = """
CREATE TABLE IF NOT EXISTS User(
    user_id TEXT PRIMARY KEY,
    user_name TEXT,
    user_email TEXT,
    user_birthdate TEXT,
    user_service_plan TEXT,
    user_signup_date TEXT
);
CREATE TABLE IF NOT EXISTS Content(
    content_id TEXT PRIMARY KEY,
    content_title TEXT,
    content_type TEXT,
    content_genre TEXT
);
CREATE TABLE IF NOT EXISTS Episode(
    episode_id TEXT PRIMARY KEY,
    content_id TEXT
);
CREATE TABLE IF NOT EXISTS Rating(
    rating_id TEXT PRIMARY KEY,
    grade INTEGER,
    rating_date TEXT,
    user_id TEXT,
    content_id TEXT
);
CREATE TABLE IF NOT EXISTS Device(
    device_id TEXT PRIMARY KEY,
    device_type TEXT,
    user_id TEXT
);
CREATE TABLE IF NOT EXISTS ViewHistory(
    view_id TEXT PRIMARY KEY,
    start_date TEXT,
    end_date TEXT,
    device_id TEXT,
    user_id TEXT,
    content_id TEXT,
    episode_id TEXT
);
CREATE TABLE IF NOT EXISTS Plan(
    plan_id TEXT PRIMARY KEY,
    plan_name TEXT,
    plan_price REAL,
    num_screens INTEGER
);
CREATE TABLE IF NOT EXISTS SubscriptionTransactions(
    transaction_id TEXT PRIMARY KEY,
    transaction_date TEXT,
    payment_method TEXT,
    user_id TEXT,
    plan_id TEXT
);
CREATE TABLE IF NOT EXISTS Revenue(
    revenue_id TEXT PRIMARY KEY,
    date TEXT,
    value REAL
);
"""

class EventServiceServicer(event_pb2_grpc.EventServiceServicer):
    # ----------------------------------------------------------------
    def __init__(self):
        # 1 writer simultâneo (SQLite limitation) --------------------
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.executescript(SCHEMA)          # ← garante todas as tabelas
        self.lock  = self.conn
        self.pmgr  = PipelineManager()          # controla o pipeline
    
    # --------------------------- RPC SendEvent ----------------------
    def SendEvent(self, request_iterator, context):
        """
        Recebe um *stream* de Event (com oneof) e grava na tabela correta.
        Retorna um Ack único no final.
        """
        try:
            for req in request_iterator:
                etype = req.WhichOneof("event_type")

                if etype == "user_event":
                    u = req.user_event
                    self.lock.execute(
                        "INSERT INTO User VALUES (?, ?, ?, ?, ?, ?)",
                        (u.user_id, u.user_name, u.user_email,
                         u.user_birthdate, u.user_service_plan,
                         u.user_signup_date)
                    )

                elif etype == "content_event":
                    c = req.content_event
                    self.lock.execute(
                        "INSERT INTO Content VALUES (?, ?, ?, ?)",
                        (c.content_id, c.content_title,
                         c.content_type, c.content_genre)
                    )

                elif etype == "episode_event":
                    e = req.episode_event
                    self.lock.execute(
                        "INSERT INTO Episode VALUES (?, ?)",
                        (e.episode_id, e.content_id)
                    )

                elif etype == "device_event":
                    d = req.device_event
                    self.lock.execute(
                        "INSERT INTO Device VALUES (?, ?, ?)",
                        (d.device_id, d.device_type, d.user_id)
                    )

                elif etype == "rating_event":
                    r = req.rating_event
                    self.lock.execute(
                        "INSERT INTO Rating VALUES (?, ?, ?, ?, ?)",
                        (r.rating_id, r.grade, r.rating_date,
                         r.user_id, r.content_id)
                    )

                elif etype == "view_event":
                    v = req.view_event
                    self.lock.execute(
                        "INSERT INTO ViewHistory VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (v.view_id, v.start_date, v.end_date,
                         v.device_id, v.user_id, v.content_id,
                         v.episode_id)
                    )
                elif etype == "view_history_event":
                    v = req.view_history_event
                    self.conn.execute(
                        "INSERT OR IGNORE INTO ViewHistory VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (v.view_id, v.start_date, v.end_date,
                        v.device_id, v.user_id, v.content_id, v.episode_id)
                    )

                elif etype == "subscription_event":
                    t = req.subscription_event
                    self.conn.execute(
                        "INSERT OR IGNORE INTO SubscriptionTransactions VALUES (?, ?, ?, ?, ?)",
                        (t.transaction_id, t.transaction_date,
                        t.payment_method, t.user_id, t.plan_id)
                    )

                elif etype == "plan_event":
                    p = req.plan_event
                    self.conn.execute(
                        "INSERT OR IGNORE INTO Plan VALUES (?, ?, ?, ?)",
                        (p.plan_id, p.plan_name,
                        float(p.plan_price), int(p.num_screens))
                    )

                elif etype == "transaction_event":
                    t = req.transaction_event
                    self.lock.execute(
                        "INSERT INTO SubscriptionTransactions VALUES (?, ?, ?, ?, ?)",
                        (t.transaction_id, t.transaction_date,
                         t.payment_method, t.user_id, t.plan_id)
                    )

                elif etype == "revenue_event":
                    r = req.revenue_event
                    self.lock.execute(
                        "INSERT INTO Revenue VALUES (?, ?, ?)",
                        (r.revenue_id, r.date, r.value)
                    )

                else:
                    print("[gRPC] Tipo desconhecido:", etype)
                    continue

            self.conn.commit()
            print("[gRPC] Lote gravado com sucesso.")
            return event_pb2.Ack(status="OK")

        except Exception as exc:
            print("[gRPC] Erro:", exc)
            return event_pb2.Ack(status="Error")

    # --------------------- RPC TriggerPipeline ----------------------
    def TriggerPipeline(self, request, context):
        """
        Dispara main_pipeline.py em background.
        """
        n = request.n_processes or 4
        self.pmgr.trigger(n)
        return empty_pb2.Empty()

    # -------------------- RPC GetLatestReports ----------------------
    def GetLatestReports(self, request, context):
        """
        Devolve os CSVs mais recentes, encapsulados em bytes.
        """
        out = self.pmgr.bundle()
        return event_pb2.ReportsBundle(
            events_csv       = out.get("event_count_last_hour.csv", b""),
            revenue_day_csv  = out.get("revenue_by_day.csv",        b""),
            revenue_mon_csv  = out.get("revenue_by_month.csv",      b""),
            revenue_year_csv = out.get("revenue_by_year.csv",       b""),
            genre_csv        = out.get("genre_views_last_24h.csv",  b""),
            unfinished_csv   = out.get("unfinished_by_genre.csv",   b""),
        )


# ----------------------------- bootstrap ----------------------------
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    event_pb2_grpc.add_EventServiceServicer_to_server(EventServiceServicer(),
                                                      server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[INFO] gRPC server listening on 50051.")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
