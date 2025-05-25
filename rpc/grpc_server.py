# grpc_server.py ─────────────────────────────────────────────────────
import os, sqlite3, time, grpc, threading, logging
from concurrent import futures
from pathlib import Path

import event_pb2, event_pb2_grpc
from google.protobuf import empty_pb2
from pipeline_manager import PipelineManager   # seu gerenciador de pipeline

# ───────────────────────── CONFIGURAÇÃO ────────────────────────────
ROOT_DIR   = Path(__file__).resolve().parents[1]           # pasta ExercicioA2
DB_FILE    = ROOT_DIR / "streaming_mock.db"
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("grpc_server")

# ───────────────────── SCHEMA (cria se faltar) ─────────────────────
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

# ╭──────────────────────── SERVIÇO gRPC ────────────────────────────╮
class EventServiceServicer(event_pb2_grpc.EventServiceServicer):
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.conn.executescript(SCHEMA)
        self.wlock = threading.Lock()            # serializa INSERTs
        self.pmgr  = PipelineManager()
        log.info("SQLite aberto em %s", DB_FILE)

    # ───────────── RPC SendEvent (stream) ─────────────
    def SendEvent(self, request_iterator, context):
        n_events  = 0
        t0        = time.time()

        try:
            for req in request_iterator:
                etype = req.WhichOneof("event_type")
                n_events += 1
                if log.isEnabledFor(logging.DEBUG):
                    log.debug("▼ %s", etype)

                with self.wlock:
                    match etype:
                        # ------------- cada tipo de evento -------------
                        case "user_event":
                            u = req.user_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO User VALUES (?, ?, ?, ?, ?, ?)",
                                (u.user_id, u.user_name, u.user_email,
                                 u.user_birthdate, u.user_service_plan,
                                 u.user_signup_date)
                            )
                        case "content_event":
                            c = req.content_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO Content VALUES (?, ?, ?, ?)",
                                (c.content_id, c.content_title,
                                 c.content_type, c.content_genre)
                            )
                        case "episode_event":
                            e = req.episode_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO Episode VALUES (?, ?)",
                                (e.episode_id, e.content_id)
                            )
                        case "device_event":
                            d = req.device_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO Device VALUES (?, ?, ?)",
                                (d.device_id, d.device_type, d.user_id)
                            )
                        case "rating_event":
                            r = req.rating_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO Rating VALUES (?, ?, ?, ?, ?)",
                                (r.rating_id, r.grade, r.rating_date,
                                 r.user_id, r.content_id)
                            )
                        case "view_history_event":
                            v = req.view_history_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO ViewHistory VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (v.view_id, v.start_date, v.end_date,
                                 v.device_id, v.user_id, v.content_id,
                                 v.episode_id)
                            )
                        case "plan_event":
                            p = req.plan_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO Plan VALUES (?, ?, ?, ?)",
                                (p.plan_id, p.plan_name,
                                 float(p.plan_price), int(p.num_screens))
                            )
                        case "subscription_event":
                            t = req.subscription_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO SubscriptionTransactions VALUES (?, ?, ?, ?, ?)",
                                (t.transaction_id, t.transaction_date,
                                 t.payment_method, t.user_id, t.plan_id)
                            )
                        case "revenue_event":
                            r = req.revenue_event
                            self.conn.execute(
                                "INSERT OR IGNORE INTO Revenue VALUES (?, ?, ?)",
                                (r.revenue_id, r.date, float(r.value))
                            )
                        case _:
                            log.warning("tipo desconhecido: %s", etype)

            self.conn.commit()
            log.info("▲ %d eventos gravados em %.2fs", n_events, time.time()-t0)
            return event_pb2.Ack(status="OK")

        except Exception:
            log.exception("Erro em SendEvent")
            return event_pb2.Ack(status="Error")

    # ───────────── RPC TriggerPipeline ─────────────
    def TriggerPipeline(self, request, context):
        n = request.n_processes or 4
        log.info("TriggerPipeline: %d processos", n)
        self.pmgr.trigger(n)
        return empty_pb2.Empty()

    # ───────────── RPC GetLatestReports ────────────
    def GetLatestReports(self, request, context):
        out = self.pmgr.bundle()
        return event_pb2.ReportsBundle(
            events_csv       = out.get("event_count_last_hour.csv", b""),
            revenue_day_csv  = out.get("revenue_by_day.csv",        b""),
            revenue_mon_csv  = out.get("revenue_by_month.csv",      b""),
            revenue_year_csv = out.get("revenue_by_year.csv",       b""),
            genre_csv        = out.get("genre_views_last_24h.csv",  b""),
            unfinished_csv   = out.get("unfinished_by_genre.csv",   b""),
            last_finished_ms = out.get("_last_finished_ms", 0)

        )

# ╭────────────────────────── bootstrap ─────────────────────────────╮
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    event_pb2_grpc.add_EventServiceServicer_to_server(EventServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    log.info("gRPC server listening on 50051.")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
