# sim_pipeline.py
import os, sys, time, uuid, random, datetime, threading, signal, pathlib
from itertools import cycle
from collections import deque

import grpc
from google.protobuf import empty_pb2

import event_pb2, event_pb2_grpc


###############################################################################
# Configuráveis
###############################################################################
EVENT_TYPES      = [
    "user", "content", "episode", "rating", "device",
    "view_history", "plan", "subscription", "revenue"
]
PLANS            = [("basic", 2.99, 1), ("standard", 7.99, 2), ("premium", 17.99, 4)]
BATCH_TRIGGER    = 500           # chama pipeline a cada 500 eventos
REPORT_DIR       = pathlib.Path(__file__).with_name("reports")
REPORT_DIR.mkdir(exist_ok=True)
GRPC_SERVER      = "localhost:50051"
GRPC_SERVER = os.getenv("GRPC_SERVER", "localhost:50051")


###############################################################################
# Helpers
###############################################################################
def now_iso() -> str:
    return datetime.datetime.now().isoformat()


def random_date(start_year=1970, end_year=2020):
    start = datetime.date(start_year, 1, 1)
    end   = datetime.date(end_year, 12, 31)
    return start + datetime.timedelta(days=random.randint(0, (end - start).days))


def simulate_event(event_type: str) -> event_pb2.Event:
    """Cria um Event (oneof populado) de acordo com o tipo."""
    match event_type:
        case "user":
            return event_pb2.Event(user_event=event_pb2.UserEvent(
                user_id=str(uuid.uuid4()),
                user_name=f"user_{random.randint(1000, 9999)}",
                user_email=f"{uuid.uuid4()}@example.com",
                user_birthdate=random_date(1970, 2005).isoformat(),
                user_service_plan=random.choice(PLANS)[0],
                user_signup_date=now_iso()
            ))
        case "content":
            return event_pb2.Event(content_event=event_pb2.ContentEvent(
                content_id=str(uuid.uuid4()),
                content_title=f"Content_{random.randint(1000, 9999)}",
                content_type=random.choice(["movie", "series", "podcast"]),
                content_genre=random.choice(
                    ["action", "drama", "comedy", "horror", "sci-fi", "mystery"]
                )
            ))
        case "episode":
            return event_pb2.Event(episode_event=event_pb2.EpisodeEvent(
                episode_id=str(uuid.uuid4()),
                content_id=str(uuid.uuid4())
            ))
        case "rating":
            return event_pb2.Event(rating_event=event_pb2.RatingEvent(
                rating_id=str(uuid.uuid4()),
                grade=random.randint(1, 5),
                rating_date=now_iso(),
                user_id=str(uuid.uuid4()),
                content_id=str(uuid.uuid4())
            ))
        case "device":
            return event_pb2.Event(device_event=event_pb2.DeviceEvent(
                device_id=str(uuid.uuid4()),
                device_type=random.choice(["tv", "phone", "tablet", "desktop"]),
                user_id=str(uuid.uuid4())
            ))
        case "view_history":
            start = datetime.datetime.now()
            end   = start + datetime.timedelta(minutes=random.randint(1, 120))
            return event_pb2.Event(view_history_event=event_pb2.ViewHistoryEvent(
                view_id=str(uuid.uuid4()),
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                device_id=str(uuid.uuid4()),
                user_id=str(uuid.uuid4()),
                content_id=str(uuid.uuid4()),
                episode_id=str(uuid.uuid4()) if random.random() < 0.7 else ""
            ))
        case "plan":
            plan = random.choice(PLANS)
            return event_pb2.Event(plan_event=event_pb2.PlanEvent(
                plan_id=plan[0],
                plan_name=plan[0],
                plan_price=str(plan[1]),
                num_screens=str(plan[2])
            ))
        case "subscription":
            date_part = random_date(2025, 2025).isoformat()
            time_part = f"{random.randint(0,23):02}:{random.randint(0,59):02}:00"
            return event_pb2.Event(subscription_event=event_pb2.SubscriptionEvent(
                transaction_id=str(uuid.uuid4()),
                transaction_date=f"{date_part}T{time_part}",
                payment_method=random.choice(["credit_card", "paypal", "gift_card"]),
                user_id=str(uuid.uuid4()),
                plan_id=random.choice(["basic", "standard", "premium"])
            ))
        case "revenue":
            return event_pb2.Event(revenue_event=event_pb2.RevenueEvent(
                revenue_id=str(uuid.uuid4()),
                date=datetime.date.today().isoformat(),
                value=str(round(random.uniform(2.99, 17.99), 2))
            ))
        case _:
            raise ValueError(f"Tipo '{event_type}' não reconhecido.")


###############################################################################
# Gerador & controle do pipeline
###############################################################################
from collections import deque

class Simulator:
    def __init__(self, stub):
        self.stub        = stub
        self.sent        = 0
        self._stop       = threading.Event()
        self._evt_iter   = cycle(EVENT_TYPES)

        self.send_queue  = deque()

    # ---------- produção de eventos ----------
    def _event_stream(self):
        """Gera eventos infinitamente até _stop ser sinalizado."""
        while not self._stop.is_set():

            if self.sent % BATCH_TRIGGER == 0:
                t0_ms = int(time.time() * 1000)   # epoch-millis
                self.send_queue.append(t0_ms)

            ev_type = next(self._evt_iter)
            yield simulate_event(ev_type)
            self.sent += 1

            if self.sent % BATCH_TRIGGER == 0:
                self._trigger_pipeline()

            time.sleep(0.1)  

    def _trigger_pipeline(self):
        def _call():
            try:
                # 1) Registrar o momento exato ANTES de disparar o pipeline
                pipeline_start_ms = int(time.time() * 1000)
                
                # 2) Disparar o ETL
                self.stub.TriggerPipeline(
                    event_pb2.PipelineRequest(n_processes=os.cpu_count() or 4)
                )
                print("[Simulator] Pipeline RPC OK")

                # 3) Esperar até terminar
                last_seen = 0
                while not self._stop.is_set():
                    bundle = self.stub.GetLatestReports(empty_pb2.Empty())
                    
                    # Só processar se for uma medição nova
                    if bundle.last_finished_ms > last_seen:
                        last_seen = bundle.last_finished_ms

                        # -------- Cálculo Correto da Latência ---------
                        # Mede apenas o tempo do pipeline (desde o Trigger até a conclusão)
                        lat = bundle.last_finished_ms - pipeline_start_ms
                        
                        # Verificação de sanidade (latência não pode ser negativa)
                        if lat >= 0:
                            print(f"[LAT] {lat:.1f} ms", flush=True)
                        else:
                            print(f"[WARN] Latência inválida ignorada: {lat:.1f} ms")

                        # -------- salvar CSVs ------
                        for field, fname in [
                            (bundle.events_csv,       "event_count_last_hour.csv"),
                            (bundle.revenue_day_csv,  "revenue_by_day.csv"),
                            (bundle.revenue_mon_csv,  "revenue_by_month.csv"),
                            (bundle.revenue_year_csv, "revenue_by_year.csv"),
                            (bundle.genre_csv,        "genre_views_last_24h.csv"),
                            (bundle.unfinished_csv,   "unfinished_by_genre.csv"),
                        ]:
                            if field:
                                (REPORT_DIR / fname).write_bytes(field)
                        break  # pipeline finalizado

                    time.sleep(0.2)  # 200 ms de intervalo

            except grpc.RpcError as err:
                print("[Simulator] Pipeline RPC failed:", err.details())

        threading.Thread(target=_call, daemon=True).start()
    def _fetch_reports(self):
        try:
            bundle = self.stub.GetLatestReports(empty_pb2.Empty())

            # ------ latência: fim do pipeline - início do lote ---------------
            if bundle.last_finished_ms and self.send_queue:
                t0 = self.send_queue.popleft()
                lat = bundle.last_finished_ms - t0   # em milissegundos
                print(f"[LAT] {lat:.1f} ms", flush=True)

            # ------ salvar CSVs (já existia) ---------------------------------
            saved = 0
            for field, fname in [
                (bundle.events_csv,       "event_count_last_hour.csv"),
                (bundle.revenue_day_csv,  "revenue_by_day.csv"),
                (bundle.revenue_mon_csv,  "revenue_by_month.csv"),
                (bundle.revenue_year_csv, "revenue_by_year.csv"),
                (bundle.genre_csv,        "genre_views_last_24h.csv"),
                (bundle.unfinished_csv,   "unfinished_by_genre.csv"),
            ]:
                if field:
                    (REPORT_DIR / fname).write_bytes(field)
                    saved += 1
            print(f"[Simulator] {saved} CSVs salvos em {REPORT_DIR}")

        except grpc.RpcError as err:
            print("[Simulator] GetLatestReports RPC failed:", err.details())

    # ---------- loop principal ----------
    def run(self):
        # executa SendEvent em thread separada para não bloquear stdin
        t = threading.Thread(target=self._run_stream, daemon=True)
        t.start()

        print("[Simulator] Ctrl-C p/ sair | Ctrl-P p/ rodar pipeline agora")
        try:
            while True:
                ch = sys.stdin.read(1)
                if ch.lower() == "p":
                    self._trigger_pipeline()
                    time.sleep(3)           # pequeno delay para dar tempo
                    self._fetch_reports()
        except KeyboardInterrupt:
            print("\n[Simulator] Encerrando…")
            self._stop.set()
            t.join()

    def _run_stream(self):
        """Mantém o streaming SendEvent aberto até self._stop."""
        try:
            ack = self.stub.SendEvent(self._event_stream())
            
            print("[Simulator] SendEvent finalizado — status:", ack.status)
        except grpc.RpcError as err:
            print("[Simulator] SendEvent aborted:", err.details())


###############################################################################
# Main
###############################################################################
def main():
    with grpc.insecure_channel(GRPC_SERVER) as channel:
        stub = event_pb2_grpc.EventServiceStub(channel)
        Simulator(stub).run()


if __name__ == "__main__":
    main()
