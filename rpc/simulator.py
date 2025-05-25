# sim_pipeline.py
import os, sys, time, uuid, random, datetime, threading, signal, pathlib
from itertools import cycle

import grpc
from google.protobuf import empty_pb2

import event_pb2, event_pb2_grpc
import argparse # Adicionado para argumentos de linha de comando


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
class Simulator:
    def __init__(self, stub, client_id): # Adicionado client_id
        self.stub        = stub
        self.sent        = 0
        self._stop       = threading.Event()
        self._evt_iter   = cycle(EVENT_TYPES)
        self.client_id = client_id # Armazena o ID do cliente

    # ---------- produção de eventos ----------
    def _event_stream(self):
        while not self._stop.is_set():
            ev_type = next(self._evt_iter)
            event = simulate_event(ev_type)
            # Opcional: Adicionar um ID único ao evento se for necessário para rastreamento
            # event.event_id = str(uuid.uuid4()) # Exemplo: se event_pb2.Event tivesse um campo event_id
            yield event
            self.sent += 1

            # trigger automático?
            if self.sent % BATCH_TRIGGER == 0:
                self._trigger_pipeline()

            time.sleep(0.1)          # ajusta para stress-test ou dev-test (0.1 s = 10 evt/s)

    # ---------- chamadas RPC extras ----------
    def _trigger_pipeline(self):
        def _call():
            try:
                self.stub.TriggerPipeline(
                    event_pb2.PipelineRequest(n_processes=os.cpu_count() or 4)
                )
                # log = "[Simulator] Pipeline RPC OK"
                print(f"[Simulator {self.client_id}] Pipeline RPC OK") # Log com ID do cliente
            except grpc.RpcError as err:
                # log = f"[Simulator] Pipeline RPC failed: {err.details()}"
                print(f"[Simulator {self.client_id}] Pipeline RPC failed: {err.details()}") # Log com ID do cliente
            # print(log)

        threading.Thread(target=_call, daemon=True).start()

    def _fetch_reports(self):
        try:
            bundle = self.stub.GetLatestReports(empty_pb2.Empty())
            saved  = 0
            for field, fname in [
                (bundle.events_csv,       "event_count_last_hour.csv"),
                (bundle.revenue_day_csv,  "revenue_by_day.csv"),
                (bundle.revenue_mon_csv,  "revenue_by_month.csv"),
                (bundle.revenue_year_csv, "revenue_by_year.csv"),
                (bundle.genre_csv,        "genre_views_last_24h.csv"),
                (bundle.unfinished_csv,   "unfinished_by_genre.csv"),
            ]:
                if field:
                    # Para evitar conflitos, salvar com nome diferente por cliente se rodar localmente
                    report_fname = REPORT_DIR / f"client_{self.client_id}_{fname}"
                    report_fname.write_bytes(field)
                    saved += 1
            # print(f"[Simulator] {saved} CSVs salvos em {REPORT_DIR}")
            print(f"[Simulator {self.client_id}] {saved} CSVs salvos em {REPORT_DIR}") # Log com ID do cliente
        except grpc.RpcError as err:
            # print("[Simulator] GetLatestReports RPC failed:", err.details())
            print(f"[Simulator {self.client_id}] GetLatestReports RPC failed: {err.details()}") # Log com ID do cliente

    # ---------- loop principal ----------
    def run(self):
        # executa SendEvent em thread separada para não bloquear stdin
        # Cada simulador (thread) rodará seu próprio SendEvent stream
        print(f"[Simulator {self.client_id}] Starting SendEvent stream...") # Log com ID do cliente
        try:
            ack = self.stub.SendEvent(self._event_stream())
            print(f"[Simulator {self.client_id}] SendEvent finalizado — status: {ack.status}") # Log com ID do cliente
        except grpc.RpcError as err:
            print(f"[Simulator {self.client_id}] SendEvent aborted: {err.details()}") # Log com ID do cliente

    # Novo método para parar o simulador
    def stop(self):
        self._stop.set()


###############################################################################
# Main
###############################################################################
def main():
    parser = argparse.ArgumentParser(description="Simulador de eventos para o servidor gRPC.")
    parser.add_argument(
        "--clients",
        type=int,
        default=1,
        help="Número de clientes simultâneos a serem simulados (padrão: 1)"
    )
    args = parser.parse_args()

    simulators = []
    threads = []

    print(f"Iniciando simulador com {args.clients} cliente(s)...")

    # Criar e iniciar threads para cada cliente simulado
    for i in range(args.clients):
        channel = grpc.insecure_channel(GRPC_SERVER)
        stub = event_pb2_grpc.EventServiceStub(channel)
        simulator = Simulator(stub, i + 1) # Passa um ID para cada cliente
        simulators.append(simulator)
        # Cada thread roda o método run do seu simulador
        t = threading.Thread(target=simulator.run, daemon=True)
        threads.append(t)
        t.start()

    print("[Main] Ctrl-C p/ sair")

    # Manter o thread principal vivo para que os daemon threads continuem rodando
    try:
        # O loop de leitura de input para trigger manual ('p') pode ser removido
        # ou adaptado para acionar todos os simuladores, mas para teste de carga
        # o trigger automático é mais relevante.
        # Para manter a interatividade (Ctrl-C para sair), podemos usar um loop simples ou join
        while True:
            time.sleep(1) # Espera para não consumir CPU excessivamente
            # Verificar se todas as threads ainda estão ativas, se necessário
            if not any(t.is_alive() for t in threads):
                break

    except KeyboardInterrupt:
        print("\n[Main] Encerrando simuladores...")
        for sim in simulators:
            sim.stop() # Sinaliza para cada simulador parar
        # Esperar pelas threads terminarem, embora como são daemon threads,
        # o programa principal pode sair sem esperar.
        # Para garantir um desligamento limpo, podemos adicionar .join() sem timeout
        # ou com um timeout curto para não travar se uma thread não parar corretamente.
        # for t in threads:
        #     t.join(timeout=1.0)

    print("[Main] Encerrado.")


if __name__ == "__main__":
    main()
