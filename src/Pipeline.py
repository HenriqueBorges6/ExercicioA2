import sys, os, time, multiprocessing
from multiprocessing import JoinableQueue
from datetime import datetime, timedelta
from typing import Dict

from Handler import HandlerValueCount, HandlerUnfinishedByGenre, RevenueAnalyzer
from DataRepository import DataRepository
from DataFrame import DataFrame
from utils.timing import StageTimer, log_stage

# === CONFIG ===
DEFAULT_NUM_PROCESSES = 6
CHUNK_SIZE = 10_000

OUTPUT_EVENT_CSV      = "event_count_last_hour.csv"
OUTPUT_GENRE_CSV      = "genre_views_last_24h.csv"
OUTPUT_REVENUE_DAY    = "revenue_by_day.csv"
OUTPUT_REVENUE_MONTH  = "revenue_by_month.csv"
OUTPUT_REVENUE_YEAR   = "revenue_by_year.csv"
OUTPUT_UNFINISHED_CSV = "unfinished_by_genre.csv"

def _mk(path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True); return path

BASE_DIR       = os.path.dirname(__file__)
MARKER_DIR     = _mk(os.path.join(BASE_DIR, 'markers'))
EVENT_MARKER   = _mk(os.path.join(MARKER_DIR, 'ViewHistory.marker'))
GENRE_MARKER   = _mk(os.path.join(MARKER_DIR, 'Genre.marker'))
REVENUE_MARKER = _mk(os.path.join(MARKER_DIR, 'Revenue.marker'))
UNFINISHED_MARKER = _mk(os.path.join(MARKER_DIR,'Unfinished.marker'))

DB_PATH        = os.path.join(BASE_DIR, '..', 'streaming_mock.db')
TRANSFORMED_DIR= os.path.abspath(os.path.join(BASE_DIR, '..', 'transformed_data'))
os.makedirs(TRANSFORMED_DIR, exist_ok=True)

def chunk_dataframe(df: DataFrame, size: int):
    total = len(df); start = 0
    while start < total:
        end = min(start + size, total)
        chunk = DataFrame(columns=df._columns)
        for i in range(start, end):
            chunk.add_row([df._data[col][i] for col in df._columns])
        yield chunk; start = end

def event_worker(tq, rq):
    h = HandlerValueCount()
    while True:
        df = tq.get(); tq.task_done()
        if df is None:
            break
        rq.put(h.count_events_last_hour(df))

def process_event_counts(repo: DataRepository, nproc: int):
    tq = JoinableQueue(maxsize=nproc * 2)
    rq = multiprocessing.Queue(maxsize=0)          # ilimitado

    procs = [multiprocessing.Process(target=event_worker, args=(tq, rq))
             for _ in range(nproc)]
    for p in procs: p.start()

    chunk_ct = repo.process_new_log_files(CHUNK_SIZE, tq)
    if chunk_ct == 0:
        print("  Nenhum log a processar na última hora.")
        for _ in procs: tq.put(None)
        for p in procs: p.join()
        return                                          # sai limpo

    for _ in procs: tq.put(None)                        # poison‑pills

    aggregated = DataFrame(columns=["event", "quantidade"])
    for _ in range(chunk_ct):
        aggregated.vconcat(rq.get())

    tq.join()
    for p in procs: p.join()

    acc = HandlerValueCount().group_by_sum(aggregated, "event", "quantidade")
    path = os.path.join(TRANSFORMED_DIR, OUTPUT_EVENT_CSV)
    try:
        prev = repo.read_csv_to_dataframe(path, ["event", "quantidade"])
        prev.vconcat(acc)
        acc = HandlerValueCount().group_by_sum(prev, "event", "quantidade")
    except FileNotFoundError:
        pass
    repo.save_dataframe_to_csv(acc, path)
    print(" Event stage complete.")

import traceback

def revenue_worker(tq: multiprocessing.JoinableQueue,
                   rq: multiprocessing.Queue) -> None:
    """
    Consome DataFrames da fila `tq`, calcula as métricas de receita e devolve
    três DataFrames (dia, mês, ano) na fila `rq`.

    A função só devolve após receber o *sentinel* `None`.
    """
    while True:
        df = tq.get()

        # ───────────────────────────────────────────────── sentinel ──
        if df is None:          # “poison pill” enviado pelo processo‑pai
            tq.task_done()      # marca o sentinel como processado
            print("[revenue_worker] Received poison pill. Exiting.")
            break

        try:
            print(f"[revenue_worker] Processing chunk with {len(df)} rows…")

            a = RevenueAnalyzer(df)
            day   = a.analyze_revenue_by_day()
            month = a.analyze_revenue_by_month()
            year  = a.analyze_revenue_by_year()

            # Constrói DataFrames coluna‑a‑coluna (mais barato que add_row em loop)
            df_day   = DataFrame(columns=["date",  "revenue"])
            df_month = DataFrame(columns=["month", "revenue"])
            df_year  = DataFrame(columns=["year",  "revenue"])

            for k, v in day.items():   df_day.add_row([k, v])
            for k, v in month.items(): df_month.add_row([k, v])
            for k, v in year.items():  df_year.add_row([k, v])

            rq.put((df_day, df_month, df_year))
            print("[revenue_worker] Chunk done and result enqueued.")

        except Exception as exc:
            print(f"[ERROR] revenue_worker failed: {exc}")
            traceback.print_exc()

        finally:
            # Só damos task_done quando TODOS os passos do chunk terminaram
            tq.task_done()
from multiprocessing import Pool
import time

def analyze_chunk(df: DataFrame):
    a = RevenueAnalyzer(df)
    day   = a.analyze_revenue_by_day()
    month = a.analyze_revenue_by_month()
    year  = a.analyze_revenue_by_year()

    df_day   = DataFrame(columns=["date",  "revenue"])
    df_month = DataFrame(columns=["month", "revenue"])
    df_year  = DataFrame(columns=["year",  "revenue"])

    for k, v in day.items():   df_day.add_row([k, v])
    for k, v in month.items(): df_month.add_row([k, v])
    for k, v in year.items():  df_year.add_row([k, v])

    return df_day, df_month, df_year

def process_revenue_reports(repo: DataRepository, nproc: int) -> None:
    print(" Starting revenue report processing…")
    if os.path.exists(REVENUE_MARKER):
        os.remove(REVENUE_MARKER)

    raw = repo.read_table_to_dataframe("Revenue")
    if raw is None:
        print("  Tabela 'Revenue' não encontrada.")
        return

    if len(raw) == 0:
        print("  Tabela 'Revenue' vazia.")
        for key, fname in [("date", OUTPUT_REVENUE_DAY),
                           ("month", OUTPUT_REVENUE_MONTH),
                           ("year", OUTPUT_REVENUE_YEAR)]:
            repo.save_dataframe_to_csv(
                DataFrame(columns=[key, "revenue"]),
                os.path.join(TRANSFORMED_DIR, fname)
            )
        open(REVENUE_MARKER, "a").close()
        return

    print(f" Revenue data loaded with {len(raw)} rows.")
    chunks = list(chunk_dataframe(raw, CHUNK_SIZE))

    print(f" Dispatching {len(chunks)} chunks to {nproc} processes.")
    agg_day   = DataFrame(columns=["date",  "revenue"])
    agg_month = DataFrame(columns=["month", "revenue"])
    agg_year  = DataFrame(columns=["year",  "revenue"])

    start_time = time.time()
    with Pool(processes=nproc) as pool:
        for i, (d, m, y) in enumerate(pool.imap_unordered(analyze_chunk, chunks), 1):
            print(f" [main] Received result {i}/{len(chunks)}")
            agg_day.vconcat(d)
            agg_month.vconcat(m)
            agg_year.vconcat(y)
    print(f" All chunks processed in {time.time() - start_time:.2f}s")

    def _save(df: DataFrame, key: str, fname: str) -> None:
        acc = HandlerValueCount().group_by_sum(df, key, "revenue")
        path = os.path.join(TRANSFORMED_DIR, fname)
        try:
            prev = repo.read_csv_to_dataframe(path, [key, "revenue"])
            prev.vconcat(acc)
            acc = HandlerValueCount().group_by_sum(prev, key, "revenue")
        except FileNotFoundError:
            pass
        repo.save_dataframe_to_csv(acc, path)

    _save(agg_day,   "date",  OUTPUT_REVENUE_DAY)
    _save(agg_month, "month", OUTPUT_REVENUE_MONTH)
    _save(agg_year,  "year",  OUTPUT_REVENUE_YEAR)

    open(REVENUE_MARKER, "a").close()
    print(" Revenue stage complete.")

def genre_worker(tq, rq, content):
    """
    Conta visualizações por gênero nas últimas 24 h.
    Não depende de HandlerValueCount nem de nome específico de coluna.
    """
    cutoff = datetime.now() - timedelta(days=1)

    while True:
        df = tq.get()
        tq.task_done()

        # poison‑pill?
        if df is None:
            break

        try:
            # 1) join ViewHistory × Content
            merged = df.merge(content, "content_id")

            # 2) garante que a coluna de gênero se chame 'genre'
            if "genre" not in merged._columns:
                if "content_genre" in merged._columns:
                    idx = merged._columns.index("content_genre")
                    merged._columns[idx] = "genre"
                    merged._data["genre"] = merged._data.pop("content_genre")
                else:
                    # nenhuma coluna de gênero → devolve DF vazio
                    rq.put(DataFrame(columns=["genre", "views"]))
                    continue

            # 3) filtra views das últimas 24 h e conta por gênero
            counts: Dict[str, int] = {}
            for i in range(len(merged)):
                if datetime.fromisoformat(merged._data["start_date"][i]) >= cutoff:
                    g = merged._data["genre"][i]
                    counts[g] = counts.get(g, 0) + 1

            # 4) converte dict → DataFrame e envia
            out = DataFrame(columns=["genre", "views"])
            for g, v in counts.items():
                out.add_row([g, v])
            rq.put(out)

        except Exception as e:
            print(f"[ERROR] genre_worker fail2ed: {e}")

from multiprocessing import Pool

def analyze_genre_chunk(args):
    df, content = args
    from datetime import datetime, timedelta

    cutoff = datetime.now() - timedelta(days=1)

    try:
        merged = df.merge(content, "content_id")

        # renomeia coluna para 'genre' se necessário
        if "genre" not in merged._columns:
            if "content_genre" in merged._columns:
                idx = merged._columns.index("content_genre")
                merged._columns[idx] = "genre"
                merged._data["genre"] = merged._data.pop("content_genre")
            else:
                return DataFrame(columns=["genre", "views"])

        # conta views nas últimas 24h
        counts = {}
        for i in range(len(merged)):
            if datetime.fromisoformat(merged._data["start_date"][i]) >= cutoff:
                g = merged._data["genre"][i]
                counts[g] = counts.get(g, 0) + 1

        out = DataFrame(columns=["genre", "views"])
        for g, v in counts.items():
            out.add_row([g, v])
        return out

    except Exception as e:
        print(f"[ERROR] analyze_genre_chunk failed: {e}")
        return DataFrame(columns=["genre", "views"])

def process_genre_from_db(repo: DataRepository, nproc: int):
    print(" Starting genre view processing...")
    try:
        os.remove(GENRE_MARKER)
    except FileNotFoundError:
        pass

    content = repo.read_table_to_dataframe('Content')
    chunks = repo.extract_table_from_db_incremental(
        DB_PATH, 'ViewHistory', CHUNK_SIZE, None, GENRE_MARKER, 'start_date', dry_run=True
    )
    dataframes = chunks['dataframes'] if isinstance(chunks, dict) else chunks

    if not dataframes:
        print("  Nenhum dado novo para processar.")
        return

    # monta args para cada chunk
    args = [(df, content) for df in dataframes]

    aggregated = DataFrame(columns=['genre', 'views'])
    with Pool(processes=nproc) as pool:
        for i, df in enumerate(pool.imap_unordered(analyze_genre_chunk, args), 1):
            print(f"[main] Genre chunk {i}/{len(args)} received.")
            aggregated.vconcat(df)

    acc = HandlerValueCount().group_by_sum(aggregated, 'genre', 'views')
    path = os.path.join(TRANSFORMED_DIR, OUTPUT_GENRE_CSV)
    try:
        prev = repo.read_csv_to_dataframe(path, ['genre', 'views'])
        prev.vconcat(acc)
        acc = HandlerValueCount().group_by_sum(prev, 'genre', 'views')
    except FileNotFoundError:
        pass

    repo.save_dataframe_to_csv(acc, path)
    print(" Genre stage complete.")


def analyze_unfinished_chunk(args):
    df, content = args
    try:
        merged = df.merge(content, 'content_id')

        # corrige nome da coluna se necessário
        if 'genre' not in merged._columns:
            if 'content_genre' in merged._columns:
                idx = merged._columns.index('content_genre')
                merged._columns[idx] = 'genre'
                merged._data['genre'] = merged._data.pop('content_genre')
            else:
                return DataFrame(columns=['content_genre', 'unfinished_views'])

        if 'event' not in merged._columns:
            merged._columns.append('event')
            merged._data['event'] = ['play'] * len(merged)

        h = HandlerUnfinishedByGenre()
        return h.group(merged)

    except Exception as e:
        print(f"[ERROR] analyze_unfinished_chunk failed: {e}")
        return DataFrame(columns=['content_genre', 'unfinished_views'])

def process_unfinished_by_genre(repo: DataRepository, nproc: int):
    print(" Starting unfinished-by-genre processing...")
    try:
        os.remove(UNFINISHED_MARKER)
    except FileNotFoundError:
        pass

    content = repo.read_table_to_dataframe('Content')
    chunks = repo.extract_table_from_db_incremental(
        DB_PATH, 'ViewHistory', CHUNK_SIZE, None, UNFINISHED_MARKER, 'start_date', dry_run=True
    )

    if not chunks:
        print("  Nenhum dado novo para processar.")
        return

    args = [(df, content) for df in chunks]
    aggregated = None

    with Pool(processes=nproc) as pool:
        for i, df in enumerate(pool.imap_unordered(analyze_unfinished_chunk, args), 1):
            print(f"[main] Unfinished chunk {i}/{len(args)} received.")
            if aggregated is None:
                aggregated = df
            else:
                aggregated.vconcat(df)

    if aggregated is None:
        aggregated = DataFrame(columns=['content_genre', 'unfinished_views'])

    path = os.path.join(TRANSFORMED_DIR, OUTPUT_UNFINISHED_CSV)
    repo.save_dataframe_to_csv(aggregated, path)
    open(UNFINISHED_MARKER, 'a').close()
    print(" Unfinished stage complete.")


def unfinished_worker(tq, rq, content):
    h = HandlerUnfinishedByGenre()
    while True:
        df = tq.get()
        tq.task_done()
        if df is None:
            break
        try:
            merged = df.merge(content, 'content_id')
            if 'genre' not in merged.columns:
                merged._columns.append('genre')
                merged._data['genre'] = merged._data['content_genre'].copy()
            if 'event' not in merged.columns:
                merged._columns.append('event')
                merged._data['event'] = ['play'] * len(merged)
            rq.put(h.group(merged))
        except Exception as e:
            print(f"[ERROR] unfinished_worker failed: {e}")

def main_pipeline(num_processes: int):
    t0_pipeline = time.time()
    repo = DataRepository()

    print(" Stage 1: Event Counts")
    with StageTimer("events", num_processes):
        process_event_counts(repo, max(1, num_processes))

    print(" Stage 2: Revenue Reports")
    with StageTimer("revenue", num_processes):
        process_revenue_reports(repo, max(1, num_processes))

    print(" Stage 3: Genre Views")
    with StageTimer("genre", num_processes):
        process_genre_from_db(repo, max(1, num_processes))

    print(" Stage 4: Unfinished by Genre")
    with StageTimer("unfinished", num_processes):
        process_unfinished_by_genre(repo, max(1, num_processes))

    total_secs = time.time() - t0_pipeline
    log_stage("pipeline_total", num_processes, total_secs)   # registra o total também
    print(f" Pipeline done in {total_secs:.2f}s")

if __name__ == '__main__':
    n = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_NUM_PROCESSES
    main_pipeline(n)
