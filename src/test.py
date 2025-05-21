from GenreHandlers import HandlerJoinContent, HandlerDateFilter, HandlerGroupByGenre
import multiprocessing
import os
import time
from queue import Empty

from Handler import HandlerValueCount
from DataRepository import DataRepository
from DataFrame import DataFrame


def view_extractor_worker(task_q):
    try:
        os.makedirs("markers", exist_ok=True)
        repo = DataRepository()
        repo.extract_table_from_db_incremental(
            db_path="streaming_mock.db",
            table_name="ViewHistory",
            chunk_size=25,
            task_queue=task_q,
            marker_file="markers/viewhistory.txt",
            marker_column="start_date")
    except Exception as e:
        print("[view_extractor_worker] Erro:", e)
    finally:
        task_q.put(None)


def content_extractor_worker(content_q):
    try:
        os.makedirs("markers", exist_ok=True)
        repo = DataRepository()
        repo.extract_table_from_db_incremental(
            db_path="streaming_mock.db",
            table_name="Content",
            chunk_size=50,
            task_queue=content_q,
            marker_file="markers/content.txt",
            marker_column=None)
    except Exception as e:
        print("[content_extractor_worker] Erro:", e)
    finally:
        content_q.put(None)


def join_worker(view_q, content_q, joined_q):
    print("[join_worker] Iniciando...")
    joiner = HandlerJoinContent()
    content_df = None

    while True:
        if content_df is None:
            chunk = content_q.get()
            print("[join_worker] Recebeu chunk de content_q:", "None" if chunk is None else f"{len(chunk)} linhas")
            if chunk is None:
                time.sleep(0.2)
                continue
            content_df = chunk

        view_chunk = view_q.get()
        print("[join_worker] Recebeu chunk de view_q:", "None" if view_chunk is None else f"{len(view_chunk)} linhas")
        if view_chunk is None:
            break

        joined_df = joiner.join(view_chunk, content_df)
        joined_q.put(joined_df)

    joined_q.put(None)
    print("[join_worker] Finalizado.")


def filter_worker(period_days, joined_q, filtered_q):
    print(f"[filter_worker {period_days}] Iniciando...")
    filt = HandlerDateFilter(period_days)

    while True:
        chunk = joined_q.get()
        print(f"[filter_worker {period_days}] Recebeu:", "None" if chunk is None else f"{len(chunk)} linhas")
        if chunk is None:
            break
        filtered = filt.filter(chunk)
        print(f"[filter_worker {period_days}] Filtrou para {len(filtered)} linhas")
        filtered_q.put(filtered)

    filtered_q.put(None)
    print(f"[filter_worker {period_days}] Finalizado.")


def groupby_worker(filtered_q, outfile):
    print(f"[groupby_worker {outfile}] Iniciando...")
    grouper = HandlerGroupByGenre()
    all_rows = []

    while True:
        chunk = filtered_q.get()
        print(f"[groupby_worker {outfile}] Recebeu:", "None" if chunk is None else f"{len(chunk)} linhas")
        if chunk is None:
            break
        for i in range(len(chunk)):
            all_rows.append(chunk[i].copy())

    if all_rows:
        temp_df = DataFrame(columns=list(all_rows[0].keys()))
        for row in all_rows:
            temp_df.add_row(list(row.values()))
        final = grouper.group(temp_df)
        print(f"[groupby_worker {outfile}] Agrupou para {len(final)} gêneros")
    else:
        final = DataFrame(columns=['content_genre', 'views'])
        print(f"[groupby_worker {outfile}] Nenhuma linha recebida")

    os.makedirs("transformed_data", exist_ok=True)
    DataRepository().save_dataframe_to_csv(final, outfile)
    print(f"[groupby_worker {outfile}] Finalizado e CSV salvo.")


def main_pipeline():
    mgr = multiprocessing.Manager()
    view_q = mgr.Queue()
    content_q = mgr.Queue()
    joined_q = mgr.Queue()
    day_q, week_q, month_q, all_q = [mgr.Queue() for _ in range(4)]

    processes = []

    # Extratores
    processes.append(multiprocessing.Process(target=view_extractor_worker, args=(view_q,)))
    processes.append(multiprocessing.Process(target=content_extractor_worker, args=(content_q,)))

    # Join
    processes.append(multiprocessing.Process(target=join_worker, args=(view_q, content_q, joined_q)))

    # Filtros
    periods = [(1, day_q), (7, week_q), (30, month_q), (None, all_q)]
    for days, q in periods:
        p = multiprocessing.Process(target=filter_worker, args=(days, joined_q, q))
        processes.append(p)

    # Agrupamentos finais
    outnames = {
        day_q: "views_by_genre_last_day.csv",
        week_q: "views_by_genre_last_week.csv",
        month_q: "views_by_genre_last_month.csv",
        all_q: "views_by_genre_alltime.csv"
    }
    for q, name in outnames.items():
        p = multiprocessing.Process(target=groupby_worker, args=(q, f"transformed_data/{name}"))
        processes.append(p)

    # Iniciar todos
    for p in processes:
        p.start()

    # Esperar todos terminarem
    for p in processes:
        p.join()


if __name__ == "__main__":
    multiprocessing.freeze_support()  # Necessário no Windows
    start = time.time()
    main_pipeline()
    print(f"Pipeline executado em {time.time() - start:.2f} segundos.")
