# Pipeline.py (atualização do log_worker com wrapper pool-friendly + ranking de filmes)

import multiprocessing
import os
import time
from time import sleep
from queue import Empty

from handler_pool_wrappers import count_events_last_hour_chunk
from Handler import HandlerValueCount
from DataRepository import DataRepository
from DataFrame import DataFrame
from Handler import HandlerSort

NUM_PROCESSES = 1
CHUNK_SIZE = 5000
OUTPUT_CSV_FILENAME = "event_count_last_hour.csv"
RANKING_CSV_FILENAME = "ranking_filmes.csv"


def log_worker(task_queue, result_queue):
    while True:
        df_chunk = task_queue.get()
        if df_chunk is None:
            task_queue.task_done()
            break

        if df_chunk is not None and len(df_chunk) > 0:
            result_df_chunk = count_events_last_hour_chunk(df_chunk)
            sleep(0.5)  # simula outros tratamentos
            result_queue.put(result_df_chunk)

        task_queue.task_done()

def rating_worker(task_queue, result_queue, content_df):
    while True:
        df_chunk = task_queue.get()
        if df_chunk is None:
            task_queue.task_done()
            break

        # Filtra conteúdo tipo 'movie'
        movie_ids = set()
        for i in range(len(content_df)):
            if content_df['content_type'][i] == 'movie':
                movie_ids.add(content_df['content_id'][i])

        filtered = DataFrame(columns=df_chunk.columns)
        for i in range(len(df_chunk)):
            if df_chunk['content_id'][i] in movie_ids:
                filtered.add_row(list(df_chunk[i].values()))

        # Agrupar por média
        if len(filtered) > 0:
            notas = {}
            contagens = {}
            for i in range(len(filtered)):
                cid = filtered['content_id'][i]
                grade = int(filtered['grade'][i])
                notas[cid] = notas.get(cid, 0) + grade
                contagens[cid] = contagens.get(cid, 0) + 1

            result = DataFrame(columns=['content_id', 'nota_media'])
            for cid in notas:
                media = notas[cid] / contagens[cid]
                result.add_row([cid, round(media, 2)])

            result_queue.put(result)

        task_queue.task_done()

def run_rating_ranking_pipeline():
    repo = DataRepository()
    manager = multiprocessing.Manager()
    task_queue = manager.Queue()
    result_queue = manager.Queue()

    content_q = manager.Queue()
    repo.extract_table_from_db_incremental(
        db_path="streaming_mock.db",
        table_name="Content",
        chunk_size=100,
        task_queue=content_q,
        marker_file="markers/content.txt",
        marker_column=None
    )
    content_df = None
    while not content_q.empty():
        df = content_q.get()
        if content_df is None:
            content_df = df
        else:
            content_df.vconcat(df)

    repo.extract_table_from_db_incremental(
        db_path="streaming_mock.db",
        table_name="Rating",
        chunk_size=100,
        task_queue=task_queue,
        marker_file="markers/rating.txt",
        marker_column="rating_date"
    )

    processes = [
        multiprocessing.Process(target=rating_worker, args=(task_queue, result_queue, content_df))
        for _ in range(NUM_PROCESSES)
    ]
    for p in processes:
        p.start()

    for _ in range(NUM_PROCESSES):
        task_queue.put(None)

    all_ratings = DataFrame(columns=['content_id', 'nota_media'])
    while any(p.is_alive() for p in processes):
        try:
            df = result_queue.get(timeout=1)
            if df:
                all_ratings.vconcat(df)
        except Empty:
            continue

    for p in processes:
        p.join()

    # Agrupamento final (consolidar médias)
    final = {}
    counts = {}
    for i in range(len(all_ratings)):
        cid = all_ratings['content_id'][i]
        nota = float(all_ratings['nota_media'][i])
        final[cid] = final.get(cid, 0) + nota
        counts[cid] = counts.get(cid, 0) + 1

        # Carregar dados anteriores, se existirem
    previous_df = repo.read_csv_to_dataframe(
        file_path=f'transformed_data/{RANKING_CSV_FILENAME}',
        expected_columns=['content_id', 'nota_media']
    )
    if len(previous_df) > 0:
        all_ratings.vconcat(previous_df)

    # Agrupamento final (consolidar médias)
    final = {}
    counts = {}
    for i in range(len(all_ratings)):
        cid = all_ratings['content_id'][i]
        nota = float(all_ratings['nota_media'][i])
        final[cid] = final.get(cid, 0) + nota
        counts[cid] = counts.get(cid, 0) + 1

    output = DataFrame(columns=['content_id', 'nota_media'])
    for cid in final:
        media = final[cid] / counts[cid]
        output.add_row([cid, round(media, 2)])

    sorter = HandlerSort('nota_media', reverse=True)
    output = sorter.sort(output)
    os.makedirs('transformed_data', exist_ok=True)
    repo.save_dataframe_to_csv(output, f'transformed_data/{RANKING_CSV_FILENAME}')
    print("Ranking de filmes gerado com sucesso.")


def main_pipeline():
    # Mantém pipeline atual
    ...
if __name__ == '__main__':
    # main_pipeline()  # Ative esta linha para rodar ambos os pipelines

    start = time.perf_counter()
    run_rating_ranking_pipeline()
    duration = time.perf_counter() - start
    print(f" {NUM_PROCESSES} processo(s): {duration:.2f} segundos")

