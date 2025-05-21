from datetime import datetime, timedelta, timezone
from DataFrame import DataFrame
from typing import List, Dict, Any
import os

class HandlerValueCount:
    """
    A class to handle operations on DataFrame objects, such as counting events
    within a specific time range and grouping data by columns with aggregation.
    """
    columns = ['event_type', 'quantity']
    WINDOW_HOURS = int(os.getenv("EVENT_WINDOW", "1"))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)

    def count_events_last_hour(self, df: DataFrame) -> DataFrame:
        """Conta eventos nas últimas 1 h; tolera nomes de coluna diferentes."""

        if len(df) == 0:
            return DataFrame(columns=["event", "quantidade"])

        time_col  = "time"  if "time"  in df.columns else "timestamp"
        event_col = "event" if "event" in df.columns else "event_type"

        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        counts: Dict[Any, int] = {}

        for i in range(len(df)):
            raw_ts = df[time_col][i].replace("Z", "+00:00")
            ts = datetime.fromisoformat(raw_ts).astimezone(timezone.utc)
            if ts >= cutoff:
                ev = df[event_col][i]
                counts[ev] = counts.get(ev, 0) + 1

        out = DataFrame(columns=["event", "quantidade"])
        for ev, q in counts.items():
            out.add_row([ev, q])
        return out

    def group_by_sum(self, df: DataFrame, group_col: str, sum_col: str) -> DataFrame:
        """
        Groups the DataFrame by a specified column and calculates the sum of another column.

        Args:
            df (DataFrame): A DataFrame object containing the data to group and sum.
            group_col (str): The column to group by.
            sum_col (str): The column to sum.

        Returns:
            DataFrame: A new DataFrame with two columns: the grouping column and the sum column.

        Raises:
            TypeError: If the argument is not a DataFrame object.
            ValueError: If the grouping or summing column is not found in the DataFrame.
        """
        if not isinstance(df, DataFrame):
            raise TypeError("Argument `df` must be a DataFrame object.")
    
        if group_col not in df.columns or sum_col not in df.columns:
            raise ValueError("Grouping or summing column not found in DataFrame.")

        grouped_data: Dict[Any, float] = {}
        group_col_data: List[Any] = df[group_col]
        sum_col_data: List[Any] = df[sum_col]

        for i in range(len(df)):
            key: Any = group_col_data[i]
            value: Any = sum_col_data[i]
            numeric_value: float = float(value)
            if key not in grouped_data:
                grouped_data[key] = 0
            grouped_data[key] += numeric_value

        result_df = DataFrame(columns=[group_col, sum_col])
        for key, total_sum in grouped_data.items():
            result_df.add_row([key, int(total_sum)])
                 
        return result_df

class HandlerJoin:
    """
    Une dois DataFrames com base em uma chave de junção.
    """

    def join(self, 
             left_df: DataFrame, 
             right_df: DataFrame, 
             left_on: str, 
             right_on: str, 
             select_cols_right: List[str]) -> DataFrame:
        
        if len(left_df) == 0 or len(right_df) == 0:
            return DataFrame(columns=left_df.columns + select_cols_right)

        # Criar índice rápido de lookup no DataFrame da direita
        lookup = {
            right_df[right_on][i]: {col: right_df[col][i] for col in select_cols_right}
            for i in range(len(right_df))
        }

        # Construir novo DataFrame com colunas combinadas
        joined_columns = left_df.columns + select_cols_right
        joined = DataFrame(columns=joined_columns)

        for i in range(len(left_df)):
            key = left_df[left_on][i]
            right_data = lookup.get(key, {col: 'unknown' for col in select_cols_right})
            joined_row = list(left_df[i].values()) + [right_data[col] for col in select_cols_right]
            joined.add_row(joined_row)

        return joined


class HandlerJoinContent:
    """Une ViewHistory (content_id, start_date, …) com Content (content_id, content_genre)."""
    def join(self, vh_df: DataFrame, content_df: DataFrame) -> DataFrame:
        if len(vh_df) == 0 or len(content_df) == 0:
            return DataFrame(columns=vh_df.columns + ['content_genre'])

        # índice auxiliar para lookup rápido
        genre_lookup = {content_df['content_id'][i]: content_df['content_genre'][i]
                        for i in range(len(content_df))}

        joined = DataFrame(columns=vh_df.columns + ['content_genre'])
        for i in range(len(vh_df)):
            cid = vh_df['content_id'][i]
            genre = genre_lookup.get(cid, 'unknown')
            joined.add_row(list(vh_df[i].values()) + [genre])
        return joined



def join_chunk_worker(args: tuple) -> DataFrame:
    """
    Realiza o join de um chunk com o DataFrame de referência.
    Recebe um tuple: (chunk_df, right_df, left_on, right_on, select_cols_right)
    """
    chunk_df, right_df, left_on, right_on, select_cols_right = args

    # Criar lookup uma única vez
    lookup = {
        right_df[right_on][i]: {col: right_df[col][i] for col in select_cols_right}
        for i in range(len(right_df))
    }

    joined_cols = chunk_df.columns + select_cols_right
    joined_df = DataFrame(columns=joined_cols)

    for i in range(len(chunk_df)):
        key = chunk_df[left_on][i]
        right_data = lookup.get(key, {col: 'unknown' for col in select_cols_right})
        joined_row = list(chunk_df[i].values()) + [right_data[col] for col in select_cols_right]
        joined_df.add_row(joined_row)

    return joined_df

class HandlerSort:
    def __init__(self, column: str, reverse: bool = False):
        self.column = column
        self.reverse = reverse

    def sort(self, df: DataFrame) -> DataFrame:
        if self.column not in df.columns:
            raise ValueError(f"Coluna '{self.column}' não encontrada no DataFrame.")

        indices = sorted(range(len(df)), key=lambda i: df[self.column][i], reverse=self.reverse)
        sorted_df = DataFrame(columns=df.columns)
        for i in indices:
            sorted_df.add_row([df[col][i] for col in df.columns])
        return sorted_df

class HandlerDateFilter:
    """Filtra por janela de tempo – days=1 para “último dia”, etc."""
    def __init__(self, days: int | None):
        self.days = days   # None ≡ all‑time

    def filter(self, df: DataFrame) -> DataFrame:
        if self.days is None or len(df) == 0:
            return df
        cutoff = datetime.now() - timedelta(days=self.days)
        keep = DataFrame(columns=df.columns)
        for i in range(len(df)):
            start = datetime.fromisoformat(df['start_date'][i])
            if start >= cutoff:
                keep.add_row(list(df[i].values()))
        return keep


class HandlerGroupByGenre:
    """Agrupa e conta visualizações por genre."""
    columns = ['content_genre', 'views']
    def group(self, df: DataFrame) -> DataFrame:
        result = DataFrame(columns=['content_genre', 'views'])
        counts = {}
        for i in range(len(df)):
            g = df['content_genre'][i]
            counts[g] = counts.get(g, 0) + 1
        for g, c in counts.items():
            result.add_row([g, c])
        return result

# handler_pool_wrappers.py
# Wrapper functions to use with multiprocessing.Pool


# handler_pool_wrappers.py
# Wrapper functions to use with multiprocessing.Pool


class HandlerUnfinishedByGenre:
    """Agrupa conteúdos com eventos 'play' ou 'pause' sem um 'stop', por gênero."""

    def group(self, df: DataFrame) -> DataFrame:
        # Dicionário: (user_id, content_id) → lista de eventos
        sessions = {}

        for i in range(len(df)):
            key = (df['user_id'][i], df['content_id'][i])
            event = df['event'][i]
            genre = df['genre'][i]

            if key not in sessions:
                sessions[key] = {'events': [], 'genre': genre}
            sessions[key]['events'].append(event)

        # Conta os conteúdos sem 'stop'
        genre_counts = {}
        for sess in sessions.values():
            events = sess['events']
            genre = sess['genre']
            if 'play' in events or 'pause' in events:
                if 'stop' not in events:
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1

        # Prepara DataFrame resultado
        result = DataFrame(columns=['content_genre', 'unfinished_views'])
        for genre, count in genre_counts.items():
            result.add_row([genre, count])

        return result