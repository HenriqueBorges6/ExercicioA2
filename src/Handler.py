from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from DataFrame import DataFrame
import os

# ======================== Handler: Value Count ========================

class HandlerValueCount:
    WINDOW_HOURS = int(os.getenv("EVENT_WINDOW", "1"))

    def count_events_last_hour(self, df: DataFrame) -> DataFrame:
        if len(df) == 0:
            return DataFrame(columns=["event", "quantidade"])

        time_col  = "time"  if "time"  in df.columns else "timestamp"
        event_col = "event" if "event" in df.columns else "event_type"

        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.WINDOW_HOURS)
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
        if not isinstance(df, DataFrame):
            raise TypeError("Argument `df` must be a DataFrame object.")

        if group_col not in df.columns or sum_col not in df.columns:
            raise ValueError("Grouping or summing column not found in DataFrame.")

        grouped_data: Dict[Any, float] = {}
        for i in range(len(df)):
            key = df[group_col][i]
            value = float(df[sum_col][i])
            grouped_data[key] = grouped_data.get(key, 0.0) + value

        result_df = DataFrame(columns=[group_col, sum_col])
        for key, total_sum in grouped_data.items():
            result_df.add_row([key, int(total_sum)])

        return result_df

# ======================== Handler: Revenue ========================

class RevenueAnalyzer:
    def __init__(self, df: DataFrame):
        self.df = df

    def _parse_date(self, date_str: str) -> datetime:
        return datetime.strptime(date_str, "%Y-%m-%d")

    def _analyze_revenue(self, time_format: str) -> Dict[str, float]:
        revenue = defaultdict(float)
        for i in range(len(self.df)):
            date_str = self.df["date"][i]
            value = float(self.df["value"][i])
            date = self._parse_date(date_str)
            key = date.strftime(time_format)
            revenue[key] += value
        return dict(revenue)

    def analyze_revenue_by_day(self) -> Dict[str, float]:
        return self._analyze_revenue('%Y-%m-%d')

    def analyze_revenue_by_month(self) -> Dict[str, float]:
        return self._analyze_revenue('%Y-%m')

    def analyze_revenue_by_year(self) -> Dict[str, float]:
        return self._analyze_revenue('%Y')

# ======================== Handler: Join ========================

class HandlerJoin:
    def join(self, left_df: DataFrame, right_df: DataFrame, left_on: str, right_on: str, select_cols_right: List[str]) -> DataFrame:
        if len(left_df) == 0 or len(right_df) == 0:
            return DataFrame(columns=left_df.columns + select_cols_right)

        lookup = {
            right_df[right_on][i]: {col: right_df[col][i] for col in select_cols_right}
            for i in range(len(right_df))
        }

        joined = DataFrame(columns=left_df.columns + select_cols_right)
        for i in range(len(left_df)):
            key = left_df[left_on][i]
            right_data = lookup.get(key, {col: 'unknown' for col in select_cols_right})
            joined_row = list(left_df[i].values()) + [right_data[col] for col in select_cols_right]
            joined.add_row(joined_row)

        return joined

class HandlerJoinContent:
    def join(self, vh_df: DataFrame, content_df: DataFrame) -> DataFrame:
        if len(vh_df) == 0 or len(content_df) == 0:
            return DataFrame(columns=vh_df.columns + ['content_genre'])

        genre_lookup = {content_df['content_id'][i]: content_df['content_genre'][i]
                        for i in range(len(content_df))}

        joined = DataFrame(columns=vh_df.columns + ['content_genre'])
        for i in range(len(vh_df)):
            cid = vh_df['content_id'][i]
            genre = genre_lookup.get(cid, 'unknown')
            joined.add_row(list(vh_df[i].values()) + [genre])
        return joined

def join_chunk_worker(args: tuple) -> DataFrame:
    chunk_df, right_df, left_on, right_on, select_cols_right = args
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

# ======================== Handler: Sorting and Filtering ========================

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
    def __init__(self, days: int | None):
        self.days = days

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

# ======================== Handler: Grouping ========================

class HandlerGroupByGenre:
    def group(self, df: DataFrame) -> DataFrame:
        result = DataFrame(columns=['content_genre', 'views'])
        counts = {}
        for i in range(len(df)):
            g = df['content_genre'][i]
            counts[g] = counts.get(g, 0) + 1
        for g, c in counts.items():
            result.add_row([g, c])
        return result

class HandlerUnfinishedByGenre:
    def group(self, df: DataFrame) -> DataFrame:
        sessions = {}
        for i in range(len(df)):
            key = (df['user_id'][i], df['content_id'][i])
            event = df['event'][i]
            genre = df['genre'][i]
            if key not in sessions:
                sessions[key] = {'events': [], 'genre': genre}
            sessions[key]['events'].append(event)

        genre_counts = {}
        for sess in sessions.values():
            events = sess['events']
            genre = sess['genre']
            if 'play' in events or 'pause' in events:
                if 'stop' not in events:
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1

        result = DataFrame(columns=['content_genre', 'unfinished_views'])
        for genre, count in genre_counts.items():
            result.add_row([genre, count])
        return result
