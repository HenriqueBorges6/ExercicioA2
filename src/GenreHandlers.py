from DataFrame import DataFrame
from datetime import datetime, timedelta

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