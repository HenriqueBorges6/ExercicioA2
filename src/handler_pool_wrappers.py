from Handler import HandlerValueCount, HandlerJoin, HandlerDateFilter, HandlerGroupByGenre
from typing import List, Dict, Any, Tuple
from DataFrame import DataFrame

def count_events_last_hour_chunk(df_chunk: DataFrame) -> DataFrame:
    """Wrapper for HandlerValueCount.count_events_last_hour"""
    return HandlerValueCount().count_events_last_hour(df_chunk)

def join_view_with_content(args: Tuple[DataFrame, DataFrame]) -> DataFrame:
    """Wrapper for HandlerJoin with fixed join on content_id and genre"""
    chunk, content_df = args
    return HandlerJoin().join(
        left_df=chunk,
        right_df=content_df,
        left_on='content_id',
        right_on='content_id',
        select_cols_right=['content_genre']
    )

def filter_by_days_chunk(args: Tuple[DataFrame, int | None]) -> DataFrame:
    """Wrapper for HandlerDateFilter(days).filter"""
    chunk, days = args
    return HandlerDateFilter(days).filter(chunk)

def group_by_genre_chunk(df_chunk: DataFrame) -> DataFrame:
    """Wrapper for HandlerGroupByGenre.group"""
    return HandlerGroupByGenre().group(df_chunk)
