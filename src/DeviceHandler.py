from DataFrame import DataFrame
from datetime import datetime, timedelta
class HandlerJoinDevice:
    """Une evento com device_types baseado em user_id ou session_id."""
    def join(self, event_df: DataFrame, device_df: DataFrame) -> DataFrame:
        if len(event_df) == 0 or len(device_df) == 0:
            return DataFrame(columns=event_df.columns + ['device_types'])

        lookup = {device_df['session_id'][i]: device_df['device_types'][i]
                  for i in range(len(device_df))}

        joined = DataFrame(columns=event_df.columns + ['device_types'])
        for i in range(len(event_df)):
            sid = event_df['session_id'][i]
            device = lookup.get(sid, 'unknown')
            joined.add_row(list(event_df[i].values()) + [device])
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

class HandlerGroupByDeviceEngagement:
    """Conta eventos por tipo de dispositivo."""
    def group(self, df: DataFrame) -> DataFrame:
        result = DataFrame(columns=['device_types', 'views'])

        counts = {}
        for i in range(len(df)):
            d = df['device_types'][i]
            counts[d] = counts.get(d, 0) + 1
            
        for d, c in counts.items():
            result.add_row([d, c])
        return result