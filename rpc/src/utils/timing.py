# utils/timing.py
from time import perf_counter
from datetime import datetime
import csv, os

_METRIC_FILE = os.path.join(
    os.path.dirname(__file__), "..", "transformed_data", "stage_metrics.csv"
)
os.makedirs(os.path.dirname(_METRIC_FILE), exist_ok=True)

def log_stage(stage: str, procs: int, seconds: float) -> None:
    """Acrescenta uma linha no CSV de métricas."""
    with open(_METRIC_FILE, "a", newline="") as f:
        csv.writer(f).writerow(
            [datetime.now().isoformat(timespec="seconds"), stage, procs, round(seconds, 3)]
        )

class StageTimer:
    """Context‑manager para medir e já registrar o tempo."""
    def __init__(self, stage: str, procs: int):
        self.stage = stage
        self.procs = procs
    def __enter__(self):
        from time import perf_counter
        self._t0 = perf_counter()
    def __exit__(self, *exc):
        dt = perf_counter() - self._t0
        log_stage(self.stage, self.procs, dt)
