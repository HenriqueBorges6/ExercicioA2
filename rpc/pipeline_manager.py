# rpc/pipeline_manager.py
import os
import subprocess
import threading
import time
from pathlib import Path

# ───────────── Caminhos absolutos ──────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parents[1]              # …/ExercicioA2
PIPE_PATH  = BASE_DIR / "rpc"/"src" / "Pipeline.py"                 # script ETL
TRANS_DIR  = BASE_DIR / "transformed_data"                    # saída CSVs
TRANS_DIR.mkdir(exist_ok=True)

CSV_LIST = [
    "event_count_last_hour.csv",
    "revenue_by_day.csv",
    "revenue_by_month.csv",
    "revenue_by_year.csv",
    "genre_views_last_24h.csv",
    "unfinished_by_genre.csv",
]

# ╭─────────────────────── classe controller ───────────────────────╮
class PipelineManager:
    def __init__(self, script=str(PIPE_PATH)):
        self.script    = script
        self.lock      = threading.Lock()
        self.outputs   = {}           # {nome_csv: bytes, "_last_finished_ms": int}
        self.last_run  = 0

    # ---------------- executor interno -----------------------------
    def _run(self, nproc: int):
        with self.lock:
            try:
                print(f"[Pipeline] Running with {nproc} processes…")
                subprocess.check_call(["python", self.script, str(nproc)])
                self._collect_outputs()
                self.last_run = int(time.time())
                print("[Pipeline] Finished OK.")
            except subprocess.CalledProcessError as e:
                print("[Pipeline] Failed:", e)

    # ---------------- coleta dos CSVs ------------------------------
    def _collect_outputs(self):
        for fname in CSV_LIST:
            path = TRANS_DIR / fname
            if path.exists():
                self.outputs[fname] = path.read_bytes()
        # timestamp de término (ms)
        self.outputs["_last_finished_ms"] = int(time.time() * 1000)

    # ---------------- API pública ----------------------------------
    def trigger(self, nproc: int = 4):
        """
        Dispara o pipeline em background com `nproc` processos.
        """
        threading.Thread(target=self._run, args=(nproc,), daemon=True).start()

    def bundle(self) -> dict:
        """
        Devolve {nome_csv: bytes, "_last_finished_ms": int}.
        """
        return self.outputs
