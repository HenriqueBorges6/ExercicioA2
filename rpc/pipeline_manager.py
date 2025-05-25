# pipeline_manager.py
import subprocess, threading, tempfile, os, time

TRANSFORM_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', 'transformed_data'))

class PipelineManager:
    def __init__(self, script="src/Pipeline.py"):
        self.script   = script
        self.lock     = threading.Lock()
        self.outputs  = {}
        self.last_run = 0

    def _run(self, nproc: int):
        with self.lock:
            try:
                print(f"[Pipeline] Running with {nproc} processes…")
                subprocess.check_call([ "python", self.script, str(nproc) ])
                self._collect_outputs()
                self.last_run = int(time.time())
                print("[Pipeline] Finished OK.")
            except subprocess.CalledProcessError as e:
                print("[Pipeline] Failed:", e)

    def _collect_outputs(self):
        for fname in [
            "event_count_last_hour.csv",
            "revenue_by_day.csv",
            "revenue_by_month.csv",
            "revenue_by_year.csv",
            "genre_views_last_24h.csv",
            "unfinished_by_genre.csv",
        ]:
            path = os.path.join(TRANSFORM_DIR, fname)
            if os.path.exists(path):
                with open(path, "rb") as f:
                    self.outputs[fname] = f.read()

    # API pública ----------------------------------------------------
    def trigger(self, nproc: int = 4):
        threading.Thread(target=self._run, args=(nproc,), daemon=True).start()

    def bundle(self):
        return self.outputs
