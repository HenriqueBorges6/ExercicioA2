import subprocess
import threading
import time
import sys
import os
import signal
from flask import Flask, jsonify, request

# ========== GLOBAL SETUP ==========
pipeline_lock = threading.Lock()
mock_processes = {}
_log_buffer = []

app = Flask(__name__)

# ========== LOGGING ==========
def log(msg):
    timestamped = f"[{time.ctime()}] {msg}"
    print(timestamped)
    _log_buffer.append(timestamped)
    if len(_log_buffer) > 50:
        _log_buffer.pop(0)


def get_last_logs():
    return _log_buffer

# ========== ENDPOINTS ==========
@app.route('/trigger_pipeline', methods=['POST'])
def trigger_pipeline_endpoint():
    log("app: Received request to trigger pipeline.")
    data = request.get_json() or {}
    num_processes = data.get('num_processes')
    if num_processes is None:
        log(f"app: No num_processes provided; using default.")
    else:
        log(f"app: Received num_processes={num_processes}")

    # Attempt to acquire lock
    if pipeline_lock.acquire(blocking=False):
        log("app: Lock acquired. Starting pipeline in background thread.")
        thread = threading.Thread(
            target=run_pipeline_and_release_lock,
            args=(num_processes,)
        )
        thread.daemon = True
        thread.start()
        return jsonify({"message": "Pipeline trigger accepted."}), 202
    else:
        log("app: Pipeline is already running. Request ignored.")
        return jsonify({"message": "Pipeline is already running."}), 409

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "is_running": pipeline_lock.locked(),
        "last_logs": get_last_logs()
    })

# ========== PIPELINE ==========
def run_pipeline_and_release_lock(num_processes=None):
    try:
        run_pipeline(called_from_app=True, num_processes=num_processes)
    finally:
        log("app Thread: Releasing lock after pipeline execution.")
        pipeline_lock.release()


def run_pipeline(called_from_app=False, num_processes=None):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_script = os.path.join(script_dir, 'src', 'Pipeline.py')

    # Build command
    cmd = [sys.executable, pipeline_script]
    if called_from_app and num_processes:
        cmd.append(str(num_processes))

    log(f"Pipeline Runner: Executing {' '.join(cmd)}")
    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        duration = time.time() - start_time
        log(f"Pipeline finished in {duration:.2f}s.")
        log(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        log(f"Pipeline failed in {duration:.2f}s; return code={e.returncode}")
        log(f"stdout: {e.stdout}")
        log(f"stderr: {e.stderr}")
    except Exception as e:
        duration = time.time() - start_time
        log(f"Pipeline error after {duration:.2f}s: {e}")

# ========== SCHEDULER ==========
def schedule_pipeline_runs(interval_seconds=300):
    log(f"Scheduler: scheduling every {interval_seconds}s.")
    next_time = time.time() + interval_seconds
    try:
        while True:
            now = time.time()
            if now >= next_time:
                log(f"Scheduler: Triggering scheduled run.")
                run_pipeline()
                next_time += interval_seconds
            time.sleep(1)
    except KeyboardInterrupt:
        log("Scheduler: Stopping scheduler.")

# ========== MOCK SERVICES ==========
def start_mock_process(name, rel_path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, rel_path)
    log(f"Starting mock: {name} -> {path}")
    proc = subprocess.Popen(
        [sys.executable, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    mock_processes[name] = proc
    def _stream():
        for line in proc.stdout:
            log(f"[{name}] {line.strip()}")
    t = threading.Thread(target=_stream, daemon=True)
    t.start()


def stop_all_mock_processes():
    for name, proc in mock_processes.items():
        log(f"Stopping mock: {name}")
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5)
        except:
            proc.kill()

# ========== MAIN ==========
if __name__ == '__main__':
    # Start mocks
    start_mock_process('mock_db', 'mock/mock_db.py')
    start_mock_process('mock_stream', 'mock/mock.py')

    # Start Flask
    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False), daemon=True)
    flask_thread.start()

    # Run scheduler
    schedule_pipeline_runs(interval_seconds=300)
    stop_all_mock_processes()
