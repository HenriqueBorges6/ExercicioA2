import uuid
import datetime as dt
import random
import time
import os
from typing import List
import numpy as np


# Configuration ----------------------------------------------------------------------------------

STREAMING_LOG_DIR: str = "streaming_logs"
os.makedirs(STREAMING_LOG_DIR, exist_ok=True)

NUMBER_OF_LOGS_PER_FILE: int = 1000
FILE_INTERVAL_SEC: float = 0.1 

NUMBER_OF_USERS: int = 100
EVENT_TYPES: List[str] = ['play', 'pause', 'stop', 'search', 'login', 'logout', 'like', 'dislike', 'skip_ad']
CONTENT_IDS: List[str] = [str(uuid.uuid4()) for _ in range(50)]
GENRES: List[str] = ['action', 'comedy', 'drama', 'thriller', 'sports']

# Dirichlet distribution for event sampling (α = 1 for uniform)
DIRICHLET_ALPHA: np.ndarray = np.ones(len(EVENT_TYPES))


# Utility -----------------------------------------------------------------------------------------

def now_iso() -> str:
    """Returns the current timestamp in ISO format."""
    return dt.datetime.now().isoformat(timespec='seconds')


# Log generation ----------------------------------------------------------------------------------

def generate_log_entry(event_probs: np.ndarray) -> str:
    """Generates a CSV line representing a single log event."""
    event: str = np.random.choice(EVENT_TYPES, p=event_probs)

    log_id: str = str(uuid.uuid4())
    user_id: str = f"user_{random.randint(1, NUMBER_OF_USERS)}"
    timestamp: str = now_iso()
    content_id: str = random.choice(CONTENT_IDS)
    genre: str = random.choice(GENRES)

    return f"{log_id},{user_id},{timestamp},{event},{content_id},{genre}"

def generate_log_file() -> None:
    """Creates a log file containing multiple entries."""
    filename: str = f"log_{dt.datetime.now():%Y%m%d_%H%M%S}.txt"
    filepath: str = os.path.join(STREAMING_LOG_DIR, filename)
    event_probs: np.ndarray = np.random.dirichlet(DIRICHLET_ALPHA)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("log_id,user_id,time,event,content_id,genre\n")
        for _ in range(NUMBER_OF_LOGS_PER_FILE):
            f.write(generate_log_entry(event_probs) + "\n")

    print(f"[INFO] Mock: Generated file {filepath} with {NUMBER_OF_LOGS_PER_FILE} logs.")


# Main --------------------------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"[INFO] Mock: Starting log generation. Output dir: {STREAMING_LOG_DIR}")
    try:
        while True:
            generate_log_file()
            # time.sleep(FILE_INTERVAL_SEC)
    except KeyboardInterrupt:
        print(f"\n[INFO] Mock: Stopped by user.")
