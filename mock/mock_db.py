import sqlite3
import uuid
import random
import datetime
import time
from abc import ABC, abstractmethod
from typing import List, Tuple

DB_FILE: str = "streaming_mock.db"


# Utility -----------------------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.datetime.now().isoformat()

def random_date(start_year=1970, end_year=2020):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    return start + datetime.timedelta(days=random.randint(0, (end - start).days))


# Base Generator ----------------------------------------------------------------------------------

class MockEntityGenerator(ABC):
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    @abstractmethod
    def generate(self, *args, **kwargs) -> None: ...


# Generators --------------------------------------------------------------------------------------

class MockUserGenerator(MockEntityGenerator):
    plans: List[str] = ['basic', 'standard', 'premium']

    def generate(self, n: int) -> None:
        users: List[Tuple[str]] = []
        for _ in range(n):
            user_id: str = str(uuid.uuid4())
            user_name: str = f"user_{random.randint(1000, 9999)}"
            user_email: str = f"{user_name}@example.com"
            user_birthdate: str = random_date(1970, 2005).isoformat()
            user_service_plan: str = random.choice(self.plans)
            user_signup_date: str = now_iso()
            users.append((
                user_id, user_name, user_email, user_birthdate,
                user_service_plan, user_signup_date))

        self.conn.executemany("""
            INSERT INTO User (user_id, user_name, user_email, user_birthdate,
                              user_service_plan, user_signup_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, users)
        self.conn.commit()

class MockPlanGenerator(MockEntityGenerator):
    def generate(self) -> None:
        plans: List[Tuple] = [
            ('basic', 2.99, 1),
            ('standard', 7.99, 2),
            ('premium', 17.99, 4),
        ]
        self.conn.executemany("""
            INSERT OR IGNORE INTO Plan (plan_id, plan_name, plan_price, num_screens)
            VALUES (?, ?, ?, ?)
        """, [(p[0], p[0], p[1], p[2]) for p in plans])
        self.conn.commit()

class MockContentGenerator(MockEntityGenerator):
    def generate(self, n: int) -> None:
        genres: List[str] = [
            'action', 'drama', 'comedy', 'thriller', 'horror',
            'romance', 'sports', 'sci-fi', 'adventure', 'mystery',
        ]
        content_types: List[str] = ['movie', 'series', 'play', 'documentary', 'anime', 'podcast']
        content: List[Tuple] = []
        for _ in range(n):
            content_id: str = str(uuid.uuid4())
            title: str = f"Content_{random.randint(1000, 9999)}"
            ctype: str = random.choice(content_types)
            genre: str = random.choice(genres)
            content.append((content_id, title, ctype, genre))

        self.conn.executemany("""
            INSERT INTO Content (content_id, content_title, content_type, content_genre)
            VALUES (?, ?, ?, ?)
        """, content)
        self.conn.commit()

class MockEpisodeGenerator(MockEntityGenerator):
    def generate(self, content_ids: List[str]) -> None:
        episodes: List[Tuple] = []
        for cid in content_ids:
            if random.random() < 0.5:
                continue  # Only some contents get episodes
            for i in range(1, random.randint(2, 6)):
                episode_id: str = str(uuid.uuid4())
                episodes.append((episode_id, cid))

        self.conn.executemany("""
            INSERT INTO Episode (episode_id, content_id)
            VALUES (?, ?)
        """, episodes)
        self.conn.commit()

class MockDeviceGenerator(MockEntityGenerator):
    device_types: List[str] = ['tv', 'phone', 'tablet', 'desktop']

    def generate(self, user_ids: List[str]) -> None:
        devices: List[Tuple] = []
        for uid in user_ids:
            for _ in range(random.randint(1, 2)):
                device_id: str = str(uuid.uuid4())
                dtype: str = random.choice(self.device_types)
                devices.append((device_id, dtype, uid))

        self.conn.executemany("""
            INSERT INTO Device (device_id, device_type, user_id)
            VALUES (?, ?, ?)
        """, devices)
        self.conn.commit()

class MockSubscriptionTransactionGenerator(MockEntityGenerator):
    payment_methods: List[str] = ['credit_card', 'paypal', 'gift_card', 'cashflix']

    def generate(self, user_ids: List[str]) -> None:
        transactions: List[Tuple[str]] = []
        for uid in user_ids:
            plan_id = random.choice(['basic', 'standard', 'premium'])
            t_id = str(uuid.uuid4())
            
            date_part = random_date(2025, 2025).isoformat()  # 'YYYY-MM-DD'
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            t_date = f"{date_part}T{hour:02d}:{minute:02d}:00"
            
            pay_method = random.choice(self.payment_methods)
            transactions.append((t_id, t_date, pay_method, uid, plan_id))

        # Store transactions with random timestamp
        self.conn.executemany("""
            INSERT INTO SubscriptionTransactions
                (transaction_id, transaction_date, payment_method, user_id, plan_id)
            VALUES (?, ?, ?, ?, ?)
        """, transactions)
        self.conn.commit()

        # Then populate Revenue as before
        revenue_records: List[Tuple[str]] = []
        for t_id, t_date, _, _, plan_id in transactions:
            date_part = t_date.split('T')[0]
            price = self.conn.execute(
                "SELECT plan_price FROM Plan WHERE plan_id = ?",
                (plan_id,)
            ).fetchone()[0]
            rev_id = str(uuid.uuid4())
            revenue_records.append((rev_id, date_part, price))

        self.conn.executemany("""
            INSERT INTO Revenue (revenue_id, date, value)
            VALUES (?, ?, ?)
        """, revenue_records)
        self.conn.commit()

class MockRatingGenerator(MockEntityGenerator):
    def generate(self, user_ids: List[str], content_ids: List[str], n: int = 100) -> None:
        ratings = []
        for _ in range(n):
            rid = str(uuid.uuid4())
            grade = random.randint(1, 5)
            rdate = now_iso()
            uid = random.choice(user_ids)
            cid = random.choice(content_ids)
            ratings.append((rid, grade, rdate, uid, cid))
        self.conn.executemany("""
            INSERT INTO Rating (rating_id, grade, rating_date, user_id, content_id)
            VALUES (?, ?, ?, ?, ?)
        """, ratings)
        self.conn.commit()

class MockViewHistoryGenerator(MockEntityGenerator):
    def generate(
            self, user_ids: List[str], content_ids: List[str],
            episode_ids: List[str], device_ids: List[str], n: int = 100
        ) -> None:
        views = []
        for _ in range(n):
            vid = str(uuid.uuid4())
            start = datetime.datetime.now()
            end = start + datetime.timedelta(minutes=random.randint(1, 120))
            uid = random.choice(user_ids)
            cid = random.choice(content_ids)
            eid = random.choice(episode_ids) if episode_ids and random.random() < 0.7 else None
            did = random.choice(device_ids)
            views.append((vid, start.isoformat(), end.isoformat(), did, uid, cid, eid))
        self.conn.executemany("""
            INSERT INTO ViewHistory (view_id, start_date, end_date, device_id, user_id, content_id, episode_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, views)
        self.conn.commit()


# Schema Setup ------------------------------------------------------------------------------------

def create_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS User (
        user_id TEXT PRIMARY KEY,
        user_name TEXT,
        user_email TEXT,
        user_birthdate TEXT,
        user_service_plan TEXT,
        user_signup_date TEXT
    );

    CREATE TABLE IF NOT EXISTS Content (
        content_id TEXT PRIMARY KEY,
        content_title TEXT,
        content_type TEXT,
        content_genre TEXT
    );

    CREATE TABLE IF NOT EXISTS Episode (
        episode_id TEXT PRIMARY KEY,
        content_id TEXT REFERENCES Content(content_id)
    );

    CREATE TABLE IF NOT EXISTS Rating (
        rating_id TEXT PRIMARY KEY,
        grade INTEGER,
        rating_date TEXT,
        user_id TEXT REFERENCES User(user_id),
        content_id TEXT REFERENCES Content(content_id)
    );

    CREATE TABLE IF NOT EXISTS Device (
        device_id TEXT PRIMARY KEY,
        device_type TEXT,
        user_id TEXT REFERENCES User(user_id)
    );

    CREATE TABLE IF NOT EXISTS ViewHistory (
        view_id TEXT PRIMARY KEY,
        start_date TEXT,
        end_date TEXT,
        device_id TEXT REFERENCES Device(device_id),
        user_id TEXT REFERENCES User(user_id),
        content_id TEXT REFERENCES Content(content_id),
        episode_id TEXT REFERENCES Episode(episode_id)
    );

    CREATE TABLE IF NOT EXISTS Plan (
        plan_id TEXT PRIMARY KEY,
        plan_name TEXT,
        plan_price REAL,
        num_screens INTEGER
    );

    CREATE TABLE IF NOT EXISTS SubscriptionTransactions (
        transaction_id TEXT PRIMARY KEY,
        transaction_date TEXT,
        payment_method TEXT,
        user_id TEXT REFERENCES User(user_id),
        plan_id TEXT REFERENCES Plan(plan_id)
    );
                      

    CREATE TABLE IF NOT EXISTS Revenue (
        revenue_id TEXT PRIMARY KEY,
        date TEXT,
        value REAL
    );

    """)
    conn.commit()


# Main Loop ---------------------------------------------------------------------------------------

def main_loop() -> None:
    conn = sqlite3.connect(DB_FILE)
    create_schema(conn)

    plan_gen = MockPlanGenerator(conn)
    user_gen = MockUserGenerator(conn)
    content_gen = MockContentGenerator(conn)
    episode_gen = MockEpisodeGenerator(conn)
    device_gen = MockDeviceGenerator(conn)
    trans_gen = MockSubscriptionTransactionGenerator(conn)
    rating_gen = MockRatingGenerator(conn)
    view_gen = MockViewHistoryGenerator(conn)

    plan_gen.generate()

    try:
        while True:
            print(f"[{time.ctime()}] Generating new mock data...")

            user_gen.generate(100)
            content_gen.generate(50)

            user_ids = [r[0] for r in conn.execute("SELECT user_id FROM User").fetchall()]
            content_ids = [r[0] for r in conn.execute("SELECT content_id FROM Content").fetchall()]
            episode_ids = [r[0] for r in conn.execute("SELECT episode_id FROM Episode").fetchall()]
            device_ids = [r[0] for r in conn.execute("SELECT device_id FROM Device").fetchall()]

            episode_gen.generate(content_ids)
            device_gen.generate(user_ids)
            trans_gen.generate(user_ids)

            rating_gen.generate(user_ids, content_ids, n=100)
            view_gen.generate(user_ids, content_ids, episode_ids, device_ids, n=100)

            print(f"[{time.ctime()}] Mock DB updated. Sleeping...\n")
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[{time.ctime()}] Mock DB: KeyboardInterrupt received. Stopping DB generation.")
    except Exception as e:
        print(f"[{time.ctime()}] Mock DB Error: An unexpected error occurred in the generation loop: {e}")
    finally:
        print(f"[{time.ctime()}] Mock DB: Exited.")


if __name__ == "__main__":
    main_loop()
