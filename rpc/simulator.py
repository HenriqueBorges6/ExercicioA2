import grpc
import time, uuid
import event_pb2, event_pb2_grpc
import random
import datetime


EVENT_TYPES = [
    'user', 'content', 'episode', 'rating', 'device', 'view_history',
    'plan', 'subscription', 'revenue'
]

PLANS = [('basic', 2.99, 1), ('standard', 7.99, 2), ('premium', 17.99, 4)]

def now_iso() -> str:
    return datetime.datetime.now().isoformat()

def random_date(start_year=1970, end_year=2020):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    return start + datetime.timedelta(days=random.randint(0, (end - start).days))


def simulate_event(event_type: str):
    match event_type:
        case 'user':
            return event_pb2.Event(user_event=event_pb2.UserEvent(
                user_id = str(uuid.uuid4()),
                user_name = f"user_{random.randint(1000, 9999)}",
                user_email = f"{str(uuid.uuid4())}@example.com",
                user_birthdate = random_date(1970, 2005).isoformat(),
                user_service_plan = random.choice(PLANS)[0],
                user_signup_date = now_iso()
            ))
        case 'content':
            return event_pb2.Event(content_event=event_pb2.ContentEvent(
                content_id = str(uuid.uuid4()),
                content_title = f"Content_{random.randint(1000, 9999)}",
                content_type = random.choice(['movie', 'series', 'play', 'documentary', 'anime', 'podcast']),
                content_genre = random.choice([
                    'action', 'drama', 'comedy', 'thriller', 'horror',
                    'romance', 'sports', 'sci-fi', 'adventure', 'mystery'
                ])
            ))
        case 'episode':
            return event_pb2.Event(episode_event=event_pb2.EpisodeEvent(
                episode_id = str(uuid.uuid4()),
                content_id = str(uuid.uuid4())  # Simulate new content_id reference
            ))
        case 'rating':
            return event_pb2.Event(rating_event=event_pb2.RatingEvent(
                rating_id = str(uuid.uuid4()),
                grade = str(random.randint(1, 5)),
                rating_date = now_iso(),
                user_id = str(uuid.uuid4()),
                content_id = str(uuid.uuid4())
            ))
        case 'device':
            return event_pb2.Event(device_event=event_pb2.DeviceEvent(
                device_id = str(uuid.uuid4()),
                device_type = random.choice(['tv', 'phone', 'tablet', 'desktop']),
                user_id = str(uuid.uuid4())
            ))
        case 'view_history':
            start = datetime.datetime.now()
            end = start + datetime.timedelta(minutes=random.randint(1, 120))
            return event_pb2.Event(view_history_event=event_pb2.ViewHistoryEvent(
                view_id = str(uuid.uuid4()),
                start_date = start.isoformat(),
                end_date = end.isoformat(),
                device_id = str(uuid.uuid4()),
                user_id = str(uuid.uuid4()),
                content_id = str(uuid.uuid4()),
                episode_id = str(uuid.uuid4()) if random.random() < 0.7 else ''
            ))
        case 'plan':
            plan = random.choice(PLANS)
            return event_pb2.Event(plan_event=event_pb2.PlanEvent(
                plan_id = plan[0],
                plan_name = plan[0],
                plan_price = str(plan[1]),
                num_screens = str(plan[2])
            ))
        case 'subscription':
            date_part = random_date(2025, 2025).isoformat()
            time_part = f"{random.randint(0,23):02}:{random.randint(0,59):02}:00"
            transaction_date = f"{date_part}T{time_part}"
            return event_pb2.Event(subscription_transaction_event=event_pb2.SubscriptionTransactionEvent(
                transaction_id = str(uuid.uuid4()),
                transaction_date = transaction_date,
                payment_method = random.choice(['credit_card', 'paypal', 'gift_card', 'cashflix']),
                user_id = str(uuid.uuid4()),
                plan_id = random.choice(['basic', 'standard', 'premium'])
            ))
        case 'revenue':
            date_part = datetime.date.today().isoformat()
            return event_pb2.Event(revenue_event=event_pb2.RevenueEvent(
                revenue_id = str(uuid.uuid4()),
                date = date_part,
                value = str(round(random.uniform(2.99, 17.99), 2))
            ))
        case _:
            raise ValueError(f"Event type '{event_type}' not recognized.")

channel = grpc.insecure_channel("localhost:50051")
stub = event_pb2_grpc.EventServiceStub(channel)

def run():
    print("[Simulator] Starting gRPC event generation...")
    try:
        while True:
            event_type = random.choice(EVENT_TYPES)
            event = simulate_event(event_type)
            print(event)
            ack = stub.SendEvent(event)
            print(f"[{now_iso()}] Sent {event_type} event: {ack.status}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("[Simulator] Stopping simulator...")


if __name__ == "__main__":
    run()