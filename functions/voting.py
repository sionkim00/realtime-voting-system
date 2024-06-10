import random
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaError, SerializingProducer


def delivery_report(err, msg):
    if err is not None:
        print(f"Error (kafka) Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


config = {
    "bootstrap.servers": "localhost:9092",
}

consumer = Consumer(
    config
    | {
        "group.id": "voting-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }
)

producer = SerializingProducer(config)

if __name__ == "__main__":
    conn = psycopg2.connect(
        "host=localhost dbname=voting user=postgres password=postgres"
    )
    cur = conn.cursor()

    candidnates_query = cur.execute("""
        SELECT row_to_json(col) 
        FROM (
                SELECT * FROM candidates
        ) col;
    """)
    candidates = [candidate[0] for candidate in cur.fetchall()]

    if len(candidates) == 0:
        raise Exception("No candidates found")
    else:
        print(f"{len(candidates)} Candidates found")

    # Subscribe to the voters topic
    consumer.subscribe(["voters_topic"])

    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error (Kafka): {message.error()}")
            else:
                voter = json.loads(message.value().decode("utf-8"))
                chosen_candidate = random.choice(candidates)
                vote = (
                    voter
                    | chosen_candidate
                    | {
                        "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "vote": 1,
                    }
                )

                try:
                    print(
                        f"Voter: {vote["voter_id"]} is voting for {vote["candidate_id"]}"
                    )
                    cur.execute(
                        """
                        INSERT INTO votes (voter_id, candidate_id, voting_time)
                        VALUES (%s, %s, %s)
                    """,
                        (vote["voter_id"], vote["candidate_id"], vote["voting_time"]),
                    )
                    conn.commit()

                    producer.produce(
                        "votes_topic",
                        key=str(vote["voter_id"]),
                        value=json.dumps(vote),
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)

                except Exception as e:
                    print(f"Error (Postgres): {e}")

    except Exception as e:
        print(f"Error (Kafka): {e}")
