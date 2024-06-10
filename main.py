import psycopg2
from confluent_kafka import SerializingProducer

import data.db_queries as db_queries

if __name__ == "__main__":
    # Kafka Producer
    producer = SerializingProducer({"bootstrap.servers": "localhost:9092"})

    try:
        conn = psycopg2.connect(
            "host=localhost dbname=voting user=postgres password=postgres"
        )
        cur = conn.cursor()

        db_queries.create_tables(conn, cur)

        candidates = db_queries.get_all_candidates(conn, cur)

        # print(candidates)

        if len(candidates) == 0:
            # No candidates found, generate some data
            for i in range(3):
                candidate = db_queries.generate_candidate_data(conn, cur, i, 3)
                # print(candidate)

                db_queries.insert_candidate_data(conn, cur, candidate)

        # Generate voter data
        db_queries.generate_voter_datas(conn, cur, producer, 1000)
    except Exception as e:
        print(f"Error (Postgres connection error): {e}")
