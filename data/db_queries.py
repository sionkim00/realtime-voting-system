import random

import requests

import functions.kafka as kafka

BASE_URL = "https://randomuser.me/api/?nat=us"
PARTIES = ["Republican", "Democrat", "Independent"]

random.seed(1004)


def create_tables(conn, cur):
    # CREATE TABLE candidates
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platforms TEXT,
            photo_url TEXT
        )
        """
    )

    # CREATE TABLE voters
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    # CREATE TABLE votes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote INTEGER DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()


def get_all_candidates(conn, cur):
    cur.execute("""
        SELECT * FROM candidates
    """)
    return cur.fetchall()


def generate_candidate_data(conn, cur, candidate_number, total_parties):
    gender = "female" if candidate_number % 2 == 0 else "male"
    response = requests.get(f"{BASE_URL}&gender={gender}")

    if response.status_code == 200:
        data = response.json()
        candidate = data["results"][0]

        return {
            "candidate_id": candidate["login"]["uuid"],
            "candidate_name": f"{candidate['name']['first']} {candidate['name']['last']}",
            "party_affiliation": random.choice(PARTIES),
            "biography": "A candidate for the people.",
            "campaign_platforms": "Make the world a better place.",
            "photo_url": candidate["picture"]["large"],
        }
    else:
        print(f"Error (Random User API): {response.status_code}")
        return None


def insert_candidate_data(conn, cur, candidate):
    cur.execute(
        """
        INSERT INTO candidates(candidate_id, candidate_name, party_affiliation, biography, campaign_platforms, photo_url)
        VALUES(%s, %s, %s, %s, %s, %s)
    """,
        (
            candidate["candidate_id"],
            candidate["candidate_name"],
            candidate["party_affiliation"],
            candidate["biography"],
            candidate["campaign_platforms"],
            candidate["photo_url"],
        ),
    )
    conn.commit()


def generate_voter_datas(conn, cur, producer, voter_number):
    for i in range(voter_number):
        voter_data = generate_voter_data()
        insert_voters_data(conn, cur, voter_data)
        # Produce the voter data to Kafka
        kafka.produce_voter_data(producer, voter_data)

    conn.commit()


def generate_voter_data():
    response = requests.get(f"{BASE_URL}")

    if response.status_code == 200:
        data = response.json()
        voter = data["results"][0]

        return {
            "voter_id": voter["login"]["uuid"],
            "voter_name": f"{voter['name']['first']} {voter['name']['last']}",
            "date_of_birth": voter["dob"]["date"],
            "gender": voter["gender"],
            "nationality": voter["nat"],
            "registration_number": voter["login"]["username"],
            "address": {
                "street": f"{voter["location"]["street"]["number"]} {voter['location']['street']['name']}",
                "city": voter["location"]["city"],
                "state": voter["location"]["state"],
                "country": voter["location"]["country"],
                "postcode": voter["location"]["postcode"],
            },
            "email": voter["email"],
            "phone_number": voter["phone"],
            "picture": voter["picture"]["large"],
            "registered_age": voter["registered"]["age"],
        }


def insert_voters_data(conn, cur, voter_data):
    print(voter_data["address"])
    cur.execute(
        """
        INSERT INTO voters(voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, picture, registered_age)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            voter_data["voter_id"],
            voter_data["voter_name"],
            voter_data["date_of_birth"],
            voter_data["gender"],
            voter_data["nationality"],
            voter_data["registration_number"],
            voter_data["address"]["street"],
            voter_data["address"]["city"],
            voter_data["address"]["state"],
            voter_data["address"]["country"],
            voter_data["address"]["postcode"],
            voter_data["email"],
            voter_data["phone_number"],
            voter_data["picture"],
            voter_data["registered_age"],
        ),
    )

    conn.commit()
