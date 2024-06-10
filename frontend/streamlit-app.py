import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer

st.write("Realtime Voting Dashboard")


@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect(
        "host=localhost dbname=voting user=postgres password=postgres"
    )
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute("SELECT COUNT(*) FROM votes")
    total_voters = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute("SELECT COUNT(*) FROM candidates")
    total_candidates = cur.fetchone()[0]

    return total_voters, total_candidates


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)

    return data


def plot_colored_bar_chart(results):
    data_type = results["candidate_name"]

    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results["total_votes"], color=colors)
    plt.xlabel("Candidates")
    plt.ylabel("Vote Count per Candidate")
    plt.xticks(rotation=90)
    return plt


def plot_donut_chart(data):
    labels = list(data["candidate_name"])
    sizes = list(data["total_votes"])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140)
    ax.axis("equal")
    plt.title("Vote Percentage per Candidate")
    return fig


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # fetch voting stat from postgres
    total_voters, total_candidates = fetch_voting_stats()

    st.markdown("---")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", total_voters)
    col2.metric("Total Candidates", total_candidates)

    consumer = create_kafka_consumer(topic_name)
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    # Get leading candidate
    results = results.loc[results.groupby("candidate_id")["total_votes"].idxmax()]
    leading_candidate = results.loc[results["total_votes"].idxmax()]

    # Display leading candidate
    st.markdown("---")
    st.write("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate["photo_url"], width=200)
    with col2:
        st.header(leading_candidate["candidate_name"])
        st.subheader(leading_candidate["party_affiliation"])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")

    # Display the stats and visualize the data
    st.markdown("---")
    st.write("Voting Stats")
    results = results[["candidate_id", "candidate_name", "total_votes"]]
    results = results.reset_index(drop=True)

    # Display the bar chart and donut chart
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)


st.title("Realtime Voting Dashboard")
topic_name = "aggregated_votes_per_candidate"

update_data()
