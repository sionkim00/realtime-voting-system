import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide")  # Set the page layout to wide

st.title("Realtime Voting Dashboard")


@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect(
        "host=localhost dbname=voting user=postgres password=postgres"
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM votes")
    total_voters = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM candidates")
    total_candidates = cur.fetchone()[0]

    return total_voters, total_candidates


def create_kafka_consumer(topic_name):
    return KafkaConsumer(
        topic_name,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    return [msg.value for msg_batch in messages.values() for msg in msg_batch]


def plot_colored_bar_chart(results):
    colors = plt.cm.viridis(np.linspace(0, 1, len(results)))
    plt.bar(results["candidate_name"], results["total_votes"], color=colors)
    plt.xlabel("Candidates")
    plt.ylabel("Vote Count per Candidate")
    plt.xticks(rotation=90)
    return plt


def plot_donut_chart(data):
    fig, ax = plt.subplots()
    ax.pie(
        data["total_votes"],
        labels=data["candidate_name"],
        autopct="%1.1f%%",
        startangle=140,
    )
    ax.axis("equal")
    plt.title("Vote Percentage per Candidate")
    return fig


@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    return [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]


def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    total_pages = max(1, -(-len(table_data) // batch_size))  # ceiling division
    with bottom_menu[1]:
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    st.dataframe(pages[current_page - 1], use_container_width=True)


def sidebar():
    if st.session_state.get("latest_update") is None:
        st.session_state["latest_update"] = time.time()

    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 1, 60, 5)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button("Refresh Data"):
        update_data()


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    total_voters, total_candidates = fetch_voting_stats()

    st.markdown("---")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", total_voters)
    col2.metric("Total Candidates", total_candidates)

    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    results = results.loc[results.groupby("candidate_id")["total_votes"].idxmax()]
    leading_candidate = results.loc[results["total_votes"].idxmax()]

    st.markdown("---")
    st.write("Leading Candidate")
    col1, col2 = st.columns([1, 2])  # Adjusted column widths for better layout
    with col1:
        st.image(leading_candidate["photo_url"], width=200)
    with col2:
        st.header(leading_candidate["candidate_name"])
        st.subheader(leading_candidate["party_affiliation"])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")

    st.markdown("---")
    st.write("Voting Stats")
    results = results[["candidate_id", "candidate_name", "total_votes"]].reset_index(
        drop=True
    )

    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)

    st.table(results)

    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data).groupby("state").count().reset_index()

    st.markdown("---")
    st.write("Voting Turnout by Location")
    paginate_table(location_result)


sidebar()
update_data()
