import simplejson as json


def delivery_report(err, msg):
    if err is not None:
        print(f"Error (kafka) Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Produce the voter data to Kafka
def produce_voter_data(producer, voter_data):
    producer.produce(
        "voters_topic",
        key=voter_data["voter_id"],
        value=json.dumps(voter_data),
        on_delivery=delivery_report,
    )

    print(f"Producing voter data: {voter_data}")

    producer.flush()
