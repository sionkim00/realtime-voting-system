## Docker Commands

### Kafka Broker

- `kafka-topics --list --bootstrap-server broker:29092`: get list of topics
- `kafka-console-consumer --topic voters_topic --bootstrap-server broker:29092`
- `kafka-console-consumer --topic aggregated_votes_per_candidate --bootstrap-server broker:29092`
- `kafka-topics --delete --topic aggregated_votes_per_candidate --bootstrap-server broker:29092`
- `kafka-topics --delete --topic aggregated_turnout_by_location --bootstrap-server broker:29092`

### Postgres

- `psql -U postgres`: enter postgres
