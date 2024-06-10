import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if __name__ == "__main__":
    # Create a SparkSession
    print(f"Spark Version: {pyspark.__version__}")
    spark = (
        SparkSession.builder.appName("RealTimeVoting")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .config(
            "spark.jars",
            "/Users/sionkim/Desktop/dev/realtime-voting-system/postgresql-42.7.3.jar",
        )
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    vote_schema = StructType(
        [
            StructField("voter_id", StringType(), True),
            StructField("candidate_id", StringType(), True),
            StructField("voting_time", TimestampType(), True),
            StructField("voter_name", StringType(), True),
            StructField("party_affiliation", StringType(), True),
            StructField("biography", StringType(), True),
            StructField("campaign_platform", StringType(), True),
            StructField("photo_url", StringType(), True),
            StructField("candidate_name", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("nationality", StringType(), True),
            StructField("registration_number", StringType(), True),
            StructField(
                "address",
                StructType(
                    [
                        StructField("street", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("postcode", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("picture", StringType(), True),
            StructField("registered_age", IntegerType(), True),
            StructField("vote", IntegerType(), True),
        ]
    )

    votes_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "votes_topic")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), vote_schema).alias("data"))
        .select("data.*")
    )

    # Preprocess data: typecasting and watermarking
    votes_df = votes_df.withColumn(
        "voting_time", col("voting_time").cast(TimestampType())
    ).withColumn("vote", col("vote").cast(IntegerType()))

    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # Aggregate votes per candidate and location
    votes_per_candidate = enriched_votes_df.groupBy(
        "candidate_id", "candidate_name", "party_affiliation", "photo_url"
    ).agg(Fsum("vote").alias("total_votes"))

    turnout_by_location = (
        enriched_votes_df.groupBy("address.state").count().alias("total_votes")
    )

    votes_per_candidate_to_kafka = (
        votes_per_candidate.selectExpr("to_json(struct(*)) as value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggregated_votes_per_candidate")
        .option(
            "checkpointLocation",
            "/Users/sionkim/Desktop/dev/realtime-voting-system/checkpoints/checkpoint1",
        )
        .outputMode("update")
        .start()
    )

    turnout_by_location_to_kafka = (
        turnout_by_location.selectExpr("to_json(struct(*)) as value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "aggregated_turnout_by_location")
        .option(
            "checkpointLocation",
            "/Users/sionkim/Desktop/dev/realtime-voting-system/checkpoints/checkpoint2",
        )
        .outputMode("update")
        .start()
    )

    # Wait for termination for the streams
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
