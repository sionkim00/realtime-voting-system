import pyspark
from pyspark.sql import SparkSession
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
            "spark.jars.package", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1"
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
