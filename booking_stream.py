import os

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    hour,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# ============================================================
# 1. LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT")
mysql_database = os.getenv("MYSQL_DATABASE")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")

required_variables = {
    "MYSQL_HOST": mysql_host,
    "MYSQL_PORT": mysql_port,
    "MYSQL_DATABASE": mysql_database,
    "MYSQL_USER": mysql_user,
    "MYSQL_PASSWORD": mysql_password,
}

missing_variables = [
    name for name, value in required_variables.items()
    if not value
]

if missing_variables:
    raise RuntimeError(
        "Missing environment variables: "
        + ", ".join(missing_variables)
    )


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("MovieBookingStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "com.mysql:mysql-connector-j:9.4.0"
    )
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SPARK SESSION STARTED")
print("Spark Version:", spark.version)
print("=" * 60)


# ============================================================
# 3. KAFKA CONFIGURATION
# ============================================================

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "booking_events"

print("Kafka:", kafka_bootstrap_servers)
print("Topic:", kafka_topic)


# ============================================================
# 4. READ STREAM FROM KAFKA
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        kafka_bootstrap_servers
    )
    .option(
        "subscribe",
        kafka_topic
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)

print("Connected to Kafka successfully!")


# ============================================================
# 5. DEFINE JSON SCHEMA
# ============================================================

booking_schema = StructType([
    StructField("booking_id", StringType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("movie_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("theater", StringType(), True),
    StructField("tickets", IntegerType(), True),
    StructField("ticket_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("booking_status", StringType(), True),
    StructField("booking_time", StringType(), True)
])


# ============================================================
# 6. CONVERT KAFKA VALUE TO STRING
# ============================================================

json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)


# ============================================================
# 7. PARSE JSON
# ============================================================

booking_df = (
    json_df
    .select(
        from_json(
            col("json_value"),
            booking_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# 8. TRANSFORMATION
# ============================================================

transformed_df = (
    booking_df

    # Convert string to timestamp
    .withColumn(
        "booking_time",
        to_timestamp(
            col("booking_time"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        )
    )

    # Extract booking date
    .withColumn(
        "booking_date",
        to_date(col("booking_time"))
    )

    # Extract booking hour
    .withColumn(
        "booking_hour",
        hour(col("booking_time"))
    )

    # Calculate revenue category
    .withColumn(
        "revenue_category",
        when(
            col("total_amount") < 500,
            "Low"
        )
        .when(
            (col("total_amount") >= 500)
            & (col("total_amount") < 1000),
            "Medium"
        )
        .otherwise("High")
    )
)


# ============================================================
# 9. DATA VALIDATION
# ============================================================

valid_booking_df = (
    transformed_df
    .filter(col("booking_id").isNotNull())
    .filter(col("movie_id").isNotNull())
    .filter(col("tickets").isNotNull())
    .filter(col("tickets") > 0)
    .filter(col("ticket_price").isNotNull())
    .filter(col("ticket_price") >= 0)
    .filter(col("total_amount").isNotNull())
    .filter(col("total_amount") >= 0)
    .filter(col("booking_status").isin(
        "CONFIRMED",
        "CANCELLED"
    ))
    .filter(col("booking_time").isNotNull())
)


# ============================================================
# 10. MYSQL UPSERT FUNCTION
# ============================================================

def write_to_mysql(batch_df, batch_id):

    print("=" * 60)
    print(f"PROCESSING SPARK BATCH: {batch_id}")

    if batch_df.isEmpty():
        print("No records in this batch.")
        return

    print("Records received:", batch_df.count())

    # --------------------------------------------------------
    # Select columns in MySQL table order
    # --------------------------------------------------------

    mysql_df = batch_df.select(
        "booking_id",
        "movie_id",
        "movie_name",
        "city",
        "theater",
        "tickets",
        "ticket_price",
        "total_amount",
        "booking_status",
        "booking_time",
        "booking_date",
        "booking_hour",
        "revenue_category"
    )

    # --------------------------------------------------------
    # Convert Spark rows to tuples
    # --------------------------------------------------------

    rows = mysql_df.collect()

    if not rows:
        print("No valid records after transformation.")
        return

    # --------------------------------------------------------
    # MySQL connection
    # --------------------------------------------------------

    import mysql.connector

    connection = mysql.connector.connect(
        host=mysql_host,
        port=int(mysql_port),
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )

    cursor = connection.cursor()

    # --------------------------------------------------------
    # UPSERT query
    # --------------------------------------------------------

    upsert_query = """
    INSERT INTO booking_analytics (
        booking_id,
        movie_id,
        movie_name,
        city,
        theater,
        tickets,
        ticket_price,
        total_amount,
        booking_status,
        booking_time,
        booking_date,
        booking_hour,
        revenue_category
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        movie_id = VALUES(movie_id),
        movie_name = VALUES(movie_name),
        city = VALUES(city),
        theater = VALUES(theater),
        tickets = VALUES(tickets),
        ticket_price = VALUES(ticket_price),
        total_amount = VALUES(total_amount),
        booking_status = VALUES(booking_status),
        booking_time = VALUES(booking_time),
        booking_date = VALUES(booking_date),
        booking_hour = VALUES(booking_hour),
        revenue_category = VALUES(revenue_category)
    """

    try:

        data = [
            (
                row["booking_id"],
                row["movie_id"],
                row["movie_name"],
                row["city"],
                row["theater"],
                row["tickets"],
                row["ticket_price"],
                row["total_amount"],
                row["booking_status"],
                row["booking_time"],
                row["booking_date"],
                row["booking_hour"],
                row["revenue_category"]
            )
            for row in rows
        ]

        cursor.executemany(
            upsert_query,
            data
        )

        connection.commit()

        print(
            f"Batch {batch_id} successfully "
            f"UPSERTED {len(data)} records into MySQL."
        )

        # ----------------------------------------------------
        # Show processed records
        # ----------------------------------------------------

        mysql_df.show(
            truncate=False
        )

    except mysql.connector.Error as e:

        connection.rollback()

        print(
            f"MySQL error while processing batch {batch_id}."
        )

        raise RuntimeError(
            "MySQL batch write failed."
        ) from e

    finally:

        cursor.close()
        connection.close()

        print("MySQL connection closed.")


# ============================================================
# 11. START STREAMING
# ============================================================

query = (
    valid_booking_df.writeStream
    .foreachBatch(write_to_mysql)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "file:///C:/Users/surya/Downloads/"
        "Movie_ticket_project/checkpoints/"
        "booking_mysql_v2"
    )
    .start()
)


# ============================================================
# 12. KEEP STREAM RUNNING
# ============================================================

print("=" * 60)
print("KAFKA -> PYSPARK -> MYSQL")
print("REAL-TIME BOOKING STREAMING STARTED")
print("Waiting for booking events...")
print("=" * 60)

query.awaitTermination()