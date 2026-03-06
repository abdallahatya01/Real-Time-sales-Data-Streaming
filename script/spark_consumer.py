import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sales_stream"
CASSANDRA_HOST = "localhost"



# Spark Session


def create_spark_session():

    spark = SparkSession.builder \
        .appName("SalesStreamingPipeline") \
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1"
        ) \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    logging.info("Spark session created")

    return spark



# Kafka Stream

def read_kafka_stream(spark):

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    return df



# Schema


def get_schema():

    return StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", IntegerType()),
        StructField("product", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("total_amount", DoubleType()),
        StructField("country", StringType()),
        StructField("payment_method", StringType()),
        StructField("order_time", StringType())
    ])



# Parse JSON


def parse_stream(df):

    schema = get_schema()

    parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    return parsed



# Transformations


def transform_data(df):

    df = df.withColumn(
        "order_time",
        to_timestamp("order_time")
    )

    # remove invalid rows
    df = df.filter(col("price") > 0)

    # uppercase country
    df = df.withColumn(
        "country",
        upper(col("country"))
    )

    # recompute sales value
    df = df.withColumn(
        "sales_value",
        round(col("price") * col("quantity"), 2)
    )

    # add product label
    df = df.withColumn(
        "product_label",
        concat(col("product"), lit("_"), col("category"))
    )

    # price category
    df = df.withColumn(
        "price_level",
        when(col("price") < 200, "LOW")
        .when(col("price") < 800, "MEDIUM")
        .otherwise("HIGH")
    )

    return df



# Streaming Cleaning


def clean_stream(df):

    df = df.withWatermark("order_time", "2 minutes")

    df = df.dropDuplicates(["order_id"])

    return df



# Aggregation


def aggregate_sales(df):

    agg = df.groupBy(
        window(col("order_time"), "1 minute"),
        col("country"),
        col("product")
    ).agg(
        sum("sales_value").alias("total_sales"),
        avg("price").alias("avg_price"),
        count("order_id").alias("orders_count"),
        sum("quantity").alias("items_sold")
    )

    return agg



# Ranking


def rank_products(df):

    window_spec = Window.partitionBy("country").orderBy(col("total_sales").desc())

    ranked = df.withColumn(
        "sales_rank",
        dense_rank().over(window_spec)
    )

    return ranked



# Cassandra Connection


def create_cassandra_session():

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    return session



# Create Cassandra Tables


def init_db(session):

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sales_streams
        WITH replication = {'class':'SimpleStrategy','replication_factor':1}
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS sales_streams.country_product_sales (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            country TEXT,
            product TEXT,
            total_sales DOUBLE,
            avg_price DOUBLE,
            orders_count INT,
            items_sold INT,
            sales_rank INT,
            PRIMARY KEY ((country), window_start, product)
        )
    """)

    logging.info("Cassandra tables ready")



# Write Stream


def start_stream(df):

    result = df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "country",
        "product",
        "total_sales",
        "avg_price",
        "orders_count",
        "items_sold",
        "sales_rank"
    )

    query = result.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "sales_streams") \
        .option("table", "country_product_sales") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .outputMode("update") \
        .start()

    query.awaitTermination()



# Main


if __name__ == "__main__":

    spark = create_spark_session()

    kafka_df = read_kafka_stream(spark)

    parsed_df = parse_stream(kafka_df)

    transformed_df = transform_data(parsed_df)

    cleaned_df = clean_stream(transformed_df)

    agg_df = aggregate_sales(cleaned_df)

    ranked_df = rank_products(agg_df)

    cassandra_session = create_cassandra_session()

    init_db(cassandra_session)

    logging.info("Starting streaming job...")

    start_stream(ranked_df)