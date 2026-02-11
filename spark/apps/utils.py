from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import psycopg2
from psycopg2.extras import execute_values

import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def createSparkSession(appName):
    spark = SparkSession.builder.appName(appName)\
    .config("spark.jars", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .config("spark.driver.extraClassPath", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .config("spark.executor.extraClassPath", "/opt/real-time-spark/spark/resources/postgresql-42.7.2.jar") \
    .getOrCreate()

    return spark

def subscribe_csv_stream(spark, schema, file_dir):
    df = spark \
    .readStream \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 10) \
    .schema(schema) \
    .csv(file_dir)

    return df

def subsrcribe_kafka_stream(spark, kafka_servers, topic, schema):
    try: 
        raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
        logger.info(f"Subscribed to the Broker Server Succesfully:")
       
        df = extract_value_kafa_msg(raw_stream, schema)

        return df
    except Exception as e:
        logger.error(f"Unable to Subscribe to the Broker Server: {kafka_servers}\nError :- {e}")

def extract_value_kafa_msg(df, schema):
    try:
        df = df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")
        return df
    except Exception as e:
        logger.error(f"Error:- {e}")

def transform_csv_data(df, event_type):
    if event_type == "purchase":
        df = df.withColumn("date_purchased", 
                                                        F.to_timestamp(F.col("date_purchased"), "dd-MM-yyyy, HH:mm:ss"))
        df = df.withColumnRenamed("date_purchased", "event_date")
    else:
        df = df.withColumn("date_viewed", 
                                                        F.to_timestamp(F.col("date_viewed"), "dd-MM-yyyy, HH:mm:ss"))
        df = df.withColumnRenamed("date_viewed", "event_date")
        df = df.withColumn("quantity", F.lit(None))
    df = df.withColumn("customer_name", 
                                                        F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))
    df = df.drop('customer_surname', 'customer_firstname')

    df = df.select("event_type", "product_name", "unit_price", "customer_name", "event_date", "quantity")

    return df

def transform_kafka_event_stream(df):
    
    df = df.withColumn("event_date", F.coalesce(F.col("date_purchased"), F.col("date_viewed")))\
        .withColumn("event_date", F.to_timestamp(F.col("event_date"), "dd-MM-yyyy, HH:mm:ss"))\
            .withColumn("customer_name", F.concat(F.col("customer_surname"), F.lit(" "), F.col("customer_firstname")))\
                .withColumn("quantity", F.col("quantity").cast("int"))\
                    .drop('customer_surname', 'customer_firstname')\
                        .select("event_type", "product_name", "unit_price", "customer_name", "event_date", "quantity")\
                            .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    df = df.filter(F.col("event_date").isNotNull())
        # Deduplicate based on your unique key
    df = df.dropDuplicates(
        ["event_type", "product_name", "customer_name", "event_date"]
    )


    return df

def write_to_postgres(batch_df, batch_id, output_sink):
    batch_df.write.jdbc(
        url=output_sink.get('url'),
        table="event_log",
        mode="append",
        properties=output_sink.get('properties')
    )

def write_to_postgres_2(batch_df, batch_id, output_sink):
    """
    Write batch to Postgres using UPSERT (ON CONFLICT DO UPDATE)
    """
    # Convert Spark DataFrame to Pandas for efficient batch insert
    pdf = batch_df.toPandas()

    # Connection parameters
    conn = psycopg2.connect(
        dbname=output_sink['dbname'],
        user=output_sink['properties']['user'],
        password=output_sink['properties']['password'],
        host="postgres_db",
        port=output_sink.get('port', 5432)
    )
    cursor = conn.cursor()

    # Example: assume table 'event_log' has a primary key on ('event_type', 'product_name', 'customer_name', 'event_date')
    insert_query = """
        INSERT INTO event_log (event_type, product_name, unit_price, customer_name, event_date, quantity)
        VALUES %s
        ON CONFLICT (event_type, product_name, customer_name, event_date) 
        DO UPDATE SET
            unit_price = COALESCE(EXCLUDED.unit_price, event_log.unit_price),
            quantity = COALESCE(EXCLUDED.quantity, event_log.quantity);
    """

    # Convert DataFrame rows to list of tuples
    values = [tuple(x) for x in pdf.to_numpy()]

    # Execute batch upsert
    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

def write_to_postgres_3(batch_df, batch_id, output_sink):

    (
        batch_df
        .write
        .format("jdbc")
        .mode("append")
        .option("url", output_sink["url"])
        .option("dbtable", "event_log_staging")
        .option("user", output_sink["properties"]["user"])
        .option("password", output_sink["properties"]["password"])
        .option("driver", "org.postgresql.Driver")
        .save()
    )

    upsert_sql = """
        INSERT INTO event_log (
            event_type,
            product_name,
            unit_price,
            customer_name,
            event_date,
            quantity
        )
        SELECT DISTINCT
            event_type,
            product_name,
            unit_price,
            customer_name,
            event_date,
            quantity
        FROM event_log_staging
        ON CONFLICT (event_type, product_name, customer_name, event_date)
        DO UPDATE
        SET
            unit_price = COALESCE(EXCLUDED.unit_price, event_log.unit_price),
            quantity = COALESCE(EXCLUDED.quantity, event_log.quantity);
        
    """

    truncate_sql = "TRUNCATE event_log_staging;"

    conn = psycopg2.connect(
        dbname=output_sink["dbname"],
        user=output_sink["properties"]["user"],
        password=output_sink["properties"]["password"],
        host="postgres_db",
        port=5432
    )
    try:
        with conn:
            with conn.cursor() as cur:
                # Upsert into main table
                cur.execute(upsert_sql)
                # Clear staging table for next batch
                cur.execute(truncate_sql)
    finally:
        conn.close()


def writeStream(df, output_sink):
    query = df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_postgres_3(batch_df, batch_id, output_sink)) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/opt/real-time-spark/tmp/checkpoints") \
    .start()
    
    return query

def writeStream_from_csv(df, output_sink):
    query = df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_postgres_3(batch_df, batch_id, output_sink)) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/opt/real-time-spark/tmp/csv_checkpoints") \
    .start()

    return query