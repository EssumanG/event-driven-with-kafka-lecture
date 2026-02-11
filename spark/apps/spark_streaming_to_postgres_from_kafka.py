import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
import os
from utils import *
from metrics_listener import MetricsListener


SPARK_WORKDIR = os.getenv('SPARK_WORKDIR')
POSTGRES_USER= os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD= os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB= os.getenv('POSTGRES_DB')

KAFKA_BROKER_DOCKER = os.environ.get("KAFKA_BROKER_DOCKER")
TOPIC_NAME = os.environ.get("TOPIC_NAME")

output_sink = {
    'url':f"jdbc:postgresql://postgres_db:5432/{POSTGRES_DB}",
    'dbname':f"{POSTGRES_DB}",

    'properties': {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
    }

}


# product_purchase_events_csv_dir ="../../e-commerce-user-events/product-purchase-events"
# product_view_events_csv_dir ="../../e-commerce-user-events/product-view-events"
#Create a local SparkSession
spark = createSparkSession("RealTimeEventSreamingKafka")

spark.streams.addListener(MetricsListener())

general_events_schema = StructType([
    StructField('event_type', StringType(), True), 
    StructField('product_name', StringType(), True),
    StructField('unit_price', FloatType(), True), 
    StructField('customer_surname', StringType(), True), 
    StructField('customer_firstname', StringType(), True), 
    
    # view-specific
    StructField('date_viewed', StringType(), True),

    # purchase-specific
    StructField('date_purchased', StringType(), True),
    StructField('quantity', IntegerType(), True)
])


event_stream = subsrcribe_kafka_stream(spark, KAFKA_BROKER_DOCKER, TOPIC_NAME, general_events_schema)


# query = event_stream.select(
#     F.max("unit_price").alias("max_price"),
#     F.min("unit_price").alias("min_price"),
#     F.max("quantity").alias("max_qty")
# ).writeStream\
#     .format("console")\
#         .outputMode("complete")\
#         .option("truncate", "false")\
#         .start()

cleaned_event_stream = transform_kafka_event_stream(event_stream)

event_query = writeStream(cleaned_event_stream, output_sink)

event_query.awaitTermination()

