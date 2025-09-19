from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum , window
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient



'''
IMPORTANT

from confluent_kafka.schema_registry import SchemaRegistryClient is Python-only code.

It comes from the confluent-kafka Python package (bindings to librdkafka C library).

Spark itself (Scala/Java side) doesn’t ship this package, and --packages only downloads JVM artifacts (JARs), not Python wheels.

That’s why your job blows up on Dataproc with ModuleNotFoundError.

Need to download kafka .whl file matching the python  if python version = 3.11.8 and x86_64 then 

wheel file should have cp311 and manylinux_x86_64

But in this case providing the .whl file does not work have to install the wheel file on master and worker nodes
python3 -m pip install confluent_kafka-2.11.1-cp311-cp311-manylinux_2_28_x86_64.whl
python3 -m pip install orjson
python3 -m pip install httpx
python3 -m pip install authlib

'''

# Set up logging to console only
logging.basicConfig(
    level=logging.INFO,  # Set log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.StreamHandler()  # Log to the console only
    ]
)

logger = logging.getLogger(__name__)

spark = SparkSession.builder\
            .appName("Ad stream")\
            .config("spark.sql.shuffle.partitions", "2")\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")\
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "2") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()


topic = "ad_topic"
subject_name = f'{topic}-value'

schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-777rw.asia-south2.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('WRJOICSSBPUHB5PO', 'cfltUo9wRQBmTYQBZpuzOks722lKDXx1P32D3K3eZ1OShTjWjzhhaZPlWwBf7bjg')
})

schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


kafka_config = {
    'kafka.bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.security.protocol': 'SASL_SSL',
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='VK4BL56RCIHZNVHQ' password='cfltJwDGiUwERyhAxYV4nezzm3kZ/aJsodDpHcWL3TNyFAeK9MOI4SnfMspvOtZg';",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "subscribe": topic,
    "startingOffsets": "earliest"
}

logger.info(f'Reading Schema of {topic} topic')

df = spark.readStream \
    .format('kafka') \
    .options(**kafka_config) \
    .load()

logger.info("Printing topic schema")
df.printSchema() 

print(schema_str)

payload_df = df.selectExpr("substring(value, 6, length(value)-5) as avro_payload")

decoded_df = payload_df.select(
    from_avro(col("avro_payload"), schema_str).alias("data")
).select("data.*")

grouped_df = decoded_df.groupBy("ad_id", window(decoded_df.timestamp, "1 minute", "30 seconds")).agg( 
    sum("clicks").alias("Total_clicks"),
    sum("views").alias("Total_views"), 
    sum("cost").alias("Total_cost"),
    (sum("cost") / sum("views")).alias("avg_cost_per_view")) 

logger.info("Dataframe group by applied .........")

checkpoint_dir = "gs://stream_checkpoint/kafka-spark"

query = grouped_df.writeStream \
    .outputMode("complete") \
    .format("mongodb") \
    .option('spark.mongodb.connection.uri', "mongodb+srv://rahulnewuser:deez4nuts@cluster-spark.lccoelx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-spark") \
    .option('spark.mongodb.database', 'Advertisement') \
    .option('spark.mongodb.collection', 'ad_stats') \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="5 second") \
    .start()


query.awaitTermination()