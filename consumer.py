from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import from_json, col, current_timestamp, udf
import sys
import json
from json import loads
import requests

### Setting up the Python consumer
bootstrap_servers = 'localhost:9092'
topicName = 'first_topic'


##Dependancies which required for settingup kafka
scala_version = '2.12'
spark_version = '3.1.2'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]

spark = SparkSession\
    .builder\
    .master("local")\
    .appName("kafka-example")\
    .config("spark.jars.packages",",".join(packages))\
    .enableHiveSupport()\
    .getOrCreate()

df = spark\
    .readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe","first_topic") \
    .load()

rawDF = df.selectExpr ("CAST(value AS STRING)")

##Created schema for as per out original data
original_feed_schema = StructType([
    StructField('Product', StringType(), True),
    StructField('Category', StringType(), True),
    StructField('Date', StringType(), True),
    StructField('Month', StringType(), True),
    StructField('Hour', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('ip', StringType(), True),
    StructField('url', StringType(), True),
])

df = rawDF.select((from_json(col("value"), original_feed_schema)).alias("data")).select ("data.*")

# Define UDF to get country and city from IP
def get_geo_data(ip):
    try:
        response = requests.get (f"http://ip-api.com/json/{ip}")
        data = response. json()
        country = data.get ("country")
        city = data.get ("city")
        return f"{country, city}" if country and city else None 
    except:
        return None

@udf(returnType=StringType())
def udf_get_geo_data(ip):
    return get_geo_data(ip)

df = df.withColumn("cityandcountry", udf_get_geo_data("ip"))

##New column add in DF for unique_identifier (row_key)
finalDF = df.withColumn("unique_identifier", current_timestamp())


outputDF = finalDF.writeStream.outputMode("append").format("console").start()


click_data_df = finalDF.select(
    col("unique_identifier").alias("row_key"), 
    col("Date"), 
    col("Month"), 
    col("url")
    )

outputDF = click_data_df.writeStream.outputMode("append").format("console").start()

geo_data_df = finalDF.select(
    col("unique_identifier").alias("row_key"), 
    col("Product"),
    col("Category")
)
outputDF = geo_data_df.writeStream.outputMode("append").format("console").start()

# hive_table = "test_aj"
# temp_table = "temp_stream_table"

## Define the function to write each micro-batch of the streaming DataFrame to Hive

# def write_to_hive(df, epoch_id):
#     df .createorReplaceTempVieu(temp_table)
#     spark.sq1("INSERT INTO TABLE thive table] SELECT « FROM (temp_table?")

# streaming_query = finalDF.writeStream.foreachBatch(write_to_hive).start()

# streaming_query.awaitTermination()

outputDF.awaitTermination()
