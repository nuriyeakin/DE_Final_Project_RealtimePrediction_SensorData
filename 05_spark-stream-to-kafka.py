from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

## Read From Kafka

spark = SparkSession.builder.appName("Read From Kafka") \
    .master("local[2]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

#read stream

line = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092,localhost:9292") \
    .option("subscribe","office-input") \
    .load()

#deserialize

line2 = line.selectExpr("CAST(value AS STRING)")

#Parsing
line3 = line2 \
    .withColumn("room",F.split(F.col("value"),",")[1].cast(StringType())) \
    .withColumn("co2", F.split(F.col("value"),",")[2].cast(DoubleType())) \
    .withColumn("light",F.split(F.col("value"),",")[3].cast(DoubleType())) \
    .withColumn("temperature",F.split(F.col("value"),",")[4].cast(DoubleType())) \
    .withColumn("humidity",F.split(F.col("value"),",")[5].cast(DoubleType())) \
    .drop("value")

#check point

import os

os.system("rm -r /home/train/dataops7/tmp/streaming_checkpoints/writefrom_kafka/*")
checkpointDir = "file:///home/train/dataops7/tmp/streaming_checkpoints/writefrom_kafka"

# prediction

model = PipelineModel.load(
    "file:///home/train/dataops7/finalProject/model/"
)

predictions = model.transform(line3)

line4 = predictions \
    .withColumn("key",F.col("room")) \
    .withColumn("value",F.col("prediction").cast(IntegerType())) \
    .withColumn("topic",F.when(F.col("value")> 0,"office-activity").otherwise("office-no-activity"))


#Kafka stream

predictionsQuery = (line4.selectExpr("topic","CAST(key AS STRING)","CAST(value AS STRING)")
                    .writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers","localhost:9092,localhost:9292")
                    .option("checkpointLocation",checkpointDir)
                    .start()
                    )

predictionsQuery.awaitTermination()
