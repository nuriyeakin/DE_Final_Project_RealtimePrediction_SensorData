from pyspark.sql import SparkSession, functions as F
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline

spark = SparkSession.builder \
    .appName("Read Data From File") \
    .master("local[2]") \
    .config("spark.driver.memory","3g") \
    .getOrCreate()

df = spark.read \
    .format("csv") \
    .option('header',True) \
    .option("inferSchema",True) \
    .load("file:///home/train/project_sensors_output/")

## EDA

df.describe().show()

## check null values
df_clean = df.na.drop()
df_clean.count()

## Save the data for Kafka produce

df_clean.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("file:///home/train/project_sensors_output3")

## hedef değişken PIR 1 ve 0e set edilir.

df_clean = df_clean.withColumn('pir',F.when(df_clean['pir'] > 0,1).otherwise(0)).drop("event_ts_min","ts_min_bignt","room")

df_clean.filter("pir != 0").show()

## Random forest

(trainingData, testData) = df_clean.randomSplit([0.7,0.3],seed=42)

## özellikleri birleştirip matris vektörü oluşturma

assembler =VectorAssembler(inputCols=["co2","light","temperature","humidity"],outputCol="features")

rf = RandomForestClassifier(labelCol="pir",featuresCol="features",numTrees=10)

## pipeline
pipeline = Pipeline(stages=[assembler,rf])

model =pipeline.fit(trainingData)

predictions = model.transform(testData)

## Calculate the accuracy
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction",labelCol="pir")
accuracy = evaluator.evaluate(predictions)

print(f"accuracy: {accuracy}")

## Save the model
model.write().overwrite().save("file:///home/train/dataops7/finalProject/model")

spark.stop()