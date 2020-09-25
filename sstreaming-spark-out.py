# spark / bin / spark - submit \
# - -master
# local - -driver - memory
# 4
# g \
# - -num - executors
# 2 - -executor - memory
# 4
# g \
# - -packages
# org.apache.spark: spark - sql - kafka - 0 - 10_2.11:2.4
# .0 \
#     sstreaming - spark - out.py

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark.sql.functions import explode

spark = SparkSession.builder \
    .appName("Spark Structured Streaming from Kafka") \
    .getOrCreate()

sdf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topic_hashtags").option("startingOffsets", "latest").load().selectExpr("CAST(value AS STRING)")

schema = StructType([StructField("hastags", ArrayType(StringType()))])


assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
col = split(sdf['value'], ',') #split attributes to nested array in one Column
#now expand col to multiple top-level columns
for idx, field in enumerate(schema):
    sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))

res = sdf.select([field.name for field in schema])

#exploser le tableau hastag en colonnes
#dt = res.select(explode(res.hastags))



query = res.groupBy("hastags").count()

query.writeStream.outputMode("complete").format("console").option("truncate", False).start().awaitTermination()