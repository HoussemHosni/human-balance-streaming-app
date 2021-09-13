from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

stediEventsMessageSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType())
    ]
)

spark = SparkSession.builder\
    .appName("stedi-events-app")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# Reading a streaming dataframe from the Kafka topic stedi-events as the source
dataStreamingDF = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "stedi-events")\
    .option("startingOffsets", "earliest")\
    .load()   

# cast the value column in the streaming dataframe as a STRING 
dataStreamingDF = dataStreamingDF.selectExpr("cast(value as string) value")
# parse the JSON from the single column "value"
# storing in a temporary view called CustomerRisk
dataStreamingDF\
    .withColumn("value", from_json("value", stediEventsMessageSchema))\
    .select(col('value.customer'), col('value.score'), col('value.riskDate'))\
    .createOrReplaceTempView("CustomerRisk")

# selecting the customer and the score from the temporary view
# creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# sink the customerRiskStreamingDF dataframe to the console in append mode

customerRiskStreamingDF\
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()

