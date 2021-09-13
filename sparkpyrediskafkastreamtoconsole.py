from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType
from pyspark.sql.types import ArrayType, DateType, FloatType

# StructType schema for the Kafka redis-server topic
redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)
# StructType schema for the Customer JSON that comes from Redis
customerMessageSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

# StructType schema for the Kafka stedi-events topic 
stediEventsMessageSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType())
    ]
)
# Spark application object
spark = SparkSession.builder\
    .appName("redis-stedi-customer-app")\
    .getOrCreate()

# set the spark log level to WARN
spark.sparkContext.setLogLevel("WARN")

# Reading a streaming dataframe from the Kafka topic redis-server as the source
rawEncodedRedisDataStreamingDF = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()

# Casting the value column in the streaming dataframe as a STRING 
encodedRedisDataStreamingDF = rawEncodedRedisDataStreamingDF.selectExpr("cast(value as string) value")

# parsing the column "value" with a json object in it
# storing in a temporary view RedisSortedSet
encodedRedisDataStreamingDF\
    .withColumn("value", from_json("value", redisMessageSchema))\
    .select(col("value.key"), col("value.existType"),\
            col("value.ch"), col("value.incr"),\
            col("value.zSetEntries"))\
    .createOrReplaceTempView("RedisSortedSet")

# sql statement against a temporary view and create a column called encodedCustomer
encodedStreamingDF = spark.sql("select zSetEntries[0].element as encodedCustomer from RedisSortedSet")


# base64 decode the 'encodedCustomer' column

decodedStreamingDF = encodedStreamingDF\
    .withColumn("customer", unbase64(encodedStreamingDF.encodedCustomer).cast("string"))


# parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedStreamingDF\
    .withColumn("customer", from_json("customer", customerMessageSchema))\
    .select(col("customer.*"))\
    .createOrReplaceTempView("CustomerRecords")

# select not null fields
emailAndBirthDayStreamingDF = spark.sql("""select * 
                                from CustomerRecords 
                                where email is not null AND birthDay is not null""")
# from the emailAndBirthDayStreamingDF dataframe select the email and the birth year
emailAndBirthDayStreamingDF = emailAndBirthDayStreamingDF\
                .withColumn('birthYear', split(emailAndBirthDayStreamingDF.birthDay,"-")\
                .getItem(0))
# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(col('email'), col('birthYear'))
# sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
emailAndBirthYearStreamingDF\
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()\
    .awaitTermination()

