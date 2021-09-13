from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

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
# create a spark application object
spark = SparkSession.builder\
    .appName("customer-risk-app")\
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

# parsing the JSON in the Customer record and store in a temporary view called CustomerRecords
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
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(col('email'), col('birthYear'))

# Reading a streaming dataframe from the Kafka topic stedi-events as the source
stediEventsStreamingDF = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "stedi-events")\
    .option("startingOffsets", "earliest")\
    .load()                     
# cast the value column in the streaming dataframe as a STRING 
stediEventsStreamingDF = stediEventsStreamingDF.selectExpr("cast(value as string) value")

# parse the JSON from the single column "value"
# storing in a temporary view called CustomerRisk
stediEventsStreamingDF\
    .withColumn("value", from_json("value", stediEventsMessageSchema))\
    .select(col('value.customer'), col('value.score'), col('value.riskDate'))\
    .createOrReplaceTempView("CustomerRisk")

# selecting the customer and the score from the temporary view
# creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")
# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
joinCustomerRiskDF = customerRiskStreamingDF\
                         .join(
                                 emailAndBirthYearStreamingDF, 
                                 expr("email=customer")
                              )

# sinking the joined dataframes to a new kafka topic to send the data to the STEDI graph application 

joinCustomerRiskDF.selectExpr("cast(customer as string) key", "to_json(struct(*)) value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "customer-stedi-risk-score")\
    .option("checkpointLocation", "/tmp/kafka-stedi-checkpoint")\
    .start()\
    .awaitTermination()