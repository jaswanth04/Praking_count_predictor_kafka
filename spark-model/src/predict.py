

import json
from pyspark.ml import PipelineModel
import pyspark as ps
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import udf, col
import math
from pyspark.sql.types import IntegerType
# from pyspark.streaming import StreamingContext                                                                          
# from pyspark.streaming.kafka import KafkaUtils
# from pyspark import SparkContext
from kafka import KafkaConsumer


# sc = SparkContext(appName="Something")                       
# ssc = StreamingContext(sc, 1)
spark = SparkSession.builder.appName("Parking").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Not able to work as there are some errors with Spark version
# This Docker image only runs on spark on scala 2.12
# Spark on scala 2.11 is giving errors while building the models
# So using python kafka to consume

# ks = KafkaUtils.createStream(ssc, ['parking'], {'metadata.broker.list': 'kakfa:9092'})
# ks = KafkaUtils.createStream(ssc, "zookeeper:2181", "kafka:9092", {"parking": 1})
# lines = ks.map(lambda x: x[1])


# print(lines)

# ssc.start()                                                                                                             
# ssc.awaitTermination()

# Model location - load the model
model_location = "/data/models/lr_model"
model = PipelineModel.load(model_location)
print("Model successfuly loaded")

consumer = KafkaConsumer(bootstrap_servers=["kafka:9092"], 
                    auto_offset_reset='earliest')

consumer.subscribe(['parking'])




for message in consumer:
## Pull this message from Kafka pipline
# sample_message = '{"month": "10", "hour": "10", "date": "19", "type": "F", "location": "392", "county": "BX", "code": "78"}'

    message_dict = json.loads(message.value)
    print(message_dict)

    df_input_dict = {
            "timestamp": str(datetime.datetime.now()),
            "Violation Code": int(message_dict["code"]),
            "Violation Location": int(message_dict["location"]),
            "Violation Hour": int(message_dict["hour"]),
            "month": int(message_dict["month"]),
            "date": int(message_dict["date"]),
            "Violation Hour": int(message_dict["hour"]),
            "Violation County": message_dict["county"],
            "Violation In Front Of Or Opposite": message_dict["type"]
        }

    print(df_input_dict)
    df_data = [df_input_dict]
    df_to_predict = spark.createDataFrame(df_data)

    predicted_df = model.transform(df_to_predict)


    modify_prediction_udf = udf(lambda t: 0 if t < 0 else math.ceil(t), IntegerType())

    cass_save_df = predicted_df.withColumnRenamed("Violation Code", "violation_code") \
                    .withColumnRenamed("Violation Location", "violation_location")\
                    .withColumnRenamed("Violation County", "violation_county") \
                    .withColumnRenamed("Violation In Front Of Or Opposite", "violation_type")\
                    .withColumnRenamed("Violation Hour", "hour")\
                    .withColumn("violation_count", modify_prediction_udf(col("prediction")))

    cass_columns = ["timestamp", "violation_code", "violation_location", "violation_county", "violation_type",
                    "month", "date", "hour", "violation_count"]

    cass_save_df = cass_save_df.select(cass_columns)

    cass_save_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .options(table="violation_details", keyspace="parking_keyspace")\
        .save(mode="append")

    print("Successfully wrote to cassandra")
