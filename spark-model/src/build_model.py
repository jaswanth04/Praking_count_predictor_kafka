import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, split, udf, sum, max
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor, GeneralizedLinearRegression, GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.regression import LinearRegressionModel

# /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy trial.py
# /opt/entrypoint.sh
# /opt/spark/bin/pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:2.5.2 --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.jars.ivy=/tmp/.ivy

# read_options = {"keyspace": "test_ks", "table":"user_dtl", "spark.cassandra.connection.host": "172.17.0.3"}
# df = spark.read.format("org.apache.spark.sql.cassandra").options(**read_options).load()

spark = SparkSession.builder.appName("Parking").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df_2014 = spark.read.csv("/data/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv", 
                        header=True,
                        inferSchema=True)


df_2015 = spark.read.csv("/data/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", 
                        header=True,
                        inferSchema=True)

df_2016 = spark.read.csv("/data/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", 
                        header=True,
                        inferSchema=True)

df_2017 = spark.read.csv("/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", 
                        header=True,
                        inferSchema=True)


required_cols = ["Issue Date", "Violation Code", "Violation Location", "Violation Precinct", 
                "Violation Time", "Violation County", "Violation In Front Of Or Opposite", "Plate Type",
                "Vehicle Color"]

df = df_2014.select(required_cols) \
                .union(df_2015.select(required_cols) \
                .union(df_2016.select(required_cols) \
                .union(df_2017.select(required_cols))))

# No. of distinct cols
# df.select([(df.select(col(c)).distinct().count()/total_count) for c in required_cols]).show()


def convert_time_to_hr(time):
        hour = time[0:2]
        try:
                if 'A' in time:
                        hour = int(float(hour))
                elif 'P' in time:
                        hour = int(float(hour)) + 12
                else:
                        hour = int(float(hour))
                return hour
        except ValueError:
                return 0

covert_time_to_hour_udf = udf(lambda t: convert_time_to_hr(t), IntegerType())


# for c in required_cols:
#         print(f'{c}: {df.select(col(c)).distinct().count()}')

split_col = split(df["Issue Date"], '/')
df2 = df.select(required_cols) \
        .na.drop() \
        .withColumn('month', split_col.getItem(0).cast(IntegerType())) \
        .withColumn('date', split_col.getItem(1).cast(IntegerType())) \
        .withColumn('year', split_col.getItem(2).cast(IntegerType())) \
        .withColumn('Violation Hour', covert_time_to_hour_udf(col("Violation Time")))

required_cols.remove('Issue Date')
required_cols.remove('Violation Time')
required_cols.remove('Vehicle Color')
required_cols.remove('Violation Precinct')
required_cols.remove('Plate Type')
required_cols.append('month')
required_cols.append('date')
required_cols.append('Violation Hour')

df3 = df2.groupby(required_cols).count().withColumnRenamed("count", "violation_count")

# Removing the outliers and only predicting 
df_le_30_violations = df3.filter(col("violation_count") < 30)
df_le_30_violations.printSchema()
df_le_30_violations.cache()
print(df_le_30_violations.count())

# df4.filter(col("violation_count") < 30).select(sum("count")/total_count).show()
# 85 perc is less than 30

(train, test) = df_le_30_violations.randomSplit([0.8, 0.2])

county_indexer = StringIndexer(inputCol="Violation County", outputCol="violation_county_index")
# county_indexer_model = county_indexer.fit(df_le_30_violations)
# county_indexed = county_indexer_model.transform(df_le_30_violations)

front_opp_indexer = StringIndexer(inputCol="Violation In Front Of Or Opposite", 
                                outputCol="violation_in_front_opp_index")
# front_opp_indexer_model = front_opp_indexer.fit(county_indexed)
# indexed_df = front_opp_indexer_model.transform(county_indexed)

input_cols = ["Violation Code", "Violation Location", "Violation Hour", "month", "date", 
                "violation_county_index", "violation_in_front_opp_index"]

feature_cols = [column.lower().replace(" ", "_")+"_vec" for column in input_cols]

one_hot_encoder = OneHotEncoder(inputCols=input_cols, 
                        outputCols=feature_cols)

# one_hot_model = one_hot_encoder.fit(indexed_df)
# encoded_df = one_hot_model.transform(indexed_df)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
# model_input_df = assembler.transform(encoded_df)



lr = LinearRegression(labelCol="violation_count", 
                                featuresCol="features",
                                maxIter=100, 
                                regParam=0.01, 
                                elasticNetParam=0.1)

lr_pipeline = Pipeline(stages=[county_indexer, front_opp_indexer, one_hot_encoder, assembler, lr])
lr_model = lr_pipeline.fit(df_le_30_violations)

save_model_path = "/data/models/lr_model"
lr_model.save(save_model_path)
print(f'Model saved at: {save_model_path}')


# evaluator = RegressionEvaluator(labelCol="violation_count", 
#                                 predictionCol="prediction", 
#                                 metricName="rmse")

# lr_predictions = lr_model.transform(test)
# lr_rmse = evaluator.evaluate(lr_predictions)


# lr_model.save(save_model_path)
# print(f'Model saved at: {save_model_path}')

        


# Below are the code to check various models

def build_tree_models(model_input_df, train, test):

        featureIndexer = VectorIndexer(inputCol="features", 
                                outputCol="indexedFeatures",
                                maxCategories=700).fit(model_input_df)

        dt = DecisionTreeRegressor(featuresCol="indexedFeatures", labelCol="violation_count")
        rf = RandomForestRegressor(featuresCol="indexedFeatures", labelCol="violation_count")
        gbt = GBTRegressor(featuresCol="indexedFeatures", labelCol="violation_count", maxIter=10)


        dt_pipeline = Pipeline(stages=[featureIndexer, dt])
        rf_pipeline = Pipeline(stages=[featureIndexer, rf])
        gbt_pipeline = Pipeline(stages=[featureIndexer, gbt])

        dt_model = dt_pipeline.fit(train)
        rf_model = rf_pipeline.fit(train)
        gbt_model = gbt_pipeline.fit(train)

        dt_predictions = dt_model.transform(test)
        rf_predictions = rf_model.transform(test)
        gbt_predictions = gbt_model.transform(test)


        dt_rmse = evaluator.evaluate(dt_predictions)
        rf_rmse = evaluator.evaluate(rf_predictions)
        gbt_rmse = evaluator.evaluate(gbt_predictions)

        return (dt_rmse, rf_rmse, gbt_rmse)

def build_lr_model(reg_param, elastic_net_param, save_model_path=None):

        lr = LinearRegression(labelCol="violation_count", 
                                featuresCol="features",
                                maxIter=100, 
                                regParam=reg_param, 
                                elasticNetParam=elastic_net_param)

        lr_model = lr.fit(train)
        lr_predictions = lr_model.transform(test)
        lr_rmse = evaluator.evaluate(lr_predictions)

        if save_model_path is not None:
                lr_model.save(save_model_path)
                print(f'Model saved at: {save_model_path}')

        return lr_rmse

def build_glr_model(train, test):

        glr = GeneralizedLinearRegression(labelCol="violation_count",
                                        featuresCol="features",
                                        family="poisson", 
                                        link="log", 
                                        maxIter=100, 
                                        regParam=0.3)

        glr_model = glr.fit(train)
        glr_predictions = glr_model.transform(test)

        glr_rmse = evaluator.evaluate(glr_predictions)
        return glr_rmse


# RMSE of Decision Tree: 6.295417733391423
# RMSE of Random Forest: 6.223435652609486
# RMSE of GBT: 5.7269155820405535
# RMSE of GLR: 6.769922559388151
# RMSE of LR: 5.556528833431431

# Performing Hyper Parameter tuning
# reg_params = [0.01, 0.1, 0.2, 0.5, 0.8]
# elastic_net_params = [0.01, 0.1, 0.2, 0.5, 0.8]

# lr_rmse = []

# for reg in reg_params:
#         for el in elastic_net_params:
#                 rmse = build_lr_model(reg, el)
#                 print(f'RMSE of Reg {reg}, Elastic Net {el}: {rmse}')

# RMSE of Reg 0.01, Elastic Net 0.01: 3.3735380797677625
# RMSE of Reg 0.01, Elastic Net 0.1: 3.3735380797677625
# RMSE of Reg 0.01, Elastic Net 0.2: 3.373538079767763
# RMSE of Reg 0.01, Elastic Net 0.5: 3.3735380797677625
# RMSE of Reg 0.01, Elastic Net 0.8: 3.373538079767763
# RMSE of Reg 0.1, Elastic Net 0.01: 3.4274018860506357
# RMSE of Reg 0.1, Elastic Net 0.1: 3.4274018860506357
# RMSE of Reg 0.1, Elastic Net 0.2: 3.4274018860506357
# RMSE of Reg 0.1, Elastic Net 0.5: 3.4274018860506357
# RMSE of Reg 0.1, Elastic Net 0.8: 3.427401886050636
# RMSE of Reg 0.2, Elastic Net 0.01: 3.5142108139038544
# RMSE of Reg 0.2, Elastic Net 0.1: 3.5142108139038544

# From above the best model is LR with Reg 0.01 and E Nel 0.1
# Training the best model

# rmse = build_lr_model(0.01, 0.1)
# print('Rmse: {rmse}')

# model_path = "/data/lr_model"
# rmse = build_lr_model(0.01, 0.1, model_path)
# print("Rmse: {rmse}")

# # Load model when required
# lr_model = LinearRegressionModel.load(model_path)
