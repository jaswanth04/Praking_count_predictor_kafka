import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, split, udf, sum, max


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

required_cols = ["Violation Code", "Violation Location", "Violation County", "Violation In Front Of Or Opposite"]

code_distinct = df.select("Violation Code").distinct()
code_distinct.write.csv("/data/content/code.csv")

location_distinct = df.select("Violation Location").distinct()
location_distinct.write.csv("/data/content/location.csv")

county_distinct = df.select("Violation County").distinct()
county_distinct.write.csv("/data/content/county.csv")

type_distinct = df.select("Violation In Front Of Or Opposite").distinct()
type_distinct.write.csv("/data/content/type.csv")