from pyspark.sql import SparkSession


# Create SparkSession
spark = (
    SparkSession
    .builder 
    .appName("Open-data-loader")
    .config("spark.driver.memory", "2g") 
    .getOrCreate()
)

data_origin = "../../../resources"

data_output = "../../../resources/output"

load = spark.read.options(header="true").csv(f"{data_origin}/opendatabcn-income")

load.write.partitionBy("Any").mode("overwrite").parquet(f"{data_output}/opendatabcn-income")