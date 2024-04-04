from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType



# Create SparkSession
spark = (
    SparkSession
    .builder 
    .appName("Open-data-loader")
    .config("spark.driver.memory", "2g") 
    .getOrCreate()
)

data_origin = "../../../resources"
data_output = "../../output"

# Define the schema with renamed columns in all lowercase
schema_renaming = StructType([
    StructField("any", IntegerType(), nullable=True),
    StructField("codi_districte", StringType(), nullable=True),
    StructField("nom_districte", StringType(), nullable=True),
    StructField("codi_barri", StringType(), nullable=True),
    StructField("nom_barri", StringType(), nullable=True),
    StructField("poblacio", LongType(), nullable=True),
    StructField("index_rfd_barcelona_100", StringType(), nullable=True)
])

load = spark.read.schema(schema=schema_renaming).options(header="true", inferSchema="true").csv(f"{data_origin}/opendatabcn-income")

load.write.partitionBy("any").mode("overwrite").parquet(f"{data_output}/opendatabcn-income")
