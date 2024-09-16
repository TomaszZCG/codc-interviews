from pyspark.sql import SparkSession
from utils_functions import write_csv_to_location
from pyspark.sql.functions import col, asc, split, regexp_extract, when
from pyspark.sql.types import IntegerType

# ### Output #2 - **Marketing Address Information**

# The management team wants to send some presents to team members that are work only selling 
# **Marketing** products and wants a list of only addresses and zip code, but the zip code needs to be in it's own column.

# - The output directory should be called **marketing_address_info** and you must use PySpark to save only to one **CSV** file.
filename = "dataset_exercise2.csv"
output_folder = "codc-interviews/latest/marketing_address_info"
spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

df1 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_one.csv')
df2 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_two.csv')
df3 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_three.csv')

df_joined = df1.join(df2, on="id", how="inner")
df_filtered = df_joined.filter((col("area") == "Marketing"))

df_filtered = df_filtered.withColumn("id", df_filtered["id"].cast(IntegerType()))

df_sorted = df_filtered.sort(asc("id"))
df_sorted = df_sorted.select("id","name","area","address")  #!!!!!!!!!!!!!!!!

df_splitted = df_sorted.withColumn('postal_code_temp', split(df_sorted['address'], ',').getItem(0)) \
                       .withColumn('city_temp', split(df_sorted['address'], ',').getItem(1)) 

postal_code_pattern = r'(\d{4}\s?[A-Z]{2})'

df_splitted = df_splitted.withColumn("postal_code_check", regexp_extract(col("city_temp"), postal_code_pattern, 0))

df_splitted = df_splitted.withColumn(
                            "postal_code", 
                            when(col("postal_code_check") == "", col("postal_code_temp")).otherwise(col("city_temp"))) \
                         .withColumn(
                            "city", 
                            when(col("postal_code_check") != "", col("postal_code_temp")).otherwise(col("city_temp")))

df_splitted = df_splitted.select("name","city","postal_code")

df_splitted.show()

write_csv_to_location(df_splitted, output_folder, filename)