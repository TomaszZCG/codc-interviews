"""
Output #1 - **IT Data**
The management teams wants some specific information about the people that are working in selling IT products.
- Join the two datasets.
- Filter the data on the **IT** department.
- Order the data by the sales amount, biggest should come first.
- Save only the first **100** records.
- The output directory should be called **it_data** and you must use PySpark to save only to one **CSV** file.
"""
from pyspark.sql import SparkSession
from utils_functions import write_csv_to_location
from pyspark.sql.functions import col,desc


filename = "dataset_exercise1.csv"
output_folder = "codc-interviews/latest/it_data"
spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

df1 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_one.csv')
df2 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_two.csv')
df3 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_three.csv')

df_joined = df1.join(df2, on="id", how="inner")
df_filtered = df_joined.filter((col("area") == "IT"))

df_sorted = df_filtered.orderBy(desc("sales_amount"))
df_limited = df_sorted.limit(100)
df_limited.show()

write_csv_to_location(df_limited, output_folder, filename)