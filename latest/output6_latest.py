from pyspark.sql import SparkSession
from utils_functions import write_csv_to_location
from pyspark.sql.functions import col, sum, rank, round
from pyspark.sql.window import Window

# Output #6 - **Who is the best overall salesperson per country**
# - The output directory should be called **best_salesperson** and 
# you must use PySpark to save only to one **CSV** file.


filename = "dataset_exercise6.csv"
output_folder = "codc-interviews/latest/best_salesperson"
spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

df1 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_one.csv')
df2 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_two.csv')
df3 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_three.csv')


df_joined = df3.join(df2, df3.caller_id == df2.id, "inner")

df_sales_by_country = df_joined.groupBy("country", "name").agg(sum("sales_amount").alias("total_sales"))

window = Window.partitionBy("country").orderBy(col("total_sales").desc())

df_ranked = df_sales_by_country.withColumn("rank", rank().over(window))

# Filter the result to only get the best salesperson (rank == 1)
df_best_salesperson = df_ranked.filter(col("rank") == 1)

df_best_salesperson = df_best_salesperson.withColumn('total_sales', round(df_best_salesperson['total_sales'], 2))

df_best_salesperson.select("country", "name", "total_sales").show()

write_csv_to_location(df_best_salesperson, output_folder, filename)