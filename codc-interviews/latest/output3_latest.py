""" 
Output #3 - **Department Breakdown**
The stakeholders want to have a breakdown of the sales amount of each department and 
they also want to see the total percentage of calls_succesfful/calls_made per department. 
The amount of money and percentage should be easily readable.
"""
from pyspark.sql import SparkSession
from utils import write_csv_to_location
from pyspark.sql.functions import col, sum, round


file_name = "dataset_exercise3.csv"
output_folder = "department_breakdown"

# Initialize a SparkSession
spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

df1 = spark.read.option('header', True).csv('dataset_one.csv')
df2 = spark.read.option('header', True).csv('dataset_two.csv')

df2 = df2.withColumn("sales_amount", col("sales_amount").cast("decimal(9,2)"))

df_joined = df1.join(df2, on="id", how="inner")

df_group = df_joined.groupBy("area").agg(sum("sales_amount").alias("total"),sum("calls_made").alias("total_calls_made"),\
                                          sum("calls_successful").alias("total_calls_successful"))
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
df_total = df_group.withColumn("total_calls_made", col("total_calls_made").cast("decimal(9,0)"))\
                    .withColumn("total_calls_successful", col("total_calls_successful").cast("decimal(9,0)"))  
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
df_total = df_total.withColumn("success_percentage",round((col("total_calls_successful") / col("total_calls_made")) * 100, 2))

df_total.show()

write_csv_to_location(df_total, output_folder, file_name)