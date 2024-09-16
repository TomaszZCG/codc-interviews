from pyspark.sql import SparkSession
from utils_functions import write_csv_to_location
from pyspark.sql.functions import col, sum, round, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
### Output #5 - **Department Breakdown**

# Top 3 most sold products per department in the Netherlands

# Initialize a SparkSession
spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

df1 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_one.csv')
df2 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_two.csv')
df3 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_three.csv')


# Filter data for only the Netherlands
df3_netherlands = df3.filter(col("country") == "Netherlands")

# Join df1 and df3 on the caller_id
df_joined = df3_netherlands.join(df1, df3_netherlands.caller_id == df1.id, how='inner')

# Group by department and product and aggregate the total quantity sold
df_grouped = df_joined.groupBy("area", "product_sold").agg(sum("quantity").alias("total_quantity"))

# Window function to rank products within each department by quantity sold
windowSpec = Window.partitionBy("area").orderBy(col("total_quantity").desc())

# Add rank and filter the top 3 per department
df_ranked = df_grouped.withColumn("rank", rank().over(windowSpec)).filter(col("rank") <= 3)

# Show the result
df_ranked.select("area", "product_sold", "total_quantity", "rank").orderBy("area", "rank").show()


# filename = "dataset_exercise5.csv"
# output_folder = "codc-interviews/latest/top_3_most_sold_per_department_netherlands"
# write_csv_to_location(top_employees, output_folder, filename)