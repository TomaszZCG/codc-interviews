from pyspark.sql import SparkSession
from utils_functions import write_csv_to_location
from pyspark.sql.functions import col, sum, round, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
### Output #4 - **Department Breakdown**

# The management team wants to reward it's best employees with a bonus
# and therefore it wants to know the name of the top 3 best performers per department. 
# That is the ones that have a percentage of calls_succesfful/calls_made higher than 75%. 
# It also wants to know the sales amount of these employees
# to see who best deserves the bonus. In your opinion, who should get it and why?

# Initialize a SparkSession
spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

df1 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_one.csv')
df2 = spark.read.option('header', True).csv('codc-interviews/latest/dataset_two.csv')

df1_filtered = df1.withColumn("success_rate", round(col("calls_successful") / col("calls_made")*100,2)) \
                  .filter(col("success_rate") > 75)

df_joined = df1_filtered.join(df2, on="id")

windowSpec = Window.partitionBy("area").orderBy(col("sales_amount").desc())

df_ranked = df_joined.withColumn("rank", rank().over(windowSpec)).filter(col("rank") <= 3)
top_employees = df_ranked.select("name", "area", "sales_amount","success_rate") \
                         .orderBy("area", "rank")

top_employees.show()

filename = "dataset_exercise4.csv"
output_folder = "codc-interviews/latest/top_3"
write_csv_to_location(top_employees, output_folder, filename)