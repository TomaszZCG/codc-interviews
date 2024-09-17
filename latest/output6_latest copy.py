from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# Initialize Spark session
spark = SparkSession.builder.appName("Best Salesperson").getOrCreate()

# Define schema and create DataFrames for the second and third datasets
data2 = [(1, 'Sep Cant-Vandenbergh', '2588 VD, Kropswolde', 57751.6),
         (2, 'Evie Godfrey van Alemannië-Smits', '1808 KR, Benningbroek', 69087.12),
         (3, 'Vincent Mathurin', 'Lindehof 5, 4133 HB, Nederhemert', 44933.21),
         (4, 'Jolie Tillmanno', '4273 SW, Wirdum Gn', 44052.15),
         (5, 'Faas Haring', '4431 BT, Balinge', 43985.41),
         (6, 'Vera Wolters', 'Thijmenweg 38, 7801 OC, Grijpskerk', 63660.33),
         (7, 'Noah Schellekens', '8666 XB, Exloo', 49655.4),
         (8, 'Phileine Stamrood', 'Tessapad 82, 8487 PZ, Sambeek', 50421.79),
         (9, 'Aron Roukes', '7149 RI, Purmerend', 75983.52),
         (10, 'Rosa Kuipers', 'Jetlaan 816, 8779 EM, Holwierde', 37606.9)]

schema2 = ["id", "name", "address", "sales_amount"]

df2 = spark.createDataFrame(data2, schema2)

data3 = [(1, 1, 'Verbruggen-Vermeulen CommV', 'Anny Claessens', 45, 'Belgium', 'Banner', 50),
         (2, 2, 'Hendrickx CV', 'Lutgarde Van Loock', 41, 'Belgium', 'Sign', 23),
         (3, 3, 'Buysse-Van Dessel VOF', 'Georges Jacobs', 22, 'Belgium', 'Scanner', 48),
         (4, 4, 'Heremans VOF', 'Josephus Torfs Lemmens', 45, 'Belgium', 'Desktop', 36),
         (5, 9, 'Koninklijke Aelftrud van Wessex', 'Mustafa Ehlert', 34, 'Netherlands', 'Headset', 1),
         (6, 9, 'Ardagh Group', 'Mila Adriaense-Maas', 30, 'Netherlands', 'Billboard', 31),
         (7, 20, 'Claessens, Verfaillie en Dewulf CommV', 'Hasan Claeys', 60, 'Belgium', 'Social Media Ad', 48),
         (8, 97, 'Schenk Kohl e.V.', 'Irmela Dörschner B.A.', 60, 'Germany', 'Scanner', 4),
         (9, 10, 'Brizee BV', 'Olaf van Beek', 53, 'Netherlands', 'Printer', 41),
         (10, 8, 'de Ruiter Groep', 'Tom van Dooren-van der Ven', 38, 'Netherlands', 'Scanner', 14)]

schema3 = ["id", "caller_id", "company", "recipient", "age", "country", "product_sold", "quantity"]

df3 = spark.createDataFrame(data3, schema3)

# Join the second and third datasets on 'id' and 'caller_id'
df_joined = df3.join(df2, df3.caller_id == df2.id, "inner")

# Group by 'country' and 'name', then sum the 'sales_amount'
df_sales_by_country = df_joined.groupBy("country", "name").agg(sum("sales_amount").alias("total_sales"))

df_sales_by_country.show()

# Find the best salesperson per country by selecting the maximum sales per country
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Define a window partitioned by country and ordered by total sales in descending order
window = Window.partitionBy("country").orderBy(col("total_sales").desc())

# Apply rank function to get the top salesperson in each country
df_ranked = df_sales_by_country.withColumn("rank", rank().over(window))

# Filter the result to only get the best salesperson (rank == 1)
df_best_salesperson = df_ranked.filter(col("rank") == 1)

# Show the result
df_best_salesperson.select("country", "name", "total_sales").show()