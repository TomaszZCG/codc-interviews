"""
coalesce - reduces the number of partitions and lose parallelism
Function write_csv_to_location takes 3 parameters: dataframe - data to be saved, 
location - path where file needs to be saved, filename - name of file that need to be saved
Dataframe write generate a file with multiple part files. That is why this 
will concatenate all of the part files into 1 csv. 
"""
import os
import shutil
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import os

def write_csv_to_location(df: DataFrame, location: str, filename: str) -> None:
    os.makedirs(location, exist_ok=True)
    filePathDestTemp = location + "/tmp"
    df.coalesce(1).write.option("header","true").mode("overwrite").format("csv").save(filePathDestTemp)

    name = ''
    for fileName in os.listdir(filePathDestTemp):
        if fileName.endswith('.csv'):
            name = fileName
            break
        
    shutil.copy(os.path.join(filePathDestTemp, name), os.path.join(location, filename))            
    shutil.rmtree(filePathDestTemp)


def filter_data(df: DataFrame) -> DataFrame:
    """
    Function take dataframe and filter data only for United Kingdom and Netherlands then rename 3 columns
    """
    filtered_df = df.filter((col("country") == "United Kingdom") | (col("country") == "Netherlands"))
    filtered_df = filtered_df.withColumnRenamed("id","client_identifier")\
                             .withColumnRenamed("btc_a","bitcoin_address")\
                             .withColumnRenamed("cc_t", "credit_card_type")
    return filtered_df


def read_dataset(spark, file_name: str) -> DataFrame:
    """Function take parameter as csv file name and returns data frame with data from file"""
    df = spark.read.option('header', True).csv(file_name)
    return df