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
import os

def write_csv_to_location(dataframe, location: str, filename: str) -> None:
    os.makedirs(location, exist_ok=True)
    filePathDestTemp = location + "/tmp"
    dataframe.coalesce(1).write.option("header","true").mode("overwrite").format("csv").save(filePathDestTemp)

    name = ''
    for fileName in os.listdir(filePathDestTemp):
        if fileName.endswith('.csv'):
            name = fileName
            break
        
    shutil.copy(os.path.join(filePathDestTemp, name), os.path.join(location, filename))            
    shutil.rmtree(filePathDestTemp)


def filter_data(dataframe):
    
    filtered_df = dataframe.filter((col("country") == "United Kingdom") | (col("country") == "Netherlands"))
    filtered_df = filtered_df.withColumnRenamed("id","client_identifier")\
                         .withColumnRenamed("btc_a","bitcoin_address")\
                         .withColumnRenamed("cc_t", "credit_card_type")
    return filtered_df