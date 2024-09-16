import os
import shutil
from pyspark.sql.functions import col
import os

def write_csv_to_location(dataframe, location, filename):

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