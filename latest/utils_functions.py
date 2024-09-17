"""coalesce - reduces the number of partitions"""

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
