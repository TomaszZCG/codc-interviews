"""
Background:
 A very small company called **KommatiPara** that deals with bitcoin 
 trading has two separate datasets dealing with clients that they want 
 to collate to starting interfacing more with their clients. 
 One dataset contains information about the clients and the 
 other one contains information about their financial details.

 The company now needs a dataset containing the emails of 
 the clients from the United Kingdom and the Netherlands and 
 some of their financial details to starting reaching out to 
 them for a new marketing push.

 example run of application: tzwarycz@LCE64920:~/codc_interviews/src/version1$ /usr/bin/python3 /home/tzwarycz/codc_interviews/src/version1/exercise_version1.py file_name.csv 

"""
from pyspark.sql import SparkSession
from utils import write_csv_to_location, filter_data, read_dataset
import sys


def main(file_name):
    output_folder = "client_data"  
    spark = SparkSession.builder.appName("JoinDatasets").getOrCreate()

    df1 = read_dataset(spark,'dataset_one.csv')
    df2 = read_dataset(spark,'dataset_two.csv')

    df_joined = df1.join(df2, on="id", how="inner")

    df_drop = df_joined.drop("cc_n")\
                    .drop("first_name")\
                    .drop("last_name")

    df_filtered = filter_data(df_drop)
    df_filtered.show()

    write_csv_to_location(df_filtered, output_folder, file_name)


if __name__ == "__main__":
    """ file name """
    file_name = sys.argv[1]
    """ run the main function """
    main(file_name)