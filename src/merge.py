from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
            .builder \
            .appName("client_list_merge") \
            .getOrCreate()

transformed_file_path = '../data/transformation/'
merged_file_path = '../data/merged/'






def read_transformation(ts,name):
    dir = f'{transformed_file_path}{ts}-{name}/*.csv'
    df = spark.read.option("delimiter", ",").option("header", "true").csv(dir)
    return df

def save_merged_client_list(df,ts):
    dir = f'{ts}-leads'
    df.write.format("csv").option('header', 'true').mode('overwrite').save(merged_file_path + dir)

def merge_transformation(ts):
    donation_df = read_transformation(ts,'donations')
    print(donation_df.printSchema)
    sales_df = read_transformation(ts,'sales')
    print(sales_df.printSchema)

    merged_df = donation_df.alias('donation').join(sales_df.alias('sales'), donation_df.emailaddress == sales_df.EmailAddress, 'outer')
    merged_df.show()
    print(merged_df.count())
    check_df = merged_df.filter((F.col('donation.emailaddress') != '') & (F.col('sales.EmailAddress') != '') )
    print(check_df.count())
    #save_merged_client_list(merged_df,ts)

merge_transformation('1111')