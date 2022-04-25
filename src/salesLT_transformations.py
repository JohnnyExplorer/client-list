from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
            .builder \
            .appName("client_list_transformation") \
            .getOrCreate()

sales_file_path = '../data/salesLT/'
sales_tables = ['Customer','CustomerAddress','Address']

save_transformed_file_path = '../data/transformation/'

sales_columns = ['CustomerID',
'Title',
'FirstName',
'LastName',
'EmailAddress',
'CompanyName',
'ModifiedDate',
'City',
'StateProvince',
'PostalCode',
'CountryRegion',
'AddressLine1',
'AddressLine2',
'Phone']

def read_sales(ts,table):
    dir = f'{sales_file_path}{ts}-{table}/*.csv'
    df = spark.read.option("delimiter", ",").option("header", "true").csv(dir)
    return df

def save_sales_transformations(df,ts):
    dir = f'{ts}-sales'

    df.write.format("csv").option('header', 'true').mode('overwrite').save(save_transformed_file_path + dir)

def merge_sales_data(ts):
    df_cust = read_sales(ts,'Customer')
    df_cust_add = read_sales(ts,'CustomerAddress')
    df_cust_add = df_cust_add.filter(F.col('AddressType') == 'Main Office')
    df_add = read_sales(ts,'Address')

    df_join_cust = df_cust.join(df_cust_add.alias('ca'), df_cust.CustomerID == df_cust_add.CustomerID,'left').drop(F.col('ca.CustomerId')).drop(F.col('ca.ModifiedDate'))
    df_join_cust = df_join_cust.join(df_add.alias('a'), df_join_cust.AddressID == df_add.AddressID, 'left').drop(F.col('a.AddressID')).drop(F.col('a.ModifiedDate'))

    df_sales = df_join_cust.select(sales_columns)
    df_sales = df_sales.withColumn("isSalesLT",F.lit('true'))
    df_sales = df_sales.withColumn('Address', F.concat_ws(",",df_sales.AddressLine1, df_sales.AddressLine2))
    df_sales = df_sales.drop('AddressLine1','AddressLine2')
    return df_sales

def process_sales(ts):
    df_sales = merge_sales_data(ts)
    save_sales_transformations(df_sales,ts)
    spark.stop()

process_sales('1111')