from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession \
            .builder \
            .appName("client_list_transformation") \
            .getOrCreate()

donation_file_path = '../data/donation/'
donation_file_name = 'contacts.json'

leads_file_path = '../data/leads'
leads_file_names = ['Customer','CustomerAddress','Address']

save_transformed_file_path = '../data/transformation/'


donations_columns = [
'contactid',
'jobtitle',
'firstname',
'lastname',
'company',
'donotphone',
'createdon',
'emailaddress1',
'emailaddress2',
'emailaddress3',
'address1_line1',
'address1_line2',
'address2_line1',
'address2_line2',
'address3_line1',
'address3_line2',
'address1_city',
'address2_city',
'address3_city',
'address1_stateorprovince',
'address2_stateorprovince',
'address3_stateorprovince',
'address1_postalcode',
'address2_postalcode',
'address3_postalcode',
'address1_country',
'address2_country',
'address3_country',
'telephone1',
'telephone2',
'telephone3'
]
def get_sparkSession():
    return spark


def read_donation(ts):
    spark = get_sparkSession()
    location = f'{donation_file_path}{ts}{donation_file_name}'
    print(location)
    df = spark.read.option("multiline","true").json(location)
    #print(df.printSchema())
    #df.show()
    return filter_raw_donation(df)

def filter_raw_donation(df):
    reduced_df = df.select(donations_columns)
    reduced_df.show()
    donotContact_df = reduced_df.filter(F.col('donotphone') == 'false')
    return donotContact_df

def process_priority(df,column):
    rdd2=df.rdd.map(lambda x:
    (x[0], x[1] or x[2] or x[3] or 'Null')
    )
    return rdd2.toDF(['contactid',column])

def save_donation_transformations(df,ts):
    dir = f'{ts}-donations'

    df.write.format("csv").option('header', 'true').mode('overwrite').save(save_transformed_file_path + dir)

def process_donation_columns(df):

    email_df = process_priority(df.select(['contactid','emailaddress1','emailaddress2','emailaddress3']),'emailaddress')
    city_df = process_priority(df.select('contactid','address1_city','address2_city','address3_city'),'city')
    state_df = process_priority(df.select(['contactid','address1_stateorprovince','address2_stateorprovince','address3_stateorprovince']),'state')
    postal_df = process_priority(df.select(['contactid','address1_postalcode','address2_postalcode','address3_postalcode']),'zip')
    country_df = process_priority(df.select(['contactid','address1_country','address2_country','address3_country']),'country')
    phone_df = process_priority(df.select(['contactid','telephone1','telephone2','telephone3']),'phone')
    address_line1_df = process_priority(df.select(['contactid','address1_line1','address2_line1','address3_line1']),'address_line1')
    address_line2_df = process_priority(df.select(['contactid','address1_line2','address2_line2','address3_line2']),'address_line2')
    base_df = df.select(['contactid','jobtitle','firstname','lastname','company','donotphone','createdon'])


    address_df = address_line1_df.join(address_line2_df.alias('line2'), address_line1_df.contactid == address_line2_df.contactid).drop(F.col('line2.contactid'))
    address_df = address_df.na.fill(value='',subset=["address_line1"])
    address_df = address_df.na.fill(value='',subset=["address_line2"])
    address_df.show()
    #address_df = address_df.withColumn('address', (F.col('address_line1') if (F.col('address_line2') == None) else F.concat_ws(",","address_line1",'address_line2')) )

    merge_df = base_df.join(email_df.alias('email'), base_df.contactid == email_df.contactid,'left').drop(F.col('email.contactid')) \
    .join(address_df.alias('address'), base_df.contactid == city_df.contactid,'left').drop(F.col('address.contactid')) \
    .join(city_df.alias('city'), base_df.contactid == city_df.contactid,'left').drop(F.col('city.contactid')) \
    .join(state_df.alias('state'), base_df.contactid == state_df.contactid,'left').drop(F.col('state.contactid')) \
    .join(postal_df.alias('postal'), base_df.contactid == postal_df.contactid,'left').drop(F.col('postal.contactid')) \
    .join(country_df.alias('country'), base_df.contactid == country_df.contactid,'left').drop(F.col('country.contactid')) \
    .join(phone_df.alias('phone'), base_df.contactid == phone_df.contactid,'left').drop(F.col('phone.contactid'))
    merge_df = merge_df.withColumn("isDynamics",F.lit('true'))
    return merge_df

def process_donation(ts):
    df = read_donation(ts)
    processed_df = process_donation_columns(df)
    processed_df.show()
    print(processed_df.count())
    save_donation_transformations(processed_df,ts)
    spark.stop()

process_donation('1111')