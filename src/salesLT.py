from pyspark import SparkContext, SparkConf, SQLContext

LEADS_DB_URL = ''
LEADS_DB_DB = ''
LEADS_DB_USER = ''
LEADS_DB_PASSWORD = ''
LEADS_DB_PORT = 1433

appName = "Client_List"
master = "local[2]"

schema = 'SalesLT'
sales_tables = ['Customer','CustomerAddress','Address']
file_path = '../data/salesLT/'

def get_sql_spark_session():
    print('in session')
    conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) \
        .set("spark.driver.extraClassPath","/opt/sqljdbc_6.0/enu/jre8/sqljdbc42.jar")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    return sqlContext.sparkSession


def connect_to_sql(
    spark, jdbc_hostname, jdbc_port, database, data_table, username, password
):
    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(
        jdbc_hostname, jdbc_port, database)
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        # "driver": "ODBC Driver 17 for SQL Server",

    }

    df = spark.read.jdbc(url=jdbc_url, table=data_table,
                         properties=connection_details)
    return df

def saveCsv(ts,df,table):
    dir = f'{ts}-{table}'
    df.write.format("csv").option('header', 'true').mode('overwrite').save(file_path + dir)

def get_leads(ts):
    spark = get_sql_spark_session()
    for table in sales_tables:
        table_name = f'{schema}.{table}'
        df = connect_to_sql(spark, LEADS_DB_URL, LEADS_DB_PORT, LEADS_DB_DB,
                            table_name, LEADS_DB_USER, LEADS_DB_PASSWORD)
        print('count {}'.format(df.count()))
        saveCsv(ts,df,table)
    spark.stop()


get_leads('1111')