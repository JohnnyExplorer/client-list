from pyspark import SparkContext, SparkConf, SQLContext



def get_spark_session():
    appName = "Client_List"
    master = "local"
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
    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(jdbc_hostname, jdbc_port, database)
    print('aasfasdf')
    print(jdbc_url)
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        #"driver": "ODBC Driver 17 for SQL Server",
                    
    }

    df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    return df


LEADS_DB_URL = 'koretechinterview.database.windows.net'
LEADS_DB_DB =  'KORESampleDatabase'
LEADS_DB_TABLE = 'leads'
LEADS_DB_USER = 'koreinterview'
LEADS_DB_PASSWORD =  'xxxx'
LEADS_DB_PORT = 1433




spark = get_spark_session()
df = connect_to_sql(spark,LEADS_DB_URL,LEADS_DB_PORT,LEADS_DB_DB,LEADS_DB_TABLE,LEADS_DB_USER,LEADS_DB_PASSWORD)

