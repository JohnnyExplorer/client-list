from datetime import datetime, timedelta
from pyspark import SparkContext, SparkConf, SQLContext

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, \
    BranchPythonOperator



default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


LEADS_DB_URL = 'koretechinterview.database.windows.net'
LEADS_DB_DB = 'KORESampleDatabase'
LEADS_DB_TABLE = 'leads'
LEADS_DB_USER = 'koreinterview'
LEADS_DB_PASSWORD = 'xxxx'
LEADS_DB_PORT = 1433

def get_spark_session():
    print('in session')
    appName = "Client_List"
    master = "local"
    conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) \
        .set("spark.driver.extraClassPath","/opt/sqljdbc_6.0/enu/jre8/sqljdbc42.jar")
    print(conf)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    return sqlContext.sparkSession

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "start_time": datetime.now().minute,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def connect_to_sql(
    spark, jdbc_hostname, jdbc_port, database, data_table, username, password
):
    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2}".format(
        jdbc_hostname, jdbc_port, database)
    print('aasfasdf')
    print(jdbc_url)
    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        # "driver": "ODBC Driver 17 for SQL Server",

    }

    df = spark.read.jdbc(url=jdbc_url, table=data_table,
                         properties=connection_details)
    return df


def SyncDonors():
    spark = get_spark_session()
    df = connect_to_sql(spark, LEADS_DB_URL, LEADS_DB_PORT, LEADS_DB_DB,
                        LEADS_DB_TABLE, LEADS_DB_USER, LEADS_DB_PASSWORD)


def is_lead_sync(*args, **context):
    execution_date = context['execution_date']
    print('date')
    print(execution_date)
    date = datetime.fromisoformat(str(execution_date))
    mins = date.minute
    print('mins {}'.format(mins))
    return 'pushLeads' if mins % 2 == 0 else 'none'


def pushLeads():
    pass


with DAG(dag_id="sync_client_list",
         schedule_interval="*/1  * * * *",
         default_args=default_args,
         catchup=False) as dag:

    # task_sync_donors = PythonOperator(
    #     task_id='SyncDonors',
    #     python_callable=SyncDonors
    # )

    task_push_leads = PythonOperator(
        task_id='pushLeads',
        python_callable=pushLeads
    )

    task_do_lead_sync = BranchPythonOperator(
        task_id='is_lead_sync',
        python_callable=is_lead_sync,
        provide_context=True
    )

    task_none = DummyOperator(
        task_id='none'
    )


    #task_sync_donors >> task_do_lead_sync >> [task_push_leads, task_none]
    task_do_lead_sync >> [task_push_leads, task_none]
