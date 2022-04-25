from datetime import datetime, timedelta


# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator, \
#     BranchPythonOperator



default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def SyncDonors():
    spark = get_sql_spark_session()
    df = connect_to_sql(spark, LEADS_DB_URL, LEADS_DB_PORT, LEADS_DB_DB,
                        LEADS_DB_TABLE, LEADS_DB_USER, LEADS_DB_PASSWORD)
    print('count {}'.format(df.count()))
    

    df.write.format("csv").save('../data/leads/' + filename)

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

SyncDonors()
# with DAG(dag_id="sync_client_list",
#          schedule_interval="*/1  * * * *",
#          default_args=default_args,
#          catchup=False) as dag:

#     # task_sync_donors = PythonOperator(
#     #     task_id='SyncDonors',
#     #     python_callable=SyncDonors
#     # )

#     task_push_leads = PythonOperator(
#         task_id='pushLeads',
#         python_callable=pushLeads
#     )

#     task_do_lead_sync = BranchPythonOperator(
#         task_id='is_lead_sync',
#         python_callable=is_lead_sync,
#         provide_context=True
#     )

#     task_none = DummyOperator(
#         task_id='none'
#     )


#     #task_sync_donors >> task_do_lead_sync >> [task_push_leads, task_none]
#     task_do_lead_sync >> [task_push_leads, task_none]
