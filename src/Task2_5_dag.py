from datetime import timedelta
from airflow import DAG #The dag object which will be initalized later
from airflow.operators.dummy_operator import DummyOperator #We have to import the dummy operator from the library

#Providing the default arguments that are going to be used later in the various tasks

default_args = {
    'owner': 'spathak',
    'depends_on_past': False, #To make sure we remove the success dependency from the past runs for the overall task. Else we have to make sure that past run is successful to make this run
    'email': ['abc@abc.com'], #The email to which notifications will be sent
    'email_on_failure': True, #Notifcation of failure
    'email_on_retry': True, #Notification if the job is being retried
    'retries': 3, #Number of retires before the job stop running in case of failures
    'retry_delay': timedelta(minutes=5), #The wait time for retires
    }

dag = DAG(
    'dataframe_api_task', #name which appears in airflow
    default_args=default_args, #Assigning the default argument defined above
    description='dataframe_api_tasks',
    schedule_interval=timedelta(days=1), #How often this dag would run
    start_date=days_ago(1) #The start date for the dag post deployment .
)


#Using the dummy operator to define the various tasks

t1 = DummyOperator(
    task_id='download_parquet_file',
    dag=dag
)

t2 = DummyOperator(
    task_id='min_max_row_cnt',
    dag=dag
)

t3 = DummyOperator(
    task_id='avg_bathroom_bedroom',
    dag=dag
)

t4 = DummyOperator(
    task_id='occupancy',
    dag=dag
)

t5 = DummyOperator(
    task_id='dummy_task1',
    dag=dag
)

t6 = DummyOperator(
    task_id='dummy_task2',
    dag=dag
)


#Scheduling the tasks as per the requirement

t1 >> [t2,t3] >> [t4,t5,t6]
