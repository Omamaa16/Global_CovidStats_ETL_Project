import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator

default_arguments = {"owner":"omama",
                     "start_date":datetime(2024, 7, 2),
                     "past_dependent":False,
                     "email":["omamamashhood10@gmail.com"],
                     "email_on_failure":False,
                     "email_on_retry":False,
                     "retries":1,
                     "retry_delay":timedelta(minutes=3)}

dag = DAG("covid_data_dag", default_args=default_arguments, description="automatically fetching the data", 
          schedule_interval="@daily", catchup=False)

with dag:
    task = BashOperator(task_id="extraction_automation",
                        bash_command='python /home/airflow/gcs/dags/dataset/combined_data.py')
