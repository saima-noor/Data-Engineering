from airflow import DAG
#importing DAG class
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime
#to schedule the data pipeline

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    #xcom is used to share data between tasks. it is cross communication message.
    #ti is the task instance object. it is used to fetch the random int accuracy values in the database
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


def _training_model():
    return randint(1, 10) # these values are pushed into the db of airflow

with DAG("my_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

    #with is context manager. A DAG object is created
    #"my_dag" is the unique identifier. it can be any name. the name of the dag

    # when to trigger the dag  schedule_interval="@daily" is used. "@daily" is a cron expression. the dag will be triggered on 2nd jan 2021
    # catchup=False. this will not create many many instances of dag runs which are not triggered. Will only create the recent triggered dag
    #

        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        #the pythonoperator must have two arguments - the unique identifier of the task and the python funtion that will be called

        training_model_B = PythonOperator(
            task_id="training_model_B",
            python_callable=_training_model
        )

        training_model_C = PythonOperator(
            task_id="training_model_C",
            python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'"
        )

        [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]