from airflow import DAG
import datetime
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.helpers import chain, cross_downstream
from random import seed, random
#dag = DAG('core_concepts', schedule_interval='@daliy', catchup=False)

default_arguements = {'owner': 'MD.Shafiqul Islam', 'start_date': datetime.datetime(2021, 7, 13)}
# op = Op(dag=dag)

with DAG(
    'core_concepts',
     schedule_interval=None,
     catchup=False,
     default_args = default_arguements
) as dag:

    bash_task = BashOperator(
        task_id ="bash_command", bash_command="echo $TODAY", env={"TODAY":"2021-07-13"}
    )

    def print_random_number(number):
        seed(number)
        print(random())

    python_task = PythonOperator(
        task_id = "python_function", python_callable=print_random_number, op_args=[1]
    )

    bash_task >> python_task

    # cross_downstream([op1, op2], [op3, op4])
    #
    # [op1,op2] >> op3
    # [op1,op2] >> op4

    # op1>>op2>>op3>>op4

    # chain(op1, op2, op3, op4)

    # bash_operator.set_downstream(pyhton_operator)
#    op = Op()
