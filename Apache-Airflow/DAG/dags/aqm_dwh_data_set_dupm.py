from email.policy import default
from re import template
from unittest import removeResult
from airflow import DAG
import datetime 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.mysql_hook import MySqlHook
import MySQLdb
import pandas as pd
import numpy as np
import sqlalchemy as db
import os


default_args = {
    'owner' : 'Md. Mahmudul Hasan Shahin',
    'start_date': datetime.datetime(2022, 1, 20),
    'retries' : 1,
    'retry_delay' : datetime.timedelta(seconds=5),
}
############################################## Define DAG #################################################
with DAG (
    'prism_aqm_dwh_data_set_dump',
    default_args=default_args,
    schedule_interval = None,#'0 1 * * *',
    #template_searchpath='',
    catchup=False,
) as dag :
    def engine_create():
        engine = db.create_engine('mysql+mysqldb://shahin:XX(Q7E*<[!*T@13.67.109.109:3306/prism_aqm_dwh')
        conn = engine.connect()
        return conn
    ############################################## Table : daily_sales_msr_fact #################################################
    def get_daily_sales_msr_fact_table(ds, **kwargs):
        conn = engine_create()
        
        print(kwargs['start_date'])
        print(kwargs['end_date'])

        start_date = kwargs['start_date']
        end_date = kwargs['end_date']
        delta = end_date - start_date

        days_list = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

        for day in days_list:
            try:
                qry_str = "SELECT * FROM daily_sales_msr_fact WHERE date BETWEEN '"+day+"' AND '"+day+"'"
                print(qry_str)
                data = conn.execute(qry_str)
                result = data.fetchall()
                df = pd.DataFrame(result)
                print(df.describe())
                df.to_csv('/root/airflow/data_set/daily_sales_msr_fact/daily_sales_msr_fact_'+day+'.csv')
            except:
                pass
                
            # break

        conn.close()
    ############################################## Table Query Sku #################################################
    def get_daily_sales_by_sku_fact_table(ds, **kwargs):
        # db = MySQLdb.connect(
        #     host = "13.67.109.109",
        #     port = 3306,
        #     user = "shahin",
        #     password = "XX(Q7E*<[!*T",
        #     db = "prism_aqm_dwh",
        # )
        # cursor = db.cursor()
        # table_data = cursor.execute()
        #engine = db.create_engine('dialect+driver://user:pass@host:port/db')
        conn = engine_create()
        
        print(kwargs['start_date'])
        print(kwargs['end_date'])

        start_date = kwargs['start_date']
        end_date = kwargs['end_date']

        delta = end_date - start_date

        days_list = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

        for day in days_list:
            try:
                print('------------------------- INSIDE Loop----------------- ',day)
                qry_str = "SELECT * FROM sales_by_sku_fact WHERE date BETWEEN '"+day+"' AND '"+day+"'"
                print(qry_str)
                data = conn.execute(qry_str)
                # SELECT * FROM sales_by_sku_fact WHERE date BETWEEN '2021-01-01' AND '2021-01-01'
                # data = conn.execute("SELECT * FROM sales_by_sku_fact WHERE date BETWEEN '2022-01-12' AND '2022-01-12'")
                result = data.fetchall()
                df = pd.DataFrame(result)
                print(df.describe())
                df.to_csv('/root/airflow/data_set/sales_by_sku_fact/sales_by_sku_fact_'+day+'.csv')
                
                print('------------------------- OUT Loop----------------- ')
            except:
                pass
                
            # break

        print('Shahin')
        conn.close()
############################################## Table : sales_by_brand_fact #################################################
    def get_sales_by_brand_fact_table(ds, **kwargs):
        conn = engine_create()
        
        print(kwargs['start_date'])
        print(kwargs['end_date'])

        start_date = kwargs['start_date']
        end_date = kwargs['end_date']
        delta = end_date - start_date

        days_list = []
        for i in range(delta.days + 1):
            day = start_date + datetime.timedelta(days=i)
            days_list.append(datetime.datetime.strftime(day, '%Y-%m-%d'))

        for day in days_list:
            try:
                qry_str = "SELECT * FROM sales_by_brand_fact WHERE date BETWEEN '"+day+"' AND '"+day+"'"
                print(qry_str)
                data = conn.execute(qry_str)
                result = data.fetchall()
                df = pd.DataFrame(result)
                print(df.describe())
                df.to_csv('/root/airflow/data_set/sales_by_brand_fact/sales_by_brand_fact_'+day+'.csv')
            except:
                pass
                
            # break

        conn.close()

    ############################################## Table : rtl_dimension #################################################
    def get_rtl_dimension_table(ds, **kwargs):
        conn = engine_create()
        
        try:
            qry_str = "SELECT * FROM rtl_dimension"
            print(qry_str)
            data = conn.execute(qry_str)
            result = data.fetchall()
            df = pd.DataFrame(result)
            print(df.describe())
            df.to_csv('/root/airflow/data_set/rtl_dimension/rtl_dimension.csv')
        except:
            pass

        conn.close()
########################################################### END DAG ######################################################
    ######################################################## Set Operator ####################################################
    start_operator = DummyOperator(task_id = 'Begin_execution')

    get_daily_sales_msr_fact_table_operator = PythonOperator(
        task_id="get_daily_sales_msr_fact_table",
        python_callable=get_daily_sales_msr_fact_table,
        op_kwargs={'start_date': datetime.datetime(2021, 1, 1), 'end_date': datetime.datetime(2022,1,19)},
    )

    get_daily_sales_by_sku_fact_table_operator = PythonOperator(
        task_id="get_daily_sales_by_sku_fact_table",
        python_callable=get_daily_sales_by_sku_fact_table,
        op_kwargs={'start_date': datetime.datetime(2021, 1, 1), 'end_date': datetime.datetime(2022,1,19)},
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
        
    )

    get_sales_by_brand_fact_table_operator = PythonOperator(
        task_id="get_sales_by_brand_fact_table",
        python_callable=get_sales_by_brand_fact_table,
        op_kwargs={'start_date': datetime.datetime(2021, 1, 1), 'end_date': datetime.datetime(2022,1,19)},
    )

    get_rtl_dimension_table_operator = PythonOperator(
        task_id="get_rtl_dimension_table",
        python_callable=get_rtl_dimension_table,
    )

    end_operator = DummyOperator(task_id = 'Stop_execution')
    ######################################################## End Operator ####################################################

    #################################################### Trigger the Process ############################################
    start_operator >> get_daily_sales_msr_fact_table_operator
    get_daily_sales_msr_fact_table_operator >> get_daily_sales_by_sku_fact_table_operator
    get_daily_sales_by_sku_fact_table_operator >> get_sales_by_brand_fact_table_operator
    get_sales_by_brand_fact_table_operator >> get_rtl_dimension_table_operator
    get_rtl_dimension_table_operator >> end_operator
