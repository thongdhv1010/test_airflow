#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
               ..
              ( '`<
               )(
        ( ----'  '.
        (         ;
         (_______,' 
    ~^~^~^~^~^~^~^~^~^~^~
    Author: nvt
    Date Created: 11/11/2020
"""
import time

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 1),
    'email': ['nguyenvanthong@gapo.com.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('sleep_task',
          default_args=default_args,
          schedule_interval='*/5 * * * *')

run_this_bash_first = BashOperator(task_id='run_this_bash_first',
                                   bash_command='echo start',
                                   dag=dag)

run_this_bash_last = BashOperator(
    task_id='run_this_bash_last',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag)


def my_sleeping_function(random_base):
    time.sleep(random_base)


for i in range(10):
    task_python = PythonOperator(
        task_id='sleep_task_' + str(i),
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': 10},
        dag=dag)

    run_this_bash_first.set_downstream(task_python)
    task_python >> run_this_bash_last
