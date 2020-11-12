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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,11,11),
    'email': ['nguyenvanthong@gapo.com.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='aggregate_action_user',
    default_args=default_args,
    # schedule_interval='0 * * * *', // Moi gio chay 1 lan vao dau gio
    schedule_interval='*/2 * * * *',
    catchup=False
)

run_this_bash_first = BashOperator(task_id='get_user_action',
                                   bash_command='python3 /home/nvt/PycharmProjects/test_airflow/scripts/user_action.py',
                                   dag=dag)


