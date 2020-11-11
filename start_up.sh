#!/bin/bash

export AIRFLOW_HOME=$(pwd)

nohup airflow worker >> worker.out 2>&1 &
nohup airflow scheduler >> scheduler.out  2>&1 &
nohup airflow webserver -p 8080 >> webserver.out  2>&1 &