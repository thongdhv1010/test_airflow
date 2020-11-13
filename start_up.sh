#!/bin/bash

export AIRFLOW_HOME=$(pwd)
export MYSQL_URI=mysql://thongnv:2TmI6EskMeI6K7Qe@10.19.96.8:3306/gapo_api
export CASSANDRA_USER=cassandra
export CASSANDRA_PWD=cassandra
export CASSANDRA_HOST=127.0.0.1
export CASSANDRA_KEYSPACE=tutorialspoint

# Run airflow
nohup airflow worker >> worker.out 2>&1 &
nohup airflow scheduler >> scheduler.out  2>&1 &
nohup airflow webserver -p 8080 >> webserver.out  2>&1 &