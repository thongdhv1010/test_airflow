#!/bin/bash

/bin/kill -9 `ps -e -o pid,command | grep 'airflow'   | awk '{print $1}'`