#!/usr/bin/env bash
airflow db init
airflow db upgrade

airflow users create -e admin@admin.com -f admin -l admin -p admin -r Admin -u admin 

airflow webserver
