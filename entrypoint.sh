#!/bin/bash

function create_admin_user() {
  airflow users create \
    --username "$_AIRFLOW_WWW_USER_USERNAME" \
    --firstname "Admin" \
    --lastname "User" \
    --role "Admin" \
    --email "admin@example.com" \
    --password "$_AIRFLOW_WWW_USER_PASSWORD"
}

case "$1" in
  webserver)
    airflow db init
    create_admin_user
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  init)
    airflow db init
    create_admin_user
    ;;
  *)
    exec "$@"
    ;;
esac