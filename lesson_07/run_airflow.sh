#!/usr/bin/env bash
# NB:install Apache Airflow first using install_airflow.sh script

# TODO: Change this to the path where airflow directory is located
# (default is ~/airflow)
export AIRFLOW_HOME='/Users/v.lytvyniuk/Documents/библиотечка/DE online1/git/de_7/lesson_07/airflow'

# fixes issue on Mac:
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy="*"

airflow standalone