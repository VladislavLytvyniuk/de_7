#!/usr/bin/env bash


AIRFLOW_VERSION=2.3.4
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

echo "Installing Apache Airflow ${AIRFLOW_VERSION} on Python${PYTHON_VERSION}"
echo "Your Python environment: $(which python)"
echo

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
echo "Constrained URL used: ${CONSTRAINT_URL}"
echo
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"