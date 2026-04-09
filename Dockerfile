FROM FROM python:3.12-slim

USER root
RUN apt-get update && apt-get install -y git

# Set proper working directory
WORKDIR /usr/local/airflow

# Make sure the dbt_packages directory exists and has proper permissions
RUN mkdir -p dags/dbt/mars_dbt/dbt_packages && \
    chmod -R 777 dags/dbt/mars_dbt/dbt_packages && \
    chown -R astro:astro dags

# Create and set up Python virtual environment
RUN rm -rf dbt_venv && python3 -m venv dbt_venv && \
    chown -R astro:astro dbt_venv

# Switch to astro user
USER astro
WORKDIR /usr/local/airflow

# Install DBT in the virtual environment
RUN . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.9.0 && \
    deactivate

# Make sure we're in the right directory for DBT
WORKDIR /usr/local/airflow/dags/dbt/mars_dbt

# Switch to root user
USER root
RUN cd /usr/local/airflow/dags/dbt/mars_dbt && \
    source /usr/local/airflow/dbt_venv/bin/activate && \
    dbt deps --no-partial-parse && \
    chown -R astro:astro /usr/local/airflow/dags/dbt/mars_dbt/dbt_packages && \
    deactivate

# Switch to astro user
USER astro

# Back to main directory
WORKDIR /usr/local/airflow