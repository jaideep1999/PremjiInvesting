# Use the official Apache Airflow image as a base
FROM apache/airflow:2.8.1

# Set the Airflow user
USER root

# Create a directory for our project-specific requirements
RUN mkdir -p /opt/airflow/config

# Copy requirements file and install Python packages
COPY requirements.txt /opt/airflow/config/
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/config/requirements.txt

# Copy the dags and scripts directories into the Airflow container
USER root
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
RUN chown -R airflow: /opt/airflow/dags /opt/airflow/scripts
USER airflow


