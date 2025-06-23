#!/bin/bash

# This script builds and starts the Dockerized Airflow application.

# Stop and remove any existing containers to ensure a clean start
echo "--- Stopping and removing existing containers... ---"
docker-compose down --volumes --remove-orphans

# Build the Docker image
# --no-cache can be added to ensure a fresh build
echo "--- Building Docker image... ---"
docker-compose build

# Start the application in detached mode
echo "--- Starting Airflow services... ---"
docker-compose up -d

echo "---"
echo "Airflow setup is complete!"
echo "The Airflow Webserver should be available at: http://localhost:8080"
echo "It may take a few minutes for the webserver and scheduler to be fully operational."
echo "Default credentials: airflow / airflow"
echo "---"
