# Build a Streaming ETL Pipeline using Kafka

## Project Scenario
As vehicles pass through toll plazas, their data is streamed in real-time. This project builds a pipeline to stream, consume, and store this toll data into a MySQL database.

## Objectives
- Start a MySQL database server and create a table
- Start Kafka server and create a topic `toll`
- Use a Python producer to stream simulated toll data to Kafka
- Use a Python consumer to write Kafka messages into the MySQL table
- Verify data is collected successfully

## Technologies
- Apache Kafka
- MySQL
- Python (kafka-python, mysql-connector-python)
