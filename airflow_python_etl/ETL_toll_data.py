# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests
import tarfile
import csv
#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
'owner': 'Prince',
'start_date': days_ago(0),
'email': ['princeappiah181@gmail.com'],
'retries': 1,
'retry_delay': timedelta(minutes=5),
}


# defining the DAG
# define the DAG
dag = DAG(
'ETL_toll_data',
default_args=default_args,
description='Apache Airflow Final Assignment',
schedule_interval=timedelta(days=1),
)


def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    dest_dir = "/home/project/airflow/dags/python_etl/staging"
    file_name = "tolldata.tgz"
    file_path = os.path.join(dest_dir, file_name)

    # Ensure destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # Download and save the file
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    
    print(f"File downloaded successfully: {file_path}")



#untar_dataset function
def untar_dataset():
    source_file = "/home/project/airflow/dags/python_etl/tolldata.tgz"
    destination = "/home/project/airflow/dags/python_etl/"
    with tarfile.open(source_file) as tar:
        tar.extractall(path=destination)
    print("Dataset unzipped successfully.")



#extract_data_from_csv function
def extract_data_from_csv():
    input_file = "/home/project/airflow/dags/python_etl/vehicle-data.csv"
    output_file = "/home/project/airflow/dags/python_etl/csv_data.csv"
    
    with open(input_file, mode='r') as infile, open(output_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            writer.writerow(row[0:4])  # Rowid, Timestamp, Anonymized Vehicle number, Vehicle type
    print("CSV data extracted successfully.")


#extract_data_from_tsv function
def extract_data_from_tsv():
    input_file = "/home/project/airflow/dags/python_etl/tollplaza-data.tsv"
    output_file = "/home/project/airflow/dags/python_etl/tsv_data.csv"

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            parts = line.strip().split('\t')
            selected = parts[4:7]  # Number of axles, Tollplaza id, Tollplaza code
            outfile.write(','.join(selected) + '\n')
    print("TSV data extracted successfully.")


#extract_data_from_fixed_width function
def extract_data_from_fixed_width():
    input_file = "/home/project/airflow/dags/python_etl/payment-data.txt"
    output_file = "/home/project/airflow/dags/python_etl/fixed_width_data.csv"

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            fields = line.strip().split()
            outfile.write(f"{fields[5]},{fields[6]}\n")  # Type of Payment code, Vehicle Code
    print("Fixed-width data extracted successfully.")


#consolidate_data function
def consolidate_data():
    csv_file = "/home/project/airflow/dags/python_etl/csv_data.csv"
    tsv_file = "/home/project/airflow/dags/python_etl/tsv_data.csv"
    fixed_width_file = "/home/project/airflow/dags/python_etl/fixed_width_data.csv"
    output_file = "/home/project/airflow/dags/python_etl/extracted_data.csv"

    with open(csv_file, 'r') as file1, open(tsv_file, 'r') as file2, open(fixed_width_file, 'r') as file3, open(output_file, 'w') as outfile:
        for line1, line2, line3 in zip(file1, file2, file3):
            outfile.write(line1.strip() + ',' + line2.strip() + ',' + line3.strip() + '\n')
    print("Data consolidated successfully.")


#transform_data function
def transform_data():
    input_file = "/home/project/airflow/dags/python_etl/extracted_data.csv"
    output_file = "/home/project/airflow/dags/python_etl/transformed_data.csv"

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(',')
            fields[3] = fields[3].upper()  # Convert vehicle_type to uppercase
            outfile.write(','.join(fields) + '\n')
    print("Data transformed successfully.")



# Define PythonOperator tasks
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_fixed_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Set dependencies
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_task] >> consolidate_task >> transform_task








