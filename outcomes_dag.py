import os
os.system("pip install s3fs")
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.hooks.S3_hook import S3Hook
import s3fs
import boto3
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO 
import json
import numpy as np
from collections import OrderedDict
from s3fs.core import S3FileSystem
from sqlalchemy import create_engine
import psycopg2
from transform import transform_data
def get_data(**kwargs):
    api_endpoint = "https://data.austintexas.gov/resource/9t4d-g238.json"
    response = requests.get(api_endpoint)
    data = response.json()
    df = pd.DataFrame(data)
    return df

def load_data_aws(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='Extract')

    # Upload the CSV file to S3
    s3_bucket = "airflow-dtsc"
    s3_key = "austin_animal_shelter_data.csv"

    # AWS credentials
    aws_access_key_id = "AKIA3VIUACNUPCKQQ57X"
    aws_secret_access_key = "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"

    # Save to S3
    df.to_csv(f"s3://{s3_bucket}/{s3_key}", index=False, storage_options={
        "key": aws_access_key_id,
        "secret": aws_secret_access_key
    })

def execute_sql_script(engine, sql_script):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # Execute the SQL script
            connection.execute(sql_script)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            raise e

def load_to_redshift(**kwargs):
    # AWS credentials
    aws_access_key_id = "AKIA3VIUACNUPCKQQ57X"
    aws_secret_access_key = "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"

    # Redshift credentials and connection information
    redshift_user = "anila"
    redshift_password = "Adgjlsfhk#1"
    redshift_host = "default-workgroup.801590219624.us-east-2.redshift-serverless.amazonaws.com"
    redshift_port = 5439
    redshift_db = "dev"
    redshift_schema = "public"

    # S3 bucket and key
    s3_bucket = "airflow-dtsc"
    #s3_key = "your_data_folder"

    # Initialize S3 filesysteme
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

    # Redshift connection string
    redshift_conn_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}"

    # Create Redshift engine
    engine = create_engine(redshift_conn_str)

    # Read and execute the init.sql file
    with open("dags/db/init.sql", "r") as init_sql_file:
        init_sql_script = init_sql_file.read()
        execute_sql_script(engine, init_sql_script)
# 'dimension_animals', 
    # List of tables to load
    table_names = ['dimension_dates', 'dimension_outcome_types','fct_outcomes']
#'dimension_animals', 
    for table_name in table_names:
        # Define S3 file path
        s3_file_path = f"s3://{s3_bucket}/{table_name}.csv"

        # Load data into Redshift using COPY command
        copy_command =  f"COPY {table_name} FROM '{s3_file_path}' CREDENTIALS 'aws_iam_role=arn:aws:iam::801590219624:role/role-s3-redshift' FORMAT AS CSV IGNOREHEADER 1;"
        engine.execute(copy_command)


# DAG definition
with DAG(
    dag_id="dag",
    start_date=datetime(2023,11,1),
    schedule_interval='@daily',
) as dag:

    Extract = PythonOperator(
        task_id="Extract",
        python_callable=get_data,
    )
    Upload = PythonOperator(
        task_id="Upload",
        python_callable=load_data_aws,
        provide_context=True,  
    )
    Transform_Load = PythonOperator(
        task_id="Transform_Load",
        python_callable=transform_data,
        provide_context=True,  
    )
    load_dim_animals = PythonOperator(
        task_id="load_dim_animals",
        python_callable=load_to_redshift,
        op_kwargs={"file_name": 'dimension_animals.csv', "table_name": 'dimension_animals'},
    )
    load_dim_outcome= PythonOperator(
        task_id="load_dim_outcome",
        python_callable=load_to_redshift,
        op_kwargs={"file_name": 'dimension_outcome_types.csv', "table_name": 'dimension_outcome_types'},
    )
    load_dim_dates = PythonOperator(
        task_id="load_dim_dates",
        python_callable=load_to_redshift,
        op_kwargs={"file_name": 'dimension_dates.csv', "table_name": 'dimension_dates'},
    )
    load_fct_outcomes = PythonOperator(
        task_id="load_fct_outcomes_tab",
        python_callable=load_to_redshift,
        op_kwargs={"file_name": 'fact_outcomes.csv', "table_name": 'fact_outcomes'},
    )

    Extract >> Upload >> Transform_Load >> [load_dim_animals, load_dim_outcome, load_dim_dates] >> load_fct_outcomes

   