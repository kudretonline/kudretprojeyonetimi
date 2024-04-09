import os
import json
from airflow import DAG
import time
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils import dates
from datetime import date, datetime, timedelta, timezone
from pendulum import duration
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
import requests



GCS_PROJECT_NAME = 'kudret-proje'
STAGED_DATASET = "kudret_dataset"
STAGED_DATASET_TABLE = "smartphones_web_api"

GCS_DESTINATION_BUCKET = "kudret_bucket"
DAG_OWNER = "Website API -> GCS -> BQ"



MAIN_URL = 'https://dummyjson.com/products/category/smartphones'
finalURL = MAIN_URL
report_end_time = date.today()
file_suffix = '/api_response.json'

default_dag_args = {
    'owner': DAG_OWNER,
    'start_date' : datetime(2024, 4, 9),
    'catchup' : False,
    'depends_on_past': False,
    "retries": 1,
    "retry_delay": duration(seconds=15)
    }

def make_api_request(url):
    cwd = os.getcwd()
    print(cwd)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json_string = response.json()
            with open(cwd + file_suffix, "w") as outfile:
                json.dump(json_string, outfile)
                outfile.close()
                print('Successfully wrote to: ', cwd + file_suffix)
        elif response.status_code == 404:
            raise Exception("url_not_found")
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")
    except requests.exceptions.ConnectionError:
        return "Connection problem; check URL"
    except Exception as e:
        if str(e) == "url_not_found":
            return "Sorry, URL wasn't found"
        elif str(e) == "auth_problem":
            return "Authentication error"
        else:
            return f"An error occurred: {e}"




def delete_file():
    file_path = os.getcwd() + file_suffix
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{file_path} has been deleted.")
    else:
        print(f"{file_path} does not exist.")


with DAG(
    dag_id = 'website-api-json-to-gcs-to-bigquery',
    description = 'Extract data from website api, write to staged & curated BQ tables',
    schedule_interval='@once',
    default_args = default_dag_args
):

    extract_from_web_api = PythonOperator(
    task_id='extract_from_web_api_task',
    python_callable=make_api_request,
    op_kwargs = {'url':finalURL})

    delay_python = PythonOperator(task_id="delay_python_task",
                                                
                                                   python_callable=lambda: time.sleep(15))
    

    gcs_object_path = f"{report_end_time.strftime('%Y/%m/%d')}" + file_suffix

    upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_to_gcs_task',
            src=os.getcwd() + file_suffix,
            dst=gcs_object_path,
            gcp_conn_id="google_cloud_default",
            bucket = GCS_DESTINATION_BUCKET 

        )

    gcs_to_staged_bq = GCSToBigQueryOperator(
    task_id = "gcs_to_staged_bq_task",
    bucket = GCS_DESTINATION_BUCKET,
    write_disposition = 'WRITE_APPEND',
    source_objects = [gcs_object_path],
    gcp_conn_id="google_cloud_default",
    destination_project_dataset_table = f"{GCS_PROJECT_NAME}.{STAGED_DATASET}.{STAGED_DATASET_TABLE}",
    source_format = 'NEWLINE_DELIMITED_JSON',
    schema_fields=[

  {
    "name": "products",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "mode": "NULLABLE"
      },
      {
        "name": "title",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "description",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "price",
        "type": "INTEGER",
        "mode": "NULLABLE"
      },
      {
        "name": "discountPercentage",
        "type": "FLOAT",
        "mode": "NULLABLE"
      },
      {
        "name": "rating",
        "type": "FLOAT",
        "mode": "NULLABLE"
      },
      {
        "name": "stock",
        "type": "INTEGER",
        "mode": "NULLABLE"
      },
      {
        "name": "brand",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "category",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "thumbnail",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "images",
        "type": "STRING",
        "mode": "REPEATED"
      }
    ]
  },
  {
    "name": "total",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "skip",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "limit",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }

    ]
    )


    delete = PythonOperator(
    task_id='delete_file_task',
    python_callable=delete_file
)

    extract_from_web_api >> delay_python >> upload_to_gcs >> gcs_to_staged_bq >> delete 
