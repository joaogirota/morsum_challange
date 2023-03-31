# Python libraries
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine
import pandas as pd


# Set variables, using a .toml file
DAG_NAME = "dag-morsum-challange"
LOGGER = logging.getLogger(DAG_NAME)
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURR_DIR)

LOGGER.info(f"Reading configuration for profile {COMPOSER_PROFILE}")
CONFIG = configparser.ConfigParser()
CONFIG.read(CURR_DIR + f"/dag-config-morsum-challange.toml")
GCP_CONFIG = CONFIG["GCP"]

SERVICE_ACCOUNT = Variable.get("SERVICE_ACCOUNT")

engine = create_engine(f"mysql+pymysql://{GCP_CONFIG['mysqlroot']}:{GCP_CONFIG['mysqpassword']}@localhost/{GCP_CONFIG['database_name']}")


def load_mysql_product_prices():
    # Load data from GCS
    data = pd.read_csv(f"{GCP_CONFIG['bucket']}/2022-07-01.csv")
    
    # Write data to MySQL
    data.to_sql({GCP_CONFIG['table']}, con=engine, if_exists='append', chunksize=1000)





with DAG(

description="Example DAG to synch MySQL to BQ",
schedule_interval="30 1 * * *",
max_active_runs=1,
catchup=True

default_arguments = {
    'owner': 'Morsum',
    'start_date':days_ago(1),
    'depends_on_past': False,
    'email': GCP_CONFIG['owner_email'],
    'email_on_failure': True,
    'email_on_retry': True,
    'provide_context': True,
    'retries': 2,
    'retry_delay':timedelta(minutes=5)
    'GCP_BUCKET_price': GCP_CONFIG['bucket_origin'],
    'GCP_BUCKET_price_destiny': GCP_CONFIG['bucket_destiny'],
    'GCP_BUCKET_python': GCP_CONFIG['python_script'],
    }

) as dag:

    truncate_gcs_product_prices = BashOperator(
    task_id='truncate_gcs_target',
    bash_command=f'gsutil -m rm -r gs://{GCP_CONFIG['bucket_origin']}',
    dag=dag
    )

    copy_product_prices = BashOperator(
    task_id='copy_gcs_product_prices',
    bash_command=f'gsutil cp gs://{GCP_CONFIG['bucket_destiny']} gs://{GCP_CONFIG['bucket_origin']}',
    dag=dag
    )

    clean_product_prices = BashOperator(
    task_id='copy_gcs_product_prices',
    bash_command=f'python {GCP_CONFIG['python_script']}',
    )

    load_mysql = PythonOperator(
    task_id='load_mysql',
    python_callable=load_mysql_product_prices,
    dag=dag
    )

    copy_product_prices >> truncate_gcs_product_prices >>  clean_product_prices
    load_mysql