from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


   
default_args = {
    'owner': 'airflow',
    # 'depends_on_past': False,
    'start_date': datetime(2024, 2, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
}

with DAG(
    dag_id='missed_deadline_job_dag',
    default_args=default_args,
    description='Insert missed deadline orders into PostgreSQL',
    schedule_interval='@daily' 
) as dag:
    # Task 1: Create table olist_missed_deadline_tb
    create_missed_deadline_table = PostgresOperator(
        task_id="create_missed_deadline_table",
        postgres_conn_id="olist_pg_conn",
        sql="sql/init_tb_olist_missed_deadline_job.sql"
    )

    
    # Task 2: Extract zip file and move csv file to aws s3 Bucket
    spark_ingestion_to_s3 = BashOperator(
        task_id="spark_ingestion_to_s3",
        bash_command= 'python /home/nhut/etl-airflow/pipeline/spark_ingestion_to_s3.py',
    )

    
    # Task 3: Transform data
    spark_transform_missed_deadline_job = BashOperator(
        task_id = "spark_transform_missed_deadline_job",
        bash_command= "python /home/nhut/etl-airflow/pipeline/spark_missed_deadline_job.py",
    )
    
    # Task 4: Load transformed data to PotsgreSQL
    spark_load_data_to_postgtrsql = BashOperator(
        task_id = "spark_load_data_to_postgresql",
        bash_command= "python /home/nhut/etl-airflow/pipeline/spark_load_data_to_posgres.py"
    )
    create_missed_deadline_table >> spark_ingestion_to_s3 >> spark_transform_missed_deadline_job >> spark_load_data_to_postgtrsql