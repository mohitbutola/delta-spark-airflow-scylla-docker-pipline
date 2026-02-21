from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "mohit",
    "retries": 2
}

with DAG(
    dag_id="delta_spark_airflow_scylla_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)
    
    create_delta_table = SparkSubmitOperator(
        task_id='create_delta_table',
        application='/opt/spark/work-dir/create_delta_table.py',
        conn_id='spark-conn',  # Configure this in Airflow UI
        # deploy_mode='client',  # Changed from cluster
        # master='spark://spark-master:7077',
        dag=dag
)

    # create_delta = SparkSubmitOperator(
    #     task_id="create_delta_table",
    #     application="/opt/spark/work-dir/create_delta_table.py",
    #     conn_id="spark_default",
    #     master="spark://spark-master:7077"
    #     # conf={
    #     #     "spark.master": "spark://spark-master:7077"
    #     # }
    # )

#     create_delta = SparkSubmitOperator(
#     task_id="create_delta_table",
#     application="/opt/spark/work-dir/create_delta_table.py",
#     spark_binary="spark-submit",
#     conf={
#         "spark.master": "spark://spark-master:7077"
#     }
# )

    # create_delta = SparkSubmitOperator(
    #     task_id="create_delta_table",
    #     application="/opt/spark/work-dir/create_delta_table.py",
    #     conn_id="spark_default",
    # )



    # transform = BashOperator(
    #     task_id="spark_transform",
    #     bash_command="/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/etl_transform.py"
    # )

    # load_scylla = BashOperator(
    #     task_id="load_to_scylla",
    #     bash_command="/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 /opt/spark/work-dir/load_to_scylla.py"
    # )

    start >>create_delta_table #>> transform >> load_scylla
