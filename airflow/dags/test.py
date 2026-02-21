from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_delta_job",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_delta_table = SparkSubmitOperator(
        task_id="create_delta_table",
        application="/opt/spark/work-dir/create_delta_table.py",
        conn_id="spark_default",
        # Pass delta jars explicitly
        jars="/opt/spark/jars/delta-spark_2.12-3.1.0.jar,"
             "/opt/spark/jars/delta-storage-3.1.0.jar",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.driver.extraClassPath": "/opt/spark/jars/delta-spark_2.12-3.1.0.jar:"
                                           "/opt/spark/jars/delta-storage-3.1.0.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/delta-spark_2.12-3.1.0.jar:"
                                             "/opt/spark/jars/delta-storage-3.1.0.jar",
        },
        # Force use of correct spark-submit binary
        spark_binary="/opt/spark/bin/spark-submit",
        verbose=False,
    )


    transform = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/spark/work-dir/etl_transform.py",
        conn_id="spark_default",
        # Pass delta jars explicitly
        jars="/opt/spark/jars/delta-spark_2.12-3.1.0.jar,"
             "/opt/spark/jars/delta-storage-3.1.0.jar",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.driver.extraClassPath": "/opt/spark/jars/delta-spark_2.12-3.1.0.jar:"
                                           "/opt/spark/jars/delta-storage-3.1.0.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/delta-spark_2.12-3.1.0.jar:"
                                             "/opt/spark/jars/delta-storage-3.1.0.jar",
        },
        # Force use of correct spark-submit binary
        spark_binary="/opt/spark/bin/spark-submit",
        verbose=True,
    )

    # load_scylla = SparkSubmitOperator(
    #     task_id="load_to_scylla",
    #     application="/opt/spark/work-dir/load_to_scylla.py",
    #     conn_id="spark_default",
    #     # Pass cassandra connector jars explicitly
    #     jars="/opt/spark/jars/spark-cassandra-connector_2.12-3.3.0.jar",
    #     conf={
    #         "spark.sql.extensions": "com.datastax.spark.connector.CassandraSparkExtensions",
    #         "spark.cassandra.connection.host": "scylla",
    #         "spark.cassandra.connection.port": "9042",
    #     },
    #     # Force use of correct spark-submit binary
    #     spark_binary="/opt/spark/bin/spark-submit",
    #     verbose=True,
    # )

    load_to_scylla = SparkSubmitOperator(
    task_id="load_to_scylla",
    application="/opt/spark/work-dir/load_to_scylla.py",
    conn_id="spark_default",
    conf={
        # Both Delta AND Cassandra extensions together
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.cassandra.connection.host": "scylla",
        "spark.cassandra.connection.port": "9042",
        "spark.driver.extraClassPath": "/opt/spark/jars/spark-cassandra-connector-assembly.jar:"
                                       "/opt/spark/jars/delta-spark_2.12-3.1.0.jar:"
                                       "/opt/spark/jars/delta-storage-3.1.0.jar",
        "spark.executor.extraClassPath": "/opt/spark/jars/spark-cassandra-connector-assembly.jar:"
                                         "/opt/spark/jars/delta-spark_2.12-3.1.0.jar:"
                                         "/opt/spark/jars/delta-storage-3.1.0.jar",
    },
    # Assembly jar contains ALL dependencies (Logging class included)
    jars="/opt/spark/jars/spark-cassandra-connector-assembly.jar,"
         "/opt/spark/jars/delta-spark_2.12-3.1.0.jar,"
         "/opt/spark/jars/delta-storage-3.1.0.jar",
    verbose=True,
)

    create_delta_table >> transform >> load_to_scylla