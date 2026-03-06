from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}


with DAG(
    dag_id="sales_stream_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    
    start_kafka_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command="python /opt/airflow/script/kafka_producer.py"
    )

    
    start_spark_stream = BashOperator(
        task_id="start_spark_stream",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
        /opt/airflow/script/spark_consumer.py
        """
    )

    
    start_kafka_producer >> start_spark_stream