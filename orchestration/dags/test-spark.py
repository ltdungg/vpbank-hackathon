import datetime
from airflow.sdk import dag, task
from pyspark.sql import SparkSession
import json
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@dag(
    dag_id='test_spark_dag',
    schedule=None,
    start_date=datetime.datetime(2023, 10, 1),
    catchup=False,
)
def test_spark_dag():
    spark = SparkSession.builder \
        .appName("TestSparkApp") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

    @task
    def create_dataframe():
        data = [
            ("Alice", 30, "New York"),
            ("Bob", 25, "Los Angeles"),
            ("Charlie", 35, "Chicago")
        ]

        df = spark.createDataFrame(data, schema=schema)
        df.show()
        return "DataFrame created"


    create_dataframe()


test_spark_dag()