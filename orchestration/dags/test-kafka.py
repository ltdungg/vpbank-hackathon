import datetime
from airflow.sdk import dag, task
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

@dag(
    dag_id='test_kafka_dag',
    schedule=None,
    start_date=datetime.datetime(2023, 10, 1),
    catchup=False,
)
def test_kafka_dag():
    producer = KafkaProducer(bootstrap_servers='kafka:9092')

    @task
    def send_message():
        message = {'key': 'value'}
        try:
            future = producer.send('test-topic', json.dumps(message).encode('utf-8'))
            result = future.get(timeout=10)
            print(f"Message sent successfully: {result}")
        except KafkaError as e:
            print(f"Failed to send message: {e}")

    @task
    def consume_message():
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='test-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            print(f"Received message: {message.value}")
            break

    send_message_task = send_message()
    consume_message_task = consume_message()
    send_message_task >> consume_message_task

test_kafka_dag()