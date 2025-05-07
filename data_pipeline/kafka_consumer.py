from kafka import KafkaConsumer

def get_kafka_consumer(topic="movielog7", server="localhost:9092"):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: m.decode('utf-8')
    )
    return consumer
