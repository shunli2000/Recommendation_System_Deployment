import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from unittest.mock import patch, MagicMock, ANY
from data_pipeline.kafka_consumer import get_kafka_consumer  # Replace with actual filename

@patch("kafka_consumer.KafkaConsumer")
def test_get_kafka_consumer_receives_message(mock_kafka_consumer):
    # Arrange: Set up a fake message
    mock_message = MagicMock()
    mock_message.value = "test message"

    # Mock the consumer instance
    mock_consumer_instance = MagicMock()
    mock_consumer_instance.__iter__.return_value = [mock_message]
    mock_kafka_consumer.return_value = mock_consumer_instance

    # Act: Get consumer and iterate through messages
    consumer = get_kafka_consumer()
    messages = [msg.value for msg in consumer]

    # Assert: We received the expected message
    assert messages == ["test message"]

    # Also check that KafkaConsumer was called with expected args
    mock_kafka_consumer.assert_called_with(
        "movielog7",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=ANY
    )
