import json
from dataclasses import dataclass
from typing import Dict, List, NamedTuple, cast

import pika
import pika.adapters.blocking_connection
import pika.spec

from src.scripts.pdf_demo import generate_pdf
from src.utils.logging import LOGGER

# Global Counter
num_request = 0


# CONSTANTS
@dataclass
class CONSTANTS(NamedTuple):
    RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
    CHANNEL_QUEUE_PREFIX = "file_generate_status"
    ML_MODEL_FALLBACK_TOKEN_RESULT = "<NO-RESULT>"


# TYPES
class PredictionDict(NamedTuple):
    prediction: str


def predict(contents: Dict) -> PredictionDict:
    LOGGER.debug(f"Contents: {contents}")
    value: str = contents.get("value")
    query: Dict = contents.get("query_params")
    LOGGER.debug(value)
    LOGGER.debug(query)
    query_types: List[str] = cast(str, query.get("type")).split(",")
    LOGGER.debug(query_types)
    version = cast(int, query.get("version"))
    LOGGER.debug(version)

    # Add your code logic for data processing, AI Agent, and PDF generation here
    return_file_name = generate_pdf(_markdown=value)

    return {"file_name": (return_file_name if len(return_file_name) > 0 else "")}


def main(
    rabbitmq_url: str = CONSTANTS.RABBITMQ_URL,
    queue_prefix: str = CONSTANTS.CHANNEL_QUEUE_PREFIX,
) -> None:
    print("Hello World Test:")
    print("Testing Logging Capabilities")

    # Test logging capabilities (REMOVE LATER)
    LOGGER.info("Hello World - LOGGING (INFO)")
    LOGGER.debug("Hello World - LOGGING (DEBUG)")
    LOGGER.warning("Hello World - LOGGING (WARNING)")
    LOGGER.error("Hello World - LOGGING (ERROR)")
    LOGGER.critical("Hello World - LOGGING (CRITICAL)")

    # Connect to RabbitMQ
    predict_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = predict_connection.channel()

    REQUEST_QUEUE = f"{queue_prefix}_request_queue"
    RESPONSE_QUEUE = f"{queue_prefix}_response_queue"

    active_queues: List[str] = [REQUEST_QUEUE, RESPONSE_QUEUE]
    # Ensure that the queues exist (or are created)
    for queue_name in active_queues:
        channel.queue_declare(queue=queue_name, durable=True)

    def on_queue_request_received(
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        global num_request

        # Parse the incoming request
        request_data: Dict = json.loads(body)
        LOGGER.debug(
            f" [{num_request}] Received Request Number: {properties.correlation_id}"
        )
        LOGGER.debug(f" [{num_request}] Received Request Message: {request_data}")

        # Perform the prediction
        response_data = predict(contents=request_data)
        LOGGER.debug(
            f" [{num_request}] Sending Response Number: {properties.correlation_id}"
        )
        LOGGER.debug(f" [{num_request}] Sending Response Message: {response_data}")

        num_request += 1

        # Send the response to the Response Queue
        channel.basic_publish(
            exchange="",
            routing_key=properties.reply_to,
            body=json.dumps(response_data),
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
        )

        # Acknowledge the request
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return None

    # Start consuming from the Request Queue
    channel.basic_consume(
        queue=REQUEST_QUEUE, on_message_callback=on_queue_request_received
    )
    channel.start_consuming()

    return None


if __name__ == "__main__":
    main()
