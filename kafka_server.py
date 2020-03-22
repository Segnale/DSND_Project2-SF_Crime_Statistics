import producer_server
import logging
import logging.config
from pathlib import Path

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

logger = logging.getLogger(__name__)

def run_kafka_server():
	# DONE get the json file path
    input_file = "police-department-calls-for-service.json"

    # DONE fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="sf.police.calls",
        bootstrap_servers="localhost:9092",
        client_id="None"
    )
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
