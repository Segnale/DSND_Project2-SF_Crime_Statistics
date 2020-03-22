from kafka import KafkaProducer
import json
import time
import logging

logger = logging.getLogger(__name__)

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        if self.bootstrap_connected():
            logging.info("Producer Connected")
        else:
            logging.info("Producer Fails to Connect")

    #DONE we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            for line in json.load(f):
                message = self.dict_to_binary(line)
                # DONE send the correct data
                record = self.send(self.topic, message)
                logging.info(f"Message : {record.get()}")
                time.sleep(1)

    # DONE fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
