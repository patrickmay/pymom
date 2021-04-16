#!/usr/bin/env python3
import logging.config
import sys
from os import path

from pymom.PyMomAbstractConsumer import PyMomAbstractConsumer
from pymom.PyMom import PyMom

# This class is a simple example of how to use PyMom.  It listens for
# messages on the test.pymom.consume topic, which can be sent via the
# Kafka console producer or PyMomTestProducer, and writes them to the
# test.pymom.produce topic.

log_file_path = path.join(path.dirname(path.abspath(__file__)),'logger.config')
logging.config.fileConfig(fname=log_file_path,disable_existing_loggers=True)
logger = logging.getLogger("PyMomTestConsumer")


class PyMomTestConsumer(PyMomAbstractConsumer):
    """ PyMomTestConsumer is a simple example of using PyMom. """

    def __init__(self):
        logger.info('Listening on Test')
        self.pymom = PyMom()
        self.producer = self.pymom.producer('test.pymom.produce')

    def on_message(self,message):
        """ Process messages. """
        id = message['id']
        payload = message['payload']
        print("Received message:  ({}) {}".format(id,payload))
        try:
            self.producer.write(id,payload)
            print("Wrote message.")
        except Exception as error:
            print("Unable to send message:  {}".format(error))


if __name__ == "__main__":
    logger.info('Starting........')
    pymom = PyMom()
    consumer = PyMomTestConsumer()
    pymom.register(consumer,'PyMomTestConsumerGroup','test.pymom.consume')
    pymom.run()

    print("PyMomTestConsumer terminated.")
    sys.exit(0)
