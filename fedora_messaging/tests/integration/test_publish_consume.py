"""Test the publish and consume APIs on a real broker running on localhost."""

from collections import defaultdict
import multiprocessing
import time
import unittest

from fedora_messaging import api, message, exceptions


class PubSubTests(unittest.TestCase):

    def test_pub_sub(self):
        """Publish 3 messages via the publish API, assert they arrive at the consumer."""

        # Consumer setup
        def counting_callback(message, storage=defaultdict(int)):
            storage[message.topic] += 1
            if storage[message.topic] == 3:
                raise exceptions.HaltConsumer

        bindings = [{
            'exchange': 'amq.topic',
            'queue_name': 'fedora_messages_integration_tests',
            'routing_key': 'nice.message',
        }]
        consumer_process = multiprocessing.Process(
            target=api.consume, args=(counting_callback, bindings))
        msg = message.Message(topic=u'nice.message', headers={u'niceness': u'very'},
                              body={u'encouragement': u"You're doing great!"})

        consumer_process.start()
        # Allow the consumer time to create the queues and bindings
        time.sleep(5)

        for _ in range(0, 3):
            api.publish(msg)

        consumer_process.join(timeout=30)
        self.assertEqual(0, consumer_process.exitcode)
