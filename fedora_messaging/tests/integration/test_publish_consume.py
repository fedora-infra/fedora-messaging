"""Test the publish and consume APIs on a real broker running on localhost."""

from collections import defaultdict
import multiprocessing
import time
import unittest

from fedora_messaging import api, message, exceptions


class PubSubTests(unittest.TestCase):

    def test_pub_sub_default_settings(self):
        """
        Assert publishing and subscribing works with the default configuration.

        This should work because the publisher uses the 'amq.topic' exchange by
        default and the consumer also uses the 'amq.topic' exchange with its
        auto-named queue and a default subscription key of '#'.
        """

        # Consumer setup
        def counting_callback(message, storage=defaultdict(int)):
            storage[message.topic] += 1
            if storage[message.topic] == 3:
                raise exceptions.HaltConsumer()

        consumer_process = multiprocessing.Process(
            target=api.consume, args=(counting_callback,))
        msg = message.Message(topic=u'nice.message', headers={u'niceness': u'very'},
                              body={u'encouragement': u"You're doing great!"})

        consumer_process.start()
        # Allow the consumer time to create the queues and bindings
        time.sleep(5)

        for _ in range(0, 3):
            try:
                api.publish(msg)
            except exceptions.ConnectionException:
                consumer_process.terminate()
                self.fail('Failed to publish message, is the broker running?')

        consumer_process.join(timeout=30)
        self.assertEqual(0, consumer_process.exitcode)
