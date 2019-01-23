"""Test the publish and consume APIs on a real broker running on localhost."""

from collections import defaultdict
import multiprocessing
import threading
import time
import unittest
import uuid
import socket

from twisted.internet import reactor, task
import mock
import pika
import pkg_resources
import pytest
import pytest_twisted

from fedora_messaging import api, message, exceptions
from fedora_messaging.twisted import service
from fedora_messaging.twisted.protocol import _pika_version


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
            target=api.consume, args=(counting_callback,)
        )
        msg = message.Message(
            topic=u"nice.message",
            headers={u"niceness": u"very"},
            body={u"encouragement": u"You're doing great!"},
        )

        consumer_process.start()
        # Allow the consumer time to create the queues and bindings
        time.sleep(5)

        for _ in range(0, 3):
            try:
                api.publish(msg)
            except exceptions.ConnectionException:
                consumer_process.terminate()
                self.fail("Failed to publish message, is the broker running?")

        consumer_process.join(timeout=30)
        self.assertEqual(0, consumer_process.exitcode)

    @mock.patch("fedora_messaging.api._session_cache", threading.local())
    def test_pub_connection_refused(self):
        """Assert ConnectionException is raised on connection refused."""
        # Because we don't call accept, we can be sure of a connection refusal
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("", 0))
        url = "amqp://localhost:{port}/".format(port=sock.getsockname()[1])
        api._session_cache.session = api._session.PublisherSession(amqp_url=url)

        self.assertRaises(exceptions.ConnectionException, api.publish, api.Message())


@pytest.mark.skipif(
    _pika_version < pkg_resources.parse_version("1.0.0b1"),
    reason="Twisted with confirms only supported on pika-1.0.0b1+",
)
@pytest_twisted.inlineCallbacks
def test_check_confirms():
    """Assert confirmations are enabled by default."""
    serv = service.FedoraMessagingService(amqp_url="amqp://")
    serv.startService()
    client = yield serv.getFactory().whenConnected()
    channel = yield client._allocate_channel()
    assert channel._delivery_confirmation is True
    serv.stopService()


@pytest_twisted.inlineCallbacks
def test_basic_pub_sub():
    """Basic test of the Twisted publishing/subscribing support"""
    queue = str(uuid.uuid4())
    queues = [
        {"queue": queue, "auto_delete": True, "arguments": {"x-expires": 60 * 1000}}
    ]
    msg = message.Message(
        topic=u"nice.message",
        headers={u"niceness": u"very"},
        body={u"encouragement": u"You're doing great!"},
    )
    expected_headers = {
        u"fedora_messaging_severity": 20,
        u"fedora_messaging_schema": u"base.message",
        u"niceness": u"very",
    }
    messages_received = []
    serv = service.FedoraMessagingService(amqp_url="amqp://")

    serv.startService()
    client = yield serv.getFactory().whenConnected()
    yield client.declare_queues(queues)
    yield client.bind_queues(
        [{"queue": queue, "exchange": "amq.topic", "routing_key": "#"}]
    )

    def callback(message):
        messages_received.append(message)
        if len(messages_received) == 3:
            raise exceptions.HaltConsumer()

    yield client.consume(callback, queue)
    for _ in range(0, 3):
        yield client.publish(msg, "amq.topic")
    yield task.deferLater(reactor, 3.0, lambda: True)
    serv.stopService()

    assert len(messages_received) == 3
    for m in messages_received:
        assert u"nice.message" == m.topic
        assert {u"encouragement": u"You're doing great!"} == m.body
        assert "sent-at" in m._headers
        del m._headers["sent-at"]
        assert expected_headers == m._headers


@pytest_twisted.inlineCallbacks
def test_unhandled_exception_cancels_consumer():
    """Assert any unhandled Exception results in the consumer being canceled."""
    queue = str(uuid.uuid4())
    queues = [
        {"queue": queue, "auto_delete": True, "arguments": {"x-expires": 60 * 1000}}
    ]
    serv = service.FedoraMessagingService(amqp_url="amqp://")

    serv.startService()
    client = yield serv.getFactory().whenConnected()
    yield client.declare_queues(queues)
    yield client.bind_queues(
        [{"queue": queue, "exchange": "amq.topic", "routing_key": "#"}]
    )

    def callback(message):
        raise Exception("Panic!")

    yield client.consume(callback, queue)
    assert len(client._consumers) == 1

    yield client.publish(message.Message(), "amq.topic")
    yield task.deferLater(reactor, 3.0, lambda: True)
    assert len(client._consumers) == 0
    serv.stopService()


@pytest_twisted.inlineCallbacks
def test_nack_handled():
    """Assert raising Nack in a consumer works and messages are re-delivered"""
    queue = str(uuid.uuid4())
    queues = [
        {"queue": queue, "auto_delete": True, "arguments": {"x-expires": 60 * 1000}}
    ]
    messages = []
    serv = service.FedoraMessagingService(amqp_url="amqp://")

    serv.startService()
    client = yield serv.getFactory().whenConnected()
    yield client.declare_queues(queues)
    yield client.bind_queues(
        [{"queue": queue, "exchange": "amq.topic", "routing_key": "#"}]
    )

    def callback(message):
        messages.append(message)
        if len(messages) < 3:
            raise exceptions.Nack()

    yield client.consume(callback, queue)
    assert len(client._consumers) == 1

    yield client.publish(message.Message(), "amq.topic")
    yield task.deferLater(reactor, 3.0, lambda: True)

    assert len(messages) == 3
    assert len(set([m.id for m in messages])) == 1
    assert len(client._consumers) == 1

    yield client.cancel(queue)
    serv.stopService()


@pytest_twisted.inlineCallbacks
def test_drop_handled():
    """Assert raising Drop in a consumer works and messages are not re-delivered"""
    queue = str(uuid.uuid4())
    messages = []
    serv = service.FedoraMessagingService(amqp_url="amqp://")
    serv.startService()
    client = yield serv.getFactory().whenConnected()
    queues = [
        {"queue": queue, "auto_delete": True, "arguments": {"x-expires": 60 * 1000}}
    ]
    yield client.declare_queues(queues)
    yield client.bind_queues(
        [{"queue": queue, "exchange": "amq.topic", "routing_key": "#"}]
    )

    def callback(message):
        messages.append(message)
        raise exceptions.Drop()

    yield client.consume(callback, queue)
    assert len(client._consumers) == 1

    yield client.publish(message.Message(), "amq.topic")
    yield task.deferLater(reactor, 3.0, lambda: True)  # Just wait a few seconds

    assert len(messages) == 1
    assert len(client._consumers) == 1
    yield client.cancel(queue)
    serv.stopService()


@pytest_twisted.inlineCallbacks
def test_declare_queue_failures():
    """Assert that if a queue can't be declared, it results in an exception."""
    serv = service.FedoraMessagingService(amqp_url="amqp://")
    serv.startService()
    client = yield serv.getFactory().whenConnected()

    queues = [{"queue": str(uuid.uuid4()), "passive": True}]
    try:
        yield client.declare_queues(queues)
    except exceptions.BadDeclaration as e:
        assert "queue" == e.obj_type
        assert queues[0] == e.description
        assert isinstance(e.reason, pika.exceptions.ChannelClosed)
    serv.stopService()


@pytest_twisted.inlineCallbacks
def test_declare_exchange_failures():
    """Assert that if an exchange can't be declared, it results in an exception."""
    serv = service.FedoraMessagingService(amqp_url="amqp://")
    serv.startService()
    client = yield serv.getFactory().whenConnected()

    exchanges = [{"exchange": str(uuid.uuid4()), "passive": True}]
    try:
        yield client.declare_exchanges(exchanges)
    except exceptions.BadDeclaration as e:
        assert "exchange" == e.obj_type
        assert exchanges[0] == e.description
        assert isinstance(e.reason, pika.exceptions.ChannelClosed)
    serv.stopService()


@pytest_twisted.inlineCallbacks
def test_declare_binding_failure():
    """Assert that if a binding can't be declared, it results in an exception."""
    serv = service.FedoraMessagingService(amqp_url="amqp://")
    serv.startService()
    client = yield serv.getFactory().whenConnected()

    binding = [
        {"exchange": str(uuid.uuid4()), "queue": str(uuid.uuid4()), "routing_key": "#"}
    ]
    try:
        yield client.bind_queues(binding)
    except exceptions.BadDeclaration as e:
        assert "binding" == e.obj_type
        assert binding[0] == e.description
        assert isinstance(e.reason, pika.exceptions.ChannelClosed)
    serv.stopService()
