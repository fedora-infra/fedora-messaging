"""Test the :mod:`fedora_messaging.api` APIs on a real broker running on localhost."""

from collections import defaultdict
import uuid
import six
import mock
import time

from twisted.internet import reactor, defer, task, threads
import treq
import pika
import pytest
import pytest_twisted
import socket

from fedora_messaging import api, message, exceptions, config
from fedora_messaging.twisted import service
from fedora_messaging.twisted.protocol import _add_timeout


HTTP_API = "http://localhost:15672/api/"
HTTP_AUTH = ("guest", "guest")


def setup_function(function):
    """Ensure each test starts with a fresh Service and configuration."""
    config.conf = config.LazyConfig()
    config.conf["client_properties"]["app"] = function.__name__
    if api._twisted_service:
        pytest_twisted.blockon(api._twisted_service.stopService())
    api._twisted_service = None


@pytest.fixture
def admin_user():
    """
    Fixture that creates a random admin user and deletes the user afterwards.

    Useful if the test wishes to alter permissions to test failure cases. Default
    permissions is complete access to the "/" vhost.

    Returns:
        The username of the new administrator. The password is "guest".
    """
    # Create a user with no permissions
    username = str(uuid.uuid4())
    url = "{base}users/{user}".format(base=HTTP_API, user=username)
    body = {"username": username, "password": "guest", "tags": "administrator"}
    deferred_resp = treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)

    @pytest_twisted.inlineCallbacks
    def cp(resp):
        assert resp.code == 201
        url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=username)
        body = {"configure": ".*", "write": ".*", "read": ".*"}
        resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
        assert resp.code == 201

    deferred_resp.addCallbacks(cp, cp)
    pytest_twisted.blockon(deferred_resp)
    yield username

    # Cleanup
    deferred_resp = treq.delete(url, auth=HTTP_AUTH, timeout=3)
    pytest_twisted.blockon(deferred_resp)


@pytest_twisted.inlineCallbacks
def get_queue(queue_dict, delay=10):
    """
    Retrieve data about a queue in the broker in the default vhost ("/").

    Args:
        delay: Time to wait before querying the server. It can take some time for
            everything to settle and become available in the HTTP API. 10 seconds
            was arrived at after intermittent failures at lower values.

    Returns:
        dict: A dictionary representation of the queue.
    """
    queues = list(queue_dict.keys())
    name = queues[0]
    # Assert both messages are delivered, no messages are un-acked, and only one
    # message got a positive acknowledgment.
    url = "{base}queues/%2F/{queue}".format(base=HTTP_API, queue=name)
    server_queue = yield task.deferLater(
        reactor, delay, treq.get, url, auth=HTTP_AUTH, timeout=3
    )
    server_queue = yield server_queue.json()
    defer.returnValue(server_queue)


@pytest.fixture
def queue_and_binding():
    queue = str(uuid.uuid4())
    queues = {
        queue: {
            "durable": False,
            "exclusive": False,
            "auto_delete": False,
            "arguments": {"x-expires": 60 * 1000},
        }
    }
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
    yield queues, bindings


@pytest_twisted.inlineCallbacks
def test_twisted_consume_halt_consumer(queue_and_binding):
    """
    Assert raising HaltConsumer works with :func:`fedora_messaging.api.twisted_consume`
    API.
    """
    queues, bindings = queue_and_binding
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

    def callback(message):
        """Count to 3 and quit."""
        messages_received.append(message)
        if len(messages_received) == 3:
            raise exceptions.HaltConsumer()

    consumers = yield api.twisted_consume(callback, bindings, queues)

    # Assert the server reports a consumer
    server_queue = yield get_queue(queues)
    assert server_queue["consumers"] == 1

    for _ in range(0, 3):
        yield threads.deferToThread(api.publish, msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer as e:
        assert len(messages_received) == 3
        assert e.exit_code == 0
        for m in messages_received:
            assert u"nice.message" == m.topic
            assert {u"encouragement": u"You're doing great!"} == m.body
            assert "sent-at" in m._headers
            del m._headers["sent-at"]
            assert expected_headers == m._headers
        server_queue = yield get_queue(queues)
        assert server_queue["consumers"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_stop_service(queue_and_binding):
    """
    Assert stopping the service, which happens when the reactor shuts down, waits
    for consumers to finish processing and then cancels them before closing the connection.
    """
    queues, bindings = queue_and_binding
    message_received, message_processed = defer.Deferred(), defer.Deferred()

    def callback(message):
        """Callback when the message is received, introduce a delay, then callback again."""
        reactor.callFromThread(message_received.callback, None)
        time.sleep(5)
        reactor.callFromThread(message_processed.callback, None)

    consumers = yield api.twisted_consume(callback, bindings, queues)
    yield threads.deferToThread(api.publish, message.Message(), "amq.topic")

    _add_timeout(consumers[0].result, 10)
    _add_timeout(message_received, 10)
    try:
        yield message_received
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer receiving message!")

    assert not message_processed.called
    deferred_stop = api._twisted_service.stopService()

    _add_timeout(message_processed, 10)
    try:
        yield message_processed
        # The request to stop should wait on the message to be processed
        assert deferred_stop.called is False
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer processing message")
    yield deferred_stop


@pytest_twisted.inlineCallbacks
def test_twisted_consume_cancel(queue_and_binding):
    """Assert canceling works with :func:`fedora_messaging.api.twisted_consume` API."""
    queues, bindings = queue_and_binding

    # Assert that the number of consumers we think we started is the number the
    # server things we started. This will fail if other tests don't clean up properly.
    # If it becomes problematic perhaps each test should have a vhost.
    consumers = yield api.twisted_consume(lambda m: m, bindings, queues)
    consumers[0].result.addErrback(pytest.fail)

    server_queue = yield get_queue(queues)
    assert server_queue["consumers"] == 1

    try:
        d = consumers[0].cancel()
        _add_timeout(d, 5)
        yield d

        # Assert the consumer.result deferred has called back when the cancel
        # deferred fires.
        assert consumers[0].result.called

        # Finally make sure the server agrees that the consumer is canceled.
        server_queue = yield get_queue(queues)
        assert server_queue["consumers"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_consume_halt_consumer_requeue(queue_and_binding):
    """Assert raising HaltConsumer with requeue=True re-queues the message."""
    queues, bindings = queue_and_binding
    msg = message.Message(
        topic=u"nice.message",
        headers={u"niceness": u"very"},
        body={u"encouragement": u"You're doing great!"},
    )

    def callback(message):
        """Count to 3 and quit."""
        raise exceptions.HaltConsumer(exit_code=1, requeue=True)

    # Assert that the number of consumers we think we started is the number the
    # server things we started. This will fail if other tests don't clean up properly.
    # If it becomes problematic perhaps each test should have a vhost.
    consumers = yield api.twisted_consume(callback, bindings, queues)
    yield threads.deferToThread(api.publish, msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer as e:
        # Assert there are no consumers for the queue, and that there's a ready message
        assert e.exit_code == 1

        server_queue = yield get_queue(queues)
        assert server_queue["consumers"] == 0
        assert server_queue["messages_ready"] == 1
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_consume_drop_message(queue_and_binding):
    """Assert raising Drop causes the message to be dropped, but processing continues."""
    queues, bindings = queue_and_binding
    msg = message.Message(
        topic=u"nice.message",
        headers={u"niceness": u"very"},
        body={u"encouragement": u"You're doing great!"},
    )
    dropped_messages = []

    def callback(message):
        """Drop 1 message and then halt on the second message."""
        dropped_messages.append(message)
        if len(dropped_messages) == 2:
            raise exceptions.HaltConsumer()
        raise exceptions.Drop()

    # Assert that the number of consumers we think we started is the number the
    # server things we started. This will fail if other tests don't clean up properly.
    # If it becomes problematic perhaps each test should have a vhost.
    consumers = yield api.twisted_consume(callback, bindings, queues)
    yield threads.deferToThread(api.publish, msg, "amq.topic")
    yield threads.deferToThread(api.publish, msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer:
        # Assert both messages are delivered, no messages are un-acked, and only one
        # message got a positive acknowledgment.
        server_queue = yield get_queue(queues)
        assert server_queue["consumers"] == 0
        assert server_queue["messages"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    finally:
        yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_nack_message(queue_and_binding):
    """Assert raising Nack causes the message to be replaced in the queue."""
    queues, bindings = queue_and_binding
    msg = message.Message(
        topic=u"nice.message",
        headers={u"niceness": u"very"},
        body={u"encouragement": u"You're doing great!"},
    )
    nacked_messages = []

    def callback(message):
        """Nack the message, then halt."""
        nacked_messages.append(message)
        if len(nacked_messages) == 2:
            raise exceptions.HaltConsumer()
        raise exceptions.Nack()

    # Assert that the number of consumers we think we started is the number the
    # server things we started. This will fail if other tests don't clean up properly.
    # If it becomes problematic perhaps each test should have a vhost.
    consumers = yield api.twisted_consume(callback, bindings, queues)
    yield threads.deferToThread(api.publish, msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer:
        # Assert the message was delivered, redelivered when Nacked, then acked by HaltConsumer
        server_queue = yield get_queue(queues)
        assert server_queue["consumers"] == 0
        assert server_queue["messages"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    finally:
        yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_general_exception(queue_and_binding):
    """
    Assert if the callback raises an unhandled exception, it is passed on to the
    consumer.result and the message is re-queued.
    """
    queues, bindings = queue_and_binding
    msg = message.Message(
        topic=u"nice.message",
        headers={u"niceness": u"very"},
        body={u"encouragement": u"You're doing great!"},
    )

    def callback(message):
        """An *exceptionally* useless callback"""
        raise Exception("Oh the huge manatee")

    # Assert that the number of consumers we think we started is the number the
    # server things we started. This will fail if other tests don't clean up properly.
    # If it becomes problematic perhaps each test should have a vhost.
    consumers = yield api.twisted_consume(callback, bindings, queues)
    yield threads.deferToThread(api.publish, msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
        pytest.fail("Expected an exception to be raised.")
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    except Exception as e:
        # Assert the message was delivered and re-queued when the consumer crashed.
        assert e.args[0] == "Oh the huge manatee"
        server_queue = yield get_queue(queues)
        assert server_queue["consumers"] == 0
        assert server_queue["messages"] == 1
    finally:
        yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_connection_reset(queue_and_binding):
    """
    Assert consuming works across connections and handles connection resets.

    This test sets up a queue, publishes 2 messages to itself, then kills all active
    connections on the broker. It then sends a third message and asserts the consumer
    gets it.
    """
    queues, bindings = queue_and_binding
    msg = message.Message(
        topic=u"nice.message",
        headers={u"niceness": u"very"},
        body={u"encouragement": u"You're doing great!"},
    )
    messages_received = []
    two_received = defer.Deferred()  # Fired by the callback on 2 messages

    def callback(message):
        """Count to, 2, fire a deferred, then count to 3 and quit."""
        messages_received.append(message)
        if len(messages_received) == 2:
            two_received.callback(None)
        if len(messages_received) == 3:
            raise exceptions.HaltConsumer()

    consumers = yield api.twisted_consume(callback, bindings, queues)

    # Wait for two messages to get through, kill the connection, and then send
    # the third and wait for the consumer to finish
    yield threads.deferToThread(api.publish, msg, "amq.topic")
    yield threads.deferToThread(api.publish, msg, "amq.topic")
    _add_timeout(two_received, 10)
    try:
        yield two_received
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without receiving first two messages")

    for _ in range(10):
        conns = yield task.deferLater(
            reactor, 1, treq.get, HTTP_API + "connections", auth=HTTP_AUTH, timeout=3
        )
        conns = yield conns.json()
        this_conn = [
            c
            for c in conns
            if "app" in c["client_properties"]
            and c["client_properties"]["app"] == "test_twisted_consume_connection_reset"
        ]
        if this_conn:
            cname = six.moves.urllib.parse.quote(this_conn[0]["name"])
            yield treq.delete(
                HTTP_API + "connections/" + cname, auth=HTTP_AUTH, timeout=3
            )
            break
    else:
        pytest.fail("Unable to find and kill connection!")

    # The consumer should receive this third message after restarting its connection
    # and then it should exit gracefully.
    yield threads.deferToThread(api.publish, msg, "amq.topic")
    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer:
        assert len(messages_received) == 3
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_consume_serverside_cancel(queue_and_binding):
    """
    Assert the consumer halts and ``consumer.result`` errbacks when the server
    explicitly cancels the consumer (by deleting the queue).
    """
    queues, bindings = queue_and_binding

    consumers = yield api.twisted_consume(lambda x: x, bindings, queues)

    # Delete the queue and assert the consumer errbacks
    url = "{base}queues/%2F/{queue}".format(base=HTTP_API, queue=list(queues.keys())[0])
    yield treq.delete(url, auth=HTTP_AUTH, timeout=3)

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
        pytest.fail("Consumer did not errback!")
    except exceptions.ConsumerCanceled:
        pass
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer calling its errback!")


@pytest_twisted.inlineCallbacks
def test_no_vhost_permissions(admin_user, queue_and_binding):
    """Assert a hint is given if the user doesn't have any access to the vhost"""
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    resp = yield treq.delete(url, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    queues, _ = queue_and_binding

    amqp_url = "amqp://{user}:guest@localhost:5672/%2F".format(user=admin_user)
    with mock.patch.dict(config.conf, {"amqp_url": amqp_url}):
        try:
            yield api.twisted_consume(lambda x: x, [], queues)
        except exceptions.ConnectionException as e:
            assert e.reason == (
                "The TCP connection appears to have started, but the TLS or AMQP "
                "handshake with the broker failed; check your connection and "
                "authentication parameters and ensure your user has permission "
                "to access the vhost"
            )


@pytest_twisted.inlineCallbacks
def test_no_read_permissions_queue_read_failure_pika1(admin_user, queue_and_binding):
    """
    Assert an errback occurs when unable to read from the queue due to
    permissions. This is a bit weird because the consumer has permission to
    register itself, but not to actually read from the queue so the result is
    what errors back.
    """
    queues, bindings = queue_and_binding
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    body = {"configure": ".*", "write": ".*", "read": ""}
    resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    amqp_url = "amqp://{user}:guest@localhost:5672/%2F".format(user=admin_user)
    with mock.patch.dict(config.conf, {"amqp_url": amqp_url}):
        try:
            consumers = api.twisted_consume(lambda x: x, [], queues)
            _add_timeout(consumers, 5)
            yield consumers
            pytest.fail("Call failed to raise an exception")
        except exceptions.PermissionException as e:
            assert e.reason == (
                "ACCESS_REFUSED - access to queue '{}' in vhost '/' refused for user "
                "'{}'".format(list(queues.keys())[0], admin_user)
            )
        except (defer.TimeoutError, defer.CancelledError):
            pytest.fail("Timeout reached without consumer calling its errback!")


@pytest_twisted.inlineCallbacks
def test_no_read_permissions_bind_failure(admin_user, queue_and_binding):
    """Assert the call to twisted_consume errbacks on read permissions errors on binding."""
    queues, bindings = queue_and_binding
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    body = {"configure": ".*", "write": ".*", "read": ""}
    resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    amqp_url = "amqp://{user}:guest@localhost:5672/%2F".format(user=admin_user)
    try:
        with mock.patch.dict(config.conf, {"amqp_url": amqp_url}):
            yield api.twisted_consume(lambda x: x, bindings, queues)
        pytest.fail("Call failed to raise an exception")
    except exceptions.BadDeclaration as e:
        assert e.reason.args[0] == 403
        assert e.reason.args[1] == (
            "ACCESS_REFUSED - access to exchange 'amq.topic' in vhost '/' refused for user"
            " '{}'".format(admin_user)
        )


@pytest_twisted.inlineCallbacks
def test_no_write_permissions(admin_user, queue_and_binding):
    """Assert the call to twisted_consume errbacks on write permissions errors."""
    queues, bindings = queue_and_binding
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    body = {"configure": ".*", "write": "", "read": ".*"}
    resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    amqp_url = "amqp://{user}:guest@localhost:5672/%2F".format(user=admin_user)
    try:
        with mock.patch.dict(config.conf, {"amqp_url": amqp_url}):
            yield api.twisted_consume(lambda x: x, bindings, queues)
        pytest.fail("Call failed to raise an exception")
    except exceptions.BadDeclaration as e:
        assert e.reason.args[0] == 403
        assert e.reason.args[1] == (
            "ACCESS_REFUSED - access to queue '{}' in vhost '/' refused for user"
            " '{}'".format(list(queues.keys())[0], admin_user)
        )


@pytest_twisted.inlineCallbacks
def test_twisted_consume_invalid_message(queue_and_binding):
    """Assert messages that fail validation are nacked."""
    queues, bindings = queue_and_binding
    bindings[0]["routing_keys"] = ["test_twisted_invalid_message"]

    consumers = yield api.twisted_consume(
        lambda x: pytest.fail("Message should be nacked"), bindings, queues
    )

    url = HTTP_API + "exchanges/%2F/amq.topic/publish"
    body = {
        "properties": {},
        "routing_key": "test_twisted_invalid_message",
        "payload": "not json",
        "payload_encoding": "string",
    }
    response = yield treq.post(url, json=body, auth=HTTP_AUTH, timeout=3)
    response = yield response.json()
    assert response["routed"] is True
    yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_update_callback(queue_and_binding):
    """Assert a second call to consume updates an existing callback."""
    queues, bindings = queue_and_binding

    callback1 = defer.Deferred()
    callback2 = defer.Deferred()

    consumers1 = yield api.twisted_consume(
        lambda m: reactor.callFromThread(callback1.callback, m), bindings, queues
    )
    yield threads.deferToThread(api.publish, message.Message(), "amq.topic")
    _add_timeout(callback1, 10)
    try:
        yield callback1
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Never received message for initial callback")

    consumers2 = yield api.twisted_consume(
        lambda m: reactor.callFromThread(callback2.callback, m), bindings, queues
    )
    yield threads.deferToThread(api.publish, message.Message(), "amq.topic")
    _add_timeout(callback2, 10)
    try:
        yield callback2
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Never received message for updated callback")

    assert consumers1[0]._tag == consumers2[0]._tag

    yield consumers2[0].cancel()


@pytest_twisted.inlineCallbacks
def test_publish_channel_error(queue_and_binding):
    """Assert publishing recovers from a channel error."""
    queues, bindings = queue_and_binding

    def counting_callback(message, storage=defaultdict(int)):
        storage[message.topic] += 1
        if storage[message.topic] == 2:
            raise exceptions.HaltConsumer()

    @pytest_twisted.inlineCallbacks
    def delayed_publish():
        """Publish, break the channel, and publish again."""
        yield threads.deferToThread(api.publish, message.Message(), "amq.topic")
        protocol = yield api._twisted_service._service.factory.when_connected()
        yield protocol._publish_channel.close()
        yield threads.deferToThread(api.publish, message.Message(), "amq.topic")

    reactor.callLater(5, delayed_publish)

    deferred_consume = threads.deferToThread(
        api.consume, counting_callback, bindings, queues
    )
    deferred_consume.addTimeout(30, reactor)
    try:
        yield deferred_consume
        pytest.fail("consume should have raised an exception")
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Consumer did not receive both messages")
    except exceptions.HaltConsumer as e:
        assert 0 == e.exit_code


@pytest_twisted.inlineCallbacks
def test_protocol_publish_connection_error(queue_and_binding):
    """Assert individual protocols raise connection exceptions if closed."""
    api._init_twisted_service()
    protocol = yield api._twisted_service._service.factory.when_connected()
    yield protocol.close()
    try:
        yield protocol.publish(message.Message(), "amq.topic")
        pytest.fail("Expected a ConnectionException")
    except exceptions.ConnectionException:
        pass


@pytest_twisted.inlineCallbacks
def test_pub_sub_default_settings(queue_and_binding):
    """
    Assert publishing and subscribing works with the default configuration.

    This should work because the publisher uses the 'amq.topic' exchange by
    default and the consumer also uses the 'amq.topic' exchange with its
    auto-named queue and a default subscription key of '#'.
    """
    queues, bindings = queue_and_binding

    # Consumer setup
    def counting_callback(message, storage=defaultdict(int)):
        storage[message.topic] += 1
        if storage[message.topic] == 3:
            raise exceptions.HaltConsumer()

    deferred_consume = threads.deferToThread(
        api.consume, counting_callback, bindings, queues
    )

    @pytest_twisted.inlineCallbacks
    def delayed_publish():
        """Give the consumer time to setup."""
        msg = message.Message(
            topic=u"nice.message",
            headers={u"niceness": u"very"},
            body={u"encouragement": u"You're doing great!"},
        )
        for _ in range(0, 3):
            try:
                yield threads.deferToThread(api.publish, msg, "amq.topic")
            except exceptions.ConnectionException:
                pytest.fail("Failed to publish message, is the broker running?")

    reactor.callLater(5, delayed_publish)
    deferred_consume.addTimeout(30, reactor)
    try:
        yield deferred_consume
        pytest.fail("consume should have raised an exception")
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    except exceptions.HaltConsumer as e:
        assert 0 == e.exit_code


@pytest_twisted.inlineCallbacks
def test_pub_timeout():
    """Assert PublishTimeout is raised if a connection just hangs."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    config.conf["amqp_url"] = "amqp://localhost:{port}/".format(
        port=sock.getsockname()[1]
    )

    try:
        yield threads.deferToThread(api.publish, api.Message(), 5)
    except Exception as e:
        if not isinstance(e, exceptions.PublishTimeout):
            pytest.fail("Expected a timeout exception, not {}".format(e))
    finally:
        sock.close()
    # Ensure the deferred has been renewed
    assert api._twisted_service._service.factory.when_connected().called is False


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
        {
            "queue": queue,
            "auto_delete": True,
            "durable": False,
            "exclusive": False,
            "arguments": {"x-expires": 60 * 1000},
        }
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
    yield serv.stopService()

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
        {
            "queue": queue,
            "auto_delete": True,
            "durable": False,
            "exclusive": False,
            "arguments": {"x-expires": 60 * 1000},
        }
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
    yield serv.stopService()


@pytest_twisted.inlineCallbacks
def test_nack_handled():
    """Assert raising Nack in a consumer works and messages are re-delivered"""
    queue = str(uuid.uuid4())
    queues = [
        {
            "queue": queue,
            "auto_delete": True,
            "durable": False,
            "exclusive": False,
            "arguments": {"x-expires": 60 * 1000},
        }
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
        {
            "queue": queue,
            "auto_delete": True,
            "durable": False,
            "exclusive": False,
            "arguments": {"x-expires": 60 * 1000},
        }
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
    yield serv.stopService()


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
    yield serv.stopService()


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
    yield serv.stopService()


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
    yield serv.stopService()
