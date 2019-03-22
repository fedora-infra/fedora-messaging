"""Test the Twisted consume APIs on a real broker running on localhost."""

import uuid
import six
import mock
import time

from twisted.internet import reactor, defer, task
import treq
import pkg_resources
import pytest
import pytest_twisted

from fedora_messaging import api, message, exceptions, config
from fedora_messaging.twisted.protocol import _add_timeout, _pika_version


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
def get_queue(name, delay=10):
    """
    Retrieve data about a queue in the broker in the default vhost ("/").

    Args:
        delay: Time to wait before querying the server. It can take some time for
            everything to settle and become available in the HTTP API. 10 seconds
            was arrived at after intermittent failures at lower values.

    Returns:
        dict: A dictionary representation of the queue.
    """
    # Assert both messages are delivered, no messages are un-acked, and only one
    # message got a positive acknowledgment.
    url = "{base}queues/%2F/{queue}".format(base=HTTP_API, queue=name)
    server_queue = yield task.deferLater(
        reactor, delay, treq.get, url, auth=HTTP_AUTH, timeout=3
    )
    server_queue = yield server_queue.json()
    defer.returnValue(server_queue)


@pytest_twisted.inlineCallbacks
def test_twisted_consume_halt_consumer():
    """
    Assert raising HaltConsumer works with :func:`fedora_messaging.api.twisted_consume`
    API.
    """
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
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
    server_queue = yield get_queue(queue)
    assert server_queue["consumers"] == 1

    for _ in range(0, 3):
        api.publish(msg, "amq.topic")

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
        server_queue = yield get_queue(queue)
        assert server_queue["consumers"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_stop_service():
    """
    Assert stopping the service, which happens when the reactor shuts down, waits
    for consumers to finish processing and then cancels them before closing the connection.
    """
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
    message_received, message_processed = defer.Deferred(), defer.Deferred()

    def callback(message):
        """Callback when the message is received, introduce a delay, then callback again."""
        reactor.callFromThread(message_received.callback, None)
        time.sleep(5)
        reactor.callFromThread(message_processed.callback, None)

    consumers = yield api.twisted_consume(callback, bindings, queues)
    api.publish(message.Message(), "amq.topic")

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
def test_twisted_consume_cancel():
    """Assert canceling works with :func:`fedora_messaging.api.twisted_consume` API."""
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]

    # Assert that the number of consumers we think we started is the number the
    # server things we started. This will fail if other tests don't clean up properly.
    # If it becomes problematic perhaps each test should have a vhost.
    consumers = yield api.twisted_consume(lambda m: m, bindings, queues)
    consumers[0].result.addErrback(pytest.fail)

    server_queue = yield get_queue(queue)
    assert server_queue["consumers"] == 1

    try:
        d = consumers[0].cancel()
        _add_timeout(d, 5)
        yield d

        # Assert the consumer.result deferred has called back when the cancel
        # deferred fires.
        assert consumers[0].result.called

        # Finally make sure the server agrees that the consumer is canceled.
        server_queue = yield get_queue(queue)
        assert server_queue["consumers"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_consume_halt_consumer_requeue():
    """Assert raising HaltConsumer with requeue=True re-queues the message."""
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
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
    api.publish(msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer as e:
        # Assert there are no consumers for the queue, and that there's a ready message
        assert e.exit_code == 1

        server_queue = yield get_queue(queue)
        assert server_queue["consumers"] == 0
        assert server_queue["messages_ready"] == 1
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_consume_drop_message():
    """Assert raising Drop causes the message to be dropped, but processing continues."""
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
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
    api.publish(msg, "amq.topic")
    api.publish(msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer:
        # Assert both messages are delivered, no messages are un-acked, and only one
        # message got a positive acknowledgment.
        server_queue = yield task.deferLater(
            reactor,
            5,
            treq.get,
            HTTP_API + "queues/%2F/" + queue,
            auth=HTTP_AUTH,
            timeout=3,
        )
        server_queue = yield server_queue.json()
        assert server_queue["consumers"] == 0
        assert server_queue["messages"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    finally:
        yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_nack_message():
    """Assert raising Nack causes the message to be replaced in the queue."""
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
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
    api.publish(msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer:
        # Assert the message was delivered, redelivered when Nacked, then acked by HaltConsumer
        server_queue = yield task.deferLater(
            reactor,
            5,
            treq.get,
            HTTP_API + "queues/%2F/" + queue,
            auth=HTTP_AUTH,
            timeout=3,
        )
        server_queue = yield server_queue.json()
        assert server_queue["consumers"] == 0
        assert server_queue["messages"] == 0
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    finally:
        yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_general_exception():
    """
    Assert if the callback raises an unhandled exception, it is passed on to the
    consumer.result and the message is re-queued.
    """
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
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
    api.publish(msg, "amq.topic")

    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
        pytest.fail("Expected an exception to be raised.")
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer halting!")
    except Exception as e:
        # Assert the message was delivered and re-queued when the consumer crashed.
        assert e.args[0] == "Oh the huge manatee"
        server_queue = yield task.deferLater(
            reactor,
            10,
            treq.get,
            HTTP_API + "queues/%2F/" + queue,
            auth=HTTP_AUTH,
            timeout=3,
        )
        server_queue = yield server_queue.json()
        assert server_queue["consumers"] == 0
        assert server_queue["messages"] == 1
    finally:
        yield consumers[0].cancel()


@pytest_twisted.inlineCallbacks
def test_twisted_consume_connection_reset():
    """
    Assert consuming works across connections and handles connection resets.

    This test sets up a queue, publishes 2 messages to itself, then kills all active
    connections on the broker. It then sends a third message and asserts the consumer
    gets it.
    """
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]
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
    api.publish(msg, "amq.topic")
    api.publish(msg, "amq.topic")
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
            if c["client_properties"]["app"] == "test_twisted_consume_connection_reset"
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
    api.publish(msg, "amq.topic")
    _add_timeout(consumers[0].result, 10)
    try:
        yield consumers[0].result
    except exceptions.HaltConsumer:
        assert len(messages_received) == 3
    except (defer.TimeoutError, defer.CancelledError):
        yield consumers[0].cancel()
        pytest.fail("Timeout reached without consumer halting!")


@pytest_twisted.inlineCallbacks
def test_twisted_consume_serverside_cancel():
    """
    Assert the consumer halts and ``consumer.result`` errbacks when the server
    explicitly cancels the consumer (by deleting the queue).
    """
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]

    consumers = yield api.twisted_consume(lambda x: x, bindings, queues)

    # Delete the queue and assert the consumer errbacks
    url = "{base}queues/%2F/{queue}".format(base=HTTP_API, queue=queue)
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
def test_no_vhost_permissions(admin_user):
    """Assert a hint is given if the user doesn't have any access to the vhost"""
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    resp = yield treq.delete(url, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}

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
@pytest.mark.skipif(
    _pika_version >= pkg_resources.parse_version("1.0.0b1"),
    reason="This is currently broken in pika 1.0.0b1 and b2",
)
def test_no_read_permissions_queue_read_failure(admin_user):
    """
    Assert an errback occurs when unable to read from the queue due to
    permissions. This is a bit weird because the consumer has permission to
    register itself, but not to actually read from the queue so the result is
    what errors back.
    """
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    body = {"configure": ".*", "write": ".*", "read": ""}
    resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}

    amqp_url = "amqp://{user}:guest@localhost:5672/%2F".format(user=admin_user)
    with mock.patch.dict(config.conf, {"amqp_url": amqp_url}):
        consumers = yield api.twisted_consume(lambda x: x, [], queues)
    _add_timeout(consumers[0].result, 5)
    try:
        yield consumers[0].result
        pytest.fail("Call failed to raise an exception")
    except exceptions.PermissionException as e:
        assert e.reason == (
            "ACCESS_REFUSED - access to queue '{}' in vhost '/' refused for user "
            "'{}'".format(queue, admin_user)
        )
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Timeout reached without consumer calling its errback!")


@pytest_twisted.inlineCallbacks
def test_no_read_permissions_bind_failure(admin_user):
    """Assert the call to twisted_consume errbacks on read permissions errors on binding."""
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    body = {"configure": ".*", "write": ".*", "read": ""}
    resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]

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
def test_no_write_permissions(admin_user):
    """Assert the call to twisted_consume errbacks on write permissions errors."""
    url = "{base}permissions/%2F/{user}".format(base=HTTP_API, user=admin_user)
    body = {"configure": ".*", "write": "", "read": ".*"}
    resp = yield treq.put(url, json=body, auth=HTTP_AUTH, timeout=3)
    assert resp.code == 204

    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]

    amqp_url = "amqp://{user}:guest@localhost:5672/%2F".format(user=admin_user)
    try:
        with mock.patch.dict(config.conf, {"amqp_url": amqp_url}):
            yield api.twisted_consume(lambda x: x, bindings, queues)
        pytest.fail("Call failed to raise an exception")
    except exceptions.BadDeclaration as e:
        assert e.reason.args[0] == 403
        assert e.reason.args[1] == (
            "ACCESS_REFUSED - access to queue '{}' in vhost '/' refused for user"
            " '{}'".format(queue, admin_user)
        )


@pytest_twisted.inlineCallbacks
def test_twisted_consume_invalid_message():
    """Assert messages that fail validation are nacked."""
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [
        {
            "queue": queue,
            "exchange": "amq.topic",
            "routing_keys": ["test_twisted_invalid_message"],
        }
    ]

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
def test_twisted_consume_update_callback():
    """Assert a second call to consume updates an existing callback."""
    queue = str(uuid.uuid4())
    queues = {queue: {"auto_delete": False, "arguments": {"x-expires": 60 * 1000}}}
    bindings = [{"queue": queue, "exchange": "amq.topic", "routing_keys": ["#"]}]

    callback1 = defer.Deferred()
    callback2 = defer.Deferred()

    consumers1 = yield api.twisted_consume(
        lambda m: reactor.callFromThread(callback1.callback, m), bindings, queues
    )
    api.publish(message.Message(), "amq.topic")
    _add_timeout(callback1, 10)
    try:
        yield callback1
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Never received message for initial callback")

    consumers2 = yield api.twisted_consume(
        lambda m: reactor.callFromThread(callback2.callback, m), bindings, queues
    )
    api.publish(message.Message(), "amq.topic")
    _add_timeout(callback2, 10)
    try:
        yield callback2
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Never received message for updated callback")

    assert consumers1[0]._tag == consumers2[0]._tag

    yield consumers2[0].cancel()
