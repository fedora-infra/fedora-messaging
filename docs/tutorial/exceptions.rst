Handling exceptions
===================

.. highlight:: python

All exceptions are located in the :py:mod:`fedora_messaging.exceptions` module.

When publishing
---------------

When calling :py:func:`fedora_messaging.api.publish`, the following
exceptions can be raised:

- ``ValidationError``: raised if the message fails validation with
  its JSON schema. This only depends on the message you are trying to
  send, the AMQP server is not involved.
- ``PublishReturned``: raised if the broker rejects the message.
- ``ConnectionException``: raised if a connection error occurred before the
  publish confirmation arrived.

The ``ValidationError`` exception means you should fix either the schema (and
maybe make a new version) or the message. No need to catch it, this should
crash your app during development and testing.

Your app may handle the other two exceptions in whichever way is relevant. It
should involve logging, and sending again or discarding may be valid options.

You already noticed the ``ValidationError`` being raised when you tried sending
an invalid message in the previous chapter.


When consuming
--------------

Invalid messages according to the JSON schema are automatically rejected by the
client.

The callback function can raise the following exceptions:

- ``Nack``: raise this to return the message to the queue
- ``Drop``: raise this to drop the message
- ``HaltConsumer``: raise this to shutdown the consumer and return the message
  to the queue.

Any other exception will bubble up in the consumer as a ``HaltConsumer``
exception, shutdown the consumer, and return pending messages to the queue.
Your app will have to handle the ``HaltConsumer`` exception.

Modify the callback function to raise those exceptions and see what happens.

When returning ``Nack`` systematically, the consumer will just loop on that one
message, as it is put back in the queue and delivered again forever.

Notice how raising ``HaltConsumer`` or another exception stops the consumer,
but does not consume the message: it will be re-delivered on the next startup.
