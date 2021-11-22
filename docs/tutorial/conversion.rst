===============================
Converting a fedmsg application
===============================

.. highlight:: python


Converting publishers
=====================

Converting a Flask app
----------------------

.. elections, fedocal

Let's use the `elections`_ app as an example. Clone the code using the
following command::

    git clone https://pagure.io/elections.git

And change to this directory.

.. _elections: https://pagure.io/elections/

In the ``elections`` app, all calls to publish messages on fedmsg are going
through the ``fedora_elections.fedmsgshim.publish`` wrapper function. We can
thus modify this function to make it call Fedora Messaging instead of fedmsg.

JSON schema
~~~~~~~~~~~
First, you will need a Message schema. To write this schema you must know what
kind of messages are sent on the bus. A ``git grep`` command will reveal that
all calls are made from the ``admin.py`` file. Open that file and examine those
calls.

In parallel, copy the ``docs/sample_schema_package/`` directory from the
``fedora-messaging`` git clone to your app directory. Rename it to
``elections-messages``. Edit the ``setup.py`` file like you did before,
to change the package metadata (including the entry
point). Use ``fedora_elections_messages`` for the name. Rename the
``mailman_messages`` directory to ``fedora_elections_messages`` and adapt
the ``setup.py`` metadata.

Edit the ``messages.py`` file and write the basic structure for the elections
message schema. According to the different calls in ``admin.py``, it could be
something like::

    {
        'id': 'http://fedoraproject.org/message-schema/elections#',
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for Fedora Elections',
        'type': 'object',
        'properties': {
            'agent': {'type': 'string'},
            'election': {'type': 'object'},
            'candidate': {'type': 'object'},
        },
        'required': ['agent', 'election'],
    }

This could be sufficient, but it would be best to list what properties are
available in the ``election`` and ``candidate`` keys. Unfortunately, those are
just JSON dumps of the database model, so you'll have to look further to know
the structure.

Examining the ``to_json()`` methods in ``models.py`` shows which keys are
dumped to JSON. The schema could be written as::


    {
        'id': 'http://fedoraproject.org/message-schema/elections#',
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for Fedora Elections',
        'type': 'object',
        'properties': {
            'agent': {'type': 'string'},
            'election': {
                'type': 'object',
                'properties': {
                    'shortdesc': {'type': 'string'},
                    'alias': {'type': 'string'},
                    'description': {'type': 'string'},
                    'url': {'type': 'string', 'format': 'uri'},
                    'start_date': {'type': 'string'},
                    'end_date': {'type': 'string'},
                    'embargoed': {'type': 'number'},
                    'voting_type': {'type': 'string'},
                },
                'required': [
                    'shortdesc', 'alias', 'description', 'url',
                    'start_date', 'end_date', 'embargoed', 'voting_type',
                ],
            },
            'candidate': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'url': {'type': 'string', 'format': 'uri'},
                },
                'required': ['name', 'url'],
            },
        },
        'required': ['agent', 'election'],
    }

Use this schema and adapt the ``__str__()`` method and the ``summary`` property.

Since the schema is distributed in a separate python package, it must be added
to the ``election`` app's dependencies in ``requirements.txt``.

Wrapper function
~~~~~~~~~~~~~~~~
Now you can import this class in ``fedora_elections/fedmsgshim.py`` and use it
to encapsulate the messages. The wrapper could look like::

    import logging

    from fedora_elections_messages.schema import Message
    from fedora_messaging.api import publish as fm_publish
    from fedora_messaging.exceptions import PublishReturned, PublishForbidden, ConnectionException

    LOGGER = logging.getLogger(__name__)

    def publish(topic, msg):
        try:
            fm_publish(Message(
                topic="fedora.elections." + topic,
                body=msg,
            ))
        except (PublishReturned, PublishForbidden) as e:
            LOGGER.warning(
                "Fedora Messaging broker rejected message %s: %s",
                msg.id, e
            )
        except ConnectionException as e:
            LOGGER.warning("Error sending the message %s: %s", msg.id, e)


With this you'll get a couple of nice features over the previous state of
things:

- the message format is validated, so it's your responsability to update the
  schema when you decide to change the format, and not the receiver's
  responsability to handle any database schema changes you may make that may
  bleed into the message dictionary. And you'll know during development if you
  break compatibility.
- you may handle messaging errors in anyway you deem relevant. Here we're just
  logging them but you could choose to re-send the messages, store them for
  further analysis, etc.
- when there are no exceptions, you know that the message has reached the
  broker and has been distributed.

Testing
~~~~~~~
Let's start the election app and make sure messages are properly sent on the
bus. First, we'll create a virtualenv, and install election and
fedora-messaging with the following commands::

    virtualenv venv
    source ./venv/bin/activate
    pushd elections-message-schemas
    python setup.py develop
    popd
    pip install -r requirements.txt
    python setup.py develop

Make sure the Fedora Messaging configuration file is correct in
``/etc/fedora-messaging/config.toml``. We will add a queue binding to route
messages with the ``fedora.elections`` topic to the ``tutorial`` queue. Add
this entry in the ``bindings`` list::

    [[bindings]]
    queue = "tutorial"
    exchange = "amq.topic"
    routing_keys = ["fedora.elections.#"]

You could also add ``"fedora.elections.#"`` to the ``"routing_keys"`` value in
the existing entry.

Now make sure that RabbitMQ is still running, and run the ``consume.py`` script
:ref:`we used before <consume-script>`. Make sure it is not systematically
raising exceptions in the callback function (as we did before).

Now we'll run the election app, but first we need to create a configuration
file. Create a file called ``config.py`` with the following content::

    FEDORA_ELECTIONS_ADMIN_GROUP = ""

This will allow any Fedora account to be an admin on your instance, which is
good enough for this tutorial. Now start the app with::

    python createdb.py
    python runserver.py -c config.py

Open your browser to http://localhost:5000/admin/new. Login with FAS, then
create an election. Check the terminal where the ``consume.py`` script is
running. You should see the message that the ``elections`` app has sent on
election creation. Edit the election, and you should see the corresponding
message in the terminal where ``consume.py`` is running.


Converting a Pyramid app
------------------------

Let's use the `github2fedmsg`_ app as an example. It is a Pyramid webapp that
registers a webhook with Github on all subscribed projects, and then broadcasts
actions (commits, pull-request, tickets) received on this webhook to the
message bus.

.. _github2fedmsg: https://github.com/fedora-infra/github2fedmsg

Clone the code using the following command::

    git clone git@github.com:fedora-infra/github2fedmsg.git

And change to this directory.

JSON Schema
~~~~~~~~~~~
The only call to fedmsg is in ``github2fedmsg/views/webhooks.py``. Since the
app transmits the webhook payload almost transparently to the message bus, the
structure isn't obvious, so it's harder to define a schema. Fortunately, the
Github documentation has a `comprehensive list`_ of payload formats.

.. _comprehensive list: https://developer.github.com/v3/activity/events/types/

It would be to long to define precise JSON schemas for each event type, so
we'll just use the generic schema.

Sending the messages
~~~~~~~~~~~~~~~~~~~~
Now you can replace the current call to fedmsg with a call to
:py:func:`fedora_messaging.api.publish <pub-api>`. Add these lines in the
``github2fedmsg.views.webhook`` module::

    import logging
    from fedora_messaging.api import Message, publish
    from fedora_messaging.exceptions import PublishReturned, PublishForbidden, ConnectionException

    LOGGER = logging.getLogger(__name__)

And replace the call to ``fedmsg.publish`` with::

    try:
        msg = Message(
            topic="github." + event_type,
            body=payload,
        )
        publish(msg)
    except (PublishReturned, PublishForbidden) as e:
        LOGGER.warning(
            "Fedora Messaging broker rejected message %s: %s",
            msg.id, e
        )
    except ConnectionException as e:
        LOGGER.warning("Error sending message %s: %s", msg.id, e)

Testing it
~~~~~~~~~~
Make sure the Fedora Messaging configuration file is correct in
``/etc/fedora-messaging/config.toml``. We will add a queue binding to route
messages with the ``github`` topic to the ``tutorial`` queue. Add
this entry in the ``bindings`` list::

    [[bindings]]
    queue = "tutorial"
    exchange = "amq.topic"
    routing_keys = ["github.#"]

You could also add ``"github.#"`` to the ``"routing_keys"`` value in the
existing entry.

Now make sure that RabbitMQ is still running, and run the ``consume.py`` script
:ref:`we used before <consume-script>`. Make sure it is not systematically
raising exceptions in the callback function (as we did before).

To setup the ``github2fedmsg`` application, follow the ``README.rst`` file::

    virtualenv venv
    source ./venv/bin/activate
    python setup.py develop
    pip install waitress

Go off and `register your development application with GitHub
<https://github.com/settings/applications>`_.  Save the oauth tokens and add
the secret one to a new file you create called ``secret.ini``.  Use the example
``secret.ini.example`` file.

Create the database and start the application::

  initialize_github2fedmsg_db development.ini
  pserve development.ini --reload



Converting consumers
====================

.. the-new-hotness

Let's use `the-new-hotness`_ app as an example. Clone the code and switch to state
before conversion by using the following commands::

    git clone https://github.com/fedora-infra/the-new-hotness.git
    git checkout 0.10.1

And change to this directory.

.. _the-new-hotness: https://github.com/fedora-infra/the-new-hotness

In ``the-new-hotness`` app, all calls to consume messages on fedmsg are going
through the ``hotness.consumers.BugzillaTicketFiler.consume`` method. We can
thus modify this function to make it use Fedora Messaging instead of fedmsg.

Configuration
-------------
First we need to convert configuration file from fedmsg format to Fedora
Messaging. Unlike fedmsg, fedora-messaging does not allow for arbitrary
configuration keys.

The converted configuration ``config.toml`` could look like following::

    # Define the callback function
    # This will allow you to call only ``fedora-messaging consume`` without explicitly
    # specifying the callback every time you starting ``the-new-hotness``.
    callback = "hotness.consumers:BugzillaTicketFiler"

    # In case of the-new-hotness we are listening to three topics, so we
    # create a new binding for them
    [[bindings]]
    queue = "the-new-hotness"
    exchange = "amq.topic"
    routing_keys = [
       "org.release-monitoring.prod.anitya.project.version.update",
       "org.release-monitoring.prod.anitya.project.map.new",
       "org.fedoraproject.prod.buildsys.task.state.change",
    ]

    # Define a queue
    [queues.the-new-hotness]
    durable = true
    auto_delete = false
    exclusive = false
    arguments = {}

Any application specific configuration should go to ``consumer_config`` section
:ref:`config`.

Init method
-----------
The ``BugzillaTicketFiler`` class in ``consumers.py`` is doing all the consuming work.
First we need to change the inheritance of this class.

Then we need to modify the ``__init__`` method and use the ``fedora_messaging.config.conf``
dictionary instead of the fedmsg configuration. The ``__init__`` method could look something
like this after the change::

    from fedora_messaging.config import conf

    class BugzillaTicketFiler:
    """
    A fedora-messaging consumer that is the heart of the-new-hotness.

    This consumer subscribes to the following topics:

    * 'org.fedoraproject.prod.buildsys.task.state.change'
      handled by :method:`BugzillaTicketFiler.handle_buildsys_scratch`

    * 'org.release-monitoring.prod.anitya.project.version.update'
      handled by :method:`BugzillaTicketFiler.handle_anitya_version_update`

    * 'org.release-monitoring.prod.anitya.project.map.new'
      handled by :method:`BugzillaTicketFiler.handle_anitya_map_new`
    """

    def __init__(self):

        # This is just convenient.
        self.config = conf["consumer_config"]

        ...

.. note::
    Unrelated code was deleted from the example.

Wrapper function
----------------
The next step is to change ``consume`` method to ``__call__`` method. This is pretty
straightforward. After this modification ``__call__`` method should look like this::

    def __call__(self, msg):
        """
        Called when a message is received from queue.

        Params:
            msg (fedora_messaging.message.Message) The message we received
                from the queue.
        """
        topic, body, msg_id = msg.topic, msg.body, msg.id
        _log.debug("Received %r" % msg_id)

        if topic.endswith("anitya.project.version.update"):
            self.handle_anitya_version_update(msg)
        elif topic.endswith("anitya.project.map.new"):
            self.handle_anitya_map_new(msg)
        elif topic.endswith("buildsys.task.state.change"):
            self.handle_buildsys_scratch(msg)
        else:
            _log.debug("Dropping %r %r" % (topic, body))
            pass

In this case we are working with the message using the standard
``fedora_messaging.message.Message`` methods. It is always better to use schema specific
methods for any topic you are receiving.

Testing
-------
To prepare ``the-new-hotness`` for testing checkout the ``requirements.txt`` file and ``devel``
folder from ``master`` branch::

    git checkout master devel requirements.txt

This will convert development environment to the state when it's ready for Fedora Messaging.
In a `configured development environment
<https://the-new-hotness.readthedocs.io/en/latest/dev-guide.html#development-environment>`_
we can easily test our conversion.

Start app by using alias ``hotstart``, this will start the systemd
service with following command ``fedora-messaging consume``. The systemd unit could be find in
``.config/systemd/user/``.

For testing you can use any message from `datagrepper <https://apps.fedoraproject.org/datagrepper>`_.
Just add ``/raw?category=<application name>&delta=259200`` to URL and pick any message.
For example category for ``Anitya`` is ``anitya``.

To send the message you need simple publisher. One is created for the new hotness in
``devel/fedora_messaging_replay.py``. To send the message you can use any message id
found in `datagrepper <https://apps.fedoraproject.org/datagrepper>`_::

    python3 devel/fedora_messaging_replay.py <msg_id>

And now you can check if the message was received using ``hotlog`` alias, which shows
the journal log for ``the-new-hotness``.
