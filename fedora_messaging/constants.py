
import pkg_resources

_fedora_version = pkg_resources.get_distribution('fedora_messaging').version
_pika_version = pkg_resources.get_distribution('pika').version

#: The default client properties reported to the AMQP broker in the "start-ok"
#: method of the connection negotiation. This allows the broker administrators
#: to easily identify what a connection is being used for and the client's
#: capabilities.
DEFAULT_CLIENT_PROPERTIES = {
    'app': 'Unknown',
    'product': 'Fedora Messaging with Pika',
    'information': 'https://fedora-messaging.readthedocs.io/en/stable/',
    'version': 'fedora_messaging-{} with pika-{}'.format(_fedora_version, _pika_version),
}
