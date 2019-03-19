A new API, :func:`fedora_messaging.api.twisted_consume`, has been added to
support consuming using the popular async framework Twisted. The
fedora-messaging command-line interface has been switched to use this API. As
a result, Twisted 12.2+ is now a dependency of fedora-messsaging.
