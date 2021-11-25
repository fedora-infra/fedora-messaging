from twisted.internet import reactor, task


def sleep(delay):
    # Returns a deferred that calls do-nothing function
    # after `delay` seconds
    return task.deferLater(reactor, delay, lambda: None)
