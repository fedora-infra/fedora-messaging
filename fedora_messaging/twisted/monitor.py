# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
A Twisted HTTP service to monitor a Fedora Messaging Service.

This module provides a HTTP service that can be used to implement health checks in OpenShift, as
described here: https://docs.openshift.com/container-platform/4.16/applications/application-health.html

The webserver will listen on the port set in the configuration file, and provides two endpoints that
return JSON data:
- `/live` to check when the program is up
- `/ready` to check when the consumer is connected, and get the statistics
"""

import abc
import json
import typing

from twisted.application.internet import TCPServer
from twisted.web import resource, server


if typing.TYPE_CHECKING:
    from .service import FedoraMessagingServiceV2


class FMServiceResource(resource.Resource, metaclass=abc.ABCMeta):
    """An abstract class for service-monitoring endpoints."""

    def __init__(self, *args, **kwargs):
        self._fm_service = kwargs.pop("fm_service")
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def _get_response(self) -> dict:
        """Return the response as a dictionary."""
        raise NotImplementedError

    def render_GET(self, request):
        request.setHeader("Content-Type", "application/json ")
        return json.dumps(self._get_response()).encode("utf-8") + b"\n"


class Live(FMServiceResource):
    """The `/live` endpoint, returns JSON"""

    isLeaf = True

    def _get_response(self):
        return {"status": "OK"}


class Ready(FMServiceResource):
    """The `/ready` endpoint

    Returns the consumer state and some statistics about messages consumed and produced in
    JSON format.
    """

    isLeaf = True

    def _get_response(self):
        response = {"consuming": self._fm_service.consuming}
        response.update(self._fm_service.stats.as_dict())
        return response


class MonitoringSite(server.Site):
    """A subclass of Twisted's site to redefine its name in the logs."""

    def logPrefix(self):
        return "Monitoring HTTP server"


def monitor_service(
    fm_service: "FedoraMessagingServiceV2", *, address: str, port: int
) -> TCPServer:
    """Add the Twisted service for HTTP-based monitoring to the provided Fedora Messaging Service.

    Args:
        fm_service: the service to monitor
        address: the IP address to listen on
        port: the TCP port to listen on

    Returns:
        The monitoring service
    """
    root = resource.Resource()
    root.putChild(b"live", Live(fm_service=fm_service))
    root.putChild(b"ready", Ready(fm_service=fm_service))
    site = MonitoringSite(root)
    monitor_service = TCPServer(port, site, interface=address)
    monitor_service.setName("monitoring")
    monitor_service.setServiceParent(fm_service)
    return monitor_service
