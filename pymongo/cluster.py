# Copyright 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Internal classes to monitor clusters of one or more servers."""

import threading
import time

from pymongo.cluster_description import (update_cluster_description,
                                         ClusterType)
from pymongo.errors import InvalidOperation, ConnectionFailure
from pymongo.server import Server


class Cluster(object):
    """Monitor a cluster of one or more servers."""
    def __init__(
            self,
            cluster_description,
            pool_class,
            monitor_class,
            condition_class):
        self._cluster_description = cluster_description
        self._pool_class = pool_class
        self._monitor_class = monitor_class
        self._condition_class = condition_class

        self._opened = False
        self._lock = threading.Lock()
        self._condition = self._condition_class(self._lock)
        self._servers = {}

    def open(self):
        """Start monitoring."""
        with self._lock:
            if self._opened:
                raise InvalidOperation('Cluster already opened')

            self._opened = True
            self._update_servers()

    def select_servers(self, selector, server_wait_time=5):
        """Return all Servers matching selector, or time out.

        Raises AutoReconnect after maxWaitTime with no matching servers.
        """
        with self._lock:
            self._cluster_description.check_compatible()

            # TODO: use settings.server_wait_time.
            # TODO: use monotonic time if available.
            now = time.time()
            end_time = now + server_wait_time
            server_descriptions = self._apply_selector(selector)

            while True:
                if server_descriptions:
                    return [self.get_server_by_address(sd.address)
                            for sd in server_descriptions]

                # No suitable servers.
                if now > end_time:
                    # TODO: more error diagnostics. E.g., if state is
                    # ReplicaSet but every server is Unknown, and the host list
                    # is non-empty, and doesn't intersect with settings.seeds,
                    # the set is probably configured with internal hostnames or
                    # IPs and we're connecting from outside. Or if state is
                    # ReplicaSet and clusterDescription.server_descriptions is
                    # empty, we have the wrong set_name. Include
                    # ClusterDescription's stringification in exception msg.
                    raise ConnectionFailure("No suitable servers available")

                self._request_check_all()

                timeout = end_time - now

                # Release the lock and wait for the cluster description to
                # change, or for a timeout. We won't miss any changes that
                # came after our most recent selector() call, since we've
                # held the lock until now.
                self._condition.wait(timeout)
                now = time.time()
                server_descriptions = self._apply_selector(selector)

    def on_change(self, server_description):
        """Process a new ServerDescription after an ismaster call completes."""
        # We do no I/O holding the lock.
        with self._lock:
            if not self._cluster_description.has_server(
                    server_description.address):
                # The server was once in the cluster description, otherwise
                # we wouldn't have been monitoring it, but an intervening
                # state-change removed it. E.g., we got a host list from
                # the primary that didn't include this server.
                return

            cd = update_cluster_description(
                self._cluster_description, server_description)

            self._cluster_description = cd
            self._update_servers()

            # Wake waiters in select_servers().
            self._condition.notify_all()

    def get_server_by_address(self, address):
        """Get a Server or None."""
        return self._servers.get(address)

    def has_server(self, address):
        return address in self._servers

    def close(self):
        raise NotImplementedError()

    @property
    def description(self):
        return self._cluster_description

    def _request_check_all(self):
        """Wake all monitors. Hold the lock when calling this."""
        for s in self._servers.values():
            s.request_check()

    def _apply_selector(self, selector):
        if self._cluster_description.cluster_type == ClusterType.Single:
            # Ignore the selector.
            return self._cluster_description.known_servers
        else:
            return selector(self._cluster_description.known_servers)

    def _update_servers(self):
        """Sync our set of Servers from ClusterDescription.server_descriptions.

        Hold the lock while calling this.
        """
        for address, sd in self._cluster_description.server_descriptions:
            if address not in self._servers:
                m = self._monitor_class(
                    address,
                    self,
                    self._create_pool(address),
                    self._condition_class)

                s = Server(sd, self._create_pool(address), m)
                self._servers[address] = s
                s.open()
            else:
                self._servers[address].description = sd

        for address, server in list(self._servers.items()):
            if not self._cluster_description.has_server(address):
                server.close()
                self._servers.pop(address)

    def _create_pool(self, address):
        # TODO: Need PoolSettings, SocketSettings, and SSLContext classes.
        return self._pool_class(
            address,
            max_size=100,
            net_timeout=None,
            conn_timeout=20,
            use_ssl=False,
            use_greenlets=False)
