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

"""Class to monitor a MongoDB server on a background thread."""

import atexit
import socket
import threading
import weakref
import time

from pymongo import helpers, message
from pymongo.errors import OperationFailure
from pymongo.server_description import (parse_ismaster_response,
                                        ServerDescription)


class Monitor(threading.Thread):
    def __init__(self, address, cluster, pool):
        """Pass a (host, port) pair, a Cluster, and a Pool.

        The Cluster is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        super(Monitor, self).__init__()
        self.daemon = True  # Python 2.6's way to do setDaemon(True).
        self._address = address
        self._cluster = weakref.proxy(cluster)
        self._pool = pool
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stopped = False

    def open(self):
        self.start()

    def close(self):
        self._stopped = True
        self._pool.reset()

        # Awake the thread so it notices that _stopped is True.
        self.request_check()

    def request_check(self):
        with self._lock:
            self._condition.notify()

    def run(self):
        # TODO: minHeartbeatFrequencyMS.
        while not self._stopped:
            sock_info = None
            try:
                try:
                    # TODO: could cache an ismaster message globally.
                    request_id, msg, _ = message.query(
                        0, 'admin.$cmd', 0, -1, {'ismaster': 1})

                    sock_info = self._pool.get_socket()

                    # TODO: monotonic time.
                    start = time.time()
                    sock_info.send_message(msg)
                    raw_response = sock_info.receive_message(
                        1, request_id)

                    round_trip_time = time.time() - start
                    result = helpers._unpack_response(raw_response)
                    response = result['data'][0]

                    # TODO: average RTTs.
                    sd = parse_ismaster_response(
                        self._address,
                        response,
                        round_trip_time)

                    self._cluster.on_change(sd)
                except (socket.error, OperationFailure):
                    # TODO: try once more if ServerType isn't Unknown.
                    self._pool.maybe_return_socket(sock_info)

                    # Set ServerType to Unknown.
                    sd = ServerDescription(self._address)
                    self._cluster.on_change(sd)

            except weakref.ReferenceError:
                # Cluster was garbage collected.
                self.close()

            else:
                # TODO: heartbeatFrequencyMS.
                with self._lock:
                    self._condition.wait(5)


MONITORS = set()


def register_monitor(monitor):
    ref = weakref.ref(monitor, _on_monitor_deleted)
    MONITORS.add(ref)


def _on_monitor_deleted(ref):
    MONITORS.remove(ref)


def shutdown_monitors():
    # Keep a local copy of MONITORS as
    # shutting down threads has a side effect
    # of removing them from the MONITORS set()
    monitors = list(MONITORS)
    for ref in monitors:
        monitor = ref()
        if monitor:
            monitor.close()
            monitor.join()

atexit.register(shutdown_monitors)

