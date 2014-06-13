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
from pymongo.ismaster import IsMaster, ServerType
from pymongo.read_preferences import MovingAverage
from pymongo.server_description import ServerDescription


def call_ismaster(sock_info):
    """Get an IsMaster, or raise socket.error or PyMongoError."""
    # TODO: could cache an ismaster message globally.
    request_id, msg, _ = message.query(0, 'admin.$cmd', 0, -1, {'ismaster': 1})
    sock_info.send_message(msg)
    raw_response = sock_info.receive_message(1, request_id)
    result = helpers._unpack_response(raw_response)
    return IsMaster(result['data'][0])


class Monitor(threading.Thread):
    def __init__(
            self,
            server_description,
            cluster,
            pool,
            call_ismaster_fn=call_ismaster):
        """Pass an initial ServerDescription, a Cluster, and a Pool.

        Optionally override call_ismaster with a function that takes a
        SocketInfo and returns an IsMaster.

        The Cluster is weakly referenced. The Pool must be exclusive to this
        Monitor.
        """
        super(Monitor, self).__init__()
        self.daemon = True  # Python 2.6's way to do setDaemon(True).
        self._server_description = server_description
        self._cluster = weakref.proxy(cluster)
        self._pool = pool
        self._call_ismaster_fn = call_ismaster_fn
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
            try:
                self._server_description = call_ismaster_with_retry(
                    self._server_description,
                    self._cluster,
                    self._pool,
                    self._call_ismaster_fn)

                self._cluster.on_change(self._server_description)
            except weakref.ReferenceError:
                # Cluster was garbage collected.
                self.close()
            else:
                # TODO: heartbeatFrequencyMS.
                with self._lock:
                    self._condition.wait(5)


def call_ismaster_with_retry(
        server_description,
        cluster,
        pool,
        call_ismaster_fn):
    # According to the spec, if an ismaster call fails we reset the
    # server's pool. If a server was once connected, change its type
    # to Unknown only after retrying once.
    retry = server_description.server_type != ServerType.Unknown
    address = server_description.address
    server_description = call_ismaster_once(address, pool, call_ismaster_fn)
    if server_description:
        return server_description
    else:
        cluster.reset_pool(address)
        if retry:
            server_description = call_ismaster_once(
                address, pool, call_ismaster_fn)

            if server_description:
                return server_description

    # ServerType defaults to Unknown.
    return ServerDescription(address)


def call_ismaster_once(address, pool, call_ismaster_fn):
    try:
        sock_info = pool.get_socket()
    except socket.error:
        return None

    try:
        # TODO: monotonic time.
        start = time.time()
        ismaster_response = call_ismaster_fn(sock_info)
        round_trip_time = time.time() - start

        # TODO: average RTTs.
        sd = ServerDescription(
            address, ismaster_response, MovingAverage([round_trip_time]))

        return sd
    except socket.error:
        sock_info.close()
        return None
    except Exception:
        # TODO: This is unexpected. Log.
        return None
    finally:
        pool.maybe_return_socket(sock_info)


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
