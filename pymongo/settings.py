# Copyright 2014 MongoDB, Inc.
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

"""Represent MongoClient's configuration."""

import threading

from pymongo import common, monitor, pool
from pymongo.cluster_description import ClusterType
from pymongo.server_description import ServerDescription


class ClusterSettings(object):
    def __init__(
        self,
        seeds=None,
        set_name=None,
        heartbeatFrequencyMS=None,
        pool_class=None,
        monitor_class=monitor.Monitor,
        condition_class=threading.Condition
    ):
        """Represent MongoClient's configuration.

        Take a list of (host, port) pairs, optional replica set name,
        optional frequency in seconds for calling ismaster on servers.
        """
        self._seeds = seeds or [('localhost', 27017)]
        self._set_name = set_name

        # Convert from milliseconds to seconds.
        f = common.validate_timeout_or_none(
            'heartbeatFrequencyMS', heartbeatFrequencyMS)

        self._frequency = f or common.HEARTBEAT_FREQUENCY

        self._pool_class = pool_class or pool.Pool
        self._monitor_class = monitor_class or monitor.Monitor
        self._condition_class = condition_class or threading.Condition
        self._direct = (len(self._seeds) == 1 and not set_name)

    @property
    def seeds(self):
        """List of server addresses."""
        return self._seeds

    @property
    def set_name(self):
        return self._set_name

    @property
    def heartbeat_frequency(self):
        """How often to call ismaster on servers, in seconds."""
        return self._frequency

    @property
    def pool_class(self):
        return self._pool_class

    @property
    def monitor_class(self):
        return self._monitor_class

    @property
    def condition_class(self):
        return self._condition_class

    @property
    def direct(self):
        """Connect directly to a single server, or use a set of servers?

        True if there is one seed and no set_name.
        """
        return self._direct

    def get_cluster_type(self):
        if self.direct:
            return ClusterType.Single
        elif self.set_name is not None:
            return ClusterType.ReplicaSetNoPrimary
        else:
            return ClusterType.Unknown

    def get_server_descriptions(self):
        return [ServerDescription(address) for address in self.seeds]


class SocketSettings(object):
    # TODO.
    pass
