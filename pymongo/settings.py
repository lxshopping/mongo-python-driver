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
from pymongo.cluster_description import ClusterType
from pymongo.server_description import ServerDescription


class ClusterSettings(object):
    def __init__(self, seeds=None, set_name=None):
        """Take a list of (host, port) pairs and optional replica set name."""
        self._seeds = seeds or [('localhost', 27017)]

        self._set_name = set_name
        self._direct = (len(self._seeds) == 1 and not set_name)

    @property
    def seeds(self):
        """List of server addresses."""
        return self._seeds

    @property
    def set_name(self):
        return self._set_name

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
