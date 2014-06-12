# Copyright 2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Parse a response to the 'ismaster' command."""

import itertools

from pymongo import common


class ServerType:
    class Unknown: pass
    class Mongos: pass
    class RSPrimary: pass
    class RSSecondary: pass
    class RSArbiter: pass
    class RSOther: pass
    class RSGhost: pass
    class Standalone: pass


def get_server_type(doc):
    """Determine the ServerType from an ismaster response."""
    if not doc.get('ok'):
        return ServerType.Unknown

    if doc.get('isreplicaset'):
        return ServerType.RSGhost
    elif doc.get('setName'):
        if doc.get('hidden'):
            return ServerType.RSOther
        elif doc.get('ismaster'):
            return ServerType.RSPrimary
        elif doc.get('secondary'):
            return ServerType.RSSecondary
        elif doc.get('arbiterOnly'):
            return ServerType.RSArbiter
        else:
            return ServerType.RSOther
    elif doc.get('msg') == 'isdbgrid':
        return ServerType.Mongos
    else:
        return ServerType.Standalone


class IsMaster(object):
    __slots__ = ('_doc', '_server_type')

    def __init__(self, doc):
        """Parse an ismaster response from the server."""
        self._server_type = get_server_type(doc)
        self._doc = doc

    @property
    def server_type(self):
        return self._server_type

    @property
    def all_hosts(self):
        """List of hosts, passives, and arbiters known to this server."""
        return map(common.partition_node, itertools.chain(
            self._doc.get('hosts', []),
            self._doc.get('passives', []),
            self._doc.get('arbiters', [])))

    @property
    def tags(self):
        """Replica set member tags or empty dict."""
        return self._doc.get('tags', {})

    @property
    def primary(self):
        """This server's opinion about who the primary is, or None."""
        if self._doc.get('primary'):
            return common.partition_node(self._doc['primary'])
        else:
            return None

    @property
    def set_name(self):
        """Replica set name or None."""
        return self._doc.get('setName')

    @property
    def max_bson_size(self):
        return self._doc.get('maxBsonObjectSize', common.MAX_BSON_SIZE)

    @property
    def max_message_size(self):
        return self._doc.get('maxMessageSizeBytes', 2 * self.max_bson_size)

    @property
    def max_write_batch_size(self):
        return self._doc.get('maxWriteBatchSize', common.MAX_WRITE_BATCH_SIZE)

    @property
    def min_wire_version(self):
        return self._doc.get('minWireVersion', common.MIN_WIRE_VERSION)

    @property
    def max_wire_version(self):
        return self._doc.get('maxWireVersion', common.MAX_WIRE_VERSION)
