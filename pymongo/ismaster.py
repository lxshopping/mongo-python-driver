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


class IsMasterResponse(object):
    def __init__(self, doc):
        """Parse an ismaster response from the server."""
        self.server_type = get_server_type(doc)
        self.ok = doc.get('ok')
        self.all_hosts = map(common.partition_node, itertools.chain(
            doc.get('hosts', []),
            doc.get('passives', []),
            doc.get('arbiters', [])))

        if doc.get('primary'):
            self.primary = common.partition_node(doc['primary'])
        else:
            self.primary = None

        self.tags = doc.get('tags')
        self.set_name = doc.get('setName')
        self.max_bson_size = doc.get('maxBsonObjectSize')
        self.max_message_size = doc.get('maxMessageSizeBytes')
        self.max_write_batch_size = doc.get('maxWriteBatchSize')
        self.min_wire_version = doc.get('minWireVersion')
        self.max_wire_version = doc.get('maxWireVersion')
