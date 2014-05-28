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

"""Represent one server in the cluster."""

import itertools

from pymongo import common
from pymongo.read_preferences import MovingAverage


class ServerType:
    class Unknown: pass
    class Mongos: pass
    class RSPrimary: pass
    class RSSecondary: pass
    class RSArbiter: pass
    class RSOther: pass
    class RSGhost: pass
    class Standalone: pass


def get_server_type(ismaster_response):
    if not ismaster_response.get('ok'):
        return ServerType.Unknown
    
    if ismaster_response.get('isreplicaset'):
        return ServerType.RSGhost
    elif ismaster_response.get('setName'):
        if ismaster_response.get('hidden'):
            return ServerType.RSOther
        elif ismaster_response.get('ismaster'):
            return ServerType.RSPrimary
        elif ismaster_response.get('secondary'):
            return ServerType.RSSecondary
        elif ismaster_response.get('arbiterOnly'):
            return ServerType.RSArbiter
        else:
            return ServerType.RSOther
    elif ismaster_response.get('msg') == 'isdbgrid':
        return ServerType.Mongos
    else:
        return ServerType.Standalone


def parse_ismaster_response(
        address,
        response,
        round_trip_time,
        round_trip_times=None):

    server_type = get_server_type(response)
    max_bson_size = response.get('maxBsonObjectSize', common.MAX_BSON_SIZE),
    all_hosts = map(common.partition_node, itertools.chain(
        response.get('hosts', []),
        response.get('passives', []),
        response.get('arbiters', [])))

    if round_trip_times is not None:
        rtt_avg = round_trip_times.clone_with(round_trip_time)
    else:
        rtt_avg = MovingAverage([round_trip_time])

    if response.get('primary'):
        primary = common.partition_node(response['primary'])
    else:
        primary = None

    return ServerDescription(
        address,
        server_type=server_type,
        round_trip_times=rtt_avg,
        all_hosts=all_hosts,
        tags=response.get('tags', {}),
        set_name=response.get('setName'),
        primary=primary,
        max_bson_size=max_bson_size,
        max_message_size=response.get(
            'maxMessageSizeBytes', 2 * max_bson_size),
        max_write_batch_size=response.get(
            'maxWriteBatchSize', common.MAX_WRITE_BATCH_SIZE),
        min_wire_version=response.get(
            'minWireVersion', common.MIN_WIRE_VERSION),
        max_wire_version=response.get(
            'maxWireVersion', common.MAX_WIRE_VERSION))


class ServerDescription(object):
    """Immutable representation of one server.

    :Parameters:
      - `address`: A (host, port) pair
      - `server_type`: Optional ServerType
      - `round_trip_time`: Optional MovingAverage
      - `ismaster_response`: Optional dict, MongoDB's ismaster response
    """
    def __init__(
            self,
            address,
            server_type=ServerType.Unknown,
            round_trip_times=None,
            all_hosts=None,
            tags=None,
            set_name=None,
            primary=None,
            max_bson_size=None,
            max_message_size=None,
            max_write_batch_size=None,
            min_wire_version=None,
            max_wire_version=None):

        self._address = address
        self._server_type = server_type
        self._round_trip_times = round_trip_times
        self._all_hosts = all_hosts
        self._tags = tags
        self._set_name = set_name
        self._primary = primary
        self._max_bson_size = max_bson_size
        self._max_message_size = max_message_size
        self._max_write_batch_size = max_write_batch_size
        self._min_wire_version = min_wire_version
        self._max_wire_version = max_wire_version

        self._is_writable = self._server_type in (
            ServerType.RSPrimary,
            ServerType.Standalone,
            ServerType.Mongos)

        self._is_readable = (self._server_type == ServerType.RSSecondary
                             or self._is_writable)

    @property
    def address(self):
        return self._address

    @property
    def server_type(self):
        return self._server_type

    @property
    def round_trip_times(self):
        return self._round_trip_times

    @property
    def all_hosts(self):
        """Hosts, passives, and arbiters known to this server."""
        return self._all_hosts

    @property
    def tags(self):
        return self._tags

    @property
    def set_name(self):
        return self._set_name

    @property
    def primary(self):
        """This server's opinion of who the primary is, if any."""
        return self._primary

    @property
    def max_bson_size(self):
        return self._max_bson_size

    @property
    def max_message_size(self):
        return self._max_message_size

    @property
    def max_write_batch_size(self):
        return self._max_write_batch_size

    @property
    def min_wire_version(self):
        return self._min_wire_version

    @property
    def max_wire_version(self):
        return self._max_wire_version

    @property
    def round_trip_times(self):
        """A MovingAverage."""
        return self._round_trip_times

    @property
    def round_trip_time(self):
        """The current average duration."""
        return self._round_trip_times.get()

    @property
    def is_writable(self):
        return self._is_writable

    @property
    def is_readable(self):
        return self._is_readable

    @property
    def is_server_type_known(self):
        return self._server_type != ServerType.Unknown
