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

from pymongo import common
from pymongo.ismaster import IsMasterResponse, ServerType


class ServerDescription(object):
    """Immutable representation of one server.

    :Parameters:
      - `address`: A (host, port) pair
      - `round_trip_times`: Optional MovingAverage
      - `ismaster_response`: Optional IsMasterResponse
    """
    def __init__(
            self,
            address,
            ismaster_response=None,
            round_trip_times=None):

        self._address = address
        if ismaster_response:
            self._ismaster_response = ismaster_response
        else:
            self._ismaster_response = IsMasterResponse({})

        self._round_trip_times = round_trip_times
        self._is_writable = self.server_type in (
            ServerType.RSPrimary,
            ServerType.Standalone,
            ServerType.Mongos)

        self._is_readable = (
            self.server_type == ServerType.RSSecondary
            or self._is_writable)

    @property
    def address(self):
        return self._address

    @property
    def server_type(self):
        return self._ismaster_response.server_type

    @property
    def round_trip_times(self):
        """A MovingAverage or None."""
        return self._round_trip_times

    @property
    def round_trip_time(self):
        """The current average duration."""
        return self._round_trip_times.get()

    @property
    def all_hosts(self):
        """Hosts, passives, and arbiters known to this server."""
        return self._ismaster_response.all_hosts

    @property
    def tags(self):
        return self._ismaster_response.tags

    @property
    def set_name(self):
        return self._ismaster_response.set_name

    @property
    def primary(self):
        """This server's opinion of who the primary is, if any."""
        return self._ismaster_response.primary

    @property
    def max_bson_size(self):
        return self._ismaster_response.max_bson_size or common.MAX_BSON_SIZE

    @property
    def max_message_size(self):
        return (self._ismaster_response.max_message_size
                or 2 * self.max_bson_size)

    @property
    def max_write_batch_size(self):
        return (self._ismaster_response.max_write_batch_size
                or common.MAX_WRITE_BATCH_SIZE)

    @property
    def min_wire_version(self):
        return (self._ismaster_response.min_wire_version
                or common.MIN_WIRE_VERSION)

    @property
    def max_wire_version(self):
        return (self._ismaster_response.max_wire_version
                or common.MAX_WIRE_VERSION)

    @property
    def is_writable(self):
        return self._is_writable

    @property
    def is_readable(self):
        return self._is_readable

    @property
    def is_server_type_known(self):
        return self.server_type != ServerType.Unknown
