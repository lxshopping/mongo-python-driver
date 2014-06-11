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

"""Represent the cluster of servers."""

from pymongo import common
from pymongo.errors import ConfigurationError
from pymongo.server_description import ServerDescription, ServerType


class ClusterType:
    class Single: pass

    class ReplicaSetNoPrimary: pass

    class ReplicaSetWithPrimary: pass

    class Sharded: pass

    class Unknown: pass


class ClusterDescription(object):
    """Represent a cluster of servers.

    Initialize with a ClusterType, list of ServerDescriptions, and an optional
    replica set name.
    """

    def __init__(self, cluster_type, server_descriptions, set_name):
        self._cluster_type = cluster_type
        self._set_name = set_name
        self._server_descriptions = dict(
            (s.address, s)
            for s in server_descriptions)

    def get_server_description(self, address):
        return self._server_descriptions.get(address)

    def has_server(self, address):
        return address in self._server_descriptions

    def check_compatible(self):
        for s in self._server_descriptions.values():
            # s.min/max_wire_version is the server's wire protocol.
            # MIN/MAX_SUPPORTED_WIRE_VERSION is what PyMongo supports.
            server_too_new = (
                # Server too new.
                s.min_wire_version is not None
                and s.min_wire_version > common.MAX_SUPPORTED_WIRE_VERSION)

            server_too_old = (
                # Server too old.
                s.max_wire_version is not None
                and s.max_wire_version < common.MIN_SUPPORTED_WIRE_VERSION)

            if server_too_new or server_too_old:
                raise ConfigurationError(
                    "Server at %s:%d "
                    "uses wire protocol versions %d through %d, "
                    "but PyMongo only supports %d through %d"
                    % (s.address[0], s.address[1],
                       s.min_wire_version, s.max_wire_version,
                       common.MIN_SUPPORTED_WIRE_VERSION,
                       common.MAX_SUPPORTED_WIRE_VERSION))

    @property
    def server_descriptions(self):
        """List of (address, ServerDescription)."""
        return self._server_descriptions.items()

    @property
    def cluster_type(self):
        return self._cluster_type

    @property
    def set_name(self):
        """The replica set name."""
        return self._set_name

    @property
    def min_wire_version(self):
        return min(s.min_wire_version
                   for (address, s) in self.server_descriptions)

    @property
    def max_wire_version(self):
        return max(s.max_wire_version
                   for (address, s) in self.server_descriptions)

    @property
    def known_servers(self):
        return [s for (address, s) in self.server_descriptions
                if s.is_server_type_known]


# If ClusterType is Unknown and we receive an ismaster response, what should
# the new ClusterType be?
cluster_types = {
    ServerType.Mongos: ClusterType.Sharded,
    ServerType.RSPrimary: ClusterType.ReplicaSetWithPrimary,
    ServerType.RSSecondary: ClusterType.ReplicaSetNoPrimary,
    ServerType.RSArbiter: ClusterType.ReplicaSetNoPrimary,
    ServerType.RSOther: ClusterType.ReplicaSetNoPrimary,
}


def update_cluster_description(cd, sd):
    """Return an updated ClusterDescription, using a ServerDescription.

    Called after attempting (successfully or not) to call ismaster on the
    server at sd.address.
    """
    cluster_type = cd.cluster_type
    sds = dict(cd.server_descriptions)
    set_name = cd.set_name
    server_type = sd.server_type

    # We already have this ServerDescription, otherwise we wouldn't be
    # monitoring it. Thus this is "replace" not "add".

    sds[sd.address] = sd

    if cd.cluster_type == ClusterType.Single:
        # Single type never changes.
        return ClusterDescription(
            ClusterType.Single,
            sds.values(),
            cd.set_name)

    if cd.cluster_type == ClusterType.Unknown:
        if server_type == ServerType.Standalone:
            sds.pop(sd.address)

        elif server_type not in (ServerType.Unknown, ServerType.RSGhost):
            cluster_type = cluster_types[server_type]

    if cd.cluster_type == ClusterType.Sharded:
        if sd.server_type != ServerType.Mongos:
            sds.pop(sd.address)

    elif cd.cluster_type == ClusterType.ReplicaSetNoPrimary:
        if server_type in (ServerType.Standalone, ServerType.Mongos):
            sds.pop(sd.address)

        elif server_type == ServerType.RSPrimary:
            cluster_type, set_name = _update_rs_with_primary_from_primary(
                sds, set_name, sd)

        elif server_type in (
                ServerType.RSSecondary,
                ServerType.RSArbiter,
                ServerType.RSOther):
            cluster_type, set_name = _update_rs_without_primary(
                sds, set_name, sd)

    elif cd.cluster_type == ClusterType.ReplicaSetWithPrimary:
        if server_type in (ServerType.Standalone, ServerType.Mongos):
            sds.pop(sd.address)
            cluster_type = _check_has_primary(sds)

        elif server_type == ServerType.RSPrimary:
            cluster_type = _update_rs_with_primary_from_primary(
                sds, set_name, sd)

        elif server_type in (
                ServerType.RSSecondary,
                ServerType.RSArbiter,
                ServerType.RSOther):
            cluster_type = _update_rs_with_primary_from_member(
                sds, set_name, sd)

        else:
            # ServerType is Unknown or RSGhost: did we just lose the primary?
            cluster_type = _check_has_primary(sds)

    # Return updated copy.
    return ClusterDescription(cluster_type, sds.values(), set_name)


def _update_rs_with_primary_from_primary(sds, set_name, sd):
    """Update cluster description from a primary's ismaster response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new cluster type, new set_name).
    """
    cluster_type = ClusterType.ReplicaSetWithPrimary
    if set_name is None:
        set_name = sd.set_name

    elif set_name != sd.set_name:
        # We found a primary but it doesn't have the set_name
        # provided by the user.
        sds.pop(sd.address)
        cluster_type = ClusterType.ReplicaSetNoPrimary
        return cluster_type, set_name

    # We've heard from the primary. Is it the same primary as before?
    for server in sds.values():
        if server.address != sd.address:
            if server.server_type is ServerType.RSPrimary:
                # Reset old primary's type to Unknown.
                sds[server.address] = ServerDescription(server.address)

                # There can be only one prior primary.
                break

    # Discover new hosts from this primary's response.
    for new_address in sd.all_hosts:
        if new_address not in sds:
            sds[new_address] = ServerDescription(new_address)

    # Remove hosts not in the response.
    all_hosts = set(sd.all_hosts)
    for old_sd in sds.values():
        if old_sd.address not in all_hosts:
            sds.pop(old_sd.address)

    return cluster_type, set_name


def _update_rs_with_primary_from_member(sds, set_name, sd):
    """RS with known primary. Process a response from a non-primary.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns new cluster type.
    """
    assert set_name is not None

    if set_name != sd.set_name:
        sds.pop(sd.address)

    # Had this member been the primary?
    return _check_has_primary(sds)


def _update_rs_without_primary(sds, set_name, sd):
    """RS without known primary. Update from a non-primary's response.

    Pass in a dict of ServerDescriptions, current replica set name, and the
    ServerDescription we are processing.

    Returns (new cluster type, new set_name).
    """
    cluster_type = ClusterType.ReplicaSetNoPrimary
    if set_name is None:
        set_name = sd.set_name

    elif set_name != sd.set_name:
        sds.pop(sd.address)
        return cluster_type, set_name

    # This isn't the primary's response, so don't remove any servers
    # it doesn't report. Only add new servers.
    for address in sd.all_hosts:
        if address not in sds:
            sds[address] = ServerDescription(address)

    return cluster_type, set_name


def _check_has_primary(sds):
    """Current ClusterType is ReplicaSetWithPrimary. Is primary still known?

    Pass in a dict of ServerDescriptions.

    Returns new cluster type.
    """
    for s in sds.values():
        if s.server_type == ServerType.RSPrimary:
            return ClusterType.ReplicaSetWithPrimary
    else:
        return ClusterType.ReplicaSetNoPrimary
