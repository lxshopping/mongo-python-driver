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
from pymongo.errors import InvalidOperation, ConfigurationError
from pymongo.server_description import ServerDescription, ServerType


def create_cluster_description(settings):
    """Create a Cluster from ClusterSettings."""
    if settings.direct:
        cluster_type = ClusterType.Single
    elif settings.set_name is not None:
        cluster_type = ClusterType.ReplicaSetNoPrimary
    else:
        cluster_type = ClusterType.Unknown

    servers = [ServerDescription(address) for address in settings.seeds]
    return ClusterDescription(cluster_type, servers, settings.set_name)


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

    A new ClusterDescription's attributes are mutable until we call freeze().
    Create an unfrozen clone with copy().
    """
    def __init__(self, cluster_type, server_descriptions, set_name):
        self._cluster_type = cluster_type
        self._set_name = set_name
        self._server_descriptions = dict(
            (s.address, s)
            for s in server_descriptions)

        self._frozen = False

    def freeze(self):
        self._frozen = True

    def copy(self):
        """Get an unfrozen clone."""
        # Since ServerDescriptions are immutable we need not deepcopy.
        return ClusterDescription(
            self.cluster_type,
            self.server_descriptions,
            self.set_name)

    def get_server_description(self, address):
        return self._server_descriptions.get(address)

    def has_server(self, address):
        return address in self._server_descriptions

    def add_server_description(self, server_description):
        self._check_frozen()
        address = server_description.address
        assert not self.has_server(address)
        self._server_descriptions[address] = server_description

    def replace_server_description(self, server_description):
        self._check_frozen()
        address = server_description.address
        assert self.has_server(address)
        self._server_descriptions[address] = server_description

    def remove_address(self, address):
        self._check_frozen()
        self._server_descriptions.pop(address, None)

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
        return self._server_descriptions.values()

    @property
    def cluster_type(self):
        return self._cluster_type

    @cluster_type.setter
    def cluster_type(self, cluster_type):
        self._check_frozen()
        self._cluster_type = cluster_type

    @property
    def set_name(self):
        """The replica set name."""
        return self._set_name

    @set_name.setter
    def set_name(self, set_name):
        self._check_frozen()
        self._set_name = set_name

    @property
    def min_wire_version(self):
        return min(s.min_wire_version for s in self.server_descriptions)

    @property
    def max_wire_version(self):
        return max(s.max_wire_version for s in self.server_descriptions)

    @property
    def known_servers(self):
        return [s for s in self.server_descriptions if s.is_server_type_known]

    def _check_frozen(self):
        if self._frozen:
            raise InvalidOperation(
                "Attempt to modify frozen ClusterDescription")


# If ClusterType is Unknown and we receive an ismaster response, what should
# the new ClusterType be?
cluster_types = {
    ServerType.Mongos:          ClusterType.Sharded,
    ServerType.RSPrimary:       ClusterType.ReplicaSetWithPrimary,
    ServerType.RSSecondary:     ClusterType.ReplicaSetNoPrimary,
    ServerType.RSArbiter:       ClusterType.ReplicaSetNoPrimary,
    ServerType.RSOther:         ClusterType.ReplicaSetNoPrimary,
}


def update_cluster_description(cd, sd):
    """Update a ClusterDescription from a ServerDescription.

    Called after attempting (successfully or not) to call ismaster on the
    server at sd.address.
    """
    cd.replace_server_description(sd)

    server_type = sd.server_type

    if cd.cluster_type == ClusterType.Single:
        # Single type never changes.
        return

    if cd.cluster_type == ClusterType.Unknown:
        if server_type == ServerType.Standalone:
            cd.remove_address(sd.address)

        elif server_type not in (ServerType.Unknown, ServerType.RSGhost):
            cd.cluster_type = cluster_types[server_type]

    if cd.cluster_type == ClusterType.Sharded:
        _update_sharded(cd, sd)

    elif cd.cluster_type == ClusterType.ReplicaSetNoPrimary:
        if server_type in (ServerType.Standalone, ServerType.Mongos):
            cd.remove_address(sd.address)

        elif server_type == ServerType.RSPrimary:
            cd.cluster_type = ClusterType.ReplicaSetWithPrimary
            _update_rs_with_primary_from_primary(cd, sd)

        elif server_type in (
                ServerType.RSSecondary,
                ServerType.RSArbiter,
                ServerType.RSOther):
            _update_rs_without_primary(cd, sd)

    elif cd.cluster_type == ClusterType.ReplicaSetWithPrimary:
        if server_type in (ServerType.Standalone, ServerType.Mongos):
            cd.remove_address(sd.address)
            _check_has_primary(cd)

        elif server_type == ServerType.RSPrimary:
            _update_rs_with_primary_from_primary(cd, sd)

        elif server_type in (
                ServerType.RSSecondary,
                ServerType.RSArbiter,
                ServerType.RSOther):
            _update_rs_with_primary_from_member(cd, sd)

        else:
            # ServerType is Unknown or RSGhost: did we just lose the primary?
            _check_has_primary(cd)


def _update_sharded(cd, sd):
    if sd.server_type != ServerType.Mongos:
        cd.remove_address(sd.address)


def _update_rs_with_primary_from_primary(cd, sd):
    """Update cluster description from a primary's ismaster response."""
    if cd.set_name is None:
        cd.set_name = sd.set_name
    
    elif cd.set_name != sd.set_name:
        # We found a primary but it doesn't have the set_name
        # provided by the user.
        cd.remove_address(sd.address)
        cd.type = ClusterType.ReplicaSetNoPrimary
        return

    # We've heard from the primary. Is it the same primary as before?
    for server in cd.server_descriptions:
        if server.address != sd.address:
            if server.server_type is ServerType.RSPrimary:
                # Reset old primary's type to Unknown.
                cd.replace_server_description(
                    ServerDescription(server.address))

                # There can be only one prior primary.
                break

    # Discover new hosts from this primary's response.
    for new_address in sd.all_hosts:
        if not cd.has_server(new_address):
            cd.add_server_description(ServerDescription(new_address))

    # Remove hosts not in the response.
    all_hosts = set(sd.all_hosts)
    for old_sd in cd.server_descriptions:
        if old_sd.address not in all_hosts:
            cd.remove_address(old_sd.address)


def _update_rs_with_primary_from_member(cd, sd):
    assert cd.set_name is not None

    if cd.set_name != sd.set_name:
        cd.remove_address(sd.address)
    
    # Had this member been the primary?
    _check_has_primary(cd)


def _update_rs_without_primary(cd, sd):
    """RS without known primary. Update from a non-primary's response."""
    if cd.set_name is None:
        cd.set_name = sd.set_name
    
    elif cd.set_name != sd.set_name:
        cd.remove_address(sd.address)
        return

    # This isn't the primary's response, so don't remove any servers
    # it doesn't report. Only add new servers.
    for address in sd.all_hosts:
        if not cd.has_server(address):
            cd.add_server_description(ServerDescription(address))


def _check_has_primary(cd):
    assert cd.cluster_type == ClusterType.ReplicaSetWithPrimary

    for s in cd.server_descriptions:
        if s.server_type == ServerType.RSPrimary:
            break
    else:
        cd.cluster_type = ClusterType.ReplicaSetNoPrimary
