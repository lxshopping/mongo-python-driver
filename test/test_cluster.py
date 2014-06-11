# Copyright 2009-2014 MongoDB, Inc.
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

"""Test the cluster module."""

import sys
import threading
from pymongo.cluster import Cluster

sys.path[0:0] = [""]

from pymongo import common
from pymongo.cluster_description import ClusterType, ClusterDescription
from pymongo.errors import ConfigurationError, ConnectionFailure
from pymongo.settings import ClusterSettings
from pymongo.server_description import (ServerDescription,
                                        parse_ismaster_response, ServerType)
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from test import unittest


class MockPool(object):
    def __init__(self, *args, **kwargs):
        pass

    def reset(self):
        pass


class MockMonitor(object):
    def __init__(self, address, cluster, pool):
        self._address = address
        self._cluster = cluster

    def open(self):
        pass

    def request_check(self):
        pass

    def close(self):
        pass

address = ('a', 27017)


def create_mock_cluster(seeds=None, set_name=None):
    partitioned_seeds = map(common.partition_node, seeds or ['a'])
    settings = ClusterSettings(partitioned_seeds, set_name=set_name)
    cluster_description = ClusterDescription(
        settings.get_cluster_type(),
        settings.get_server_descriptions(),
        settings.set_name)

    c = Cluster(
        cluster_description,
        pool_class=MockPool,
        monitor_class=MockMonitor,
        condition_class=threading.Condition)

    c.open()
    return c


def got_ismaster(cluster, server_address, ismaster_response):
    server_description = parse_ismaster_response(
        server_address, ismaster_response, 0)

    cluster.on_change(server_description)


def disconnected(cluster, server_address):
    # Create new description of server type Unknown.
    cluster.on_change(ServerDescription(server_address))


def get_type(cluster, hostname):
    description = cluster.get_server_by_address((hostname, 27017)).description
    return description.server_type


class TestSingleServerCluster(unittest.TestCase):
    def test_direct_connection(self):
        for server_type, ismaster_response in [
            (ServerType.RSPrimary, {
                'ok': 1,
                'ismaster': True,
                'hosts': ['a'],
                'setName': 'rs'}),

            (ServerType.RSSecondary, {
                'ok': 1,
                'ismaster': False,
                'secondary': True,
                'hosts': ['a'],
                'setName': 'rs'}),

            (ServerType.Mongos, {
                'ok': 1,
                'ismaster': True,
                'msg': 'isdbgrid'}),

            (ServerType.RSArbiter, {
                'ok': 1,
                'ismaster': False,
                'arbiterOnly': True,
                'hosts': ['a'],
                'setName': 'rs'}),

            (ServerType.Standalone, {
                'ok': 1,
                'ismaster': True}),

            # Slave.
            (ServerType.Standalone, {
                'ok': 1,
                'ismaster': False}),
        ]:
            c = create_mock_cluster()

            # Can't select a server while the only server is of type Unknown.
            self.assertRaises(
                ConnectionFailure,
                c.select_servers, any_server_selector, server_wait_time=0)

            got_ismaster(c, address, ismaster_response)

            # ClusterType never changes.
            self.assertEqual(ClusterType.Single, c.description.cluster_type)

            # No matter whether the server is writable,
            # select_servers() returns it.
            s = c.select_servers(writable_server_selector)[0]
            self.assertEqual(server_type, s.description.server_type)

    def test_unavailable_seed(self):
        c = create_mock_cluster()
        disconnected(c, address)
        self.assertEqual(ServerType.Unknown, get_type(c, 'a'))


class TestMultiServerCluster(unittest.TestCase):
    def test_unexpected_host(self):
        # Received ismaster response from host not in cluster.
        # E.g., a race where the host is removed before it responds.
        c = create_mock_cluster(['a', 'b'], set_name='rs')

        # 'b' is not in the set.
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'hosts': ['a'],
            'setName': 'rs'})

        self.assertFalse(c.has_server(('b', 27017)))

        # 'b' still thinks it's in the set.
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'hosts': ['a', 'b'],
            'setName': 'rs'})

        # We don't add it.
        self.assertFalse(c.has_server(('b', 27017)))

    def test_ghost_seed(self):
        c = create_mock_cluster(['a', 'b'])
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': False,
            'isreplicaset': True})

        self.assertEqual(ServerType.RSGhost, get_type(c, 'a'))
        self.assertEqual(ClusterType.Unknown, c.description.cluster_type)

    def test_standalone_removed(self):
        c = create_mock_cluster(['a', 'b'])
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True})

        self.assertEqual(1, len(c.description.server_descriptions))
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False})

        self.assertEqual(0, len(c.description.server_descriptions))

    def test_mongos_ha(self):
        c = create_mock_cluster(['a', 'b'])
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'msg': 'isdbgrid'})

        self.assertEqual(ClusterType.Sharded, c.description.cluster_type)
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': True,
            'msg': 'isdbgrid'})

        self.assertEqual(ServerType.Mongos, get_type(c, 'a'))
        self.assertEqual(ServerType.Mongos, get_type(c, 'b'))

    def test_rs_discovery(self):
        c = create_mock_cluster(set_name='rs')

        # At first, A, B, and C are secondaries.
        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b', 'c']})

        self.assertEqual(3, len(c.description.server_descriptions))
        self.assertEqual(ServerType.RSSecondary, get_type(c, 'a'))
        self.assertEqual(ServerType.Unknown, get_type(c, 'b'))
        self.assertEqual(ServerType.Unknown, get_type(c, 'c'))
        self.assertEqual(ClusterType.ReplicaSetNoPrimary,
                         c.description.cluster_type)

        # Admin removes A, adds a high-priority member D which becomes primary.
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'primary': 'd',
            'hosts': ['b', 'c', 'd']})

        self.assertEqual(4, len(c.description.server_descriptions))
        self.assertEqual(ServerType.RSSecondary, get_type(c, 'a'))
        self.assertEqual(ServerType.RSSecondary, get_type(c, 'b'))
        self.assertEqual(ServerType.Unknown, get_type(c, 'c'))
        self.assertEqual(ServerType.Unknown, get_type(c, 'd'))
        self.assertEqual(ClusterType.ReplicaSetNoPrimary,
                         c.description.cluster_type)

        # Primary responds.
        got_ismaster(c, ('d', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['b', 'c', 'd']})

        self.assertEqual(3, len(c.description.server_descriptions))
        self.assertEqual(ServerType.RSSecondary, get_type(c, 'b'))
        self.assertEqual(ServerType.Unknown, get_type(c, 'c'))
        self.assertEqual(ServerType.RSPrimary, get_type(c, 'd'))
        self.assertEqual(ClusterType.ReplicaSetWithPrimary,
                         c.description.cluster_type)

        # Stale response from C.
        got_ismaster(c, ('c', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b', 'c']})

        # We don't add A back.
        self.assertEqual(3, len(c.description.server_descriptions))
        self.assertEqual(ServerType.RSSecondary, get_type(c, 'b'))
        self.assertEqual(ServerType.RSSecondary, get_type(c, 'c'))
        self.assertEqual(ServerType.RSPrimary, get_type(c, 'd'))

    def test_wire_version(self):
        c = create_mock_cluster(set_name='rs')
        self.assertEqual(c.description.min_wire_version, None)
        self.assertEqual(c.description.max_wire_version, None)
        c.description.check_compatible()  # No error.

        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a']})

        # Use defaults.
        self.assertEqual(c.description.min_wire_version, 0)
        self.assertEqual(c.description.max_wire_version, 0)

        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a'],
            'minWireVersion': 1,
            'maxWireVersion': 5})

        self.assertEqual(c.description.min_wire_version, 1)
        self.assertEqual(c.description.max_wire_version, 5)

        s = c.select_servers(any_server_selector)[0]
        self.assertEqual(s.description.min_wire_version, 1)
        self.assertEqual(s.description.max_wire_version, 5)

        # Incompatible.
        got_ismaster(c, address, {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a'],
            'minWireVersion': 11,
            'maxWireVersion': 12})

        try:
            c.select_servers(any_server_selector)
        except ConfigurationError as e:
            # Error message should say which server failed and why.
            self.assertTrue('a:27017' in str(e))
            self.assertTrue('wire protocol versions 11 through 12' in str(e))
        else:
            self.fail('No error with incompatible wire version')

    def test_max_write_batch_size(self):
        c = create_mock_cluster(seeds=['a', 'b'], set_name='rs')

        def write_batch_size():
            s = c.select_servers(writable_server_selector)[0]
            return s.description.max_write_batch_size

        got_ismaster(c, ('a', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 1})

        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': False,
            'secondary': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 2})

        # Uses primary's max batch size.
        self.assertEqual(1, write_batch_size())

        # b becomes primary.
        got_ismaster(c, ('b', 27017), {
            'ok': 1,
            'ismaster': True,
            'setName': 'rs',
            'hosts': ['a', 'b'],
            'maxWriteBatchSize': 2})

        self.assertEqual(2, write_batch_size())


if __name__ == "__main__":
    unittest.main()
