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

"""Test the server_description module."""

import sys

sys.path[0:0] = [""]

from pymongo.server_description import (ServerDescription, ServerType,
                                        parse_ismaster_response)
from test import unittest

address = ('localhost', 27017)


class TestServerDescription(unittest.TestCase):
    def test_unknown(self):
        # Default, no ismaster_response.
        s = ServerDescription(address)
        self.assertEqual(ServerType.Unknown, s.server_type)

    def test_mongos(self):
        s = parse_ismaster_response(address, {'ok': 1, 'msg': 'isdbgrid'}, 0)
        self.assertEqual(ServerType.Mongos, s.server_type)

    def test_primary(self):
        s = parse_ismaster_response(
            address, {'ok': 1, 'ismaster': True, 'setName': 'rs'}, 0)

        self.assertEqual(ServerType.RSPrimary, s.server_type)

    def test_secondary(self):
        s = parse_ismaster_response(
            address,
            {'ok': 1, 'ismaster': False, 'secondary': True, 'setName': 'rs'},
            0)

        self.assertEqual(ServerType.RSSecondary, s.server_type)

    def test_arbiter(self):
        s = parse_ismaster_response(
            address,
            {'ok': 1, 'ismaster': False, 'arbiterOnly': True, 'setName': 'rs'},
            0)

        self.assertEqual(ServerType.RSArbiter, s.server_type)

    def test_other(self):
        s = parse_ismaster_response(
            address, {'ok': 1, 'ismaster': False, 'setName': 'rs'}, 0)

        self.assertEqual(ServerType.RSOther, s.server_type)

        s = parse_ismaster_response(
            address,
            {
                'ok': 1,
                'ismaster': False,
                'secondary': True,
                'hidden': True,
                'setName': 'rs'},
            0)

        self.assertEqual(ServerType.RSOther, s.server_type)

    def test_ghost(self):
        s = parse_ismaster_response(
            address, {'ok': 1, 'isreplicaset': True}, 0)

        self.assertEqual(ServerType.RSGhost, s.server_type)

    def test_standalone(self):
        s = parse_ismaster_response(address, {'ok': 1, 'ismaster': True}, 0)
        self.assertEqual(ServerType.Standalone, s.server_type)

        # Mongod started with --slave.
        s = parse_ismaster_response(address, {'ok': 1, 'ismaster': False}, 0)
        self.assertEqual(ServerType.Standalone, s.server_type)

    def test_ok_false(self):
        s = parse_ismaster_response(address, {'ok': 0, 'ismaster': True}, 0)
        self.assertEqual(ServerType.Unknown, s.server_type)

    def test_all_hosts(self):
        s = parse_ismaster_response(
            address,
            {
                'ok': 1,
                'ismaster': True,
                'hosts': ['a'],
                'passives': ['b:27018'],
                'arbiters': ['c']
            },
            0)

        self.assertEqual(
            [('a', 27017), ('b', 27018), ('c', 27017)],
            sorted(s.all_hosts))

    def test_round_trip_time(self):
        response = {'ok': 1, 'ismaster': True}
        s = parse_ismaster_response(address, response, round_trip_time=1)
        self.assertEqual(1, s.round_trip_time)
        rtts = s.round_trip_times
        s2 = parse_ismaster_response(
            address, response, round_trip_time=3, round_trip_times=rtts)

        # Average of 1 and 3.
        self.assertEqual(2, s2.round_trip_time)


if __name__ == "__main__":
    unittest.main()
