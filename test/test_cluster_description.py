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

"""Test the cluster_description module."""

import sys

sys.path[0:0] = [""]

from pymongo.cluster_description import ClusterType, create_cluster_description
from pymongo.errors import InvalidOperation
from pymongo.settings import ClusterSettings
from pymongo.server_description import ServerDescription
from test import unittest


class TestClusterDescription(unittest.TestCase):
    def test_freeze(self):
        cd = create_cluster_description(
            ClusterSettings('a', set_name='rs'))

        sd = ServerDescription(('b', 27017))
        cd.add_server_description(sd)

        cd.remove_address(('b', 27017))
        cd.cluster_type = ClusterType.ReplicaSetNoPrimary
        cd.set_name = 'rs'

        cd.freeze()
        self.assertRaises(InvalidOperation, cd.add_server_description, sd)
        self.assertRaises(InvalidOperation, cd.replace_server_description, sd)
        self.assertRaises(InvalidOperation,
                          cd.remove_address, ('b', 27017))

        with self.assertRaises(InvalidOperation):
            cd.cluster_type = ClusterType.ReplicaSetNoPrimary

        with self.assertRaises(InvalidOperation):
            cd.set_name = 'rs'


if __name__ == "__main__":
    unittest.main()
