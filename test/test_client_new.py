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

"""Test the mongo_client module."""

import sys

sys.path[0:0] = [""]

from pymongo.mongo_client_new import MongoClientNew
from test import host, port, unittest


class TestClientNew(unittest.TestCase):
    def test_buildinfo(self):
        c = MongoClientNew(host, port)
        assert 'version' in c.proto_command('admin', 'buildinfo', True)

    def test_buildinfo_readonly(self):
        # Assuming there's an RS member on port 27018.
        c = MongoClientNew(host, 27018)
        assert 'version' in c.proto_command('admin', 'buildinfo', False)


if __name__ == "__main__":
    unittest.main()
