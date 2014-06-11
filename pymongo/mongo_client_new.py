# Copyright 2009-2014 MongoDB, Inc.
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

"""TODO: help string."""

import random
import threading

from bson.py3compat import (string_type)
from pymongo import (helpers,
                     message,
                     monitor,
                     pool,
                     uri_parser)
from pymongo.cluster import Cluster
from pymongo.cluster_description import ClusterDescription
from pymongo.errors import (ConfigurationError)
from pymongo.settings import ClusterSettings


class MongoClientNew(object):
    """Connection to one or more MongoDB servers.
    """

    def __init__(self, host='localhost', port=27017, replicaSet=None):
        """TODO: docstring"""
        if isinstance(host, string_type):
            host = [host]

        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        seeds = set()

        for entity in host:
            seeds.update(uri_parser.split_hosts(entity, port))
        if not seeds:
            raise ConfigurationError("need to specify at least one host")

        self._settings = ClusterSettings(seeds, set_name=replicaSet)

        # TODO: parse URI, socket timeouts, ssl args, auth, use_greenlets,
        # pool_class, document_class, pool options, condition_class,
        # default database.

        cluster_description = ClusterDescription(
            self._settings.get_cluster_type(),
            self._settings.get_server_descriptions(),
            self._settings.set_name)

        self._cluster = Cluster(
            cluster_description,
            pool_class=pool.Pool,
            monitor_class=monitor.Monitor,
            condition_class=threading.Condition)

        self._cluster.open()

    def proto_command(self, database_name, commandname, must_use_master):
        """Just prove we can talk to a server."""
        spec = {commandname: 1}
        request_id, msg, _ = message.query(
            0, database_name + '.$cmd', 0, -1, spec)

        # A selector takes a list of ServerDescriptions and returns a list
        # of suitable ServerDescriptions.
        if must_use_master:
            def selector(sds):
                return [sd for sd in sds if sds.is_writable]
        else:
            def selector(sds):
                return [sd for sd in sds if sds.is_readable]

        server = random.choice(self._cluster.select_servers(selector))
        raw_response = server.send_message_with_response(msg, request_id)
        response = helpers._unpack_response(raw_response)['data'][0]
        msg = "command %r failed: %%s" % spec
        helpers._check_command_response(response, None, msg)
        return response
