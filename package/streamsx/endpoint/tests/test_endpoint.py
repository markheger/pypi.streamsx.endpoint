from unittest import TestCase

import streamsx.endpoint as endpoint

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr

import datetime
import os
import json



class Test(TestCase):
    
    def _build_only(self, name, topo):

        inetserver_toolkit_home = os.environ["STREAMSX_INETSERVER_TOOLKIT"]
        if inetserver_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, inetserver_toolkit_home)

        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)

        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)


    def test_basic_json_injection(self):
        name = 'test_basic_json_injection'
        topo = Topology(name)
        res = endpoint.json_injection(topo)
        res.print()
        self._build_only(name, topo)

    def test_basic_view_tuples(self):
        name = 'test_basic_view_tuples'
        topo = Topology(name)
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        endpoint.view_tuples(s.last(10).trigger(1))
        self._build_only(name, topo)

