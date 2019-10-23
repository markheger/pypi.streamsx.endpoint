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


    def _get_inetserver_toolkit():
        result = True
        try:
            os.environ['STREAMSX_INETSERVER_TOOLKIT']
        except KeyError: 
            result = False
        if result:
            inetserver_toolkit_home = os.environ["STREAMSX_INETSERVER_TOOLKIT"]
        else:    
            inetserver_toolkit_home = endpoint.download_toolkit()
        return inetserver_toolkit_home


    def _build_only(self, name, topo):
        if self._tk is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self._tk)

        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)

        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    @classmethod
    def setUpClass(self):
        self._tk = self._get_inetserver_toolkit()

    def test_basic_json_injection(self):
        name = 'test_basic_json_injection'
        topo = Topology(name)
        res = endpoint.inject(topo, name='jsoninject', monitor=None, context='sample')
        res.print()
        self._build_only(name, topo)

    def test_basic_xml_injection(self):
        name = 'test_basic_xml_injection'
        topo = Topology(name)
        res = endpoint.inject(topo, name='jsoninject', schema=CommonSchema.XML, monitor='xml', context='sample')
        self._build_only(name, topo)

    def test_basic_string_injection(self):
        name = 'test_basic_string_injection'
        topo = Topology(name)
        res = endpoint.inject(topo, name='jsoninject', schema=CommonSchema.String, monitor='sample', context='sample')
        res.print()
        self._build_only(name, topo)

    def test_basic_stream_schema_injection(self):
        name = 'test_basic_stream_schema_injection'
        topo = Topology(name)
        res = endpoint.inject(topo, name='jsoninject', schema=StreamSchema('tuple<int32 a, boolean alert>'), monitor=None, context='sample')
        res.print()
        self._build_only(name, topo)


    def test_basic_expose(self):
        name = 'test_basic_expose'
        topo = Topology(name)
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        endpoint.expose(s.last(10).trigger(1), name='tupleview', context='sample', monitor=None)
        self._build_only(name, topo)

    def test_monitor_expose(self):
        name = 'test_monitor_expose'
        topo = Topology(name)
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        endpoint.expose(s.last(10).trigger(1), name='tupleview', context='sample', monitor='sample')
        self._build_only(name, topo)



