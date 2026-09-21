# Copyright 2022-present Distributed Systems @ University of Groningen.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
PTF test for splitter.p4
"""

import logging

from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
from p4testutils.misc_utils import *
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
import bfrt_grpc.client as gc
import random
import time

from lib.Interface import *

logger = get_logger()
swports = get_sw_ports()

num_pipes = int(testutils.test_param_get('num_pipes'))
pipes = list(range(num_pipes))

# Hitless HA Support
client_id = 0
p4_name = "splitter"
profile_name = 'pipe'
base_pick_path = testutils.test_param_get("base_pick_path")
base_put_path = testutils.test_param_get("base_put_path")
arch = testutils.test_param_get("arch")
if not base_pick_path:
  base_pick_path = "install/share/" + arch + "pd/"
if not base_put_path:
  base_put_path = "/tmp"


class TumblingCountBasedWindow(Interface):
    def setUp(self):
        Interface.setUp(self)

    def runTest(self):
        type=0x800
        count=10
        self.window_spec_table_add_count_based_window_init_hit(port=1, type=type, stream_id=0, count=count, shift=count)

        self.operators_max_table_add_operators_max_hit(stream_id=0, max=4)

        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=0, port=2)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=1, port=3)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=2, port=4)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=3, port=5)

        pkt = simple_eth_packet(pktlen=104, eth_dst='00:22:33:44:55:66', eth_src='00:11:22:33:44:55', eth_type=type)

        port=2
        print("Sending %d packets on port %d" % (count, 1))
        for x in range(count): 
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting packet #%d on port %d" % (x, port))
          verify_packets(self, pkt, ports=[port])

        port=3
        print("Sending %d packets on port %d" % (count, 1))
        for x in range(count): 
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting packet #%d on port %d" % (x, port))
          verify_packets(self, pkt, ports=[port])

        port=4
        print("Sending %d packets on port %d" % (count, 1))
        for x in range(count): 
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting packet #%d on port %d" % (x, port))
          verify_packets(self, pkt, ports=[port])

        port=5
        print("Sending %d packets on port %d" % (count, 1))
        for x in range(count): 
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting packet #%d on port %d" % (x, port))
          verify_packets(self, pkt, ports=[port])

        port=2
        print("Sending %d packets on port %d" % (count, 1))
        for x in range(count): 
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting packet #%d on port %d" % (x, port))
          verify_packets(self, pkt, ports=[port])

    def tearDown(self):
        Interface.tearDown(self)
