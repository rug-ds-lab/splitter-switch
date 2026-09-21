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
PTF test for Time-based Windows.
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

# from scapy.all import sendp, send, get_if_list, get_if_hwaddr
from scapy.all import Packet
from scapy.all import Ether, IP, UDP, Raw

from lib.EventPacket import Event

ETHERTYPE_EVENT = 0x8819

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


class TumblingTimeBasedWindow(Interface):
    def setUp(self):
        Interface.setUp(self)

    def runTest(self):
        ethertype=0x800
        duration=5 
        shift=duration
        overlap=0
        stream_id=0
        self.window_spec_table_add_time_based_window_init_hit(port=1, ethertype=ethertype, stream_id=stream_id, duration=duration, shift=shift, overlap=overlap)
        self.operators_max_table_add_operators_max_hit(stream_id=0, max=6)

        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=0, port=2)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=1, port=3)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=2, port=4)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=3, port=5)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=4, port=6)
        self.operators_table_add_operators_output_port_hit(stream_id=0, operator_id=5, port=7)
        
        pkt1 = Ether(src='00:11:22:33:44:55', dst='00:22:33:44:55:66')/IP(src="16.0.0.1",dst="48.0.0.1")/UDP(dport=12,sport=1025)

        port=2
        for x in range(3):
          t = x+1 ## has to be a non zero value
          pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
          print("Sending %d packets on port %d, t:%d" % (1, 1, t))
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting %d packets on port %d" % (1, port))
          verify_packets(self, pkt, ports=[port])

        port=3
        # Range 7 .. 13
        for x in range(6,9): 
          t = x
          pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
          print("Sending %d packets on port %d, t:%d" % (1, 1, t))
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting %d packets on port %d" % (1, port))
          verify_packets(self, pkt, ports=[port])

        # Out-Of-Order / Late Packet
        # t = 2
        # port=2
        # pkt = Ether(src='00:11:22:33:44:55', dst='00:22:33:44:55:66')/IP(src="16.0.0.1",dst="48.0.0.1")/UDP(dport=12,sport=int(t))/(10*'x')
        # print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        # send_packet(self, port_id=1, pkt=pkt, count=1)
        # print("Expecting %d packets on port %d" % (1, port))
        # verify_packets(self, pkt, ports=[port])

        port=4
        for x in range(12,17):
          t = x
          pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
          print("Sending %d packets on port %d, t:%d" % (1, 1, t))
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting %d packets on port %d" % (1, port))
          verify_packets(self, pkt, ports=[port])

        port=5
        for x in range(18,23):
          t = x
          pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
          print("Sending %d packets on port %d, t:%d" % (1, 1, t))
          send_packet(self, port_id=1, pkt=pkt, count=1)
          print("Expecting %d packets on port %d" % (1, port))
          verify_packets(self, pkt, ports=[port])

        # t = 1
        # pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(14*'x')
        # print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        # send_packet(self, port_id=1, pkt=pkt, count=1)

    def tearDown(self):
        Interface.tearDown(self)
