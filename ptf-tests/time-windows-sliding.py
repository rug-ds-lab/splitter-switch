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


class SlidingTimeBasedWindows_Overlap(Interface):
    def setUp(self):
        Interface.setUp(self)

    def runTest(self):
        ethertype=0x800
        duration=9 
        shift=3
        overlap=3 
        stream_id=0
        self.window_spec_table_add_time_based_window_init_hit(port=1, ethertype=ethertype, stream_id=stream_id, duration=duration, shift=shift, overlap=overlap)
        self.operators_max_table_add_operators_max_hit(stream_id=0, max=6)

        # egress_hit 0 0 0 => 2
        self.time_sliding_windows_table_add_time_sliding_windows_egress_hit(stream_id=stream_id, curr_overlap=0, operator_id=0, port=2)
        # self.time_sliding_windows_table_add_time_sliding_windows_egress_hit(stream_id=stream_id, curr_overlap=0, operator_id=1, port=3)
        # self.time_sliding_windows_table_add_time_sliding_windows_egress_hit(stream_id=stream_id, curr_overlap=0, operator_id=2, port=4)
        # self.time_sliding_windows_table_add_time_sliding_windows_egress_hit(stream_id=stream_id, curr_overlap=0, operator_id=3, port=5)

        # mcast_hit 0 1 1 => 1
        # mcast_hit 0 2 2 => 2
        # mcast_hit 0 3 3 => 3
        # mcast_hit 0 3 4 => 4
        # mcast_hit 0 3 5 => 5
        # mcast_hit 0 3 0 => 6
        # mcast_hit 0 3 1 => 7
        # mcast_hit 0 3 2 => 2
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=1, operator_id=1, mcast_grp=1)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=2, operator_id=2, mcast_grp=2)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=3, operator_id=3, mcast_grp=3)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=3, operator_id=4, mcast_grp=4)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=3, operator_id=5, mcast_grp=5)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=3, operator_id=0, mcast_grp=6)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=3, operator_id=1, mcast_grp=7)
        self.time_sliding_windows_table_add_time_sliding_windows_mcast_hit(stream_id=stream_id, curr_overlap=3, operator_id=2, mcast_grp=2)

        self.mc_mgrp_create(mcast_grp=1)
        self.mc_mgrp_create(mcast_grp=2)
        self.mc_mgrp_create(mcast_grp=3)
        self.mc_mgrp_create(mcast_grp=4)
        self.mc_mgrp_create(mcast_grp=5)
        self.mc_mgrp_create(mcast_grp=6)
        self.mc_mgrp_create(mcast_grp=7)

        self.mc_node_create(node_id=1, rid=1, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=1, node_id=1, xid=1)
        self.mc_node_create(node_id=2, rid=2, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=1, node_id=2, xid=1)

        self.mc_node_create(node_id=3, rid=3, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=2, node_id=3, xid=1)
        self.mc_node_create(node_id=4, rid=4, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=2, node_id=4, xid=1)
        self.mc_node_create(node_id=5, rid=5, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=2, node_id=5, xid=1)

        self.mc_node_create(node_id=6, rid=6, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=6, xid=1)
        self.mc_node_create(node_id=7, rid=7, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=7, xid=1)
        self.mc_node_create(node_id=8, rid=8, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=8, xid=1)

        self.mc_node_create(node_id=9, rid=9, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=9, xid=1)
        self.mc_node_create(node_id=10, rid=10, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=10, xid=1)
        self.mc_node_create(node_id=11, rid=11, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=11, xid=1)

        self.mc_node_create(node_id=12, rid=12, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=12, xid=1)
        self.mc_node_create(node_id=13, rid=13, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=13, xid=1)
        self.mc_node_create(node_id=14, rid=14, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=14, xid=1)

        self.mc_node_create(node_id=15, rid=15, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=15, xid=1)
        self.mc_node_create(node_id=16, rid=16, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=16, xid=1)
        self.mc_node_create(node_id=17, rid=17, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=17, xid=1)

        self.mc_node_create(node_id=18, rid=18, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=18, xid=1)
        self.mc_node_create(node_id=19, rid=19, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=19, xid=1)
        self.mc_node_create(node_id=20, rid=20, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=20, xid=1)

        pkt1 = Ether(src='00:11:22:33:44:55', dst='00:22:33:44:55:66')/IP(src="16.0.0.1",dst="48.0.0.1")/UDP(dport=12,sport=1025)

        t=1
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on port %d" % (1, 2))
        verify_packets(self, pkt, ports=[2])

        t=6
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d and %d" % (2, 2, 3))
        verify_packets(self, pkt, ports=[2, 3])
        verify_no_other_packets(self)

        t=9
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 2, 3, 4))
        verify_packets(self, pkt, ports=[2, 3, 4])
        verify_no_other_packets(self)

        t=13
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 3, 4, 5))
        verify_packets(self, pkt, ports=[3, 4, 5])
        verify_no_other_packets(self)

        t=17
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 4, 5, 6))
        verify_packets(self, pkt, ports=[4, 5, 6])
        verify_no_other_packets(self)

        t=21
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 5, 6, 7))
        verify_packets(self, pkt, ports=[5, 6, 7])
        verify_no_other_packets(self)

        t=25
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 6, 7, 2))
        verify_packets(self, pkt, ports=[6, 7, 2])
        verify_no_other_packets(self)

        t=29
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 7, 2, 3))
        verify_packets(self, pkt, ports=[7, 2, 3])
        verify_no_other_packets(self)

        t=33
        pkt = pkt1/Event(type=0xbabe, timestamp=int(t))/(10*'x')
        print("Sending %d packets on port %d, t:%d" % (1, 1, t))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 2, 3, 4))
        verify_packets(self, pkt, ports=[3, 2, 3, 4])
        verify_no_other_packets(self)

    def tearDown(self):
        Interface.tearDown(self)
