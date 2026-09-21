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


class SlidingCountBasedWindowShift1(Interface):
    def setUp(self):
        Interface.setUp(self)

    def runTest(self):
        type=0x8809
        count=3
        shift=1
        stream_id=1
        self.window_spec_table_add_count_based_window_init_hit(port=1, type=type, stream_id=stream_id, count=count, shift=shift)
        self.operators_max_table_add_operators_max_hit(stream_id=stream_id, max=6)

        # egress_hit 0 0 0 => 2
        self.overlapping_windows_table_add_overlapping_windows_egress_hit(stream_id=stream_id, operator_id=0, curr_overlap=0, port=2)

        # mcast_hit 0 1 1 => 1
        # mcast_hit 0 2 2 => 2
        # mcast_hit 0 3 2 => 3
        # mcast_hit 0 4 2 => 4
        # mcast_hit 0 5 2 => 5
        # mcast_hit 0 0 2 => 6
        # mcast_hit 0 1 2 => 7
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=1, curr_overlap=1, mcast_grp=1)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=2, curr_overlap=2, mcast_grp=2)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=3, curr_overlap=2, mcast_grp=3)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=4, curr_overlap=2, mcast_grp=4)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=5, curr_overlap=2, mcast_grp=5)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=0, curr_overlap=2, mcast_grp=6)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=1, curr_overlap=2, mcast_grp=7)

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

        pkt = simple_eth_packet(pktlen=104, eth_dst='00:22:33:44:55:66', eth_src='00:11:22:33:44:55', eth_type=type)

        # Packet #1
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on port %d" % (1, 2))
        verify_packets(self, pkt, ports=[2])
        verify_no_other_packets(self)

        # Packet #2
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d and %d" % (2, 2, 3))
        verify_packets(self, pkt, ports=[2, 3])
        verify_no_other_packets(self)

        # Packet #3
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 2, 3, 4))
        verify_packets(self, pkt, ports=[2, 3, 4])
        verify_no_other_packets(self)

        # Packet #4
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 3, 4, 5))
        verify_packets(self, pkt, ports=[3, 4, 5])
        verify_no_other_packets(self)

        # Packet #5
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 4, 5, 6))
        verify_packets(self, pkt, ports=[4, 5, 6])
        verify_no_other_packets(self)

        # Packet #6
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 5, 6, 7))
        verify_packets(self, pkt, ports=[5, 6, 7])
        verify_no_other_packets(self)

        # Packet #7
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 6, 7, 2))
        verify_packets(self, pkt, ports=[6, 7, 2])
        verify_no_other_packets(self)

        # Packet #8
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 7, 2, 3))
        verify_packets(self, pkt, ports=[7, 2, 3])
        verify_no_other_packets(self)

        # Packet #9
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 2, 3, 4))
        verify_packets(self, pkt, ports=[2, 3, 4])
        verify_no_other_packets(self)

        time.sleep(1)
        # Clean up 
        self.mc_dissociate_node(mgid=1, node_id=1)
        self.mc_dissociate_node(mgid=1, node_id=2)
        self.mc_mgrp_delete(mgid=1)

        self.mc_dissociate_node(mgid=2, node_id=3)
        self.mc_dissociate_node(mgid=2, node_id=4)
        self.mc_dissociate_node(mgid=2, node_id=5)
        self.mc_mgrp_delete(mgid=2)

        self.mc_dissociate_node(mgid=3, node_id=6)
        self.mc_dissociate_node(mgid=3, node_id=7)
        self.mc_dissociate_node(mgid=3, node_id=8)
        self.mc_mgrp_delete(mgid=3)

        self.mc_dissociate_node(mgid=4, node_id=9)
        self.mc_dissociate_node(mgid=4, node_id=10)
        self.mc_dissociate_node(mgid=4, node_id=11)
        self.mc_mgrp_delete(mgid=4)

        self.mc_dissociate_node(mgid=5, node_id=12)
        self.mc_dissociate_node(mgid=5, node_id=13)
        self.mc_dissociate_node(mgid=5, node_id=14)
        self.mc_mgrp_delete(mgid=5)

        self.mc_dissociate_node(mgid=6, node_id=15)
        self.mc_dissociate_node(mgid=6, node_id=16)
        self.mc_dissociate_node(mgid=6, node_id=17)
        self.mc_mgrp_delete(mgid=6)

        self.mc_dissociate_node(mgid=7, node_id=18)
        self.mc_dissociate_node(mgid=7, node_id=19)
        self.mc_dissociate_node(mgid=7, node_id=20)
        self.mc_mgrp_delete(mgid=7)

    def tearDown(self):
        Interface.tearDown(self)


class SlidingCountBasedWindowShift1_Win5_1_Op6(Interface):
    def setUp(self):
        Interface.setUp(self)

    def runTest(self):
        type=0x8809
        count=5
        shift=1
        stream_id=123
        self.window_spec_table_add_count_based_window_init_hit(port=1, type=type, stream_id=stream_id, count=count, shift=shift)
        self.operators_max_table_add_operators_max_hit(stream_id=stream_id, max=6)

        # egress_hit 0 0 0 => 2
        # mcast_hit 0 1 1 => 1
        # mcast_hit 0 2 2 => 2
        # mcast_hit 0 3 3 => 3
        # mcast_hit 0 4 4 => 4
        # mcast_hit 0 5 4 => 5
        # mcast_hit 0 0 4 => 6
        # mcast_hit 0 1 4 => 7
        # mcast_hit 0 2 4 => 8
        # mcast_hit 0 3 4 => 9
        self.overlapping_windows_table_add_overlapping_windows_egress_hit(stream_id=stream_id, operator_id=0, curr_overlap=0, port=2)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=1, curr_overlap=1, mcast_grp=1)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=2, curr_overlap=2, mcast_grp=2)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=3, curr_overlap=3, mcast_grp=3)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=4, curr_overlap=4, mcast_grp=4)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=5, curr_overlap=4, mcast_grp=5)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=0, curr_overlap=4, mcast_grp=6)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=1, curr_overlap=4, mcast_grp=7)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=2, curr_overlap=4, mcast_grp=8)
        self.overlapping_windows_table_add_overlapping_windows_mcast_hit(stream_id=stream_id, operator_id=3, curr_overlap=4, mcast_grp=9)

        self.mc_mgrp_create(mcast_grp=1)
        self.mc_mgrp_create(mcast_grp=2)
        self.mc_mgrp_create(mcast_grp=3)
        self.mc_mgrp_create(mcast_grp=4)
        self.mc_mgrp_create(mcast_grp=5)
        self.mc_mgrp_create(mcast_grp=6)
        self.mc_mgrp_create(mcast_grp=7)
        self.mc_mgrp_create(mcast_grp=8)
        self.mc_mgrp_create(mcast_grp=9)

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

        self.mc_node_create(node_id=6, rid=6, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=6, xid=1)
        self.mc_node_create(node_id=7, rid=7, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=7, xid=1)
        self.mc_node_create(node_id=8, rid=8, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=8, xid=1)
        self.mc_node_create(node_id=9, rid=9, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=3, node_id=9, xid=1)

        self.mc_node_create(node_id=10, rid=10, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=10, xid=1)
        self.mc_node_create(node_id=11, rid=11, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=11, xid=1)
        self.mc_node_create(node_id=12, rid=12, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=12, xid=1)
        self.mc_node_create(node_id=13, rid=13, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=13, xid=1)
        self.mc_node_create(node_id=14, rid=14, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=4, node_id=14, xid=1)

        self.mc_node_create(node_id=15, rid=15, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=15, xid=1)
        self.mc_node_create(node_id=16, rid=16, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=16, xid=1)
        self.mc_node_create(node_id=17, rid=17, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=17, xid=1)
        self.mc_node_create(node_id=18, rid=18, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=18, xid=1)
        self.mc_node_create(node_id=19, rid=19, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=5, node_id=19, xid=1)

        self.mc_node_create(node_id=20, rid=20, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=20, xid=1)
        self.mc_node_create(node_id=21, rid=21, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=21, xid=1)
        self.mc_node_create(node_id=22, rid=22, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=22, xid=1)
        self.mc_node_create(node_id=23, rid=23, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=23, xid=1)
        self.mc_node_create(node_id=24, rid=24, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=6, node_id=24, xid=1)

        self.mc_node_create(node_id=25, rid=25, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=25, xid=1)
        self.mc_node_create(node_id=26, rid=26, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=26, xid=1)
        self.mc_node_create(node_id=27, rid=27, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=27, xid=1)
        self.mc_node_create(node_id=28, rid=28, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=28, xid=1)
        self.mc_node_create(node_id=29, rid=29, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=7, node_id=29, xid=1)

        self.mc_node_create(node_id=30, rid=30, mbr_ports=[6], mbr_lags=[])
        self.mc_associate_node(mcast_grp=8, node_id=30, xid=1)
        self.mc_node_create(node_id=31, rid=31, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=8, node_id=31, xid=1)
        self.mc_node_create(node_id=32, rid=32, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=8, node_id=32, xid=1)
        self.mc_node_create(node_id=33, rid=33, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=8, node_id=33, xid=1)
        self.mc_node_create(node_id=34, rid=34, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=8, node_id=34, xid=1)

        self.mc_node_create(node_id=35, rid=35, mbr_ports=[7], mbr_lags=[])
        self.mc_associate_node(mcast_grp=9, node_id=35, xid=1)
        self.mc_node_create(node_id=36, rid=36, mbr_ports=[2], mbr_lags=[])
        self.mc_associate_node(mcast_grp=9, node_id=36, xid=1)
        self.mc_node_create(node_id=37, rid=37, mbr_ports=[3], mbr_lags=[])
        self.mc_associate_node(mcast_grp=9, node_id=37, xid=1)
        self.mc_node_create(node_id=38, rid=38, mbr_ports=[4], mbr_lags=[])
        self.mc_associate_node(mcast_grp=9, node_id=38, xid=1)
        self.mc_node_create(node_id=39, rid=39, mbr_ports=[5], mbr_lags=[])
        self.mc_associate_node(mcast_grp=9, node_id=39, xid=1)

        pkt = simple_eth_packet(pktlen=104, eth_dst='00:22:33:44:55:66', eth_src='00:11:22:33:44:55', eth_type=type)

        # Packet #1
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on port %d" % (1, 2))
        verify_packets(self, pkt, ports=[2])
        verify_no_other_packets(self)

        # Packet #2
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d and %d" % (2, 2, 3))
        verify_packets(self, pkt, ports=[2, 3])
        verify_no_other_packets(self)

        # Packet #3
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d and %d" % (3, 2, 3, 4))
        verify_packets(self, pkt, ports=[2, 3, 4])
        verify_no_other_packets(self)

        # Packet #4
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d and %d" % (4, 2, 3, 4, 5))
        verify_packets(self, pkt, ports=[2, 3, 4, 5])
        verify_no_other_packets(self)

        # Packet #5
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 2, 3, 4, 5, 6))
        verify_packets(self, pkt, ports=[2, 3, 4, 5, 6])
        verify_no_other_packets(self)

        # Packet #6
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 3, 4, 5, 6, 7))
        verify_packets(self, pkt, ports=[3, 4, 5, 6, 7])
        verify_no_other_packets(self)

        # Packet #7
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 4, 5, 6, 7, 2))
        verify_packets(self, pkt, ports=[4, 5, 6, 7, 2])
        verify_no_other_packets(self)

        # Packet #8
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 5, 6, 7, 2, 3))
        verify_packets(self, pkt, ports=[5, 6, 7, 2, 3])
        verify_no_other_packets(self)

        # Packet #9
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 6, 7, 2, 3, 4))
        verify_packets(self, pkt, ports=[6, 7, 2, 3, 4])
        verify_no_other_packets(self)

        # Packet #10
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 7, 2, 3, 4, 5))
        verify_packets(self, pkt, ports=[7, 2, 3, 4, 5])
        verify_no_other_packets(self)

        # Packet #11
        print("Sending %d packets on port %d" % (1, 1))
        send_packet(self, port_id=1, pkt=pkt, count=1)
        print("Expecting %d packets on ports %d, %d, %d, %d and %d" % (5, 2, 3, 4, 5, 6))
        verify_packets(self, pkt, ports=[2, 3, 4, 5, 6])
        verify_no_other_packets(self)


    def tearDown(self):
        # Clean up
        time.sleep(1)

        self.mc_dissociate_node(mgid=1, node_id=1)
        self.mc_dissociate_node(mgid=1, node_id=2)
        self.mc_mgrp_delete(mgid=1)

        self.mc_dissociate_node(mgid=2, node_id=3)
        self.mc_dissociate_node(mgid=2, node_id=4)
        self.mc_dissociate_node(mgid=2, node_id=5)
        self.mc_mgrp_delete(mgid=2)

        self.mc_dissociate_node(mgid=3, node_id=6)
        self.mc_dissociate_node(mgid=3, node_id=7)
        self.mc_dissociate_node(mgid=3, node_id=8)
        self.mc_dissociate_node(mgid=3, node_id=9)
        self.mc_mgrp_delete(mgid=3)

        self.mc_dissociate_node(mgid=4, node_id=10)
        self.mc_dissociate_node(mgid=4, node_id=11)
        self.mc_dissociate_node(mgid=4, node_id=12)
        self.mc_dissociate_node(mgid=4, node_id=13)
        self.mc_dissociate_node(mgid=4, node_id=14)
        self.mc_mgrp_delete(mgid=4)

        self.mc_dissociate_node(mgid=5, node_id=15)
        self.mc_dissociate_node(mgid=5, node_id=16)
        self.mc_dissociate_node(mgid=5, node_id=17)
        self.mc_dissociate_node(mgid=5, node_id=18)
        self.mc_dissociate_node(mgid=5, node_id=19)
        self.mc_mgrp_delete(mgid=5)

        self.mc_dissociate_node(mgid=6, node_id=20)
        self.mc_dissociate_node(mgid=6, node_id=21)
        self.mc_dissociate_node(mgid=6, node_id=22)
        self.mc_dissociate_node(mgid=6, node_id=23)
        self.mc_dissociate_node(mgid=6, node_id=24)
        self.mc_mgrp_delete(mgid=6)

        self.mc_dissociate_node(mgid=7, node_id=25)
        self.mc_dissociate_node(mgid=7, node_id=26)
        self.mc_dissociate_node(mgid=7, node_id=27)
        self.mc_dissociate_node(mgid=7, node_id=28)
        self.mc_dissociate_node(mgid=7, node_id=29)
        self.mc_mgrp_delete(mgid=7)

        self.mc_dissociate_node(mgid=8, node_id=30)
        self.mc_dissociate_node(mgid=8, node_id=31)
        self.mc_dissociate_node(mgid=8, node_id=32)
        self.mc_dissociate_node(mgid=8, node_id=33)
        self.mc_dissociate_node(mgid=8, node_id=34)
        self.mc_mgrp_delete(mgid=8)

        self.mc_dissociate_node(mgid=9, node_id=35)
        self.mc_dissociate_node(mgid=9, node_id=36)
        self.mc_dissociate_node(mgid=9, node_id=37)
        self.mc_dissociate_node(mgid=9, node_id=38)
        self.mc_dissociate_node(mgid=9, node_id=39)
        self.mc_mgrp_delete(mgid=9)
        Interface.tearDown(self)

