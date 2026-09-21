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
 
"""

import logging

from ptf import config
import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import bfrt_grpc.bfruntime_pb2 as bfruntime_pb2
import bfrt_grpc.client as gc
import random
import time

num_pipes = int(testutils.test_param_get('num_pipes'))
pipes = list(range(num_pipes))

# Hitless HA Support
client_id = 0
p4_name = "splitter"
profile_name = 'pipe'
base_pick_path = testutils.test_param_get("base_pick_path")
base_put_path = testutils.test_param_get("base_put_path")
arch = testutils.test_param_get("arch")
testutils.test_param_get("arch") == "tofino"
if not base_pick_path:
  base_pick_path = "install/share/" + arch + "pd/"
if not base_put_path:
  base_put_path = "/tmp"

class Interface(BfRuntimeTest):
    def setUp(self):
        client_id = 0
        p4_name = "splitter"
        BfRuntimeTest.setUp(self, client_id, p4_name)
        self.bfrt_info = self.interface.bfrt_info_get(p4_name)
        self.target = gc.Target(device_id=0, pipe_id=0xffff)

    def window_spec_table_add_count_based_window_init_hit(self, port, type, stream_id, count, shift):
        # Window Specification table
        print("Adding Count-based Window : Port %d Stream Type 0x%x => Stream ID %d Count %d Shift %d" % (port, type, stream_id, count, shift))
        window_spec = self.bfrt_info.table_get("SwitchIngress.window_spec")
        key = window_spec.make_key([gc.KeyTuple('ig_intr_md.ingress_port', port), 
                                    gc.KeyTuple('hdr.ethernet.ether_type', type)])
        data = window_spec.make_data([gc.DataTuple('stream_id', stream_id),
                                        gc.DataTuple('count', count),
                                        gc.DataTuple('delta', shift)],
                                        'SwitchIngress.count_based_window_init_hit')
        window_spec.entry_add(self.target, [key], [data])

    def window_spec_table_mod_count_based_window_init_hit(self, port, type, stream_id, count, shift):
        print("Modifying Count-based Window : Port %d Stream Type 0x%x => Stream ID %d Count %d Shift %d" % (port, type, stream_id, count, shift))
        window_spec = self.bfrt_info.table_get("SwitchIngress.window_spec")
        key = window_spec.make_key([gc.KeyTuple('ig_intr_md.ingress_port', port), 
                                    gc.KeyTuple('hdr.ethernet.ether_type', type)])
        new_data = window_spec.make_data([gc.DataTuple('stream_id', stream_id),
                                        gc.DataTuple('count', count),
                                        gc.DataTuple('delta', shift)],
                                        'SwitchIngress.count_based_window_init_hit')
        window_spec.entry_mod(self.target, [key], [new_data])

    def window_spec_table_add_time_based_window_init_hit(self, port, ethertype, stream_id, duration, shift, overlap):
        # Window Specification table
        window_spec = self.bfrt_info.table_get("SwitchIngress.window_spec")
        print("Adding Time-based Window : Port %d Stream Type 0x%x => Stream ID %d Duration %d (in sec) Shift %d" % (port, ethertype, stream_id, duration, shift))
        key = window_spec.make_key([gc.KeyTuple('ig_intr_md.ingress_port', port), 
                                    gc.KeyTuple('hdr.ethernet.ether_type', ethertype)])
        data = window_spec.make_data([gc.DataTuple('stream_id', stream_id),
                                        gc.DataTuple('duration', duration),
                                        gc.DataTuple('shift', shift),
                                        gc.DataTuple('max_overlap', overlap)],
                                        'SwitchIngress.time_based_window_init_hit')
        window_spec.entry_add(self.target, [key], [data])

    def window_spec_table_mod_time_based_window_init_hit(self, port, ethertype, stream_id, duration, shift, overlap):
        # Window Specification table
        print("Modifying Time-based Window : Port %d Stream Type 0x%x => Stream ID %d Duration %d (in sec) Shift %d" % (port, ethertype, stream_id, duration, shift))
        window_spec = self.bfrt_info.table_get("SwitchIngress.window_spec")
        key = window_spec.make_key([gc.KeyTuple('ig_intr_md.ingress_port', port), 
                                    gc.KeyTuple('hdr.ethernet.ether_type', ethertype)])
        new_data = window_spec.make_data([gc.DataTuple('stream_id', stream_id),
                                        gc.DataTuple('duration', duration),
                                        gc.DataTuple('shift', shift),
                                        gc.DataTuple('max_overlap', overlap)],
                                        'SwitchIngress.time_based_window_init_hit')
        window_spec.entry_mod(self.target, [key], [new_data])

    def operators_max_table_add_operators_max_hit(self, stream_id, max):
        print("Maximum number of Operators : Stream ID %d  => Max Operator %d" % ( stream_id, max))
        operators_max = self.bfrt_info.table_get("SwitchIngress.operators_max")
        key = operators_max.make_key([gc.KeyTuple('ig_md.stream_id', stream_id)])
        data = operators_max.make_data([gc.DataTuple('num', max)],
                                        'SwitchIngress.operators_max_hit')
        operators_max.entry_add(self.target, [key], [data])

    def operators_max_table_mod_operators_max_hit(self, stream_id, max):
        print("Modifying Maximum number of Operators : Stream ID %d  => Max Operator %d" % (stream_id, max))
        operators_max = self.bfrt_info.table_get("SwitchIngress.operators_max")
        key = operators_max.make_key([gc.KeyTuple('ig_md.stream_id', stream_id)])
        new_data = operators_max.make_data([gc.DataTuple('num', max)],
                                        'SwitchIngress.operators_max_hit')
        operators_max.entry_mod(self.target, [key], [new_data])

    def egress_ports_table_add_egress_ports_hit(self, port, mac_addr):
        # print("Egress port table : Port %d  => Mac Addr %x" % ( port, mac_addr))
        egress_ports = self.bfrt_info.table_get("SwitchEgress.egress_ports")
        key = egress_ports.make_key([gc.KeyTuple('eg_intr_md.egress_port', port)])
        data = egress_ports.make_data([gc.DataTuple('dst_addr', mac_addr)], 'SwitchEgress.egress_ports_hit')
        egress_ports.entry_add(self.target, [key], [data])

    def operators_table_add_operators_output_port_hit(self, stream_id, operator_id, port):
        # CEP Operators table : Operator ID to Egress Port ID
        print("Adding Stream ID %d, Operator ID %d => Egress Port %d " % (stream_id, operator_id, port))
        operators = self.bfrt_info.table_get("SwitchIngress.operators")
        key = operators.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                     gc.KeyTuple('ig_md.operator_id', operator_id)])
        data = operators.make_data([gc.DataTuple('port', port)],
                                        'SwitchIngress.operators_output_port_hit')
        operators.entry_add(self.target, [key], [data])

    def operators_table_entry_del(self, stream_id, operator_id):
        # CEP Operators table : Operator ID to Egress Port ID
        print("Deleting Stream ID %d, Operator ID %d " % (stream_id, operator_id))
        operators = self.bfrt_info.table_get("SwitchIngress.operators")
        key = operators.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                     gc.KeyTuple('ig_md.operator_id', operator_id)])

        operators.entry_del(self.target, [key])

    def operators_table_mod_operators_output_port_hit(self, stream_id, operator_id, port):
        # CEP Operators table : Operator ID to Egress Port ID
        print("Modifying Stream ID %d, Operator ID %d => Egress Port %d " % (stream_id, operator_id, port))
        operators = self.bfrt_info.table_get("SwitchIngress.operators")
        key = operators.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                     gc.KeyTuple('ig_md.operator_id', operator_id)])
        new_data = operators.make_data([gc.DataTuple('port', port)],
                                        'SwitchIngress.operators_output_port_hit')
        operators.entry_mod(self.target, [key], [new_data])

    def operators_table_add_operators_output_port_vlan_hit(self, stream_id, operator_id, port, vlan):
        # CEP Operators table : Operator ID to Egress Port ID
        operators = self.bfrt_info.table_get("SwitchIngress.operators")
        key = operators.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                     gc.KeyTuple('ig_md.operator_id', operator_id)])
        data = operators.make_data([gc.DataTuple('port', port),
                                    gc.DataTuple('vlan', vlan)],
                                        'SwitchIngress.operators_output_port_vlan_hit')
        operators.entry_add(self.target, [key], [data])

    def replicas_table_add_rid_vlan_hit(self, rid, vlan):
        print("Adding Replica ID %d ==> VLAN ID %d  " % (rid, vlan))
        replicas = self.bfrt_info.table_get("SwitchEgress.replicas")
        key = replicas.make_key([gc.KeyTuple('eg_intr_md.egress_rid', rid)])
        data = replicas.make_data([gc.DataTuple('vlan', vlan)],
                                        'SwitchEgress.rid_vlan_hit')
        replicas.entry_add(self.target, [key], [data])

    def overlapping_windows_table_add_overlapping_windows_mcast_hit(self, stream_id, operator_id, curr_overlap, mcast_grp):
        print("Overlapping Windows Entry : Stream ID %d Opertor ID %d Current Overlap %d => MCast Group %d" % (stream_id, operator_id, curr_overlap, mcast_grp))
        overlapping_windows = self.bfrt_info.table_get("SwitchIngress.count_windows.overlapping_windows")
        key = overlapping_windows.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                        gc.KeyTuple('ig_md.operator_id', operator_id),
                                        gc.KeyTuple('ig_md.curr_overlap', curr_overlap)])
        data = overlapping_windows.make_data([gc.DataTuple('mcast_grp', mcast_grp)],
                                        'SwitchIngress.count_windows.overlapping_windows_mcast_hit')
        overlapping_windows.entry_add(self.target, [key], [data])

    def overlapping_windows_table_add_overlapping_windows_egress_hit(self, stream_id, operator_id, curr_overlap, port):
        print("Overlapping Windows Entry : Stream ID %d Opertor ID %d Current Overlap %d => Egress port %d" % (stream_id, operator_id, curr_overlap, port))
        overlapping_windows = self.bfrt_info.table_get("SwitchIngress.count_windows.overlapping_windows")
        key = overlapping_windows.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                        gc.KeyTuple('ig_md.operator_id', operator_id),
                                        gc.KeyTuple('ig_md.curr_overlap', curr_overlap)])
        data = overlapping_windows.make_data([gc.DataTuple('port', port)],
                                        'SwitchIngress.count_windows.overlapping_windows_egress_hit')
        overlapping_windows.entry_add(self.target, [key], [data])


    def time_sliding_windows_table_add_time_sliding_windows_egress_hit(self, stream_id, curr_overlap, operator_id, port):
        print("Time-based Sliding Windows Entry: Stream ID %d Opertor ID %d Current Overlap %d => Egress port %d" % (stream_id, operator_id, curr_overlap, port))
        time_sliding_windows = self.bfrt_info.table_get("SwitchIngress.time_windows.time_sliding_windows")
        key = time_sliding_windows.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                        gc.KeyTuple('ig_md.curr_overlap', curr_overlap),
                                        gc.KeyTuple('ig_md.operator_id', operator_id)])
        data = time_sliding_windows.make_data([gc.DataTuple('port', port)],
                                        'SwitchIngress.time_windows.time_sliding_windows_egress_hit')
        time_sliding_windows.entry_add(self.target, [key], [data])
    
    def time_sliding_windows_table_add_time_sliding_windows_mcast_hit(self, stream_id, curr_overlap, operator_id, mcast_grp):
        print("Time-based Sliding Windows Entry: Stream ID %d Opertor ID %d Current Overlap %d => MCast Group %d" % (stream_id, operator_id, curr_overlap, mcast_grp))
        time_sliding_windows = self.bfrt_info.table_get("SwitchIngress.time_windows.time_sliding_windows")
        key = time_sliding_windows.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                        gc.KeyTuple('ig_md.curr_overlap', curr_overlap),
                                        gc.KeyTuple('ig_md.operator_id', operator_id)])
        data = time_sliding_windows.make_data([gc.DataTuple('mcast_grp', mcast_grp)],
                                        'SwitchIngress.time_windows.time_sliding_windows_mcast_hit')
        time_sliding_windows.entry_add(self.target, [key], [data])


    def mc_mgrp_create(self, mcast_grp):
        print("Creating Mcast Group ID: %d " %  mcast_grp)
        self.mgid_table = self.bfrt_info.table_get("$pre.mgid")

        self.mgid_table.entry_add(
            self.target,
            [self.mgid_table.make_key([gc.KeyTuple('$MGID', mcast_grp)])])

    def mc_node_create(self, node_id, rid, mbr_ports, mbr_lags):
        print("Creating Mcast Node ID: %d " %  node_id)
        self.node_table = self.bfrt_info.table_get("$pre.node")

        self.node_table.entry_add(
            self.target,
            [self.node_table.make_key([gc.KeyTuple('$MULTICAST_NODE_ID', node_id)])],
            [self.node_table.make_data([gc.DataTuple('$MULTICAST_RID', rid),
                                                  gc.DataTuple('$MULTICAST_LAG_ID', int_arr_val=mbr_lags),
                                                  gc.DataTuple('$DEV_PORT', int_arr_val=mbr_ports)])])

    def mc_associate_node(self, mcast_grp, node_id, xid):
        print("Associate Node ID: %d to Mcast Group ID: %d " %  (node_id, mcast_grp))
        if xid is None:
            xid = 0
            use_xid = 0
        else:
            use_xid = 1

        self.mgid_table.entry_mod_inc(
            self.target,
            [self.mgid_table.make_key([gc.KeyTuple('$MGID', mcast_grp)])],
            [self.mgid_table.make_data([
                gc.DataTuple('$MULTICAST_NODE_ID', int_arr_val=[node_id]),
                gc.DataTuple('$MULTICAST_NODE_L1_XID_VALID', bool_arr_val=[use_xid]),
                gc.DataTuple('$MULTICAST_NODE_L1_XID', int_arr_val=[xid])])],
            bfruntime_pb2.TableModIncFlag.MOD_INC_ADD)

    def mc_dissociate_node(self, mgid, node_id):
        print("Dissociate Node ID: %d from Mcast Group ID: %d " %  (node_id, mgid))
        self.mgid_table.entry_mod_inc(
                self.target,
                [self.mgid_table.make_key([gc.KeyTuple('$MGID', mgid)])],
                [self.mgid_table.make_data([gc.DataTuple('$MULTICAST_NODE_ID', int_arr_val=[node_id]),
                                                      gc.DataTuple('$MULTICAST_NODE_L1_XID_VALID',
                                                                       bool_arr_val=[0]),
                                                      gc.DataTuple('$MULTICAST_NODE_L1_XID', int_arr_val=[0])])],
                bfruntime_pb2.TableModIncFlag.MOD_INC_DELETE)

        self.node_table.entry_del(
            self.target,
            [self.node_table.make_key([gc.KeyTuple('$MULTICAST_NODE_ID', node_id)])])

    def mc_mgrp_delete(self, mgid):
        print("Deleting Mcast Group ID: %d " %  mgid)
        self.mgid_table.entry_del(
                self.target,
                [self.mgid_table.make_key([gc.KeyTuple('$MGID', mgid)])])

    def operators_table_get_counter_operators_output_port_hit(self, stream_id, operator_id):
        operators = self.bfrt_info.table_get("SwitchIngress.operators")
        key = operators.make_key([gc.KeyTuple('ig_md.stream_id', stream_id),
                                     gc.KeyTuple('ig_md.operator_id', operator_id)])

        resp = operators.entry_get(self.target,
                                       [key],
                                       {"from_hw": True},
                                       operators.make_data(
                                           [gc.DataTuple("$COUNTER_SPEC_BYTES"),
                                            gc.DataTuple("$COUNTER_SPEC_PKTS")],
                                           'SwitchIngress.operators_output_port_hit', get=True)
                                       )
        # parse resp to get the counter
        data_dict = next(resp)[0].to_dict()
        recv_pkts = data_dict["$COUNTER_SPEC_PKTS"]
        recv_bytes = data_dict["$COUNTER_SPEC_BYTES"]

        return recv_pkts

    def tearDown(self):
        time.sleep(1)
        window_spec = self.bfrt_info.table_get("SwitchIngress.window_spec")
        operators = self.bfrt_info.table_get("SwitchIngress.operators")
        operators_max = self.bfrt_info.table_get("SwitchIngress.operators_max")
        egress_ports = self.bfrt_info.table_get("SwitchEgress.egress_ports")
        # overlapping_windows = self.bfrt_info.table_get("SwitchIngress.count_windows.overlapping_windows")
        # replicas = self.bfrt_info.table_get("SwitchEgress.replicas")
        # time_sliding_windows = self.bfrt_info.table_get("SwitchIngress.time_windows.time_sliding_windows")
        
        # Clean up
        window_spec.entry_del(self.target, [])
        operators_max.entry_del(self.target, [])
        operators.entry_del(self.target, [])
        egress_ports.entry_del(self.target, [])
        # overlapping_windows.entry_del(self.target, [])
        # replicas.entry_del(self.target, [])
        # time_sliding_windows.entry_del(self.target, [])

        BfRuntimeTest.tearDown(self)
