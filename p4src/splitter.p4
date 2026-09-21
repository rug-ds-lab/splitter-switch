#include <core.p4>
#include <tna.p4>

#include "profile.p4"
#include "headers.p4"
#include "types.p4"
#include "parde.p4"
#include "count_windows.p4"
#include "time_windows.p4"

control SwitchIngress(
        inout splitter_header_t hdr,
        inout metadata_t ig_md,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
    
    // Count-Based Windows
    CountWindows() count_windows;
    // Time-Based Windows
    TimeWindows() time_windows;

    DirectCounter<bit<32>>(CounterType_t.PACKETS_AND_BYTES) operators_cntr;
    DirectCounter<bit<32>>(CounterType_t.PACKETS_AND_BYTES) stream_cntr;

    action drop_packet() { 
        ig_dprsr_md.drop_ctl = 0x1;
        exit;
    }

    // action send_to_port(PortId_t port) {
    //     ig_tm_md.ucast_egress_port = port;
    // }

    // action send_to_group(bit<16> mcast_grp) {
    //     ig_tm_md.mcast_grp_a = mcast_grp;
    // }

    /* Stateless Split Operator. */
    // action cep_topic_to_operator_mapping_single_hit(operator_id_t id) {
    //     meta.operator_id = id; /* can be virtual operator, hiding a pool of identical physical operators */
    // }

    // action cep_topic_to_operator_mapping_group_hit(bit<16> mcast_grp) {
    //     send_to_group(mcast_grp);
    // }

    // action cep_topic_to_operator_mapping_miss() {
    //     drop_packet();
    // }

    // /* Stateless load-balancing - topic-based task parallelism */
    // table cep_topic_to_operator_mapping {
    //     key = {
    //         hdr.event.type : exact;
    //     }
    //     actions = {
    //         cep_topic_to_operator_mapping_single_hit;
    //         cep_topic_to_operator_mapping_group_hit;
    //         cep_topic_to_operator_mapping_miss;
    //     }
    //     const default_action = cep_topic_to_operator_mapping_miss();
    //     size = 512;
    // }
    
    action count_based_window_init_hit(stream_id_t stream_id, window_size_t count, window_shift_t delta) {
        ig_md.stream_id = stream_id;
        ig_md.w_size  = count;
        ig_md.w_shift = delta;
        ig_md.w_type = 0x0;  /* 0x0 Count-based. */
        stream_cntr.count();
    }
    
    action time_based_window_init_hit(stream_id_t stream_id, local_timestamp_t duration, local_timestamp_t shift, bit<8> max_overlap) {
        ig_md.stream_id = stream_id;
        ig_md.w_duration  = duration;
        ig_md.t_shift  = shift;
        ig_md.max_overlap = max_overlap; // ig_md.w_duration / ig_md.t_shift: not supported in data plane.
        ig_md.w_type = 0x1;  /* 0x1 Time-based. */
        stream_cntr.count();
    }

    action window_init_miss() {
        stream_cntr.count();
        drop_packet();
    }

    table window_spec {
        key = {
            ig_intr_md.ingress_port : exact;
            hdr.ethernet.ether_type : exact;
        }
        actions = {
            count_based_window_init_hit;
            time_based_window_init_hit;
            window_init_miss;
        }
        size = NUM_STREAMS;
        counters = stream_cntr;
        // const entries = {
        //     // (188, 0x800) : count_based_window_init_hit(0, 100, 100);
        //     // (189, 0x800) : count_based_window_init_hit(1, 10, 10);
        //     (188, 0x800) : count_based_window_init_hit(0, 5, 1);
        //     (189, 0x800) : count_based_window_init_hit(1, 3, 1);

        //     // (188, 0x800) : time_based_window_init_hit(0, 1000, 1000, 0);
        //     // (189, 0x800) : time_based_window_init_hit(1, 2000, 2000, 0);
        //     // (188, 0x800) : time_based_window_init_hit(0, 1000, 500, 2);
        //     // (189, 0x800) : time_based_window_init_hit(1, 1500, 500, 3);
        // }
    }

    action operators_max_hit(operator_id_t num) {
        ig_md.max_operator_id = num;
    }

    table operators_max {
        key = {
            ig_md.stream_id : exact;
        }
        actions = {
            operators_max_hit;
        }
        // const entries = {
        //     (0) : operators_max_hit(6);
        //     (1) : operators_max_hit(4);
        // }
        size = NUM_STREAMS;
    }

    action operators_id_to_ip_hit(ipv4_addr_t ip) {
        operators_cntr.count();
        ig_md.operator_ip = ip;
    }

    action operators_output_port_hit(PortId_t port) {
        ig_tm_md.ucast_egress_port = port;
        operators_cntr.count();
    }

    action operators_output_port_vlan_hit(PortId_t port, bit<12> vlan) {
        ig_tm_md.ucast_egress_port = port;
        hdr.vlan.setValid();
        hdr.vlan.vlan_id = vlan;
        hdr.vlan.ether_type = hdr.ethernet.ether_type;
        hdr.ethernet.ether_type = ETHERTYPE_VLAN;
        operators_cntr.count();
    }

    action operators_miss() {
        operators_cntr.count();
        drop_packet();
    }

    table operators {
        key = {
            ig_md.stream_id   : exact;
            ig_md.operator_id : exact;
        }
        actions = {
            operators_id_to_ip_hit;  // ID to IP 
            operators_output_port_hit;  // ID to Port ID : directly connected operators
            operators_output_port_vlan_hit;
            operators_miss;
        }
        const default_action = operators_miss();
        size = NUM_OPERATORS;
        counters = operators_cntr;
        // const entries = {
        //     (0, 0) : operators_output_port_hit(190);
        //     (0, 1) : operators_output_port_hit(191);
        //     (0, 2) : operators_output_port_hit(172);
        //     (0, 3) : operators_output_port_hit(173);
        //     (0, 4) : operators_output_port_hit(174);
        //     (0, 5) : operators_output_port_hit(175);
        //     (1, 0) : operators_output_port_hit(191);
        //     (1, 1) : operators_output_port_hit(172);
        //     (1, 2) : operators_output_port_hit(173);
        //     (1, 3) : operators_output_port_hit(174);
        // }
    }

    action add_bridged_md(inout bridged_metadata_h bridged_md) {
        bridged_md.setValid();
        bridged_md.stream_id = ig_md.stream_id;
        bridged_md.max_operator_id = ig_md.max_operator_id;
        bridged_md.operator_id = ig_md.operator_id;
    }

    apply {
        window_spec.apply();
        operators_max.apply();
        
        if (ig_md.w_type == window_type.Count) {
            count_windows.apply(hdr, ig_md, ig_intr_md, ig_prsr_md, ig_dprsr_md, ig_tm_md);
        } else if (ig_md.w_type == window_type.Time) {
            time_windows.apply(hdr, ig_md, ig_intr_md, ig_prsr_md, ig_dprsr_md, ig_tm_md);
        }

        if (ig_md.is_overlapping == 0) {  /* Tumbling Windows. */
            operators.apply();
        }

        add_bridged_md(hdr.bridged_md);
    }
}

control SwitchEgress(
        inout splitter_header_t hdr,
        inout metadata_t eg_md,
        in egress_intrinsic_metadata_t eg_intr_md,
        in egress_intrinsic_metadata_from_parser_t eg_intr_md_from_prsr,
        inout egress_intrinsic_metadata_for_deparser_t eg_intr_md_for_dprsr,
        inout egress_intrinsic_metadata_for_output_port_t eg_intr_md_for_oport) {

    DirectCounter<bit<32>>(CounterType_t.PACKETS_AND_BYTES) egress_ports_cntr;
    DirectCounter<bit<32>>(CounterType_t.PACKETS_AND_BYTES) rid_cntr;

    // action drop_packet() { 
    //     eg_intr_md_for_dprsr.drop_ctl = 0x1;
    //     exit;
    // }

    action rid_vlan_hit(bit<12> vlan) {
        hdr.vlan.setValid();
        hdr.vlan.vlan_id = vlan;
        hdr.vlan.ether_type = hdr.ethernet.ether_type;
        hdr.ethernet.ether_type = ETHERTYPE_VLAN;
        rid_cntr.count();
    }

    action rid_miss() {
        rid_cntr.count();
    }

    table replicas {
        key = {
            eg_intr_md.egress_rid : exact;
        }
        actions = {
            rid_vlan_hit;
            rid_miss;
        }
        const default_action = rid_miss;
        counters = rid_cntr;
        size = NUM_OPERATORS;
    }

    action egress_ports_hit(mac_addr_t dst_addr) {
        hdr.ethernet.dst_addr = dst_addr;
        egress_ports_cntr.count();
    }

    table egress_ports {
        key = {
            eg_intr_md.egress_port : exact;
        }
        actions = {
            egress_ports_hit;
        }
        counters = egress_ports_cntr;
        size = 512;
        // const entries = {
        //     (190) : egress_ports_hit(0x001b21a59cbc);
        //     (191) : egress_ports_hit(0x001b21a59cbd);
        //     (172) : egress_ports_hit(0x001b21aeb314);
        //     (173) : egress_ports_hit(0x001b21aeb315);
        //     (174) : egress_ports_hit(0x649d99fff5e2);
        //     (175) : egress_ports_hit(0x649d99fff5e3);
        // }
    }
   
    apply {
        replicas.apply();
        egress_ports.apply();
    }
}

Pipeline(SwitchIngressParser(),
         SwitchIngress(),
         SwitchIngressDeparser(),
         SwitchEgressParser(),
         SwitchEgress(),
         SwitchEgressDeparser()) pipe;

Switch(pipe) main;
