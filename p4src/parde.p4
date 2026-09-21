#include <core.p4>
#include <tna.p4>

// ---------------------------------------------------------------------------
// Ingress parser
// ---------------------------------------------------------------------------
parser TofinoIngressParser(
        packet_in pkt,
        out ingress_intrinsic_metadata_t ig_intr_md) {
    state start {
        pkt.extract(ig_intr_md);

        transition select(ig_intr_md.resubmit_flag) {
            1 : parse_resubmit;
            0 : parse_port_metadata;
        }
    }

    state parse_resubmit {
        // Parse resubmitted packet here.
        transition reject;
    }

    state parse_port_metadata {
        pkt.advance(PORT_METADATA_SIZE);
        transition accept;
    }
}

parser SwitchIngressParser(
            packet_in pkt,
            out splitter_header_t hdr,
            out metadata_t ig_md,
            out ingress_intrinsic_metadata_t ig_intr_md) {

    TofinoIngressParser() tofino_parser;
    
    state start {
        tofino_parser.apply(pkt, ig_intr_md);
        ethernet_h tmp = pkt.lookahead<ethernet_h>();
        transition select(tmp.ether_type) {
            ETHERTYPE_BF_PKTGEN : parse_pktgen_header;
            default             : parse_ethernet;
        }
    }

    state parse_pktgen_header {
        pktgen_timer_header_t pktgen_pd_hdr = pkt.lookahead<pktgen_timer_header_t>();
        transition select(pktgen_pd_hdr.app_id) {
            1 : parse_pktgen_timer;
            // 2 : parse_pktgen_port_down;
            default : reject;
        }
    }

    state parse_pktgen_timer {
        pkt.extract(hdr.timer);
        transition accept;
    }

    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            ETHERTYPE_VLAN : parse_vlan;
            ETHERTYPE_IPV4 : parse_ipv4;
            default        : accept;
        }
    }

    state parse_vlan {
        pkt.extract(hdr.vlan);
        transition select(hdr.vlan.ether_type) {
            ETHERTYPE_IPV4 : parse_ipv4;
            default         : accept;
        }
    }

    state parse_ipv4 {
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            IP_PROTOCOLS_UDP : parse_udp;
            default : accept;
        }
    }

    state parse_udp {
        pkt.extract(hdr.udp);
        transition parse_event;
    }

    state parse_event {
        pkt.extract(hdr.event);
        transition accept;
    }
}

// ---------------------------------------------------------------------------
// Ingress Deparser
// ---------------------------------------------------------------------------
control SwitchIngressDeparser(
            packet_out pkt,
            inout splitter_header_t hdr,
            in metadata_t ig_md,
            in ingress_intrinsic_metadata_for_deparser_t dprsr_md) {
    apply {
        pkt.emit(hdr.bridged_md);
        pkt.emit(hdr.ethernet);
        pkt.emit(hdr.vlan);
        pkt.emit(hdr.ipv4);
        pkt.emit(hdr.udp);
        pkt.emit(hdr.event);
    }
}


//----------------------------------------------------------------------------
// Egress parser
//----------------------------------------------------------------------------
parser SwitchEgressParser(
            packet_in pkt,
            out splitter_header_t hdr,
            out metadata_t eg_md,
            out egress_intrinsic_metadata_t eg_intr_md) {

    @critical
    state start {
        pkt.extract(eg_intr_md);
        transition parse_bridged_md;
    }

    state parse_bridged_md {
        pkt.extract(hdr.bridged_md);
        // eg_md.ingress_port = hdr.bridged_md.base.ingress_port;
        eg_md.stream_id = hdr.bridged_md.stream_id;
        eg_md.max_operator_id = hdr.bridged_md.max_operator_id;
        eg_md.operator_id = hdr.bridged_md.operator_id;
        transition parse_ethernet;
    }
    
    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            ETHERTYPE_VLAN  : parse_vlan;
            default         : accept;
        }
    }
    
    state parse_vlan {
        pkt.extract(hdr.vlan);
        transition accept;
    }

}

//-----------------------------------------------------------------------------
// Egress Deparser
//-----------------------------------------------------------------------------
control SwitchEgressDeparser(
            packet_out pkt,
            inout splitter_header_t hdr,
            in metadata_t eg_md,
            in egress_intrinsic_metadata_for_deparser_t eg_intr_md_for_dprsr) {

    apply {
        pkt.emit(hdr.ethernet);
        pkt.emit(hdr.vlan);
    }
}
