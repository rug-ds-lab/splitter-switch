//-----------------------------------------------------------------------------
// Protocol Header Definitions
//-----------------------------------------------------------------------------

#ifndef _P4_HEADERS_
#define _P4_HEADERS_

typedef bit<48> mac_addr_t;
typedef bit<16> ethertype_t;
typedef bit<32> ipv4_addr_t;
typedef bit<8>  ip_protocol_t;
typedef bit<16> seq_num_t;

typedef bit<16> type_t;
typedef bit<8>  attribute_t;
typedef bit<8>  value_t;
typedef bit<32> timestamp_t;

const ethertype_t ETHERTYPE_EVENT = 0x8819;
const ethertype_t ETHERTYPE_BF_PKTGEN = 0x9001;
const ethertype_t ETHERTYPE_VLAN      = 0x8100;
const ethertype_t ETHERTYPE_IPV4      = 0x0800;
const ip_protocol_t IP_PROTOCOLS_UDP  = 17;


@pa_container_size("ingress", "hdr.ethernet.src_addr", 16, 32)
@pa_container_size("ingress", "hdr.ethernet.dst_addr", 16, 32)
@pa_container_size("ingress", "hdr.ethernet.$valid", 16)
header ethernet_h {
    mac_addr_t  dst_addr;
    mac_addr_t  src_addr;
    ethertype_t ether_type;
}

header vlan_h {
    bit<4>  notUsed;
    bit<12> vlan_id;
    ethertype_t ether_type;
}

header ipv4_h {
    bit<4> version;
    bit<4> ihl;
    bit<8> diffserv;
    bit<16> total_len;
    bit<16> identification;
    bit<3> flags;
    bit<13> frag_offset;
    bit<8> ttl;
    bit<8> protocol;
    bit<16> hdr_checksum;
    ipv4_addr_t src_addr;
    ipv4_addr_t dst_addr;
}

header udp_h {
    bit<16> src_port;
    bit<16> dst_port;
    bit<16> hdr_length;
    bit<16> checksum;
}

header event_h {
    type_t      type;  /* example: weather. */
//     attribute_t attribute;  /* example: humidity, temperature. */
//     value_t     value;      /* example: 45% humidity and 23 degrees celsius temperature . */
    timestamp_t timestamp;  /* time of occurance of the event. */
}

header attr_val_t {
    attribute_t attribute;  
    value_t value;  
    bit<7>  reserved;
    bit<1>  bos;
}

header debug_h {
    bit<48> ingress_time;
    bit<48> w_start_time;
    bit<48> w_end_time;
    bit<16> w_id;
}

#endif /* _P4_HEADERS_ */
