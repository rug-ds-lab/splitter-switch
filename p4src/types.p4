#ifndef _P4_TYPES_
#define _P4_TYPES_

#define TUMBLING_COUNT_WND
// #define SLIDING_COUNT_WND
// #define SLIDING_COUNT_WND_1
// #define SLIDING_COUNT_WND_2

// #define TIME_WND_TOF // tofino timestamps
// #define TIME_WND_TUMBLING 
// #define TIME_WND_SLIDING 

typedef bit<32> local_timestamp_t;  /*error: Wide operations not supported in stateful alu, will only operate on bottom 32 bits*/
typedef bit<32> stream_id_t;  /* as Register Index */
typedef bit<16> operator_id_t;
typedef bit<8>  window_shift_t;
typedef bit<8>  window_size_t;
typedef bit<8>  position_id_t;
typedef bit<8>  error_type_t;
typedef bit<1>  flag_t;

enum bit<8> error_type {
    PacketOutOfOrder = 8w0x01, 
    Unknown = 8w0xff
}

/* Error detector. Reports to the host. */
struct err_digest_metadata_t {
    type_t event_type;
    error_type_t err_type;
}

struct metadata_t {
    stream_id_t    stream_id;
    operator_id_t  max_operator_id;
    operator_id_t  operator_id;
    ipv4_addr_t    operator_ip;
    flag_t         is_overlapping;
    flag_t         is_topic_based;  /* Task-based event partitioning. */
    flag_t         w_type;
    operator_id_t  last_operator;
    bit<1>         new_window;
    bit<8>         init;
    bit<1>         curr_window_stops;
    window_shift_t w_shift;  /* Window shift δ. */
    window_size_t  w_size;  /* Window size n. */
    position_id_t  w_position;  /* Count of events within the current ongoing window. */
    timestamp_t    prev_ts;
    err_digest_metadata_t  err_digest;
    local_timestamp_t  w_start_time;
    local_timestamp_t  w_end_time;
    local_timestamp_t  next_win_time;
    local_timestamp_t  w_duration;
    local_timestamp_t  t_shift;
    timestamp_t        timestamp;
    timestamp_t        watermark;  /* Late Event */
    bit<8>             max_overlap;  /* Window Overlap */
    bit<8>             curr_overlap;  /* Window Overlap */
    bit<16>            window_id;
    timestamp_t        time_t;
    local_timestamp_t  curr_win_end_time;
    local_timestamp_t  next_win_start_time;
    bit<1>             do_overlap;
    bit<1>              order_ok;
}

enum bit<1> window_type {
    Count = 1w0x0, 
    Time = 1w0x1
}

header bridged_metadata_h {
    stream_id_t    stream_id;
    operator_id_t  max_operator_id;
    operator_id_t  operator_id;
}

struct splitter_header_t {
    pktgen_timer_header_t timer;
    bridged_metadata_h    bridged_md;
    ethernet_h            ethernet;
    vlan_h                vlan;
    ipv4_h                ipv4;
    udp_h                 udp;
    event_h               event;
    attr_val_t[3]         attr_val;
    debug_h               debug;
}

#endif /* _P4_TYPES_ */
