#ifndef _P4_TIME_WINDOWS_
#define _P4_TIME_WINDOWS_

/* Time-based windowing */
control TimeWindows(
        inout splitter_header_t hdr,
        inout metadata_t ig_md,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {

    DirectCounter<bit<32>>(CounterType_t.PACKETS_AND_BYTES) time_sliding_windows_cntr;
    //TIME_WND_1
    Register<operator_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) current_operator_reg;
    Register<local_timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) end_time_reg;
    Register<local_timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) next_win_time_reg;
    Register<local_timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) next_ts_reg;
    //TIME_WND_2
    Register<timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) next_window_start_reg;
    Register<timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) curr_window_end_reg;
    Register<timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) start_time_reg;
    Register<timestamp_t, bit<32>>(size=NUM_STREAMS, initial_value=0) watermark_reg;
    Register<operator_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) current_operator_reg2;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) overlap_reg;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) init_reg;
    
    action drop_packet() {
        ig_dprsr_md.drop_ctl = 0x1;
        // exit;
    }
   
    action time_sliding_windows_egress_hit(PortId_t port) {
        ig_tm_md.ucast_egress_port = port;
        time_sliding_windows_cntr.count();
    }

    action time_sliding_windows_mcast_hit(bit<16> mcast_grp) {
        ig_tm_md.mcast_grp_a = mcast_grp;
        time_sliding_windows_cntr.count();
    }

    action time_sliding_windows_miss() {
        time_sliding_windows_cntr.count();
        drop_packet();
    }

    table time_sliding_windows {
        key = {
            ig_md.stream_id    : exact;
            ig_md.curr_overlap : exact;
            ig_md.operator_id  : exact;
        }
        actions = {
            time_sliding_windows_egress_hit;
            time_sliding_windows_mcast_hit;
            time_sliding_windows_miss;
        }
        counters = time_sliding_windows_cntr;        
        size = NUM_ENTRIES_TBSW;
        // const entries = {
        //     (0,0,0) : time_sliding_windows_egress_hit(190);
        //     (0,1,1) : time_sliding_windows_mcast_hit(1);
        //     (0,2,2) : time_sliding_windows_mcast_hit(2);
        //     (0,2,3) : time_sliding_windows_mcast_hit(3);
        //     (0,2,4) : time_sliding_windows_mcast_hit(4);
        //     (0,2,5) : time_sliding_windows_mcast_hit(5);
        //     (0,2,0) : time_sliding_windows_mcast_hit(6);
        //     (0,2,1) : time_sliding_windows_mcast_hit(1);
        //     // (1,0,0) : time_sliding_windows_egress_hit(190);
        //     // (1,1,1) : time_sliding_windows_mcast_hit(1);
        //     // (1,2,2) : time_sliding_windows_mcast_hit(2);
        //     // (1,3,3) : time_sliding_windows_mcast_hit(3);
        //     // (1,3,4) : time_sliding_windows_mcast_hit(4);
        //     // (1,3,5) : time_sliding_windows_mcast_hit(5);
        //     // (1,3,0) : time_sliding_windows_mcast_hit(6);
        //     // (1,3,1) : time_sliding_windows_mcast_hit(7);
        //     // (1,3,2) : time_sliding_windows_mcast_hit(2);
        // }
    }

    RegisterAction<operator_id_t, bit<32>, operator_id_t>(current_operator_reg) curr_operator_reg_action = {
        void apply(inout operator_id_t value, out operator_id_t rv) {   
            if (value >= ig_md.max_operator_id - 1) {
                value = 0;
            } else {
                value = value + 1;
            }
            rv = value;
        }
    };

    action operator_register_action(bit<32> idx) {
        ig_md.operator_id = curr_operator_reg_action.execute(idx);
    }

    RegisterAction<operator_id_t, bit<32>, operator_id_t>(current_operator_reg2) curr_operator_reg_action2 = {
        void apply(inout operator_id_t value, out operator_id_t rv) {   
            if (value > ig_md.max_operator_id - 1) {
                value = 1;
            } else {
                value = value + 1;
            }
            rv = value;
        }
    };

    action operator_register_action2(bit<32> idx) {
        ig_md.operator_id = curr_operator_reg_action2.execute(idx) - 1;
    }

    RegisterAction<local_timestamp_t, bit<32>, bit<1>>(next_win_time_reg) update_next_window_time = {
        void apply(inout local_timestamp_t value, out bit<1> flag) {
            if (0 > value - (bit<32>) ig_intr_md.ingress_mac_tstamp) {
                value = ig_md.next_win_time;
                flag = 1;
            } else {
                flag = 0;
            }
        }
    };

    action update_next_window_time_action(bit<32> idx) {
        ig_md.new_window = update_next_window_time.execute(idx);
    }

    RegisterAction<local_timestamp_t, bit<32>, bit<1>>(end_time_reg) update_window_end_time = {
        void apply(inout local_timestamp_t value, out bit<1> flag) { 
            if (0 > value - (bit<32>) ig_intr_md.ingress_mac_tstamp) {
                value = ig_md.w_end_time;
                flag = 1;
            } else {
                flag = 0;
            }
        }
    };

    action update_window_end_time_action(bit<32> idx) {
        ig_md.curr_window_stops = update_window_end_time.execute(idx);
    }

    RegisterAction<position_id_t, bit<32>, position_id_t>(init_reg) init_reg_action = {
        void apply(inout position_id_t value, out position_id_t flag) {   
            if (value == 0) {
                value = value + 1;
                flag = 1;
            } else {
                flag = 0; 
            }
        }
    };

    action init_register_action(bit<32> idx) {
        ig_md.init = init_reg_action.execute(idx);
    }

    RegisterAction<position_id_t, bit<32>, position_id_t>(overlap_reg) overlap_reg_action = {
        void apply(inout position_id_t value, out position_id_t rv) {   
            if (value < ig_md.max_overlap) {
                value = value + 1;
            }
            rv = value;
        }
    };

    action overlap_register_action(bit<32> idx) {
        ig_md.curr_overlap = overlap_reg_action.execute(idx);
    }

    RegisterAction<timestamp_t, bit<32>, bit<1>>(next_window_start_reg) update_next_window_start = {
        void apply(inout timestamp_t value, out bit<1> flag) {
            // if (0 >= value - (bit<32>) hdr.udp.src_port) {
            if (0 >= value - ig_md.timestamp) {
                value = ig_md.next_win_start_time;
                flag = 1;
            } else {
                flag = 0;
            }
        }
    };

    action update_next_window_start_action(bit<32> idx) {
        ig_md.new_window = update_next_window_start.execute(idx);
    }

    RegisterAction<timestamp_t, bit<32>, bit<1>>(curr_window_end_reg) update_curr_window_end = {
        void apply(inout timestamp_t value, out bit<1> flag) { 
            // if (0 >= value - (bit<32>) hdr.udp.src_port) {
            if (0 >= value - ig_md.timestamp) {
                value = ig_md.curr_win_end_time;
                flag = 1;
            } 
            else {
                flag = 0;
            }
        }
    };

    action update_curr_window_end_action(bit<32> idx) {
        ig_md.curr_window_stops = update_curr_window_end.execute(idx);
    }

    RegisterAction<timestamp_t, bit<32>, timestamp_t>(start_time_reg) store_curr_win_start_time = {
        void apply(inout timestamp_t value, out timestamp_t rv) { 
            value = ig_md.timestamp;
            rv = value;
        }
    };

    action store_curr_win_start_time_action(bit<32> idx) {
        store_curr_win_start_time.execute(idx);
    }

    RegisterAction<timestamp_t, bit<32>, timestamp_t>(watermark_reg) store_oldest_win_start_time = {
        void apply(inout timestamp_t value, out timestamp_t rv) { 
            value = ig_md.watermark;
            rv = value;
        }
    };

    action store_oldest_win_start_time_action(bit<32> idx) {
        store_oldest_win_start_time.execute(idx);
    }

    apply { 
        if (hdr.udp.isValid()) {
            ig_md.timestamp = hdr.event.timestamp;
    #if defined(TIME_WND_TUMBLING)
            if (0 <= ig_md.t_shift - ig_md.w_duration) {  /* Tumbling Windows */
                ig_md.is_overlapping = 0;
                ig_md.curr_win_end_time = ig_md.timestamp + ig_md.w_duration;
                ig_md.next_win_start_time = ig_md.timestamp + ig_md.t_shift;
                ig_md.watermark = ig_md.timestamp - ig_md.t_shift;

                init_register_action(ig_md.stream_id);
                update_curr_window_end_action(ig_md.stream_id);
                update_next_window_start_action(ig_md.stream_id);
                // update_new_window_start_time_action(ig_md.stream_id);
                // if (ig_md.curr_window_stops == 1 && ig_md.new_window == 0) {  // next window did not start yet but current window ends
                //     drop_packet();
                // } else 
                if (ig_md.new_window == 1 && ig_md.init == 0) {  /* new window, next operator */
                    operator_register_action(ig_md.stream_id);
                    store_curr_win_start_time_action(ig_md.stream_id);
                    store_oldest_win_start_time_action(ig_md.stream_id);
                } else {  // next window did not start yet.
                    ig_md.operator_id = current_operator_reg.read(ig_md.stream_id);  // keep sending to same operator
                    ig_md.w_start_time = start_time_reg.read(ig_md.stream_id);
                    ig_md.watermark = watermark_reg.read(ig_md.stream_id);
                    // if (0 > ig_md.timestamp - ig_md.w_start_time
                    //         && 0 > ig_md.watermark - ig_md.timestamp) {
                    //     if (ig_md.operator_id == 0) {
                    //         ig_md.operator_id = ig_md.max_operator_id;
                    //     } else {
                    //         ig_md.operator_id = ig_md.operator_id - 1; 
                    //     }
                    // } else if (0 > ig_md.timestamp - ig_md.watermark) {
                    //     drop_packet();  // too late 
                    // }
                }
            }
    #endif 
    #if defined(TIME_WND_SLIDING)
    // else { // if (0 > ig_md.w_duration - ig_md.t_shift) {  /* Sliding Windows */
                ig_md.is_overlapping = 1;
                ig_md.curr_win_end_time = ig_md.timestamp + ig_md.w_duration; 
                ig_md.next_win_start_time = ig_md.timestamp + ig_md.t_shift; 
                ig_md.watermark = ig_md.timestamp - ig_md.t_shift;

                init_register_action(ig_md.stream_id);
                update_next_window_start_action(ig_md.stream_id); // if (timestamp > value); value = ig_md.next_win_start_time;
                update_curr_window_end_action(ig_md.stream_id); // if (timestamp> value); value = ig_md.curr_win_end_time;
                if (ig_md.new_window == 1 && ig_md.init == 0) {  /* new window, next operator */
                    operator_register_action(ig_md.stream_id);
                    // Only for ramp up phase
                    overlap_register_action(ig_md.stream_id);
                    store_curr_win_start_time_action(ig_md.stream_id);
                    store_oldest_win_start_time_action(ig_md.stream_id);
                } else {  // next window did not start yet.
                    ig_md.curr_overlap = overlap_reg.read(ig_md.stream_id); // keep sending to same operator
                    ig_md.operator_id = current_operator_reg.read(ig_md.stream_id);  
                    ig_md.time_t = start_time_reg.read(ig_md.stream_id);
                    ig_md.watermark = watermark_reg.read(ig_md.stream_id);
                }

                // FIXME: generic condition does not compile
                // if ((bit<32>) hdr.udp.src_port - ig_md.time_t < 0 ) {
                        // && 0 > ig_md.watermark - (bit<32>) hdr.udp.src_port) {
                // if (ig_md.timestamp == 2) {
                //     if (ig_md.operator_id == 0) {
                //         ig_md.operator_id = ig_md.max_operator_id;
                //     } else {
                //         ig_md.operator_id = ig_md.operator_id - 1; 
                //     }
                //     ig_md.curr_overlap = ig_md.curr_overlap - 1;
                // }
                
                time_sliding_windows.apply();
            #endif // }
        } else {
#if defined(TIME_WND_TOF)
            if (0 <= ig_md.t_shift - ig_md.w_duration) {  /* Tumbling Windows */
                ig_md.is_overlapping = 0;
                ig_md.w_end_time = (bit<32>) ig_intr_md.ingress_mac_tstamp + ig_md.w_duration; 
                ig_md.next_win_time = (bit<32>) ig_intr_md.ingress_mac_tstamp + ig_md.t_shift;
                update_next_window_time_action(ig_md.stream_id);
                update_window_end_time_action(ig_md.stream_id);
                if (ig_md.curr_window_stops == 1 && ig_md.new_window == 0) {  // next window did not start yet but current window ends
                    drop_packet();
                } else if (ig_md.new_window == 1) {  /* new window, next operator */
                    operator_register_action(ig_md.stream_id);
                } else {  // next window did not start yet.
                    ig_md.operator_id = current_operator_reg.read(ig_md.stream_id);  // keep sending to same operator
                }
            } 
#endif       
        }
    }
}

#endif /* _P4_TIME_WINDOWS_ */
