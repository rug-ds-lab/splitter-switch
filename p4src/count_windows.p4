//-----------------------------------------------------------------------------
// Window Semantics
//-----------------------------------------------------------------------------

#ifndef _P4_COUNT_WINDOWS_
#define _P4_COUNT_WINDOWS_

control CountWindows(
        inout splitter_header_t hdr,
        inout metadata_t ig_md,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {

    DirectCounter<bit<32>>(CounterType_t.PACKETS_AND_BYTES) overlapping_windows_cntr;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) pos_reg;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) pos_reg2;
    Register<operator_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) current_operator_reg;
    Register<operator_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) current_operator_reg2;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) overlap_reg;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) overlap_reg2;
    Register<position_id_t, bit<32>>(size=NUM_STREAMS, initial_value=0) init_reg;

    action drop_packet() { 
        ig_dprsr_md.drop_ctl = 0x1;
        // exit;
    }

    action overlapping_windows_egress_hit(PortId_t port) {
        ig_tm_md.ucast_egress_port = port;
        overlapping_windows_cntr.count();
    }

    action overlapping_windows_mcast_hit(bit<16> mcast_grp) {
        ig_tm_md.mcast_grp_a = mcast_grp;
        overlapping_windows_cntr.count();
    }

    action overlapping_windows_miss() {
        overlapping_windows_cntr.count();
        drop_packet();
    }

    table overlapping_windows {
        key = {
            ig_md.stream_id    : exact;
            ig_md.operator_id  : exact;
            ig_md.curr_overlap : exact;
        }
        actions = {
            overlapping_windows_egress_hit;
            overlapping_windows_mcast_hit;
            overlapping_windows_miss;
        }
        const default_action = overlapping_windows_miss();
        // const entries = {
        // //     (0, 0, 0) : overlapping_windows_egress_hit(190);
        // //     (0, 1, 1) : overlapping_windows_mcast_hit(1);
        // //     (0, 2, 2) : overlapping_windows_mcast_hit(2);
        // //     (0, 3, 2) : overlapping_windows_mcast_hit(3);
        // //     (0, 4, 2) : overlapping_windows_mcast_hit(4);
        // //     (0, 5, 2) : overlapping_windows_mcast_hit(5);
        // //     (0, 0, 2) : overlapping_windows_mcast_hit(6);
        // //     (0, 1, 2) : overlapping_windows_mcast_hit(7);
        //     (0, 0, 0) : overlapping_windows_egress_hit(190);
        //     (0, 1, 1) : overlapping_windows_mcast_hit(1);
        //     (0, 2, 2) : overlapping_windows_mcast_hit(2);
        //     (0, 3, 3) : overlapping_windows_mcast_hit(3);
        //     (0, 4, 4) : overlapping_windows_mcast_hit(4);
        //     (0, 5, 4) : overlapping_windows_mcast_hit(5);
        //     (0, 0, 4) : overlapping_windows_mcast_hit(6);
        //     (0, 1, 4) : overlapping_windows_mcast_hit(7);
        //     (0, 2, 4) : overlapping_windows_mcast_hit(8);
        //     (0, 3, 4) : overlapping_windows_mcast_hit(9);
        // //     (1, 0, 0) : overlapping_windows_egress_hit(191);
        // //     (1, 1, 1) : overlapping_windows_mcast_hit(11);
        // //     (1, 2, 2) : overlapping_windows_mcast_hit(12);
        // //     (1, 3, 2) : overlapping_windows_mcast_hit(13);
        // //     (1, 0, 2) : overlapping_windows_mcast_hit(14);
        // //     (1, 1, 2) : overlapping_windows_mcast_hit(15);
        // }
        counters = overlapping_windows_cntr;
        size = NUM_OVERLAP_MULTICAST_SPEC_1;
    }
    
    RegisterAction<operator_id_t, bit<32>, operator_id_t>(current_operator_reg) curr_operator_reg_action = {
        void apply(inout operator_id_t value, out operator_id_t rv) {   
            if (value < ig_md.max_operator_id - 1) {
                value = value + 1;
            } else {
                value = 0;
            }
            rv = value;
        }
    };

    action operator_register_action(bit<32> idx) {
        ig_md.operator_id = curr_operator_reg_action.execute(idx);
    }

    RegisterAction<position_id_t, bit<32>, position_id_t>(pos_reg) position_reg_action = {
        void apply(inout position_id_t value, out position_id_t rv) {   
            if (value < ig_md.w_shift - 1) {
                value = value + 1;
            } else {
                value = 0;
            }
            rv = value;
        }
    };

    action position_register_action(bit<32> idx) {
        ig_md.w_position = position_reg_action.execute(idx);
    }

    RegisterAction<position_id_t, bit<32>, bit<1>>(pos_reg2) position_reg_action2 = {
        void apply(inout position_id_t value, out bit<1> flag) {   
            if (value < ig_md.w_shift - 1) {
                value = value + 1;
                flag = 0;
            } else {
                value = 0;
                flag = 1;
            }
        }
    };

    action position_register_action2(bit<32> idx) {
        ig_md.new_window = position_reg_action2.execute(idx);
    }

    RegisterAction<position_id_t, bit<32>, position_id_t>(overlap_reg) overlap_reg_action = {
        void apply(inout position_id_t value, out position_id_t rv) {   
            if (value <= ig_md.max_overlap) {
                value = value + 1;
            }
            rv = value;
        }
    };

    action overlap_register_action(bit<32> idx) {
        ig_md.curr_overlap = overlap_reg_action.execute(idx) - 1;
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

    RegisterAction<position_id_t, bit<32>, position_id_t>(overlap_reg2) overlap_reg_action2 = {
        void apply(inout position_id_t value, out position_id_t rv) {   
            if (value < ig_md.max_overlap - 1) {
                value = value + 1;
            }
            rv = value;
        }
    };

    action overlap_register_action2(bit<32> idx) {
        ig_md.curr_overlap = overlap_reg_action2.execute(idx);
    }

    RegisterAction<position_id_t, bit<32>, bit<8>>(init_reg) init_reg_action = {
        void apply(inout position_id_t value, out bit<8> flag) {   
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

    action no_action(){}

    apply {
        /* Count-based Non-overlapping Windows */
#if defined(TUMBLING_COUNT_WND) 
        if ((ig_md.w_size - ig_md.w_shift < 0) || (ig_md.w_size == ig_md.w_shift)) {
            ig_md.is_overlapping = 0;
            init_register_action(ig_md.stream_id);
            if (ig_md.init == 0) {  // do not count 1st packet
                position_register_action(ig_md.stream_id);
                position_register_action2(ig_md.stream_id);
            }
            if (ig_md.new_window == 1) {
                operator_register_action(ig_md.stream_id);
            } else {
                ig_md.operator_id = current_operator_reg.read(ig_md.stream_id);
            }

            // if (ig_md.w_position - ig_md.w_size >= 0) {
            //     // && (ig_md.w_shift - ig_md.w_position > 0)) {
            //     /* Load Shedding. */
            //     drop_packet();
            // }
        }
#endif
#if defined(SLIDING_COUNT_WND)  /* Overlapping windows require egress packet replication. */
        ig_md.is_overlapping = 1;
        ig_md.max_overlap = ig_md.w_size - ig_md.w_shift;
#if defined(SLIDING_COUNT_WND_1) 
        if (ig_md.w_shift == 1) {
            // for every new packet a new window is created and there is a rotation: we advance to the next operator.
            operator_register_action2(ig_md.stream_id);
            // Only for starting phase
            overlap_register_action(ig_md.stream_id);
        } 
#endif
#if defined(SLIDING_COUNT_WND_2)
        position_register_action2(ig_md.stream_id);
        if (ig_md.new_window == 1) {
            operator_register_action(ig_md.stream_id);
            overlap_register_action2(ig_md.stream_id);
        } else {
            ig_md.operator_id = current_operator_reg.read(ig_md.stream_id);
            ig_md.curr_overlap = overlap_reg2.read(ig_md.stream_id);
            if (ig_md.curr_overlap == 2) {
                ig_md.curr_overlap = 1;
            }
        }
#endif
        overlapping_windows.apply();
#endif
    }
}

#endif /* _P4_COUNT_WINDOWS_ */
