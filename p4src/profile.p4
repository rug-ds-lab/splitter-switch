#ifndef _P4_PROFILE_
#define _P4_PROFILE_

/* CBTW */
const bit<32> NUM_OPERATORS_0 = 1026;
const bit<32> NUM_OPERATORS_1 = 2052;
const bit<32> NUM_OPERATORS_2 = 4104;
const bit<32> NUM_OPERATORS_3 = 8196;
const bit<32> NUM_OPERATORS_4 = 12288;
const bit<32> NUM_OPERATORS_5 = 16380;
const bit<32> NUM_OPERATORS_6 = 20472;
const bit<32> NUM_OPERATORS_7 = 24564; 
const bit<32> NUM_OPERATORS_8 = 65536; 
const bit<32> NUM_OPERATORS_9 = 95536;
const bit<32> NUM_OPERATORS_9_1 = 126304; 
const bit<32> NUM_OPERATORS_10 = 131072;
const bit<32> NUM_OPERATORS_10_1 = 142304; 
const bit<32> NUM_OPERATORS_11 = 212144; 
const bit<32> NUM_OPERATORS_12 = 262144; 
const bit<32> NUM_OPERATORS_13 = 362144;
const bit<32> NUM_OPERATORS_14 = 422144;
const bit<32> NUM_OPERATORS_MAX = 457000;  /* 12 stages used, beyond this value the program does not compiple. */

const bit<32> NUM_OPERATORS = NUM_OPERATORS_0;  /* Operator table : Operator ID x Stream ID */

const bit<32> NUM_STREAMS_0 = 1024;
const bit<32> NUM_STREAMS_1 = 2048;
const bit<32> NUM_STREAMS_2 = 4096;
const bit<32> NUM_STREAMS_3 = 8192;
const bit<32> NUM_STREAMS_4 = 16384;
const bit<32> NUM_STREAMS_5 = 32768;
const bit<32> NUM_STREAMS_6 = 62536;
const bit<32> NUM_STREAMS_7 = 65536;
const bit<32> NUM_STREAMS_8 = 126536;
const bit<32> NUM_STREAMS_9 = 131072;
const bit<32> NUM_STREAMS_10 = 161072;
const bit<32> NUM_STREAMS_11 = 172144;
const bit<32> NUM_STREAMS_12 = 189144;
const bit<32> NUM_STREAMS_13 = 212144;
const bit<32> NUM_STREAMS_14 = 252144;
const bit<32> NUM_STREAMS_15 = 286588;


const bit<32> NUM_STREAMS = NUM_STREAMS_0;  /* Stream ID 32 bits. Also size of register arrays */

/* CBSW */
const bit<32> WINDOW_SIZE = 1000; 
const bit<32> WINDOW_SHIFT = 1;
//  const bit<32> WINDOW_SHIFT = 500;
const bit<32> MAX_OVERLAP = WINDOW_SIZE - WINDOW_SHIFT;

/* Count-based Overlapping table */
/* WINDOW_SIZE >= NUM_OPERATORS */
// const bit<32> NUM_OVERLAP_MULTICAST_SPEC_1_0 = CBSW_NUM_OPERATORS_0 + 1;    
// const bit<32> NUM_OVERLAP_MULTICAST_SPEC_1_1 = CBSW_NUM_OPERATORS_1 + 1;  
// const bit<32> NUM_OVERLAP_MULTICAST_SPEC_1_2 = CBSW_NUM_OPERATORS_2 + 1;  

/* WINDOW_SIZE < NUM_OPERATORS */
const bit<32> NUM_OVERLAP_MULTICAST_SPEC_1 = MAX_OVERLAP + NUM_OPERATORS;
// const bit<32> NUM_MULTICAST_GROUP_1 = NUM_OVERLAP_MULTICAST_SPEC_1 - 1;

/* TBSW */
const bit<32> WINDOW_DURATION = 100; // seconds
const bit<32> WINDOW_TIME_SHIFT = 1; // second
const bit<32> MAX_TIME_OVERLAP = WINDOW_DURATION / WINDOW_TIME_SHIFT;
const bit<32> NUM_ENTRIES_TBSW = MAX_TIME_OVERLAP + NUM_OPERATORS - 1;

#endif /* _P4_PROFILE_ */
