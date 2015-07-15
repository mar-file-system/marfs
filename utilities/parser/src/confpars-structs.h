/*
 * 
 *   confpar-structs.h
 *
 *   config file parser header
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   06-08-2015:        initial start rocr@lanl.gov
 *   06-08-2015:        redesigned collate functions
 *
 *
 */

struct line {
   int lvl;
   int tag;
   int type;	// 1 = char
   char *ln;
   char dbg[5];
   struct line *next;
   };

struct elmPathCnt {
   char elem[32];
   int  cnt;
   struct elmPathCnt *next;
   };
