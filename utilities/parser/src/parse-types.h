/*
 * 
 *   parse-types.h
 *
 *   more like a command set 
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   6-8-2015:
 *    - initial start 
 *
 *
 */

#define DISPLAY			0
#define CREATE_STRUCT		1
#define CREATE_STRUCT_PATHS	3
#define POPULATE_STRUCT		3

#define DECONSTRUCT		0
#define NO_ORDER		1
#define NO_ORDER_DEBUG		2
#define GEN_PARSE_STRUCTS	3
#define GEN_STRUCT_SWITCH	4

#define PATH_TYPE		0
#define STRUCT_TYPE		1

#define STANDARD		0
#define	UNIQUE			1


#define GET_PTR			0
#define GET_F_PTR		1


// data type, we don't really use it yet
#define TYPE_STRUCT		0
#define TYPE_CHAR		1


// random other typedefs
#define QUIET			0
#define VERBOSE			1
