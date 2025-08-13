#ifndef _RESOURCE_LOG_LINE_H
#define _RESOURCE_LOG_LINE_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <stdint.h>

#include "tagging/tagging.h"

#define MAX_BUFFER 8192 // maximum character buffer to be used for parsing/printing log lines
                        //    program will abort if limit is exceeded when reading or writing

typedef enum
{
   MARFS_DELETE_OBJ_OP,
   MARFS_DELETE_REF_OP,
   MARFS_REBUILD_OP,
   MARFS_REPACK_OP
} operation_type;

typedef struct opinfo {
   operation_type type;  // which class of operation
   void* extendedinfo;   // extra, operation-specific, info
   char start;           // flag indicating the start of an op ( if zero, this entry indicates completion )
   size_t count;         // how many targets are there
   int errval;           // errno value of the attempted op ( always zero for operation start )
   FTAG ftag;            // which FTAG value is the target
   struct opinfo* next;  // subsequent ops in this chain ( or NULL, if none remain )
} opinfo;

typedef struct {
   size_t offset; // offset of the objects to begin deletion at ( used for spliting del ops across threads )
} delobj_info;

typedef struct {
   size_t prev_active_index; // index of the closest active ( not to be deleted ) reference in the stream
   char   delzero; // deleted zero flag, indicating that the data object(s) referenced by fileno zero have been deleted
   char   eos; // end-of-stream flag, indicating that this delete will make prev_active_index the new EOS
} delref_info;

typedef struct {
   char* markerpath; // rpath of the rebuild marker associated with this operation ( or NULL, if none present )
   RTAG* rtag;       // rebuild tag value from the marker ( or NULL, if none present )
} rebuild_info;

typedef struct {
   size_t totalbytes; // total count of bytes to be repacked
} repack_info;

opinfo* parselogline(int logfile, char* eof);
int printlogline(int logfile, opinfo* op);

#endif
