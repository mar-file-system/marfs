#ifndef _RESOURCE_LOG_LINE_H
#define _RESOURCE_LOG_LINE_H
/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
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

#endif
