#ifndef _RESOURCELOG_H
#define _RESOURCELOG_H
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

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "config/config.h"
#include "tagging/tagging.h"

typedef struct resourcelog_struct* RESOURCELOG;

typedef enum
{
   RESOURCE_RECORD_LOG = 0,
   RESOURCE_MODIFY_LOG = 1,
   RESOURCE_READ_LOG = 2
   // NOTE -- The caller should treat each of these type values as exclusive ( one of the three values ), when 
   //         providing them as arguments to resourcelog_init().
   //         However, the underlying resourcelog code treats 'RESOURCE_READ_LOG' as a bitflag for internal typing.
   //         As in, it will store internal type values for read resourcelogs as a bitwise OR between this value and  
   //         one of the other two.  This allows it to track both that it is reading and from what type of log.
} resourcelog_type;

typedef enum
{
   MARFS_DELETE_OBJ_OP,
   MARFS_DELETE_REF_OP,
   MARFS_REBUILD_OP,
   MARFS_REPACK_OP
} operation_type;

typedef struct opinfo_struct {
   operation_type type;  // which class of operation
   void* extendedinfo;   // extra, operation-specific, info
   char start;           // flag indicating the start of an op ( if zero, this entry indicates completion )
   size_t count;         // how many targets are there
   int errval;           // errno value of the attempted op ( always zero for operation start )
   FTAG ftag;            // which FTAG value is the target
   struct opinfo_struct* next; // subsequent ops in this chain ( or NULL, if none remain )
} opinfo;

typedef struct delobj_info_struct {
   size_t offset; // offset of the objects to begin deletion at ( used for spliting del ops across threads )
} delobj_info;

typedef struct delref_info_struct {
   size_t prev_active_index; // index of the closest active ( not to be deleted ) reference in the stream
   char   delzero; // deleted zero flag, indicating that the data object(s) referenced by fileno zero have been deleted
   char   eos; // end-of-stream flag, indicating that this delete will make prev_active_index the new EOS
} delref_info;

typedef struct rebuild_info_struct {
   char* markerpath; // rpath of the rebuild marker associated with this operation ( or NULL, if none present )
   RTAG* rtag;       // rebuild tag value from the marker ( or NULL, if none present )
} rebuild_info;

typedef struct repack_info_struct {
   size_t totalbytes; // total count of bytes to be repacked
} repack_info;

typedef struct operation_summary_struct {
   size_t deletion_object_count;
   size_t deletion_object_failures;
   size_t deletion_reference_count;
   size_t deletion_reference_failures;
   size_t rebuild_count;
   size_t rebuild_failures;
   size_t repack_count;
   size_t repack_failures;
} operation_summary;

typedef struct resourcelog_struct* RESOURCELOG;


/**
 * Free the given opinfo struct chain
 * @param opinfo* op : Reference to the opinfo struct to be freed
 */
void resourcelog_freeopinfo( opinfo* op );

/**
 * Duplicate the given opinfo struct chain
 * @param opinfo* op : Reference to the opinfo chain to be duplicated
 * @return opinfo* : Reference to the newly created duplicate chain
 */
opinfo* resourcelog_dupopinfo( opinfo* op );

/**
 * Generates the pathnames of logfiles and parent dirs
 * @param char create : Create flag
 *                      If non-zero, this func will attempt to create all intermediate directory paths ( not the final tgt )
 * @param const char* logroot : Root of the logfile tree
 * @param const char* iteration : ID string for this program iteration ( can be left NULL to gen parent path )
 * @param marfs_ns* ns : MarFS NS to process ( can be left NULL to gen parent path, ignored if prev is NULL )
 * @param ssize_t ranknum : Processing rank ( can be < 0 to gen parent path, ignored if prev is NULL )
 * @return char* : Path of the corresponding log location, or NULL if an error occurred
 *                 NOTE -- It is the caller's responsibility to free this string
 */
char* resourcelog_genlogpath( char create, const char* logroot, const char* iteration, marfs_ns* ns, ssize_t ranknum );

/**
 * Initialize a resourcelog, associated with the given logging root, namespace, and rank
 * @param RESOURCELOG* resourcelog : Statelog to be initialized
 *                             NOTE -- This can either be a NULL value, or a resourcelog which was 
 *                                     previously terminated / finalized
 * @param resourcelog_type type : Type of resourcelog to open
 * @param const char* logpath : Location of the resourcelog file
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_init( RESOURCELOG* resourcelog, const char* logpath, resourcelog_type type, marfs_ns* ns );

/**
 * Replay all operations from a given inputlog ( reading from a MODIFY log ) into a given
 *  outputlog ( writing to a MODIFY log ), then delete and terminate the inputlog
 * NOTE -- This function is intended for picking up state from a previously aborted run.
 * @param RESOURCELOG* inputlog : Source inputlog to be read from
 * @param RESOURCELOG* outputlog : Destination outputlog to be written to
 * @param int (*filter)( const opinfo* op ) : Function pointer defining an operation filter ( ignored if NULL )
 *                                            *param const opinfo* : Reference to the op to potentially include
 *                                            *return int : Zero if the op should be included, non-zero if not
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_replay( RESOURCELOG* inputlog, RESOURCELOG* outputlog, int (*filter)( const opinfo* op ) );

/**
 * Record that a certain number of threads are currently processing
 * @param RESOURCELOG* resourcelog : Statelog to be updated
 * @param size_t numops : Number of additional processors ( can be negative to reduce cnt )
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_update_inflight( RESOURCELOG* resourcelog, ssize_t numops );

/**
 * Process the given operation
 * @param RESOURCELOG* resourcelog : Statelog to update ( must be writing to this resourcelog )
 * @param opinfo* op : Operation ( or op sequence ) to process
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_processop( RESOURCELOG* resourcelog, opinfo* op, char* progress );

/**
 * Parse the next operation info sequence from the given RECORD resourcelog
 * @param RESOURCELOG* resourcelog : Statelog to read
 * @param opinfo** op : Reference to be populated with the parsed operation info sequence
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_readop( RESOURCELOG* resourcelog, opinfo** op );

/**
 * Deallocate and finalize a given resourcelog
 * NOTE -- this will fail if there are currently any ops in flight
 * @param RESOURCELOG* resourcelog : Statelog to be terminated
 * @param operation_summary* summary : Reference to be populated with summary values ( ignored if NULL )
 * @param char delete : Flag indicating whether the logfile should be deleted on termination
 *                      If non-zero, the file is deleted
 * @return int : Zero on success, 1 if the log was preserved due to errors, or -1 on failure
 */
int resourcelog_term( RESOURCELOG* resourcelog, operation_summary* summary, char delete );

/**
 * Deallocate and finalize a given resourcelog without waiting for completion
 * @param RESOURCELOG* resourcelog : Statelog to be terminated
 * @return int : Zero on success, or -1 on failure
 */
int resourcelog_abort( RESOURCELOG* resourcelog );

#endif // _RESOURCELOG_H

