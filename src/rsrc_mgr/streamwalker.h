#ifndef _RESOURCE_MANAGER_STREAM_WALKER_H
#define _RESOURCE_MANAGER_STREAM_WALKER_H
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
#include <time.h>

#include "datastream/datastream.h"
#include "resourcelog.h"

//   -------------   INTERNAL DEFINITIONS    -------------

// ENOATTR is not always defined, so define a convenience val
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

typedef struct {
   time_t gcthreshold;      // files newer than this will not be GCd
                            //    Recommendation -- this should be fairly old ( # of days ago )
   time_t repackthreshold;  // files newer than this will not be repacked
                            //    Recommendation -- this should be quite recent ( # of minutes ago )
   time_t rebuildthreshold; // files newer than this will not be rebuilt ( have data errors repaired )
                            //    Recommendation -- this should be quite recent ( # of minutes ago )
   time_t cleanupthreshold; // files newer than this will not be cleaned up ( i.e. repack marker files )
                            //    Recommendation -- this should be semi-recent ( # of hours ago )
   // NOTE -- setting any of these values too close to the current time risks undefined resource
   //         processing behavior ( i.e. trying to cleanup ongoing repacks )
   // NOTE -- setting any of these values to zero will cause the corresponding operations to be skipped
} thresholds;

typedef struct {
   // quota info
   size_t fileusage;   // count of active files
   size_t byteusage;   // count of active bytes
   // stream info
   size_t filecount;   // count of files in the datastream
   size_t objcount;    // count of objects in the datastream
   size_t bytecount;   // count of all bytes in the datastream
   size_t streamcount; // count of all streams ( always one, here, but having this value in the struct helps elsewhere )
   // GC info
   size_t delobjs;    // count of deleted objects
   size_t delfiles;   // count of deleted files
   size_t delstreams; // count of completely deleted datastreams
   size_t volfiles;   // count of 'volatile' files ( those deleted too recently for gc )
   // repack info
   size_t rpckfiles;  // count of files repacked
   size_t rpckbytes;  // count of bytes repacked
   // rebuild info
   size_t rbldobjs;   // count of rebuilt objects
   size_t rbldbytes;  // count of rebuilt bytes
} streamwalker_report;

typedef struct streamwalker {
   // initialization info
   marfs_position pos;
   time_t      gcthresh;       // time threshold for GCing a file (none performed, if zero)
   time_t      repackthresh;   // time threshold for repacking a file (none performed, if zero)
   time_t      rebuildthresh;  // time threshold for rebuilding a file (none performed, if zero)
   time_t      cleanupthresh;  // time threshold for cleaning up incomplete datastreams (none performed, if zero)
   ne_location rebuildloc;     // location value of objects to be rebuilt
   // report info
   streamwalker_report report; // running totals for encountered stream elements
   // iteration info
   size_t      fileno;    // current file position in the datastream
   size_t      objno;     // current object position in the datastream
   HASH_TABLE  reftable;  // NS reference position table to be used for stream iteration
   struct stat stval;     // stat value of the most recently encountered file
   FTAG        ftag;      // FTAG value of the most recently checked file (not necessarily previous)
   GCTAG       gctag;     // GCTAG value of the most recently checked file (not necessarily previous)
   // cached info
   size_t      headerlen;    // recovery header length for the active datastream
   char*       ftagstr;      // FTAG string buffer
   size_t      ftagstralloc; // allocated length of the FTAG string buffer
   // GC info
   opinfo*     gcops;        // garbage collection operation list
   size_t      activefiles;  // count of active files referencing the current object
   size_t      activeindex;  // index of the most recently encountered active file
   // repack info
   opinfo*     rpckops;      // repack operation list
   size_t      activebytes;  // active bytes in the current object
   // rebuild info
   opinfo*     rbldops;      // rebuild operation list
}* streamwalker;

/**
 * Open a streamwalker based on the given fileno zero reference target
 * @param streamwalker* swalker : Reference to be populated with the produced streamwalker
 * @param marfs_position* pos : MarFS position to be used by this walker
 * @param const char* reftgt : Reference path of the first ( fileno zero ) file of the datastream
 * @param thresholds thresh : Threshold values to be used for determining operation targets
 * @param ne_location* rebuildloc : Location-based rebuild target
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- It is possible for success to be indicated without any streamwalker being produced,
 *                       such as, if the stream is incomplete ( missing FTAG values ).
 *                       Failure will only be indicated if an unexpected condition occurred, such as, if the
 *                       datastream is improperly formatted.
 */
int streamwalker_open( streamwalker* swalker, marfs_position* pos, const char* reftgt, thresholds thresh, ne_location* rebuildloc );

/**
 * Iterate over a datastream, accumulating quota values and identifying operation targets
 * NOTE -- This func will return all possible operations, given walker settings.  It is up to the caller whether those ops
 *         will actually be executed via process_operation().
 * @param streamwalker* swalker : Reference to the streamwalker to be iterated
 * @param opinfo** gcops : Reference to be populated with generated GC operations
 * @param opinfo** repackops : Reference to be populated with generated repack operations
 * @param opinfo** rebuildops : Reference to be populated with generated rebuild operations
 * @return int : 0, if the end of the datastream was reached and no new operations were generated;
 *               1, if new operations were generated by this iteration;
 *               -1, if a failure occurred
 */
int streamwalker_iterate( streamwalker* swalker, opinfo** gcops, opinfo** repackops, opinfo** rebuildops );

/**
 * Close the given streamwalker
 * @param streamwalker* swalker : Reference to the streamwalker to be closed
 * @param streamwalker_report* report : Reference to a report to be populated with final counts
 * @return int : Zero on success, 1 if the walker was closed prior to iteration completion, or -1 on failure
 */
int streamwalker_close( streamwalker* swalker, streamwalker_report* report );

#endif
