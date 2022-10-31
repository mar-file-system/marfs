#ifndef _RESOURCEPROCESSING_H
#define _RESOURCEPROCESSING_H
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


#include "resourcelog.h"

#include "datastream/datastream.h"

typedef struct threshold_struct {
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

typedef struct streamwalker_report_struct {
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

// forward decls of internal types
typedef struct repackstreamer_struct* REPACKSTREAMER;
typedef struct streamwalker_struct* streamwalker;


//   -------------   REPACKSTREAMER FUNCTIONS    -------------

/**
 * Initialize a new repackstreamer
 * @return REPACKSTREAMER : New repackstreamer, or NULL on failure
 */
REPACKSTREAMER repackstreamer_init(void);

/**
 * Checkout a repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to checkout from
 * @return DATASTREAM* : Checked out datastream, or NULL on failure
 */
DATASTREAM* repackstreamer_getstream( REPACKSTREAMER repackst );

/**
 * Return a previously checked out repack datastream
 * @param REPACKSTREAMER repackst : Repackstreamer to return to
 * @param DATASTREAM* stream : Repack datastream to return
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_returnstream( REPACKSTREAMER repackst, DATASTREAM* stream );

/**
 * Terminate the given repackstreamer and close all associated datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to close
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_complete( REPACKSTREAMER repackst );

/**
 * Abort the given repackstreamer, bypassing all locks and releasing all datastreams
 * @param REPACKSTREAMER repackst : Repackstreamer to abort
 * @return int : Zero on success, or -1 on failure
 */
int repackstreamer_abort( REPACKSTREAMER repackst );

//   -------------   RESOURCE PROCESSING FUNCTIONS    -------------

/**
 * Process the next entry from the given refdir scanner
 * @param marfs_ns* ns : Reference to the current NS
 * @param MDAL_SCANNER refdir : Scanner reference to iterate through
 * @param char** reftgt : Reference to be populated with the next reference path tgt
 *                        Left NULL if the ref dir has been completely traversed
 * @param ssize_t* tgtval : Reference to be populated with the tgt's file/objno value
 *                          ( see ftag_metainfo() return value )
 * @return int : Value of zero -> the reference dir has been completed and closed,
 *               Value of one -> entry is populated with the start of a datastream,
 *               Value of two -> entry is populated with a rebuild marker file,
 *               Value of three -> entry is populated with a repack marker file,
 *               Value of ten -> entry is of an unknown type
 *               Value of negative one -> an error occurred
 */
int process_refdir( marfs_ns* ns, MDAL_SCANNER refdir, const char* refdirpath, char** reftgt, ssize_t* tgtval );

/**
 * Generate a rebuild opinfo element corresponding to the given marker and object
 * @param char* markerpath : Reference path of the rebuild marker ( will be bundled into the opinfo ref )
 * @param time_t rebuildthresh : Rebuild threshold value ( files more recent than this will be ignored )
 * @param size_t objno : Index of the object corresponding to the marker
 * @return opinfo* : Reference to the newly generated op, or NULL on failure
 */
opinfo* process_rebuildmarker( marfs_position* pos, char* markerpath, time_t rebuildthresh, size_t objno );

/**
 * Open a streamwalker based on the given fileno zero reference target
 * @param marfs_position* pos : MarFS position to be used by this walker
 * @param const char* reftgt : Reference path of the first ( fileno zero ) file of the datastream
 * @param thresholds thresh : Threshold values to be used for determining operation targets
 * @param ne_location* rebuildloc : Location-based rebuild target
 * @return streamwalker : Newly generated streamwalker, or NULL on failure
 */
streamwalker process_openstreamwalker( marfs_position* pos, const char* reftgt, thresholds thresh, ne_location* rebuildloc );

/**
 * Iterate over a datastream, accumulating quota values and identifying operation targets
 * NOTE -- This func will return all possible operations, given walker settings.  It is up to the caller whether those ops
 *         will actually be executed via process_operation().
 * @param streamwalker walker : Streamwalker to be iterated
 * @param opinfo** gcops : Reference to be populated with generated GC operations
 * @param opinfo** repackops : Reference to be populated with generated repack operations
 * @param opinfo** rebuildops : Reference to be populated with generated rebuild operations
 * @return int : 0, if the end of the datastream was reached and no new operations were generated;
 *               1, if new operations were generated by this iteration;
 *               -1, if a failure occurred
 */
int process_iteratestreamwalker( streamwalker walker, opinfo** gcops, opinfo** repackops, opinfo** rebuildops );

/**
 * Close the given streamwalker
 * @param streamwalker walker : Streamwalker to be closed
 * @param streamwalker_report* report : Reference to a report to be populated with final counts
 * @return int : Zero on success, 1 if the walker was closed prior to iteration completion, or -1 on failure
 */
int process_closestreamwalker( streamwalker walker, streamwalker_report* report );

/**
 * Perform the given operation
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param opinfo* op : Reference to the operation to be performed
 *                     NOTE -- this will be updated to reflect operation completion / error
 * @param RESOURCELOG* log : Resource log to be updated with op completion / error
 * @param REPACKSTREAMER rpckstr : Repack streamer to be used for repack operations
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- This func will not return 'failure' unless a critical internal error occurs.
 *                       'Standard' operation errors will simply be reflected in the op struct itself.
 */
int process_executeoperation( marfs_position* pos, opinfo* op, RESOURCELOG* rlog, REPACKSTREAMER rpkstr );


#endif // _RESOURCEPROCESSING_H

