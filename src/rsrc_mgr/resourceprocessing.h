#ifndef _RESOURCEPROCESSING_H
#define _RESOURCEPROCESSING_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */


#include "resourcelog.h"

#include "datastream/datastream.h"
#include "rsrc_mgr/repack.h"
#include "rsrc_mgr/streamwalker.h"

//   -------------   RESOURCE PROCESSING FUNCTIONS    -------------

/**
 * Attempts deletion of the specified reference dir 'branch' ( directory and all parent dirs ) based on the given gcthreshold
 * @param marfs_position* pos : Current MarFS position
 * @param const char* refdirpath : Path of the reference dir to be cleaned up
 * @param time_t gcthresh : GC threshold value
 * @return int : Zero on success, or -1 on failure
 */
int cleanup_refdir( marfs_position* pos, const char* refdirpath, time_t gcthresh );

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
 * Perform the given operation
 * @param MDAL_CTXT ctxt : MDAL_CTXT associated with the current NS
 * @param opinfo* op : Reference to the operation to be performed
 *                     NOTE -- this will be updated to reflect operation completion / error
 * @param RESOURCELOG* log : Resource log to be updated with op completion / error
 * @param REPACKSTREAMER rpckstr : Repack streamer to be used for repack operations
 * @param const char* ctag : Optional client tag for repacking
 * @return int : Zero on success, or -1 on failure
 *               NOTE -- This func will not return 'failure' unless a critical internal error occurs.
 *                       'Standard' operation errors will simply be reflected in the op struct itself.
 */
int process_executeoperation( marfs_position* pos, opinfo* op, RESOURCELOG* rlog, REPACKSTREAMER rpkstr, const char* ctag );


#endif // _RESOURCEPROCESSING_H
