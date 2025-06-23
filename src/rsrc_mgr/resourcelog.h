#ifndef _RESOURCELOG_H
#define _RESOURCELOG_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "config/config.h"
#include "rsrc_mgr/logline.h"

typedef struct resourcelog* RESOURCELOG;

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

typedef struct {
   size_t deletion_object_count;
   size_t deletion_object_failures;
   size_t deletion_reference_count;
   size_t deletion_reference_failures;
   size_t rebuild_count;
   size_t rebuild_failures;
   size_t repack_count;
   size_t repack_failures;
} operation_summary;


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
