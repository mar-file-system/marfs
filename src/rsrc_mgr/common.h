#ifndef _RESOURCE_MANAGER_COMMON_H
#define _RESOURCE_MANAGER_COMMON_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "marfs_auto_config.h"
#ifdef DEBUG_RM
#define DEBUG DEBUG_RM
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#ifndef LOG_PREFIX
#define LOG_PREFIX "resourcemanager"
#endif
#include "logging/logging.h"

#define DEFAULT_LOG_ROOT "/var/log/marfs-rman"
#define MODIFY_ITERATION_PARENT "RMAN-MODIFY-RUNS"
#define RECORD_ITERATION_PARENT "RMAN-RECORD-RUNS"

#define INACTIVE_RUN_SKIP_THRESH 60 // Age of seemingly inactive (no summary file) rman logdirs before they are skipped
                                    // Default to 1 minute ago

#define SUMMARY_FILENAME "summary.log"
#define ERROR_LOG_PREFIX "ERRORS-"
#define ITERATION_STRING_LEN 128

#if RMAN_USE_MPI
#define RMAN_ABORT() MPI_Abort(MPI_COMM_WORLD, -1)
#else
#define RMAN_ABORT() return -1
#endif

#endif
