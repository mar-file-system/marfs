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

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>

#if RMAN_USE_MPI
#include <mpi.h>
#endif

#include "rsrc_mgr/common.h"
#include "rsrc_mgr/manager.h"
#include "rsrc_mgr/resourcethreads.h"
#include "rsrc_mgr/rmanstate.h"
#include "rsrc_mgr/worker.h"

static void print_usage_info(const int rank) {
    if (rank != 0) {
        return;
    }

    printf("\n"
           "marfs-rman [-c MarFS-Config-File] [-n MarFS-NS-Target] [-r] [-i Iteraion-Name] [-l Log-Root]\n"
           "[-d] [-h]\n"
           "\n"
           " Arguments --\n"
           "  -c MarFS-Config-File : Specifies the path of the MarFS config file to use\n"
           "                         (uses the MARFS_CONFIG_PATH env val, if unspecified)\n"
           "  -n MarFS-NS-Target   : Path of the MarFS Namespace to target for processing\n"
           "                         (defaults to the root NS, if unspecified)\n"
           "  -r                   : Specifies to operate recursively on MarFS Subspaces\n"
           "  -i Iteration-Name    : Specifies the name of this marfs-rman instance\n"
           "                         (defaults to a date string, if unspecified)\n"
           "  -l Log-Root          : Specifies the dir to be used for resource log storage\n"
           "                         (defaults to \"%s\", if unspecified)\n"
           "  -d                   : Specifies a 'dry-run', logging but skipping execution of all ops\n"
           "  -h                   : Prints this usage info\n"
           "\n",
           DEFAULT_LOG_ROOT);
}

// getopt and fill in config
static int parse_args(int argc, char **argv,
                      rmanstate* rman, char **config_path, char **ns_path,
                      time_t *skip, int *recurse) {
    *config_path  = getenv("MARFS_CONFIG_PATH");
    *ns_path      = ".";

    struct timeval currenttime;
    if (gettimeofday(&currenttime, NULL)) {
        fprintf(stderr, "failed to get current time for first walk\n");
        return -1;
    }

    *skip = currenttime.tv_sec - INACTIVE_RUN_SKIP_THRESH;

    // set some quota specific values
    rman->tqopts.thread_init_func     = rthread_init;
    rman->tqopts.thread_consumer_func = rthread_all_consumer;
    rman->tqopts.thread_producer_func = rthread_quota_producer;
    rman->tqopts.thread_pause_func    = NULL;
    rman->tqopts.thread_resume_func   = NULL;
    rman->tqopts.thread_term_func     = rthread_term;

    // parse all position-independent arguments
    int print_usage = 0;
    int c;
    while ((c = getopt(argc, (char* const*)argv, "c:n:ri:l:dh")) != -1) {
        switch (c) {
            case 'c':
                *config_path = optarg;
                break;
            case 'n':
                *ns_path = optarg;
                break;
            case 'r':
                *recurse = 1;
                break;
            case 'i':
                if (strlen(optarg) >= ITERATION_STRING_LEN) {
                    fprintf(stderr, "ERROR: Iteration string exceeds allocated length %u: \"%s\"\n",
                            ITERATION_STRING_LEN, optarg);
                    return -1;
                }
                snprintf(rman->iteration, ITERATION_STRING_LEN, "%s", optarg);
                break;
            case 'l':
                rman->logroot = optarg;
                break;
            case 'd':
                rman->gstate.dryrun = 1;
                break;
            case '?':
                printf("ERROR: Unrecognized cmdline argument: \'%c\'\n", optopt);
                // fall through
            case 'h':
                print_usage = 1;
                break;
            default:
                printf("ERROR: Failed to parse command line options\n");
                return -1;
        }
    }

    if (print_usage) {
        print_usage_info(rman->ranknum);
        return -1;
    }

    const char* iteration_parent = rman->gstate.dryrun?(const char*) RECORD_ITERATION_PARENT:(const char*) MODIFY_ITERATION_PARENT;

    // construct logroot subdirectory path and replace variable
    const char* logroot = rman->logroot?rman->logroot:(char *) DEFAULT_LOG_ROOT;
    const size_t newroot_len = sizeof(char) * (strlen(logroot) + 1 + strlen(iteration_parent));
    char* newroot = malloc(newroot_len + 1);
    snprintf(newroot, newroot_len + 1, "%s/%s", rman->logroot, iteration_parent);
    rman->logroot = newroot;

    // create logging root dir
    if (mkdir(rman->logroot, 0700) && (errno != EEXIST)) {
        fprintf(stderr, "ERROR: Failed to create logging root dir: \"%s\"\n", rman->logroot);
        free(rman->logroot);
        rman->logroot = NULL;
        return -1;
    }

    // populate an iteration string, if missing
    if (rman->iteration[0] == '\0') {
        struct tm* timeinfo = localtime(&currenttime.tv_sec);
        strftime(rman->iteration, ITERATION_STRING_LEN, "%Y-%m-%d-%H:%M:%S", timeinfo);
    }

    return 0;
}

int main(int argc, char *argv[]) {
#if RMAN_USE_MPI
    // Initialize MPI
    if (MPI_Init(&argc, &argv)) {
        fprintf(stderr, "ERROR: Failed to initialize MPI\n");
        return -1;
    }
#endif

    // check how many ranks we have
    int rankcount = 1;
    int rank = 0;
#if RMAN_USE_MPI
    if (MPI_Comm_size(MPI_COMM_WORLD, &rankcount)) {
        fprintf(stderr, "ERROR: Failed to identify rank count\n");
        RMAN_ABORT();
    }
    if (MPI_Comm_rank(MPI_COMM_WORLD, &rank)) {
        fprintf(stderr, "ERROR: Failed to identify process rank\n");
        RMAN_ABORT();
    }
#endif

    rmanstate rman;
    rmanstate_init(&rman, rank, rankcount);

    rman.quotas = 1; /* this program only gets quotas */

    char *config_path = NULL;
    char *ns_path = NULL;
    time_t skip = 0;
    int recurse = 0;

    if (parse_args(argc, argv, &rman, &config_path, &ns_path, &skip, &recurse) != 0) {
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    // for multi-rank invocations, we must synchronize our iteration string across all ranks
    //     (due to clock drift + varied startup times across multiple hosts)
    if (rman.totalranks > 1  &&
#if RMAN_USE_MPI
        MPI_Bcast(rman.iteration, ITERATION_STRING_LEN, MPI_CHAR, 0, MPI_COMM_WORLD)) {
#else
        1) {
#endif
        fprintf(stderr, "ERROR: Failed to synchronize iteration string across all ranks\n");
        rmanstate_fini(&rman, 0);
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    // Initialize the MarFS Config
    if (rmanstate_complete(&rman, config_path, ns_path, skip, recurse, NULL) != 0) {
        rmanstate_fini(&rman, 0);
        RMAN_ABORT();
    }

#if RMAN_USE_MPI
    // synchronize here, to avoid having some ranks run ahead with modifications while other workers
    //    are hung on earlier initialization (which may still fail)
    if (MPI_Barrier(MPI_COMM_WORLD)) {
        fprintf(stderr, "ERROR: Failed to synchronize mpi ranks prior to execution\n");
        rmanstate_fini(&rman, 0);
        RMAN_ABORT();
    }
#endif

    // actually perform core behavior loops
    int bres = 0;
    if (rman.ranknum == 0) {
        bres = managerbehavior(&rman);
    }
    else {
        bres = workerbehavior(&rman);
    }

    if (!bres) {
        bres = (rman.fatalerror) ? -((int)rman.fatalerror) : (int)rman.nonfatalerror;
    }

    rmanstate_fini(&rman, 0);

#if RMAN_USE_MPI
    MPI_Finalize();
#endif

    return 0;
}
