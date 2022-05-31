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

#ifdef DEBUG_RM
#define DEBUG DEBUG_RM
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "rsrc_mgr"
#include <logging.h>

#include <dirent.h>
#include <errno.h>
#include <math.h>
#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/xattr.h>

#include <thread_queue.h>

#include "marfs_auto_config.h"
#include "config/config.h"
#include "datastream/datastream.h"
#include "mdal/mdal.h"
#include "tagging/tagging.h"

#define PROGNAME "marfs-rsrc_mgr"
#define OUTPREFX PROGNAME ": "

// ENOATTR isn't always defined in Linux, so define it
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

// Files created within the threshold will be ignored
#define RECENT_THRESH -1

#define N_SKIP "gc_skip"
#define IN_PROG "IN_PROG"
#define ZERO_FLAG 'z'

// Default TQ values
#define NUM_PROD 10
#define NUM_CONS 10
#define QDEPTH 100

int rank = 0;
int n_ranks = 1;

int n_prod = NUM_PROD;
int n_cons = NUM_CONS;

int del = 0;

typedef struct quota_data_struct {
  size_t size;
  size_t count;
  size_t del_objs;
  size_t del_refs;
  size_t rebuilds;
} quota_data;

typedef struct tq_global_struct {
  pthread_mutex_t lock;
  marfs_ms* ms;
  marfs_ds* ds;
  MDAL_CTXT ctxt;
  size_t next_node;
} *GlobalState;

typedef struct tq_thread_struct {
  unsigned int tID;
  GlobalState global;
  quota_data q_data;
  HASH_NODE* ref_node;
  MDAL_SCANNER scanner;
} *ThreadState;

typedef struct work_pkg_struct {
  char type; // Type of operation for consumer to perform 0 = gc/quota,
  // 1 = rebuild
  char* rpath; // Name of ref file at start of stream for gc/quota
  FTAG* ftag; // Ftag information for rebuild
  ne_state* rtag; // Rebuild tag information for rebuild
} *WorkPkg;

void free_work(WorkPkg work) {
  if (work) {
    if (work->rpath) {
      free(work->rpath);
    }
    if (work->ftag) {
      if (work->ftag->ctag) {
        free(work->ftag->ctag);
      }
      if (work->ftag->streamid) {
        free(work->ftag->streamid);
      }
      free(work->ftag);
    }
    if (work->rtag) {
      if (work->rtag->meta_status) {
        free(work->rtag->meta_status);
      }
      if (work->rtag->csum) {
        free(work->rtag->csum);
      }
      free(work->rtag);
    }
    free(work);
  }
  work = NULL;
}

/** Garbage collect the reference files and objects of a stream inside the given
 * ranges. NOTE: refs/objects corresponding to the index parameters will not be
 * garbage collected.
 * Ex:  gc(..., 1, 5, 0, 2, ...) will delete refs 2-4 and obj 1
 * @param const marfs_ms* ms : Reference to the current MarFS metadata scheme
 * @param const marfs_ds* ds : Reference to the current MarFS data scheme
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target
 * namespace
 * @param FTAG ftag : A FTAG value within the stream.
 * @param int v_ref : Index of the last valid ref before the range of refs to
 * collect.
 * @param int head_ref : Starting index of the range of refs to collect. Must be
 * >= v_ref.
 * @param int tail_ref : Ending index of the range of refs to collect.
 * @param int first_obj : Starting index of the range of objs to collect.
 * @param int curr_obj : Ending index of the range of refs to collect.
 * @param int eos : Flag indicating if the given ranges are at the end of the
 * stream.
 * @param quota data* q_data : Reference to the quota data struct to populate
 * @return int : Zero on success, or -1 on failure.
 */
int gc(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, FTAG ftag, int v_ref, int head_ref, int tail_ref, int first_obj, int curr_obj, int eos, quota_data* q_data) {
  // printf("garbage collecting refs (%d) %d:%d objs %d:%d eos(%d)\n", v_ref, head_ref, tail_ref, first_obj, curr_obj, eos);

  if (head_ref < v_ref) {
    LOG(LOG_ERR, "Rank %d: Invalid ref ranges given\n");
    return -1;
  }

  // If we are supposed to GC ref 0, we will add an indicator to the skip xattr
  int del_zero = 0;
  if (head_ref < 0) {
    del_zero = 1;
  }

  // Calculate value for xattr indicating how many refs will need to be skipped
  // on future runs
  int skip = -1;
  if (!eos) {
    if (head_ref < 0) {
      head_ref = 0;
    }
    if (v_ref < 0) {
      v_ref = 0;
    }
    skip = tail_ref - v_ref - 1;
  }

  if (!del) {
    if (first_obj != curr_obj) {
      q_data->del_objs += curr_obj - (first_obj + 1);
    }
    q_data->del_refs += tail_ref - (head_ref + 1);
    return 0;
  }

  // Determine if the specified refs/objs have already been deleted
  char* rpath;
  ftag.fileno = v_ref;
  if (v_ref < 0) {
    ftag.fileno = 0;
  }

  if ((rpath = datastream_genrpath(&ftag, ms)) == NULL) {
    LOG(LOG_ERR, "Rank %d: Failed to create rpath\n", rank);
    return -1;
  }

  MDAL_FHANDLE handle;
  if ((handle = ms->mdal->openref(ctxt, rpath, O_RDWR, 0)) == NULL) {
    LOG(LOG_ERR, "Rank %d: Failed to open handle for reference path \"%s\"\n", rank, rpath);
    free(rpath);
    return -1;
  }

  char* xattr = NULL;
  int xattr_len = ms->mdal->fgetxattr(handle, 1, N_SKIP, xattr, 0);
  if (xattr_len <= 0 && errno != ENOATTR) {
    LOG(LOG_ERR, "Rank %d: Failed to retrieve xattr skip for reference path \"%s\"\n", rank, rpath);
    ms->mdal->close(handle);
    free(rpath);
    return -1;
  }
  else if (xattr_len >= 0) {
    xattr = calloc(1, xattr_len + 1);
    if (xattr == NULL) {
      LOG(LOG_ERR, "Rank %d: Failed to allocate space for a xattr string value\n", rank);
      ms->mdal->close(handle);
      free(rpath);
      return -1;
    }

    if (ms->mdal->fgetxattr(handle, 1, N_SKIP, xattr, 0) != xattr_len) {
      LOG(LOG_ERR, "Rank %d: xattr skip value for \"%s\" changed while reading\n", rank, rpath);
      ms->mdal->close(handle);
      free(xattr);
      free(rpath);
      return -1;
    }

    // The same deletion has already been completed, no need to repeat
    if (strtol(xattr, NULL, 10) == skip) {
      ms->mdal->close(handle);
      free(rpath);
      return 0;
    }

    free(xattr);
  }

  // Delete the specified objects
  int i;
  char* objname;
  ne_erasure erasure;
  ne_location location;
  for (i = first_obj + 1; i < curr_obj; i++) {
    ftag.objno = i;
    if (datastream_objtarget(&ftag, ds, &objname, &erasure, &location)) {
      LOG(LOG_ERR, "Rank %d: Failed to generate object name\n", rank);
      free(xattr);
      free(rpath);
      return -1;
    }

    LOG(LOG_INFO, "Rank %d: Garbage collecting object %d %s\n", rank, i, objname);
    errno = 0;
    if (ne_delete(ds->nectxt, objname, location)) {
      if (errno != ENOENT) {
        LOG(LOG_ERR, "Rank %d: Failed to delete \"%s\" (%s)\n", rank, objname, strerror(errno));
        free(objname);
        free(xattr);
        free(rpath);
        return -1;
      }
    }
    else {
      q_data->del_objs++;
    }

    free(objname);
  }

  if (tail_ref <= head_ref + 1 && !del_zero) {
    return 0;
  }

  // Set temporary xattr indicating we started deleting the specified refs
  if (eos) {
    xattr_len = 4 + strlen(IN_PROG);
  }
  else if (skip == 0) {
    xattr_len = 3 + strlen(IN_PROG);
  }
  else {
    xattr_len = (int)log10(skip) + 3 + strlen(IN_PROG);
  }
  xattr_len += del_zero;

  if ((xattr = malloc(sizeof(char) * xattr_len)) == NULL) {
    LOG(LOG_ERR, "Rank %d: Failed to allocate xattr string\n", rank);
    free(rpath);
    return -1;
  }

  if (del_zero) {
    if (snprintf(xattr, xattr_len, "%s %d%c", IN_PROG, skip, ZERO_FLAG) != (xattr_len - 1)) {
      LOG(LOG_ERR, "Rank %d: Failed to populate rpath string\n", rank);
      free(xattr);
      free(rpath);
      return -1;
    }
  }
  else {
    if (snprintf(xattr, xattr_len, "%s %d", IN_PROG, skip) != (xattr_len - 1)) {
      LOG(LOG_ERR, "Rank %d: Failed to populate rpath string\n", rank);
      free(xattr);
      free(rpath);
      return -1;
    }
  }

  if (tail_ref > head_ref + 1) {
    if (ms->mdal->fsetxattr(handle, 1, N_SKIP, xattr, xattr_len, 0)) {
      LOG(LOG_ERR, "Rank %d: Failed to set temporary xattr string\n", rank);
      ms->mdal->close(handle);
      free(xattr);
      free(rpath);
      return -1;
    }

    if (ms->mdal->close(handle)) {
      LOG(LOG_ERR, "Rank %d: Failed to close handle\n", rank);
      free(xattr);
      free(rpath);
      return -1;
    }

    // Delete specified refs
    char* dpath;
    for (i = head_ref + 1; i < tail_ref; i++) {
      ftag.fileno = i;
      if ((dpath = datastream_genrpath(&ftag, ms)) == NULL) {
        LOG(LOG_ERR, "Rank %d: Failed to create dpath\n", rank);
        free(xattr);
        free(rpath);
        return -1;
      }

      LOG(LOG_INFO, "Rank %d: Garbage collecting ref %d\n", rank, i);
      errno = 0;
      if (ms->mdal->unlinkref(ctxt, dpath)) {
        if (errno != ENOENT) {
          LOG(LOG_ERR, "Rank %d: failed to unlink \"%s\" (%s)\n", rank, dpath, strerror(errno));
          free(dpath);
          free(xattr);
          free(rpath);
          return -1;
        }
      }
      else {
        q_data->del_refs++;
      }

      free(dpath);
    }

    if (head_ref < 0 && eos) {
      free(xattr);
      free(rpath);
      return 0;
    }

    // Rewrite xattr indicating that we finished the deletion
    if ((handle = ms->mdal->openref(ctxt, rpath, O_WRONLY, 0)) == NULL) {
      LOG(LOG_ERR, "Rank %d: Failed to open handle for reference path \"%s\" (%s)\n", rank, rpath, strerror(errno));
      free(xattr);
      free(rpath);
      return -1;
    }
  }

  xattr_len -= strlen(IN_PROG) + 1;
  xattr += strlen(IN_PROG) + 1;

  if (ms->mdal->fsetxattr(handle, 1, N_SKIP, xattr, xattr_len, 0)) {
    LOG(LOG_ERR, "Rank %d: Failed to set xattr string (%s)\n", rank, strerror(errno));
    ms->mdal->close(handle);
    free(xattr - strlen(IN_PROG) - 1);
    free(rpath);
    return -1;
  }

  if (ms->mdal->close(handle)) {
    LOG(LOG_ERR, "Rank %d: Failed to close handle\n", rank);
    free(xattr - strlen(IN_PROG) - 1);
    free(rpath);
    return -1;
  }

  free(xattr - strlen(IN_PROG) - 1);
  free(rpath);

  return 0;
}

/** Populate the given ftag struct based on the given reference path. If a
 * previous run of the resource manager failed while in the process of garbage
 * collecting the following reference in the stream, retry this garbage
 * collection.
 * @param const marfs_ms* ms : Reference to the current MarFS metadata scheme
 * @param const marfs_ds* ds : Reference to the current MarFS data scheme
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target
 * namespace
 * @param FTAG* ftag : Reference to the FTAG struct to be populated
 * @param const char* rpath : Reference path of the file to retrieve ftag data
 * from
 * @param struct stat* st : Reference to stat buffer to be populated
 * @param quota_data* q_data : Reference to the quota data struct to update
 * @param int* del_zero : Reference to flag to be set in case zero was already
 * deleted
 * @return int : The distance to the next active reference within the stream on
 * success, or -1 if a failure occurred.
 * Ex: If the following ref has not been garbage collected, return 1. If the
 * following two refs have already been garbage collected, return 3.
 */
int get_ftag(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, FTAG* ftag, const char* rpath, struct stat* st, quota_data* q_data, int* del_zero) {
  MDAL_FHANDLE handle;

  if (ms->mdal->statref(ctxt, rpath, st)) {
    LOG(LOG_ERR, "Rank %d: Failed to stat \"%s\" rpath\n", rank, rpath);
    return -1;
  }

  // Get ftag string from xattr
  if ((handle = ms->mdal->openref(ctxt, rpath, O_RDONLY, 0)) == NULL) {
    LOG(LOG_ERR, "Rank %d: Failed to open handle for reference path \"%s\"\n", rank, rpath);
    return -1;
  }

  char* ftagstr = NULL;
  int ftagsz;
  if ((ftagsz = ms->mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, 0)) <= 0) {
    LOG(LOG_ERR, "Rank %d: Failed to retrieve FTAG value for reference path \"%s\"\n", rank, rpath);
    ms->mdal->close(handle);
    return -1;
  }

  ftagstr = calloc(1, ftagsz + 1);
  if (ftagstr == NULL) {
    LOG(LOG_ERR, "Rank %d: Failed to allocate space for a FTAG string value\n", rank);
    ms->mdal->close(handle);
    return -1;
  }

  if (ms->mdal->fgetxattr(handle, 1, FTAG_NAME, ftagstr, ftagsz) != ftagsz) {
    LOG(LOG_ERR, "Rank %d: FTAG value for \"%s\" changed while reading \n", rank, rpath);
    free(ftagstr);
    ms->mdal->close(handle);
    return -1;
  }

  // Populate ftag with values from string
  if (ftag->ctag) {
    free(ftag->ctag);
  }
  if (ftag->streamid) {
    free(ftag->streamid);
  }
  if (ftag_initstr(ftag, ftagstr)) {
    LOG(LOG_ERR, "Rank %d: Failed to parse FTAG string for \"%s\"\n", rank, rpath);
    free(ftagstr);
    ms->mdal->close(handle);
    return -1;
  }

  free(ftagstr);

  // Look if there is an xattr indicating the following ref(s) was deleted
  char* skipstr = NULL;
  int skipsz;
  int skip = 0;
  if ((skipsz = ms->mdal->fgetxattr(handle, 1, N_SKIP, skipstr, 0)) > 0) {

    skipstr = calloc(1, skipsz + 1);
    if (skipstr == NULL) {
      LOG(LOG_ERR, "Rank %d: Failed to allocate space for a skip string value\n", rank);
      ms->mdal->close(handle);
      return -1;
    }

    if (ms->mdal->fgetxattr(handle, 1, N_SKIP, skipstr, skipsz) != skipsz) {
      LOG(LOG_ERR, "Rank %d: skip value for \"%s\" changed while reading \n", rank, rpath);
      free(skipstr);
      ms->mdal->close(handle);
      return -1;
    }

    char* end;
    errno = 0;
    // If the following ref(s) were deleted, note we need to skip them
    skip = strtol(skipstr, &end, 10);
    if (errno) {
      LOG(LOG_ERR, "Rank %d: Failed to parse skip value for \"%s\" (%s)", rank, rpath, strerror(errno));
      free(skipstr);
      ms->mdal->close(handle);
      return -1;
    }
    else if (end == skipstr) {
      int eos = 0;
      // Check if we started gc'ing the following ref(s), but got interrupted.
      // If so, try again
      skip = strtol(skipstr + strlen(IN_PROG), &end, 10);
      if (errno || (end == skipstr) || (skip == 0)) {
        LOG(LOG_ERR, "Rank %d: Failed to parse skip value for \"%s\" (%s)", rank, rpath, strerror(errno));
        free(skipstr);
        ms->mdal->close(handle);
        return -1;
      }

      if (skip < 0) {
        skip *= -1;
        eos = 1;
      }

      gc(ms, ds, ctxt, *ftag, ftag->fileno, ftag->fileno, ftag->fileno + skip + 1, -1, -1, eos, q_data);

      if (eos) {
        skip = -1;
      }
    }

    *del_zero = (*end == ZERO_FLAG);

    free(skipstr);

    if (skip == 0 && !(*del_zero)) {
      LOG(LOG_ERR, "Rank %d: Invalid skip value\n", rank);
      ms->mdal->close(handle);
      return -1;
    }

  }

  if (ms->mdal->close(handle)) {
    LOG(LOG_ERR, "Rank %d: Failed to close handle for reference path \"%s\"\n", rank, rpath);
  }

  return skip + 1;
}

/** Determine the number of the object a given file ends in
 * @param FTAG* ftag : Reference to the ftag struct of the file
 * @param size_t headerlen : Length of the file header. NOTE: all files within a
 * stream have the same header length, so this value is calculated beforehand
 * @return int : The object number
 */
int end_obj(FTAG* ftag, size_t headerlen) {
  size_t dataperobj = ftag->objsize - (headerlen + ftag->recoverybytes);
  size_t finobjbounds = (ftag->bytes + ftag->offset - headerlen) / dataperobj;
  // special case check
  if ((ftag->state & FTAG_DATASTATE) >= FTAG_FIN && finobjbounds && (ftag->bytes + ftag->offset - headerlen) % dataperobj == 0) {
    // if we exactly align to object bounds AND the file is FINALIZED,
    //   we've overestimated by one object
    finobjbounds--;
  }

  return ftag->objno + finobjbounds;
}

/** Walks the stream that starts at the given reference file, garbage collecting
 * inactive references/objects and collecting quota data
 * @param const marfs_ms* ms : Reference to the current MarFS metadata scheme
 * @param const marfs_ds* ds : Reference to the current MarFS data scheme
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target
 * namespace
 * @param const char* refpath : Path of the starting reference file within the
 * stream
 * @param quota data* q_data : Reference to the quota data struct to populate
 * @return int : Zero on success, or -1 on failure.
 */
int walk_stream(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, char** refpath, quota_data* q_data) {
  q_data->size = 0;
  q_data->count = 0;
  q_data->del_objs = 0;
  q_data->del_refs = 0;
  q_data->rebuilds = 0;

  char* rpath = strdup(*refpath);

  // Generate an ftag for the beginning of the stream
  FTAG ftag;
  ftag.fileno = 0;
  ftag.ctag = NULL;
  ftag.streamid = NULL;
  struct stat st;
  int del_zero = 0;
  int next = get_ftag(ms, ds, ctxt, &ftag, rpath, &st, q_data, &del_zero);
  if (next < 0) {
    LOG(LOG_ERR, "Rank %d: Failed to retrieve FTAG for \"%s\"\n", rank, rpath);
    return -1;
  }

  // Calculate the header size (constant throughout the stream)
  RECOVERY_HEADER header = {
    .majorversion = RECOVERY_CURRENT_MAJORVERSION,
    .minorversion = RECOVERY_CURRENT_MINORVERSION,
    .ctag = ftag.ctag,
    .streamid = ftag.streamid
  };
  size_t headerlen;
  if ((headerlen = recovery_headertostr(&(header), NULL, 0)) < 1) {
    LOG(LOG_ERR, "Rank %d: Failed to identify recovery header length for final file\n", rank);
    headerlen = 0;
  }

  // Iterate through the refs in the stream, gc'ing/ counting quota data
  int inactive = 0;
  int ignore_zero = 0;
  int last_ref = -1;
  int v_ref = -1;
  int last_obj = -1;
  while (ftag.endofstream == 0 && next > 0 && (ftag.state & FTAG_DATASTATE) >= FTAG_FIN) {
    if (difftime(time(NULL), st.st_ctime) > RECENT_THRESH) {
      if (st.st_nlink < 2) {
        // File has been deleted (needs to be gc'ed)
        if ((!inactive || ignore_zero) && ftag.objno > last_obj + 1) {
          last_obj = ftag.objno - 1;
        }
        if (!inactive) {
          if (last_ref >= 0 && ftag.fileno > last_ref + 1) {
            last_ref = ftag.fileno - 1;
          }
          inactive = 1;
        }
        if (ftag.fileno == 0 && del_zero) {
          ignore_zero = 1;
        }
        else {
          ignore_zero = 0;
        }
      }
      else {
        // File is active, so count towards quota and gc previous files if needed
        if (inactive && !ignore_zero) {
          gc(ms, ds, ctxt, ftag, v_ref, last_ref, ftag.fileno, last_obj, ftag.objno, 0, q_data);
        }
        inactive = 0;
        ignore_zero = 0;
        q_data->size += st.st_size;
        q_data->count++;
        last_ref = ftag.fileno;
        v_ref = ftag.fileno;
        last_obj = end_obj(&ftag, headerlen);
      }
    }
    else {
      // File is too young to be gc'ed/counted, but still gc previous files if
      // needed, and treat as 'active' for future gc logic
      if (inactive && !ignore_zero) {
        gc(ms, ds, ctxt, ftag, v_ref, last_ref, ftag.fileno, last_obj, ftag.objno, 0, q_data);
      }
      inactive = 0;
      ignore_zero = 0;
      last_ref = ftag.fileno;
      v_ref = ftag.fileno;
      last_obj = end_obj(&ftag, headerlen);
    }

    ftag.fileno += next;

    // Generate path and ftag of next (existing) ref in the stream
    free(rpath);
    if ((rpath = datastream_genrpath(&ftag, ms)) == NULL) {
      LOG(LOG_ERR, "Rank %d: Failed to create rpath\n", rpath);
      free(ftag.ctag);
      ftag.ctag = NULL;
      free(ftag.streamid);
      ftag.streamid = NULL;
      return -1;
    }

    if ((next = get_ftag(ms, ds, ctxt, &ftag, rpath, &st, q_data, &del_zero)) < 0) {
      LOG(LOG_ERR, "Rank %d: Failed to retrieve ftag for %s\n", rank, rpath);
      free(ftag.ctag);
      ftag.ctag = NULL;
      free(ftag.streamid);
      ftag.streamid = NULL;
      return -1;
    }
  }

  free(rpath);

  // Repeat process in loop, but for last ref in stream
  if (difftime(time(NULL), st.st_ctime) > RECENT_THRESH) {
    if (st.st_nlink < 2) {
      // Since this is the last ref in the stream, gc it along with previous
      // refs if needed
      if (!inactive) {
        if (ftag.objno > last_obj + 1) {
          last_obj = ftag.objno - 1;
        }
        if (last_ref >= 0 && ftag.fileno > last_ref + 1) {
          last_ref = ftag.fileno - 1;
        }
      }

      gc(ms, ds, ctxt, ftag, v_ref, last_ref, ftag.fileno + 1, last_obj, end_obj(&ftag, headerlen) + 1, 1, q_data);
    }
    else {
      // Same as above
      if (inactive && !ignore_zero) {
        gc(ms, ds, ctxt, ftag, v_ref, last_ref, ftag.fileno, last_obj, ftag.objno, 0, q_data);
      }
      q_data->size += st.st_size;
      q_data->count++;
    }
  }
  else {
    if (inactive && !ignore_zero) {
      // Same as above
      gc(ms, ds, ctxt, ftag, v_ref, last_ref, ftag.fileno, last_obj, ftag.objno, 0, q_data);
    }
  }

  free(ftag.ctag);
  ftag.ctag = NULL;
  free(ftag.streamid);
  ftag.streamid = NULL;

  return 0;
}

/** Attempts to rebuild the object specified by the given reference file
 * @param const marfs_ms* ms : Reference to the current MarFS metadata scheme
 * @param const marfs_ds* ds : Reference to the current MarFS data scheme
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target
 * namespace
 * @param const char* rpath : Path of the reference file containing the rebuild
 * information for the object
 * @param FTAG* ftag : Reference to a FTAG struct for the object to be rebuilt
 * @param ne_state* rtag : Reference to a libne state struct containing
 * rebuilding data
 * @return int : Zero on success, or -1 on failure
 */
int rebuild_obj(const marfs_ms* ms, const marfs_ds* ds, MDAL_CTXT ctxt, const char* rpath, FTAG* ftag, ne_state* rtag) {
  char* objID;
  ne_erasure epat;
  ne_location loc;
  if (datastream_objtarget(ftag, ds, &objID, &epat, &loc)) {
    LOG(LOG_ERR, "Rank %d: Failed to generate object name (%s)\n", rank, strerror(errno));
    return -1;
  }

  // Attempt to rebuild the object
  ne_handle handle = ne_open(ds->nectxt, objID, loc, epat, NE_REBUILD);
  if (handle) {
    if (rtag && ne_seed_status(handle, rtag)) {
      LOG(LOG_ERR, "Rank %d: Failed to seed status for object %s (%s)\n", rank, objID, strerror(errno));
    }

    if (ne_rebuild(handle, &epat, NULL) < 0) {
      LOG(LOG_ERR, "Rank %d: Failed to rebuild object %s(%s)\n", rank, objID, strerror(errno));
      ne_close(handle, NULL, NULL);
      free(objID);
      return -1;
    }

    if (ne_close(handle, NULL, NULL)) {
      LOG(LOG_ERR, "Rank %d: Failed to close object %s (%s)\n", rank, objID, strerror(errno));
    }
  }
  else if (errno == ENOENT) {
    // Attempt to stat a ref corresponding to the object, in case the object
    // still exists, but is inaccessible (e.g. the NFS mount is down)
    char* refpath;
    if ((refpath = datastream_genrpath(ftag, ms)) == NULL) {
      LOG(LOG_ERR, "Rank %d: Failed to create refpath\n", rank);
      free(objID);
      return -1;
    }

    struct stat st;
    if (!ms->mdal->statref(ctxt, refpath, &st) || errno != ENOENT) {
      LOG(LOG_ERR, "Rank %d: Failed to find object \"%s\" (%s)\n", rank, objID, strerror(errno));
      free(refpath);
      free(objID);
      return -1;
    }
  }
  else {
    LOG(LOG_ERR, "Rank %d: Failed to open object %s for rebuild (%s)\n", rank, objID, strerror(errno));
    free(objID);
    return -1;
  }

  free(objID);

  // Rebuild already completed, delete rebuild file from the ref tree
  if (ms->mdal->unlinkref(ctxt, rpath)) {
    LOG(LOG_ERR, "Rank %d: Failed to unlink rebuild ref %s\n", rank, rpath);
    return -1;
  }

  return 0;
}

/** Initialize the state for a thread in the thread queue
 * @param unsigned int tID : The ID of this thread
 * @param void* global_state : Reference to the thread queue's global state
 * @param void** state : Reference to be populated with this thread's state
 * @return int : Zero on success, or -1 on failure.
 */
int stream_thread_init(unsigned int tID, void* global_state, void** state) {
  *state = calloc(1, sizeof(struct tq_thread_struct));
  if (!*state) {
    return -1;
  }
  ThreadState tstate = ((ThreadState)*state);

  tstate->tID = tID;
  tstate->global = (GlobalState)global_state;

  return 0;
}

/** Free a work package if it is still allocated
 * @param void** state : Reference to the thread's state
 * @param void** prev_work : Reference to a possibly-allocated work package
 * @param int flg : Current control flags of the thread
 */
void stream_term(void** state, void** prev_work, TQ_Control_Flags flg) {
  if (*prev_work != NULL) {
    LOG(LOG_INFO, "prev_work allocated!\n");
    free_work(*prev_work);
  }
}

/** Consume a work package containing the reference file at the start of the
 * stream, and walk the stream
 * @param void** state : Reference to the consumer thread's state
 * @param void** work_todo : Reference to the work package containing the
 * reference file's path
 * @return int : Zero on success, or -1 on failure.
 */
int stream_cons(void** state, void** work_todo) {
  ThreadState tstate = ((ThreadState)*state);
  WorkPkg work = ((WorkPkg)*work_todo);

  if (!work) {
    LOG(LOG_ERR, "Rank %d: Thread %u received empty work package. Nothing to do!\n", rank, tstate->tID);
    return 0;
  }

  if (!(work->rpath)) {
    LOG(LOG_ERR, "Rank %d: Thread %u received invalid work package\n", rank, tstate->tID);
    return -1;
  }

  if (work->type == 0) { // Perform GC/quota
    // Walk the stream for the given ref
    // NOTE: It might be better to combine this function with walk_stream(), but I
    // left things as-is to still leave the interface for walking a stream easily
    // exposed, in case we ever decide to walk streams with something other than
    // consumer threads
    quota_data q_data;
    if (walk_stream(tstate->global->ms, tstate->global->ds, tstate->global->ctxt, &(work->rpath), &q_data)) {
      LOG(LOG_ERR, "Rank %d: Thread %u failed to walk stream\n", rank, tstate->tID);
      return -1;
    }

    tstate->q_data.size += q_data.size;
    tstate->q_data.count += q_data.count;
    tstate->q_data.del_objs += q_data.del_objs;
    tstate->q_data.del_refs += q_data.del_refs;
  }
  else { // Perform rebuild
    if (!(work->ftag)) {
      LOG(LOG_ERR, "Rank %d: Thread %u received invalid rebuild work package\n", rank, tstate->tID);
      return -1;
    }

    rebuild_obj(tstate->global->ms, tstate->global->ds, tstate->global->ctxt, work->rpath, work->ftag, work->rtag);
    tstate->q_data.rebuilds++;
  }

  free_work(work);

  return 0;
}

/** Access the reference directory of a namespace, and scan it for reference
 * files corresponding to the start of a stream. Once a reference file is found,
 * create a work package for it and return. If the given reference directory is
 * fully scanned without finding the start of a stream, access a new reference
 * directory and repeat.
 * @param void** state : Reference to the producer thread's state
 * @param void** work_tofill : Reference to be populated with the work package
 * @return int : Zero on success, -1 on failure, and 1 once all reference
 * directories within the namespace have been accessed
 */
int stream_prod(void** state, void** work_tofill) {
  ThreadState tstate = ((ThreadState)*state);
  GlobalState gstate = tstate->global;

  WorkPkg work = NULL;

  // Search for the beginning of a stream
  while (1) {
    // Access a new ref dir, if we don't still have scanning to do in our
    // current dir
    if (!(tstate->ref_node)) {
      if (tstate->scanner) {
        gstate->ms->mdal->closescanner(tstate->scanner);
        tstate->scanner = NULL;
      }

      if (pthread_mutex_lock(&(gstate->lock))) {
        LOG(LOG_ERR, "Rank %d: Failed to acquire lock\n", rank);
        return -1;
      }

      if (gstate->next_node >= gstate->ms->refnodecount) {
        pthread_mutex_unlock(&(gstate->lock));
        return 1;
      }

      tstate->ref_node = &(gstate->ms->refnodes[gstate->next_node++]);

      pthread_mutex_unlock(&(gstate->lock));
    }

    if (!(tstate->scanner)) {
      struct stat statbuf;
      if (gstate->ms->mdal->statref(gstate->ctxt, tstate->ref_node->name, &statbuf)) {
        LOG(LOG_ERR, "Rank %d: Failed to stat %s\n", rank, tstate->ref_node->name);
        tstate->ref_node = NULL;
        continue;
      }

      tstate->scanner = gstate->ms->mdal->openscanner(gstate->ctxt, tstate->ref_node->name);
    }

    // Scan through the ref dir until we find the beginning of a stream
    struct dirent* dent;
    int refno;
    char type;
    while ((dent = gstate->ms->mdal->scan(tstate->scanner))) {
      if (*dent->d_name != '.') {
        refno = ftag_metainfo(dent->d_name, &type);
        if (refno < 0) {
          LOG(LOG_ERR, "Rank %d: Failed to retrieve metainfo\n", rank);
          return -1;
        }
        else if (type == 1 || refno == 0) { // We want to submit a work
        // package for this ref
          work = malloc(sizeof(struct work_pkg_struct));
          if (!work) {
            LOG(LOG_ERR, "Rank %d: Failed to allocate new work package\n", rank);
            return -1;
          }
          work->type = type;
          work->rpath = NULL;
          work->ftag = NULL;
          work->rtag = NULL;

          size_t rpath_len = strlen(tstate->ref_node->name) + strlen(dent->d_name) + 1;
          if (rpath_len == 0) {
            LOG(LOG_ERR, "length of 0\n");
            return -1;
          }
          work->rpath = malloc(sizeof(char) * rpath_len);
          if (work->rpath == NULL) {
            LOG(LOG_ERR, "Rank %d: Failed to allocate rpath string\n", rank);
            return -1;
          }

          if (snprintf(work->rpath, rpath_len, "%s%s", tstate->ref_node->name, dent->d_name) != (rpath_len - 1)) {
            LOG(LOG_ERR, "Rank %d: Failed to populate rpath string\n", rank);
            return -1;
          }

          if (work->type == 1) { // We want to rebuild this object, pull xattrs
            // Get ftag from xattr
            MDAL_DHANDLE handle;
            if ((handle = gstate->ms->mdal->openref(gstate->ctxt, work->rpath, O_RDONLY, 0)) == NULL) {
              LOG(LOG_ERR, "Rank %d: Failed to open handle for reference path \"%s\"\n", rank, work->rpath);
              LOG(LOG_ERR, "Rank %d: Failed to open handle for reference path \"%s\"\n", rank, work->rpath);
              return -1;
            }

            char* xattrstr = NULL;
            int xattrsz;
            if ((xattrsz = gstate->ms->mdal->fgetxattr(handle, 1, FTAG_NAME, xattrstr, 0)) <= 0) {
              // Check how old the file is to make sure the missing xattr isn't
              // in the process of being added
              struct stat st;
              if (gstate->ms->mdal->statref(gstate->ctxt, work->rpath, &st)) {
                LOG(LOG_ERR, "Rank %d: Failed to retrieve FTAG value and stat reference path \"%s\" (%s)\n", rank, work->rpath, strerror(errno));
                gstate->ms->mdal->close(handle);
                return -1;
              }

              if (difftime(time(NULL), st.st_ctime) > RECENT_THRESH) {
                LOG(LOG_ERR, "Rank %d: Failed to retrieve FTAG value for reference path \"%s\" (%s)\n", rank, work->rpath, strerror(errno));
                gstate->ms->mdal->close(handle);
                return -1;
              }

              // Ignore immature files
              free(work->rpath);
              work->rpath = NULL;
              continue;
            }

            xattrstr = calloc(1, xattrsz + 1);
            if (xattrstr == NULL) {
              LOG(LOG_ERR, "Rank %d: Failed to allocate space for a FTAG string value\n", rank);
              gstate->ms->mdal->close(handle);
              return -1;
            }

            if (gstate->ms->mdal->fgetxattr(handle, 1, FTAG_NAME, xattrstr, xattrsz) != xattrsz) {
              LOG(LOG_ERR, "Rank %d: FTAG value for \"%s\" changed while reading \n", rank, work->rpath);
              free(xattrstr);
              gstate->ms->mdal->close(handle);
              return -1;
            }

            work->ftag = malloc(sizeof(struct ftag_struct));
            if (!(work->ftag)) {
              LOG(LOG_ERR, "Rank %d: Failed to allocate RTAG\n", rank);
              free(xattrstr);
              gstate->ms->mdal->close(handle);
              return -1;
            }

            if (ftag_initstr(work->ftag, xattrstr)) {
              LOG(LOG_ERR, "Rank %d: Failed to parse FTAG string for \"%s\"\n", rank, work->rpath);
              free(xattrstr);
              gstate->ms->mdal->close(handle);
              return -1;
            }

            free(xattrstr);

            // Get RTAG from xattr
            if ((xattrsz = gstate->ms->mdal->fgetxattr(handle, 1, RTAG_NAME, xattrstr, 0)) > 0) {
              xattrstr = calloc(1, xattrsz + 1);
              if (xattrstr == NULL) {
                LOG(LOG_ERR, "Rank %d: Failed to allocate space for a RTAG string value\n", rank);
                gstate->ms->mdal->close(handle);
                return -1;
              }

              if (gstate->ms->mdal->fgetxattr(handle, 1, RTAG_NAME, xattrstr, xattrsz) != xattrsz) {
                LOG(LOG_ERR, "Rank %d: RTAG value for \"%s\" changed while reading \n", rank, work->rpath);
                free(xattrstr);
                gstate->ms->mdal->close(handle);
                return -1;
              }

              work->rtag = calloc(1, sizeof(struct ne_state_struct));
              if (!(work->rtag)) {
                LOG(LOG_ERR, "Rank %d: Failed to allocate RTAG\n", rank);
                free(xattrstr);
                gstate->ms->mdal->close(handle);
                return -1;
              }

              size_t stripe_width = gstate->ds->protection.N + gstate->ds->protection.E;

              work->rtag->meta_status = malloc(sizeof(char) * 2 * (stripe_width + 1));
              if (!(work->rtag->meta_status)) {
                LOG(LOG_ERR, "Rank %d: Failed to allocate RTAG status string\n", rank);
                free(xattrstr);
                gstate->ms->mdal->close(handle);
                return -1;
              }
              work->rtag->data_status = work->rtag->meta_status + stripe_width + 1;

              if (rtag_initstr(work->rtag, stripe_width, xattrstr)) {
                LOG(LOG_ERR, "Rank %d: Failed to parse RTAG string for \"%s\"\n", rank, work->rpath);
                free(xattrstr);
                gstate->ms->mdal->close(handle);
                return -1;
              }
              free(xattrstr);
            }
            else if (errno != ENOATTR) {
              LOG(LOG_ERR, "Rank %d: Failed to retrieve RTAG value for reference path \"%s\"\n", rank, work->rpath);
              gstate->ms->mdal->close(handle);
              return -1;
            }
            if (gstate->ms->mdal->close(handle)) {
              LOG(LOG_ERR, "Rank %d: Failed to close handle for reference path \"%s\"\n", rank, work->rpath);
            }
          }

          *work_tofill = (void*)work;

          return 0;
        }
      }
    }

    gstate->ms->mdal->closescanner(tstate->scanner);
    tstate->scanner = NULL;
    tstate->ref_node = NULL;
  }
  return -1;
}

/** No-op function, just to fill out the TQ struct
 */
int stream_pause(void** state, void** prev_work) {
  return 0;
}

/** No-op function, just to fill out the TQ struct
 */
int stream_resume(void** state, void** prev_work) {
  return 0;
}

/** Launch a thread queue to access all reference directories in a namespace,
 * performing garbage collection and updating quota data.
 * @param const marfs_ns* ns : Reference to the current MarFS namespace
 * @param const char* name : Name of the current MarFS namespace
 * @return int : Zero on success, or -1 on failure.
 */
int ref_paths(const marfs_ns* ns, const char* name) {
  marfs_ms* ms = &ns->prepo->metascheme;
  marfs_ds* ds = &ns->prepo->datascheme;

  // Initialize namespace context
  char* ns_path;
  if (config_nsinfo(ns->idstr, NULL, &ns_path)) {
    LOG(LOG_ERR, "Rank %d: Failed to retrieve path of NS: \"%s\"\n", rank, ns->idstr);
    return -1;
  }

  MDAL_CTXT ctxt;
  if ((ctxt = ms->mdal->newctxt(ns_path, ms->mdal->ctxt)) == NULL) {
    LOG(LOG_ERR, "Rank %d: Failed to create new MDAL context for NS: \"%s\"\n", rank, ns_path);
    free(ns_path);
    return -1;
  }

  // Initialize global state and launch threadqueue
  struct  tq_global_struct gstate;
  if (pthread_mutex_init(&(gstate.lock), NULL)) {
    LOG(LOG_ERR, "Rank %d: Failed to initialize TQ lock\n", rank);
    ms->mdal->destroyctxt(ctxt);
    free(ns_path);
    return -1;
  }

  gstate.ms = ms;
  gstate.ds = ds;
  gstate.ctxt = ctxt;
  gstate.next_node = 0;

  int ns_prod = n_prod;
  if (ms->refnodecount < n_prod) {
    ns_prod = ms->refnodecount;
  }

  TQ_Init_Opts tqopts;
  tqopts.log_prefix = ns_path;
  tqopts.init_flags = TQ_HALT;
  tqopts.global_state = (void*)&gstate;
  tqopts.num_threads = n_cons + ns_prod;
  tqopts.num_prod_threads = ns_prod;
  tqopts.max_qdepth = QDEPTH;
  tqopts.thread_init_func = stream_thread_init;
  tqopts.thread_consumer_func = stream_cons;
  tqopts.thread_producer_func = stream_prod;
  tqopts.thread_pause_func = stream_pause;
  tqopts.thread_resume_func = stream_resume;
  tqopts.thread_term_func = stream_term;

  ThreadQueue tq = tq_init(&tqopts);
  if (!tq) {
    LOG(LOG_ERR, "Rank %d: Failed to initialize tq\n", rank);
    pthread_mutex_destroy(&(gstate.lock));
    ms->mdal->destroyctxt(ctxt);
    free(ns_path);
    return -1;
  }
  if ( tq_check_init(tq) ) {
    LOG(LOG_ERR, "Rank %d: Initialization failure in tq\n", rank);
    pthread_mutex_destroy(&(gstate.lock));
    ms->mdal->destroyctxt(ctxt);
    free(ns_path);
    return -1;
  }

  // Wait until all threads are done executing
  TQ_Control_Flags flags = 0;
  while (!(flags & TQ_ABORT) && !(flags & TQ_FINISHED)) {
    if (tq_wait_for_flags(tq, 0, &flags)) {
      LOG(LOG_ERR, "Rank %d: NS %s failed to get TQ flags\n", rank, ns_path);
      pthread_mutex_destroy(&(gstate.lock));
      ms->mdal->destroyctxt(ctxt);
      free(ns_path);
      return -1;
    }

    // Thread queue should never halt, so just clear flag
    if (flags & TQ_HALT) {
      tq_unset_flags(tq, TQ_HALT);
    }
  }
  if (flags & TQ_ABORT) {
    LOG(LOG_ERR, "Rank %d: NS %s TQ aborted\n", rank, ns_path);
    pthread_mutex_destroy(&(gstate.lock));
    ms->mdal->destroyctxt(ctxt);
    free(ns_path);
    return -1;
  }

  // Aggregate quota data from all consumer threads
  quota_data totals;
  totals.size = 0;
  totals.count = 0;
  totals.del_objs = 0;
  totals.del_refs = 0;
  totals.rebuilds = 0;
  int tres = 0;
  ThreadState tstate = NULL;
  while ((tres = tq_next_thread_status(tq, (void**)&tstate)) > 0) {
    if (!tstate) {
      LOG(LOG_ERR, "Rank %d: NS %s received NULL status for thread\n", rank, ns_path);
      pthread_mutex_destroy(&(gstate.lock));
      ms->mdal->destroyctxt(ctxt);
      free(ns_path);
      return -1;
    }

    totals.size += tstate->q_data.size;
    totals.count += tstate->q_data.count;
    totals.del_objs += tstate->q_data.del_objs;
    totals.del_refs += tstate->q_data.del_refs;
    totals.rebuilds += tstate->q_data.rebuilds;

    if (tstate->scanner) {
      ms->mdal->closescanner(tstate->scanner);
    }

    free(tstate);
  }
  if (tres < 0) {
    LOG(LOG_ERR, "Rank %d: Failed to retrieve next thread status\n", rank);
    pthread_mutex_destroy(&(gstate.lock));
    ms->mdal->destroyctxt(ctxt);
    free(ns_path);
    return -1;
  }

  tstate = NULL;

  // If any work packages are left on the TQ, dequeue and consume them
  // NOTE: In this case, we initialize a new consumer thread state to handle
  // remaining work
  if (tq_close(tq) > 0) {
    WorkPkg work = NULL;
    while (tq_dequeue(tq, TQ_ABORT, (void**)&work) > 0) {
      if (!tstate && stream_thread_init(tqopts.num_threads, (void*)&gstate, (void**)&tstate)) {
        LOG(LOG_ERR, "Rank %d: Failed to initialize thread state for cleanup\n", rank);
        pthread_mutex_destroy(&(gstate.lock));
        ms->mdal->destroyctxt(ctxt);
        free(ns_path);
        return -1;
      }

      if (stream_cons((void**)&tstate, (void**)&work)) {
        LOG(LOG_ERR, "Rank %d: Failed to consume work left on queue\n", rank);
        free(tstate);
        pthread_mutex_destroy(&(gstate.lock));
        ms->mdal->destroyctxt(ctxt);
        free(ns_path);
        return -1;
      }
    }
    tq_close(tq);
  }

  if (tstate) {
    totals.size += tstate->q_data.size;
    totals.count += tstate->q_data.count;
    totals.del_objs += tstate->q_data.del_objs;
    totals.del_refs += tstate->q_data.del_refs;
    totals.rebuilds += tstate->q_data.rebuilds;

    free(tstate);
  }

  pthread_mutex_destroy(&(gstate.lock));

  // Update quota values for the namespace
  if (ms->mdal->setdatausage(ctxt, totals.size)) {
    LOG(LOG_ERR, "Rank %d: Failed to set data usage for namespace %s\n", rank, ns_path);
  }
  if (ms->mdal->setinodeusage(ctxt, totals.count)) {
    LOG(LOG_ERR, "Rank %d: Failed to set inode usage for namespace %s\n", rank, ns_path);
  }

  char* efgc = "Eligible for GC";
  if (del) {
    efgc = "Deleted";
  }

  char msg[1024] = {0};
  snprintf(msg, 1024, "Rank: %d NS: \"%s\" Count: %lu Size: %lfTiB Rebuilds: %lu %s: (Objs: %lu Refs: %lu)", rank, name, totals.count, totals.size / 1024.0 / 1024.0 / 1024.0 / 1024.0, totals.rebuilds, efgc, totals.del_objs, totals.del_refs);
  MPI_Send(msg, 1024, MPI_CHAR, n_ranks - 1, 0, MPI_COMM_WORLD);

  ms->mdal->destroyctxt(ctxt);
  free(ns_path);

  return 0;
}

/** Iterate through all the namespaces in a given tree, performing garbage
 * collection and updating quota data on namespaces with indexes that
 * correspond to the current process rank.
 * @param const marfs_ns* ns : Reference to the namespace at the root of the
 * tree
 * @param int idx : Absolute index of the namespace at the root of the tree
 * @param const char* name : Name of the namespace at the root of the tree
 * @return int : Zero on success, or -1 on failure.
 */
int find_rank_ns(const marfs_ns* ns, int idx, const char* name, const char* ns_tgt) {
  // Access the namespace if it corresponds to rank
  if (ns_tgt) {
    if (rank != 0) {
      return 0;
    }
    if (!strcmp(name, ns_tgt)) {
      if (ref_paths(ns, name)) {
        return -1;
      }
      return 0;
    }
  }
  else if (idx % (n_ranks - 1) == rank) {
    if (ref_paths(ns, name)) {
      return -1;
    }

  }
  idx++;

  // Iterate through all subspaces
  int i;
  for (i = 0; i < ns->subnodecount; i++) {
    idx = find_rank_ns(ns->subnodes[i].content, idx, ns->subnodes[i].name, ns_tgt);
    if (idx < 0) {
      return -1;
    }
  }

  if (ns_tgt && idx) {
    return -1;
  }

  return idx;
}

int main(int argc, char** argv) {
  errno = 0;
  char* config_path = NULL;
  char* ns_tgt = NULL;
  char* delim = NULL;
  char config_v = 0;

  char pr_usage = 0;
  int c;
  // parse all position-indepenedent arguments
  while ((c = getopt(argc, argv, "c:dn:t:v")) != -1) {
    switch (c) {
    case 'c':
      config_path = optarg;

      break;
    case 'd':
      del = 1;

      break;
    case 'n':
      ns_tgt = optarg;

      break;
    case 't':
      delim = strchr(optarg, ':');
      if (!delim || (n_prod = strtol(optarg, NULL, 10)) <= 0 || (n_cons = strtol(delim + 1, NULL, 10)) <= 0) {
        fprintf(stderr, "Invalid thread argument %s\n", optarg);
        return -1;
      }

      break;
    case 'v':
      config_v = 1;

      break;
    case '?':
      pr_usage = 1;

      break;
    default:
      fprintf(stderr, "Failed to parse command line options\n");
      return -1;
    }
  }

  // check if we need to print usage info
  if (pr_usage) {
    printf(OUTPREFX "Usage info --\n");
    printf(OUTPREFX "%s -c configpath [-v] [-d] [-n namespace] [-t p:c]\n", PROGNAME);
    printf(OUTPREFX "   -c : Path of the MarFS config file\n");
    printf(OUTPREFX "   -v : Verify the MarFS config\n");
    printf(OUTPREFX "   -d : Delete resources eligible for garbage collection\n");
    printf(OUTPREFX "   -n : Name of MarFS namespace\n");
    printf(OUTPREFX "   -t : Launch p producer and c consumer threads for each namespace\n");
    printf(OUTPREFX "   -h : Print this usage info\n");
    return -1;
  }

  // verify that a config was defined
  if (config_path == NULL) {
    fprintf(stderr, OUTPREFX "no config path defined ( '-c' arg )\n");
    return -1;
  }

  // Initialize MarFS context
  marfs_config* cfg = config_init(config_path);
  if (cfg == NULL) {
    fprintf(stderr, OUTPREFX "Rank %d: Failed to initialize config: \"%s\" ( %s )\n", rank, config_path, strerror(errno));
    return -1;
  }

  if (config_v) {
    if (config_verify(cfg, ".", 1, 1, 1, 1)) {
      fprintf(stderr, OUTPREFX "Rank %d: Failed to verify config: %s\n", rank, strerror(errno));
      config_term(cfg);
      return -1;
    }
  }

  // MPI Initialization
  if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
    LOG(LOG_ERR, "Error in MPI_Init\n");
    config_term(cfg);
    return -1;
  }

  if (MPI_Comm_size(MPI_COMM_WORLD, &n_ranks) != MPI_SUCCESS) {
    LOG(LOG_ERR, "Error in MPI_Comm_size\n");
    config_term(cfg);
    return -1;
  }
  if (n_ranks < 2) {
    fprintf(stderr, "Must have at least 2 ranks\n");
    config_term(cfg);
    return -1;
  }

  if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS) {
    LOG(LOG_ERR, "Error in MPI_Comm_rank\n");
    config_term(cfg);
    return -1;
  }

  // Iterate through all namespaces, gc'ing and updating quota data
  int total_ns = find_rank_ns(cfg->rootns, 0, "root", ns_tgt);
  if (total_ns < 0) {
    if (ns_tgt) {
      fprintf(stderr, "Failed to find namespace \"%s\"\n", ns_tgt);
    }
    config_term(cfg);
    return -1;
  }

  if (ns_tgt) {
    total_ns = 1;
  }

  // Last rank collects a message for each namespace from other processes
  int i;
  if (rank == n_ranks - 1) {
    char* msg = malloc(sizeof(char) * 1024);
    for (i = 0; i < total_ns; i++) {
      MPI_Recv(msg, 1024, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf(OUTPREFX "%s\n", msg);
    }
    free(msg);
  }

  MPI_Finalize();

  config_term(cfg);

  return 0;
}
