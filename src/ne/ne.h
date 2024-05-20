#ifndef __NE_H__
#define __NE_H__

#ifdef __cplusplus
extern "C"
{
#endif

  /*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#define INT_CRC
#define META_FILES

#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>
#include <libxml/tree.h>

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif

/* MIN_PROTECTION sets the threshold for when writes will fail.  If
   fewer than n+MIN_PROTECTION blocks were written successfully, then
   the write will fail. */
#define MIN_PROTECTION 1

/* MIN_MD_CONSENSUS defines the minimum number of metadata files/xattrs we
   have to look at, which are all in agreement about the values for N and
   E, before ne_status1() will believe that it knows what N+E is.  In the
   case of META_FILES being defined, this avoids doing (MAXN + MAXE) -
   (N+E) failed stats for every ne_status(), ne_read(), etc. (In the case
   of UDAL_SOCKETS, each of those failed "stats" results in an attempt to
   connect to a non-existent server, which must then time out.  */
#define MIN_MD_CONSENSUS 2

#define MAXN 9999
#define MAXE 9999

#define MAXNAME 2048
#define MAXBUF 4096
#define MAXBLKSZ 16777216
#define BLKSZ 1048576
#define HEADSZ 70
#define TEST_SEED 57

#define METALEN 125
#define MAXPARTS (MAXN + MAXE)
#define NO_INVERT_MATRIX -2

#define UNSAFE(HANDLE, NERR) ((NERR) && (NERR > ((HANDLE)->erasure_state->E - MIN_PROTECTION)))

typedef uint32_t u32;
typedef uint64_t u64;

// NOTE -- the values of zero and 1 are reserved for internal use (by handles created via ne_stat())
typedef enum
{
 NE_ERR = 0,           // RESERVED FOR INTERNAL USE
 NE_STAT,              // RESERVED FOR INTERNAL USE
 NE_RDONLY,            //2  -- read data, only read erasure when necessary for reconstruction
 NE_RDALL,             //3  -- read data and all erasure, regardless of data state
 NE_WRONLY,            //4  -- write data and erasure to new stripe
 NE_WRALL = NE_WRONLY, //   -- same as above, defined just to avoid confusion
 NE_REBUILD            //5  -- rebuild an existing object
} ne_mode;

typedef struct ne_erasure_struct
{
 int N;
 int E;
 int O;
 size_t partsz;
} ne_erasure;

typedef struct ne_state_struct
{
 // striping size
 size_t versz;
 size_t blocksz;
 size_t totsz;

 // striping health
 char *meta_status; // user allocated region, must be at least ( sizeof(char) * max_block ) bytes; ignored if NULL
 char *data_status; // user allocated region, must be at least ( sizeof(char) * max_block ) bytes; ignored if NULL

 // per-part info
 u64 *csum; // user allocated region, must be at least ( sizeof(u64) * max_block ) bytes; ignored if NULL
} ne_state;

// location struct
typedef struct ne_location_struct
{
 int pod;
 int cap;
 int scatter;
} ne_location;

/*
 ---  Initialization/Termination functions, to produce and destroy a ne_ctxt  ---
*/

// context struct
typedef struct ne_ctxt_struct *ne_ctxt; // forward decl.

/**
 * Initializes a new ne_ctxt
 * @param xmlNode* dal_root : Root of a libxml2 DAL node describing data access
 * @param ne_location max_loc : ne_location struct containing maximum allowable pod/cap/scatter
 *                              values for this context
 * @param int max_block : Integer maximum block value ( N + E ) for this context
 * @param pthread_mutex_t* erasurelock : Reference to a pthread_mutex lock, to be used for synchronizing access
 *                                       to isa-l erasure generation functions in multi-threaded programs.
 *                                       If NULL, libne will create such a lock internally.  In such a case,
 *                                       the internal lock will continue to protect multi-threaded programs
 *                                       ONLY if they exclusively use a single ne_ctxt at a time.
 *                                       Multi-threaded programs using multiple ne_ctxt references in parallel
 *                                       MUST create + initialize their own pthread_mutex and pass it into
 *                                       ALL ne_init*() calls.
 * @return ne_ctxt : New ne_ctxt or NULL if an error was encountered
 */
ne_ctxt ne_init(xmlNode *dal_root, ne_location max_loc, int max_block, pthread_mutex_t* erasurelock);

/**
 * Initializes an ne_ctxt with a default posix DAL configuration.
 * This fucntion is intended primarily for use with test utilities and commandline tools.
 * @param const char* path : The complete path template for the erasure stripe
 * @param ne_location max_loc : The maximum pod/cap/scatter values for this context
 * @param pthread_mutex_t* erasurelock : Reference to a pthread_mutex lock, to be used for synchronizing access
 *                                       to isa-l erasure generation functions in multi-threaded programs.
 *                                       If NULL, libne will create such a lock internally.  In such a case,
 *                                       the internal lock will continue to protect multi-threaded programs
 *                                       ONLY if they exclusively use a single ne_ctxt at a time.
 *                                       Multi-threaded programs using multiple ne_ctxt references in parallel
 *                                       MUST create + initialize their own pthread_mutex and pass it into
 *                                       ALL ne_init*() calls.
 * @return ne_ctxt : The initialized ne_ctxt or NULL if an error occurred
 */
ne_ctxt ne_path_init(const char *path, ne_location max_loc, int max_block, pthread_mutex_t* erasurelock);

/**
 * Verify an existing ne_ctxt
 * @param ne_ctxt ctxt : Reference to the ne_ctxt to be verified
 * @param char fix : If non-zero, attempt to correct any problems encountered
 * @return int : Zero on a success, and -1 on a failure
 */
int ne_verify(ne_ctxt ctxt, char fix);

/**
 * Destroys an existing ne_ctxt
 * @param ne_ctxt ctxt : Reference to the ne_ctxt to be destroyed
 * @return int : Zero on a success, and -1 on a failure
 */
int ne_term(ne_ctxt ctxt);

/*
 ---  Per-Object functions, no handle required  ---
*/

/**
 * Delete a given object
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to be rebuilt
 * @param ne_location loc : Location of the object to be rebuilt
 * @return int : Zero on success and -1 on failure
 */
int ne_delete(ne_ctxt ctxt, const char *objID, ne_location loc);

/*
 ---  Per-Object Handle Creation/Destruction  ---
*/

// handle struct
typedef struct ne_handle_struct *ne_handle; // forward decl.

/**
 * Determine the erasure structure of a given object and (optionally) produce a generic handle for it
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to stat
 * @param ne_location loc : Location of the object to stat
 * @return ne_handle : Newly created ne_handle, or NULL if an error occured
 */
ne_handle ne_stat(ne_ctxt ctxt, const char *objID, ne_location loc);

/**
 * Converts a generic handle (produced by ne_stat()) into a handle for a specific operation
 * @param ne_handle handle : Reference to a generic handle (produced by ne_stat())
 * @param ne_mode mode : Mode to be set for handle (NE_RDONLY || NE_RDALL || NE_WRONLY || NE_WRALL || NE_REBUILD)
 * @return ne_handle : Reference to the modified handle, or NULL if an error occured
 */
ne_handle ne_convert_handle(ne_handle handle, ne_mode mode);

/**
 * Create a new handle for reading, writing, or rebuilding a specific object
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to be rebuilt
 * @param ne_location loc : Location of the object to be rebuilt
 * @param ne_erasure epat : Erasure pattern of the object to be rebuilt
 * @param ne_mode mode : Handle mode (NE_RDONLY || NE_RDALL || NE_WRONLY || NE_WRALL || NE_REBUILD)
 * @return ne_handle : Newly created ne_handle, or NULL if an error occured
 */
ne_handle ne_open(ne_ctxt ctxt, const char *objID, ne_location loc, ne_erasure epat, ne_mode mode);

/**
 * Close an open ne_handle
 * @param ne_handle handle : The ne_handle reference to close
 * @param ne_erasure* epat : Address of an ne_erasure struct to be populated (ignored, if NULL)
 * @param ne_state* state : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Number of blocks with errors on success, and -1 on a failure.
 */
int ne_close(ne_handle handle, ne_erasure *epat, ne_state *sref);

/**
 * Abandon a given open WRITE/REBUILD ne_handle. This is roughly equivalent to calling ne_close
 * on the handle; however, NO data changes should be applied to the underlying object (same state
 * as before the handle was opened).
 * @param ne_handle handle : The ne_handle reference to abort
 * @return int : 0 on success, and -1 on a failure.
 */
int ne_abort(ne_handle handle);

/*
 ---  Functions for retrieving/seeding object info  ---
*/

/**
 * Retrieve erasure and status info for a given object handle
 * @param ne_handle handle : Handle to retrieve info for
 * @param ne_erasure* epat : Address of an ne_erasure struct to be populated (ignored, if NULL)
 * @param ne_state* state : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero if no stripe errors have been detected, a positive bitmask if recoverable errors exist,
 *                and -1 if the stripe appears to be unrecoverable.
 */
int ne_get_info(ne_handle handle, ne_erasure *epat, ne_state *sref);

/**
 * Seed error patterns into a given handle (may useful for speeding up ne_rebuild())
 * @param ne_handle handle : Handle for which to set an error pattern
 * @param ne_state* sref : Reference to an ne_state struct containing the error pattern
 * @return int : Zero on success, and -1 on a failure
 */
int ne_seed_status(ne_handle handle, ne_state *sref);

/*
 ---  Handle functions, to read/write/rebuild a specific object  ---
*/

/**
 * Verify a given erasure striped object and reconstruct any damaged data, if possible
 * @param ne_handle handle : Handle on which to perform a rebuild
 * @param ne_erasure* epat : Erasure pattern of the object to be rebuilt
 * @param ne_state* sref : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero if all errors were repaired, a positive integer count of any remaining UNREPAIRED
 *               errors ( rerun this func ), or a negative value if an unrecoverable failure occurred
 */
int ne_rebuild(ne_handle handle, ne_erasure *epat, ne_state *sref);

/**
 * Seek to a new offset on a read ne_handle
 * @param ne_handle handle : Handle on which to seek (must be open for read)
 * @param off_t offset : Offset to seek to
 * @return off_t : The resulting handle offset or -1 on a failure
 */
off_t ne_seek(ne_handle handle, off_t offset);

/**
 * Read from a given NE_RDONLY or NE_RDALL handle
 * @param ne_handle handle : The ne_handle reference to read from
 * @param void* buffer : Reference to a buffer to be filled with read data
 * @param size_t bytes : Number of bytes to be read
 * @return ssize_t : The number of bytes successfully read, or -1 on a failure
 */
ssize_t ne_read(ne_handle handle, void *buffer, size_t bytes);

/**
 * Write to a given NE_WRONLY or NE_WRALL handle
 * @param ne_handle handle : The ne_handle reference to write to
 * @param const void* buffer : Buffer to be written to the handle
 * @param size_t bytes : Number of bytes to be written from the buffer
 * @return ssize_t : The number of bytes successfully written, or -1 on a failure
 */
ssize_t ne_write(ne_handle handle, const void *buffer, size_t nbytes);

#ifdef __cplusplus
}
#endif

#endif
