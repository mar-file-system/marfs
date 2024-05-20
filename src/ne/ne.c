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

/* ---------------------------------------------------------------------------

This file provides the implementation of multiple operations intended for
use by the MarFS MultiComponent DAL.

These include:   ne_read(), ne_write(), ne_health(), and ne_rebuild().

Additionally, each output file gets an xattr added to it (yes all 12 files
in the case of a 10+2 the xattr looks something like this:

   10 2 64 0 196608 196608 3304199718723886772 1717171

These fields, in order, are:

    N         is nparts
    E         is numerasure
    offset    is the starting position of the stripe in terms of part number
    chunksize is chunksize
    nsz       is the size of the part
    ncompsz   is the size of the part but might get used if we ever compress the parts
    totsz     is the total real data in the N part files.

Since creating erasure requires full stripe writes, the last part of the
file may all be zeros in the parts.  Thus, totsz is the real size of the
data, not counting the trailing zeros.

All the parts and all the erasure stripes should be the same size.  To fill
in the trailing zeros, this program uses truncate - punching a hole in the
N part files for the zeros.

In the case where libne is built to include support for S3-authentication,
and to use the libne sockets extensions (RDMA, etc) instead of files, then
the caller (for example, the MarFS sockets DAL) may acquire
authentication-information at program-initialization-time which we could
not acquire at run-time.  For example, access to authentication-information
may require escalated privileges, whereas fuse and pftool de-escalate
priviledges after start-up.  To support such cases, we must allow a caller
to pass cached credentials through the ne_etc() functions, to the
underlying skt_etc() functions.

--------------------------------------------------------------------------- */

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_NE
#define DEBUG DEBUG_NE
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "ne_core"
#include "logging/logging.h"

#include "ne/ne.h"
#include "io/io.h"
#include "dal/dal.h"
#include "thread_queue/thread_queue.h"

#include <isa-l.h>

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <strings.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

// Some configurable values
#define QDEPTH SUPER_BLOCK_CNT + 1

// NE context
typedef struct ne_ctxt_struct {
   // Max Block value
   int max_block;
   // DAL definitions
   DAL dal;
   // Synchronization
   pthread_mutex_t locallock;
   pthread_mutex_t* erasurelock;
} *ne_ctxt;

typedef struct ne_handle_struct {
   /* Reference back to our global context */
   ne_ctxt ctxt;

   /* Object Info */
   char* objID;
   ne_location loc;

   /* Erasure Info */
   ne_erasure epat;
   size_t versz;
   size_t blocksz;
   size_t totsz;

   /* Read/Write Info and Structures */
   ne_mode mode;
   ioblock** iob;
   off_t iob_datasz;
   off_t iob_offset;
   ssize_t sub_offset;

   /* Threading fields */
   ThreadQueue* thread_queues;
   gthread_state* thread_states;
   unsigned int ethreads_running;

   /* Erasure Manipulation Structures */
   unsigned char e_ready;
   unsigned char* prev_in_err;
   unsigned int prev_err_cnt;
   unsigned char* encode_matrix;
   unsigned char* decode_matrix;
   unsigned char* invert_matrix;
   unsigned char* g_tbls;
   unsigned char* decode_index;

} *ne_handle;

static int gf_gen_decode_matrix_simple(unsigned char* encode_matrix,
   unsigned char* decode_matrix,
   unsigned char* invert_matrix,
   unsigned char* temp_matrix,
   unsigned char* decode_index, unsigned char* frag_err_list, int nerrs, int k,
   int m);

// ---------------------- INTERNAL HELPER FUNCTIONS ----------------------

/**
 * Clear/zero out existing ne_state information
 * @param ne_state* state : Reference to the state structure to clear
 */
void zero_state(ne_state* state, int num_blocks) {
   if (state == NULL) {
      return;
   }
   state->blocksz = 0;
   state->versz = 0;
   state->totsz = 0;
   int block = 0;
   for (; block < num_blocks; block++) {
      if (state->meta_status) {
         state->meta_status[block] = 0;
      }
      if (state->data_status) {
         state->data_status[block] = 0;
      }
      if (state->csum) {
         state->csum[block] = 0;
      }
   }
}

/**
 * Cleanup thread ioblock reference and set a finished state
 * @param ioblock** iobref : Reference to the ioblock pointer for the thread
 * @param ThreadQueue tq : ThreadQueue of the thread to terminate
 * @param gthread_state* state : Reference to the global state struct of the thread
 * @param ne_mode mode : Mode value of the current ne_handle
 * @return int : Zero on success and -1 on failure
 */
int terminate_thread(ioblock** iobref, ThreadQueue tq, gthread_state* state, ne_mode mode) {
   ioblock* iob = *(iobref);
   int ret_val = 0;
   // make sure to release any remaining ioblocks
   if (iob) {
      if (mode == NE_WRONLY || mode == NE_WRALL) {
         ioblock* push_block = NULL;
         // perform final reserve calls to split any excess data
         // NOTE -- it's ok to reference our local 'iob' pointer here, as we will be NULLing
         //         out the passed reference later on regardless
         int resret;
         while ((resret = reserve_ioblock(&(iob), &(push_block), state->ioq)) > 0) {
            LOG(LOG_INFO, "Final ioblock is full, enqueueing it\n");
            if (tq_enqueue(tq, TQ_NONE, (void*)(push_block))) {
               LOG(LOG_ERR, "Failed to enqueue semi-final ioblock to thread_queue!\n");
               ret_val = -1;
            }
         }
         if (resret < 0) { // make sure we didn't hit some error during the reservation process
            LOG(LOG_ERR, "Failed to perform final ioblock reservation!\n");
            ret_val = -1;
         }
         if (iob->data_size) {
            LOG(LOG_INFO, "Enqueueing final ioblock\n");
            // if data remains, push it now
            if (tq_enqueue(tq, TQ_NONE, (void*)(iob))) {
               LOG(LOG_ERR, "Failed to enqueue final ioblock to thread_queue!\n");
               ret_val = -1;
            }
         }
         else {
            LOG(LOG_INFO, "Releasing empty final ioblock!\n");
            release_ioblock(state->ioq);
         }
      }
      else {
         LOG(LOG_INFO, "Releasing handle ioblock\n");
         release_ioblock(state->ioq);
      }
      *(iobref) = NULL;
   }
   // signal the thread to finish
   LOG(LOG_INFO, "Setting FINISHED state\n");
   if (tq_set_flags(tq, TQ_FINISHED)) {
      LOG(LOG_ERR, "Failed to set a FINISHED state!\n");
      ret_val = -1;
      // attempt to abort, ignoring errors
      tq_set_flags(tq, TQ_ABORT);
   }
   return ret_val;
}

/**
 * Allocate a new ne_handle structure
 * @param int max_block : Maximum block value
 * @return ne_handle : Allocated handle or NULL
 */
ne_handle allocate_handle(ne_ctxt ctxt, const char* objID, ne_location loc, meta_info* consensus) {
   // create the handle structure itself
   ne_handle handle = calloc(1, sizeof(struct ne_handle_struct));
   if (handle == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a ne_handle structure!\n");
      return NULL;
   }

   // set erasure struct info
   handle->epat.N = consensus->N;
   handle->epat.E = consensus->E;
   handle->epat.O = consensus->O;
   handle->epat.partsz = consensus->partsz;
   //   handle->epat.crcs = 0;
   //   if ( consensus->versz > 0 ) { handle->epat.crcs = 1; }

   // set data info values
   handle->versz = consensus->versz;
   handle->blocksz = consensus->blocksz;
   handle->totsz = consensus->totsz;

   // set some additional handle info
   handle->ctxt = ctxt;
   handle->objID = strdup(objID);
   handle->loc.pod = loc.pod;
   handle->loc.cap = loc.cap;
   handle->loc.scatter = loc.scatter;

   // allocate context elements
   int num_blocks = consensus->N + consensus->E;
   //   handle->meta_status = calloc( num_blocks, sizeof(char) );
   //   if ( handle->meta_status == NULL ) {
   //      LOG( LOG_ERR, "Failed to allocate space for a meta_status array!\n" );
   //      free( handle );
   //      return NULL;
   //   }
   //   handle->data_status = calloc( num_blocks, sizeof(char) );
   //   if ( handle->data_status == NULL ) {
   //      LOG( LOG_ERR, "Failed to allocate space for a data_status array!\n" );
   //      free( handle.state.meta_status );
   //      free( handle );
   //      return NULL;
   //   }
   handle->iob = calloc(num_blocks, sizeof(ioblock*));
   if (handle->iob == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for ioblock references! (%zubytes)\n",
         num_blocks * sizeof(ioblock*));
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->thread_queues = calloc(num_blocks, sizeof(ThreadQueue));
   if (handle->thread_queues == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for thread_queues!\n");
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->thread_states = calloc(num_blocks, sizeof(struct global_state_struct));
   if (handle->thread_states == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for global thread state structs!\n");
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->prev_in_err = calloc(num_blocks, sizeof(char));
   if (handle->prev_in_err == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a prev_error array!\n");
      free(handle->thread_states);
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->decode_index = calloc(num_blocks, sizeof(char));
   if (handle->decode_index == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a decode_index array!\n");
      free(handle->prev_in_err);
      free(handle->thread_states);
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   /* allocate matrices */
   handle->encode_matrix = calloc(num_blocks * consensus->N, sizeof(char));
   if (handle->encode_matrix == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for an encode_matrix!\n");
      free(handle->decode_index);
      free(handle->prev_in_err);
      free(handle->thread_states);
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->decode_matrix = calloc(num_blocks * consensus->N, sizeof(char));
   if (handle->decode_matrix == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a decode_matrix!\n");
      free(handle->encode_matrix);
      free(handle->decode_index);
      free(handle->prev_in_err);
      free(handle->thread_states);
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->invert_matrix = calloc(num_blocks * consensus->N, sizeof(char));
   if (handle->invert_matrix == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for an invert_matrix!\n");
      free(handle->decode_matrix);
      free(handle->encode_matrix);
      free(handle->decode_index);
      free(handle->prev_in_err);
      free(handle->thread_states);
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }
   handle->g_tbls = calloc(consensus->N * consensus->E * 32, sizeof(char));
   if (handle->g_tbls == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for g_tbls!\n");
      free(handle->invert_matrix);
      free(handle->decode_matrix);
      free(handle->encode_matrix);
      free(handle->decode_index);
      free(handle->prev_in_err);
      free(handle->thread_states);
      free(handle->thread_queues);
      free(handle->iob);
      free(handle->objID);
      free(handle);
      return NULL;
   }

   int i;
   for (i = 0; i < num_blocks; i++) {
      // assign values to thread states
      handle->thread_states[i].erasurelock = handle->ctxt->erasurelock;
      // object attributes
      handle->thread_states[i].objID = handle->objID;
      handle->thread_states[i].location.pod = loc.pod;
      handle->thread_states[i].location.block = (i + consensus->O) % num_blocks;
      handle->thread_states[i].location.cap = loc.cap;
      handle->thread_states[i].location.scatter = loc.scatter;
      handle->thread_states[i].dal = ctxt->dal;
      handle->thread_states[i].offset = 0;
      // meta info values
      handle->thread_states[i].minfo.N = consensus->N;
      handle->thread_states[i].minfo.E = consensus->E;
      handle->thread_states[i].minfo.O = consensus->O;
      handle->thread_states[i].minfo.partsz = consensus->partsz;
      handle->thread_states[i].minfo.versz = consensus->versz;
      handle->thread_states[i].minfo.blocksz = consensus->blocksz;
      handle->thread_states[i].minfo.crcsum = 0;
      handle->thread_states[i].minfo.totsz = consensus->totsz;
      handle->thread_states[i].meta_error = 0;
      handle->thread_states[i].data_error = 0;
      //      size_t iosz = consensus->versz;
      //      if ( iosz <= 0 ) { iosz = handle->dal->io_size; }
      //      handle->thread_states[i].ioq = create_ioqueue( iosz, consensus->partsz, mode );
      //      if ( handle->thread_states[i].ioq == NULL ) {
      //         LOG( LOG_ERR, "Failed to create an ioqueue for block %d!\n", i );
      //         for ( i -= 1; i >= 0; i-- ) {
      //            destroy_ioqueue( handle->thread_states[i].ioq );
      //         }
      //         free( handle->thread_states );
      //         free( handle->thread_queues );
      //         free( handle->iob );
      //         free( handle.state.data_status );
      //         free( handle.state.meta_status );
      //         free( handle );
      //         return NULL;
      //      }
   }

   // indicate that handle is ready for conversion
   handle->mode = NE_STAT;

   return handle;
}

/**
 * Free an allocated ne_handle structure
 * @param ne_handle handle : Handle to free
 */
void free_handle(ne_handle handle) {
   //   int i;
   //   for ( i = 0; i < handle->epat.N + handle->epat.E; i++ ) {
   //      destroy_ioqueue( handle->thread_states[i].ioq );
   //   }
   free(handle->g_tbls);
   free(handle->invert_matrix);
   free(handle->decode_matrix);
   free(handle->encode_matrix);
   free(handle->decode_index);
   free(handle->prev_in_err);
   free(handle->thread_states);
   free(handle->thread_queues);
   free(handle->iob);
   free(handle->objID);
   free(handle);
}

/**
 * This helper function is intended to identify the most common sensible values amongst all meta_buffers
 * for a given number of read threads and return them in a provided read_meta_buffer struct.
 * If two numbers have the same number of instances, preference will be given to the first number ( the
 * one with a lower block number ).
 * @param BufferQueue blocks[ MAXPARTS ] : Array of buffer queues for all threads
 * @param int num_threads : Number of threads with meta_info ready
 * @param read_meta_buffer ret_buf : Buffer to be populated with return values
 * @return int : Lesser of the counts of matching N/E values
 */
int check_matches(meta_info** minfo_structs, int num_blocks, int max_blocks, meta_info* ret_buf) {
   // pre-populate ret_buf with failure values
   ret_buf->N = 0;
   ret_buf->E = -1;
   ret_buf->O = -1;
   ret_buf->partsz = 0;
   ret_buf->versz = -1;
   ret_buf->blocksz = -1;
   ret_buf->totsz = -1;
   // bounds checking
   if ( num_blocks < 1 ) {
      LOG( LOG_ERR, "Called with zero blocks, nothing to check\n" );
      return 0;
   }
   // allocate space for ALL match arrays
   int* N_match = calloc(7, sizeof(int) * num_blocks);
   if (N_match == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for match count arrays!\n");
      return 0;
   }
   int* E_match = (N_match + num_blocks);
   int* O_match = (E_match + num_blocks);
   int* partsz_match = (O_match + num_blocks);
   int* versz_match = (partsz_match + num_blocks);
   int* blocksz_match = (versz_match + num_blocks);
   int* totsz_match = (blocksz_match + num_blocks);

   int i;
   for (i = 0; i < num_blocks; i++) {
      int j;
      meta_info* minfo = minfo_structs[i];
      // this macro is intended to produce counts of matching values at the index of their first appearance
#define COUNT_MATCH_AT_INDEX(VAL, MATCH_LIST, MIN_VAL, MAX_VAL) \
   if (minfo->VAL >= MIN_VAL && minfo->VAL <= MAX_VAL)          \
   {                                                            \
      for (j = 0; j < i; j++)                                   \
      {                                                         \
         if (minfo_structs[j]->VAL == minfo->VAL)               \
         {                                                      \
            break;                                              \
         }                                                      \
      }                                                         \
      MATCH_LIST[j]++;                                          \
   }
      COUNT_MATCH_AT_INDEX(N, N_match, 1, max_blocks)
         COUNT_MATCH_AT_INDEX(E, E_match, 0, max_blocks - 1)
         COUNT_MATCH_AT_INDEX(O, O_match, 0, max_blocks - 1)
         COUNT_MATCH_AT_INDEX(partsz, partsz_match, 1, minfo->partsz)    // no maximum
         COUNT_MATCH_AT_INDEX(versz, versz_match, 0, minfo->versz)       // no maximum
         COUNT_MATCH_AT_INDEX(blocksz, blocksz_match, 0, minfo->blocksz) // no maximum
         COUNT_MATCH_AT_INDEX(totsz, totsz_match, 0, minfo->totsz)       //no maximum
   }

   // find the value with the most matches
   int N_index = 0;
   int E_index = 0;
   int O_index = 0;
   int partsz_index = 0;
   int versz_index = 0;
   int blocksz_index = 0;
   int totsz_index = 0;
   for (i = 1; i < num_blocks; i++) {
      // For N/E: if two values are tied for matches, prefer the larger value (helps to avoid taking values from a single bad meta info)
      if (N_match[i] > N_match[N_index] ||
         (N_match[i] == N_match[N_index] &&
            minfo_structs[i]->N > minfo_structs[N_index]->N))
         N_index = i;
      if (E_match[i] > E_match[E_index] ||
         (E_match[i] == E_match[E_index] &&
            minfo_structs[i]->E > minfo_structs[E_index]->E))
         E_index = i;
      // For other values: if two values are tied for matches, prefer the first
      if (O_match[i] > O_match[O_index])
         O_index = i;
      if (partsz_match[i] > partsz_match[partsz_index])
         partsz_index = i;
      if (versz_match[i] > versz_match[versz_index])
         versz_index = i;
      if (blocksz_match[i] > blocksz_match[blocksz_index])
         blocksz_index = i;
      // For Totsz : if two values are tied for matches, prefer the smaller value (helps to avoid returning zero-fill as 'data')
      if (totsz_match[i] > totsz_match[totsz_index] ||
         (totsz_match[i] == totsz_match[totsz_index] &&
            minfo_structs[i]->totsz < minfo_structs[totsz_index]->totsz))
         totsz_index = i;
   }

   // assign appropriate values to our output struct
   // Note: we have to do a sanity check on the match count, to make sure
   // we don't return an out-of-bounds value.
   char anyvalid = 0;
   if (N_match[N_index]) {
      ret_buf->N = minfo_structs[N_index]->N;
      anyvalid = 1;
   }

   if (E_match[E_index]) {
      ret_buf->E = minfo_structs[E_index]->E;
      anyvalid = 1;
   }

   if (O_match[O_index]) {
      ret_buf->O = minfo_structs[O_index]->O;
      anyvalid = 1;
   }

   if (partsz_match[partsz_index]) {
      ret_buf->partsz = minfo_structs[partsz_index]->partsz;
      anyvalid = 1;
   }

   if (versz_match[versz_index]) {
      ret_buf->versz = minfo_structs[versz_index]->versz;
      anyvalid = 1;
   }

   if (blocksz_match[blocksz_index]) {
      ret_buf->blocksz = minfo_structs[blocksz_index]->blocksz;
      anyvalid = 1;
   }

   if (totsz_match[totsz_index]) {
      ret_buf->totsz = minfo_structs[totsz_index]->totsz;
      anyvalid = 1;
   }

   int retval = (N_match[N_index] > E_match[E_index]) ? E_match[E_index] : N_match[N_index];
   free(N_match);
   // special case check for no valid meta info values
   if (retval == 0 && !(anyvalid)) {
      return -1;
   }
   return retval;
}

/**
 *
 *
 */
int read_stripes(ne_handle handle) {

   // get some useful reference values
   int N = handle->epat.N;
   int E = handle->epat.E;
   ssize_t partsz = handle->epat.partsz;
   size_t stripesz = partsz * N;
#ifdef DEBUG
   size_t offset = (handle->iob_offset * N) + handle->sub_offset;
   unsigned int start_stripe = (unsigned int)(offset / stripesz); // get a stripe num based on offset
#endif

   // make sure our sub_offset is stripe aligned and at the end of our ioblocks ( or zero, if none present )
   if ( handle->sub_offset % stripesz || handle->sub_offset != (handle->iob_datasz * N) ) {
      LOG(LOG_ERR, "Called on handle with an inappropriate sub_offset (%zd)!\n", handle->sub_offset);
      errno = EBADF;
      return -1;
   }
   // update handle offset values ( NOTE : no effect if we don't yet have populated ioblocks )
   handle->iob_offset += handle->iob_datasz;
   // always start with a fresh sub_offset for new stripes
   handle->sub_offset = 0;
   handle->iob_datasz = 0;

   // if we have previous block references, we'll need to release them
   int i;
   for (i = 0; i < handle->epat.N + handle->epat.E && handle->iob[i] != NULL; i++) {
      if (release_ioblock(handle->thread_states[i].ioq)) {
         LOG(LOG_ERR, "Failed to release ioblock reference for block %d!\n", i);
         return -1;
      }
      handle->iob[i] = NULL; // NULL out this outdated reference
   }

   // ---------------------- VERIFY INTEGRITY OF ALL BLOCKS IN STRIPE ----------------------

   // First, loop through all data buffers in the stripe, looking for errors.
   // Then, starup erasure threads as necessary to deal with errors.
   // Technically, it would theoretically be more efficient to limit this to
   // only the blocks we expect to need.  However, if we hit any error in those
   // blocks, we'll suddenly need the whole stripe.  I am hoping the reduced
   // complexity of just checking them all will be worth what will probably
   // only be the slightest of performance hits.  Besides, in the expected
   // use case of reading a file start to finish, we'll eventually need this
   // entire stripe regardless.
   int cur_block;
   int stripecnt = 0;
   int nstripe_errors = 0;
   for (cur_block = 0; (cur_block < (N + nstripe_errors) || cur_block < (N + handle->ethreads_running)) && cur_block < (N + E); cur_block++) {
      // if this thread isn't running, we need to start it
      if (cur_block >= N + handle->ethreads_running) {
         LOG(LOG_INFO, "Starting up thread %d to cope with errors beyond stripe %d\n", cur_block, start_stripe);
         // first, make sure to empty any ioblocks still on the queue
         while (tq_dequeue(handle->thread_queues[cur_block], TQ_HALT, (void**)&(handle->iob[cur_block])) > 0) {
            LOG(LOG_INFO, "Releasing ioblock from queue %d, prior to reseek\n", cur_block);
            if (release_ioblock(handle->thread_states[cur_block].ioq)) {
               LOG(LOG_ERR, "Failed to release ioblock from queue %d\n", cur_block);
               errno = EBADF;
               return -1;
            }
         }
         if ( tq_wait_for_pause( handle->thread_queues[cur_block] ) ) {
            LOG( LOG_ERR, "Failed to verify that thread %d paused, prior to restarting\n", cur_block );
            errno = EBADF;
            return -1;
         }
         handle->thread_states[cur_block].offset = handle->iob_offset; // set offset for this read thread
         if (tq_unset_flags(handle->thread_queues[cur_block], TQ_HALT)) {
            LOG(LOG_ERR, "Failed to clear PAUSE state for block %d!\n", cur_block);
            errno = EBADF;
            return -1;
         }
         handle->ethreads_running++;
      }
      // retrieve a new ioblock from this thread
      if (tq_dequeue(handle->thread_queues[cur_block], TQ_HALT, (void**)&(handle->iob[cur_block])) < 0) {
         LOG(LOG_ERR, "Failed to retrieve new buffer for block %d!\n", cur_block);
         errno = EBADF;
         return -1;
      }
      LOG(LOG_INFO, "Dequeued ioblock at position %d\n", cur_block);
      // check if this new ioblock will require a rebuild
      ioblock* cur_iob = handle->iob[cur_block];
      if (cur_iob->error_end > 0) {
         LOG(LOG_ERR, "Detected an error at offset %zu of ioblock %d\n", cur_iob->error_end, cur_block);
         nstripe_errors++;
      }
      // check if we can even handle however many errors we've hit so far
      if (nstripe_errors > E) {
         LOG(LOG_ERR, "Data beyond stripe %d has too many errors (%d) to be recovered\n", start_stripe, nstripe_errors);
         errno = ENODATA;
         return -1;
      }
      // make sure our ioblock sizes are consistent
      if (handle->iob_datasz) {
         if (cur_iob->data_size != handle->iob_datasz) {
            LOG(LOG_ERR, "Detected a ioblock of size %zd from block %d which conflicts with expected value of %zd!\n",
               cur_iob->data_size, cur_block, handle->iob_datasz);
            errno = EBADF;
            return -1;
         }
      }
      else {
         stripecnt = (cur_iob->data_size / partsz);
         handle->iob_datasz = cur_iob->data_size;
      } // or set it, if we haven't yet
   }

   int block_cnt = cur_block;

   // if we'er trying to avoid unnecessary reads, halt excess erasure threads
   if (handle->mode == NE_RDONLY) {
      // keep the greater of how many erasure threads we've needed in the last couple of stripes...
      if (nstripe_errors > handle->prev_err_cnt)
         handle->prev_err_cnt = nstripe_errors;
      // ...and halt all others
      for (cur_block = (N + nstripe_errors); cur_block < (N + handle->prev_err_cnt); cur_block++) {
         LOG(LOG_INFO, "Setting HALT state for unneded thread %d\n", cur_block);
         if (tq_set_flags(handle->thread_queues[cur_block], TQ_HALT)) {
            // nothing to do besides complain
            LOG(LOG_ERR, "Failed to pause erasure thread for block %d!\n", cur_block);
         }
         else {
            handle->ethreads_running--;
         }
      }
      // need to reassign, in case the number of errors is decreasing
      handle->prev_err_cnt = nstripe_errors;
   }

   // If any errors were found, we need to try and reconstruct any missing data
   if (nstripe_errors) {
      // create some erasure structs
      unsigned char* stripe_in_err = calloc(N + E, sizeof(unsigned char));
      if (stripe_in_err == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for a stripe_in_err array!\n");
         return -1;
      }
      unsigned char* stripe_err_list = calloc(N + E, sizeof(unsigned char));
      if (stripe_err_list == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for a stripe_err_list array!\n");
         free( stripe_in_err );
         return -1;
      }

      // loop over each stripe in reverse order, fixing the ends of the buffers first
      // NOTE -- reconstructing in reverse allows us to continue using the error_end values appropriately
      int cur_stripe;
      for (cur_stripe = stripecnt - 1; cur_stripe >= 0; cur_stripe--) {

         // loop over the blocks of the stripe, establishing error counts/positions
         off_t stripe_start = cur_stripe * partsz;
         nstripe_errors = 0;
         for (cur_block = 0; cur_block < block_cnt; cur_block++) {
            // reset our error state
            stripe_err_list[cur_block] = 0; // as cur_block MUST be <= nstripe_errors at this point
            stripe_in_err[cur_block] = 0;

            // check for bad stripe data in this block
            if (stripe_start < handle->iob[cur_block]->error_end) {
               LOG(LOG_WARNING, "Detected bad data for block %d of stripe %d\n", cur_block, cur_stripe + start_stripe);
               stripe_err_list[nstripe_errors] = cur_block;
               nstripe_errors++;
               stripe_in_err[cur_block] = 1;
               // we just need to note the error, nothing to be done about it until we have all buffers ready
            }

            // check for any change in our error pattern, as that will require reinitializing erasure structs
            if (handle->prev_in_err[cur_block] != stripe_in_err[cur_block]) {
               handle->e_ready = 0;
               handle->prev_in_err[cur_block] = stripe_in_err[cur_block];
            }
         }


         if (!(handle->e_ready)) {

            LOG(LOG_INFO, "Initializing erasure structs...\n");

            unsigned char* tmpmatrix = calloc((N + E) * (N + E), sizeof(unsigned char));
            if (tmpmatrix == NULL) {
               LOG(LOG_ERR, "Failed to allocate space for a tmpmatrix!\n");
               free(stripe_in_err);
               free(stripe_err_list);
               return -1;
            }

            // critical section : we are now going to call some inlined assembly erasure funcs
            if ( pthread_mutex_lock( handle->ctxt->erasurelock ) ) {
               LOG( LOG_ERR, "Failed to acquire erasurelock prior to table generation for stripe %d\n", cur_stripe + start_stripe );
               free(tmpmatrix);
               free( stripe_err_list );
               free( stripe_in_err );
               return -1;
            }

            // Generate an encoding matrix
            gf_gen_cauchy1_matrix(handle->encode_matrix, N + E, N);

            // Generate g_tbls from encode matrix
            ec_init_tables(N, E, &(handle->encode_matrix[N * N]), handle->g_tbls);
            int ret_code = gf_gen_decode_matrix_simple(handle->encode_matrix, handle->decode_matrix,
               handle->invert_matrix, tmpmatrix, handle->decode_index, stripe_err_list,
               nstripe_errors, N, N + E);

            if (ret_code != 0) {
               // this is the only error for which we will at least attempt to continue
               LOG(LOG_ERR, "Failure to generate decode matrix, errors may exceed erasure limits (%d)!\n", nstripe_errors);
               pthread_mutex_unlock( handle->ctxt->erasurelock );
               free(tmpmatrix);
               free(stripe_in_err);
               free(stripe_err_list);
               // return the number of stripes we failed to regenerate
               errno = ENODATA;
               return -1;
            }

            LOG(LOG_INFO, "Initializing erasure tables ( nstripe_errors = %d )\n", nstripe_errors);
            ec_init_tables(N, nstripe_errors, handle->decode_matrix, handle->g_tbls);

            // exiting critical section
            if ( pthread_mutex_unlock( handle->ctxt->erasurelock ) ) {
               LOG( LOG_ERR, "Failed to relinquish erasurelock after table generation for stripe %d\n", cur_stripe + start_stripe );
               free(tmpmatrix);
               free( stripe_err_list );
               free( stripe_in_err );
               return -1;
            }
            free(tmpmatrix);

            handle->e_ready = 1; //indicate that rebuild structures are initialized
         }

         // as this struct will change depending on the head position of our queues, we must generate here
         unsigned char** recov = calloc(N + E, sizeof(unsigned char*));
         if (recov == NULL) {
            LOG(LOG_ERR, "Failed to allocate space for a recovery array!\n");
            free(stripe_in_err);
            free(stripe_err_list);
            return -1;
         }
         //unsigned char* recov[ MAXPARTS ];
         for (cur_block = 0; cur_block < N; cur_block++) {
            //BufferQueue* bq = &handle->blocks[handle->decode_index[cur_block]];
            //recov[cur_block] = bq->buffers[ bq->head ];
            recov[cur_block] = handle->iob[handle->decode_index[cur_block]]->buff + stripe_start;
         }

         unsigned char** temp_buffs = calloc(nstripe_errors, sizeof(unsigned char*));
         if (temp_buffs == NULL) {
            LOG(LOG_ERR, "Failed to allocate space for a temp_buffs array!\n");
            free(recov);
            free(stripe_in_err);
            free(stripe_err_list);
            return -1;
         }
         //unsigned char* temp_buffs[ nstripe_errors ];
         for (cur_block = 0; cur_block < nstripe_errors; cur_block++) {
            //BufferQueue* bq = &handle->blocks[stripe_err_list[ cur_block ]];
            //temp_buffs[ cur_block ] = bq->buffers[ bq->head ];

            // assign storage locations for the repaired buffers to be on top of the faulty buffers
            temp_buffs[cur_block] = handle->iob[stripe_err_list[cur_block]]->buff + stripe_start;
            // as we are regenerating over the bad buffer, mark it as usable from this point on
            handle->iob[stripe_err_list[cur_block]]->error_end = stripe_start;

            // as we are regenerating over the bad buffer, mark it as usable for future iterations
            //*(u32*)( temp_buffs[ cur_block ] + bsz ) = 1;
         }

         LOG(LOG_INFO, "Performing regeneration of stripe %d from erasure\n", cur_stripe + start_stripe);
         // critical section : we are now going to call some inlined assembly erasure funcs
         if ( pthread_mutex_lock( handle->ctxt->erasurelock ) ) {
            LOG( LOG_ERR, "Failed to acquire erasurelock prior to regeneration of stripe %d\n", cur_stripe + start_stripe );
            free(recov);
            free( stripe_err_list );
            free( stripe_in_err );
            return -1;
         }
         ec_encode_data(partsz, N, nstripe_errors, handle->g_tbls, recov, &temp_buffs[0]);
         // exiting critical section
         if ( pthread_mutex_unlock( handle->ctxt->erasurelock ) ) {
            LOG( LOG_ERR, "Failed to relinquish erasurelock after regeneration of stripe %d\n", cur_stripe + start_stripe );
            free(recov);
            free( stripe_err_list );
            free( stripe_in_err );
            return -1;
         }

         free(recov);
         free(temp_buffs);
      } // end of per-stripe loop

      // free unneeded lists
      free(stripe_in_err);
      free(stripe_err_list);

   } // end of error regeneration logic

   return 0;
}

// ---------------------- CONTEXT CREATION/DESTRUCTION/VALIDATION ----------------------

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
ne_ctxt ne_path_init(const char* path, ne_location max_loc, int max_block, pthread_mutex_t* erasurelock) {
   // create a stand-in XML config
   char* configtemplate = "<DAL type=\"posix\"><dir_template>%s</dir_template><sec_root></sec_root></DAL>";
   int len = strlen(path) + strlen(configtemplate);
   char* xmlconfig = malloc(len);
   if (xmlconfig == NULL) {
      LOG(LOG_ERR, "failed to allocate memory for the stand-in xml config!\n");
      return NULL;
   }
   if ((len = snprintf(xmlconfig, len, configtemplate, path)) < 0) {
      LOG(LOG_ERR, "Failed to populate interal XML config string\n");
      free(xmlconfig);
      errno = EBADF;
      return NULL;
   }
   xmlDoc* config = NULL;
   xmlNode* root_elem = NULL;

   /* initialize libxml and check for potential version mismatches */
   LIBXML_TEST_VERSION

      /* parse the stand-in XML config */
      config = xmlReadMemory(xmlconfig, len + 1, "noname.xml", NULL, XML_PARSE_NOBLANKS);
   if (config == NULL) {
      LOG(LOG_ERR, "Failed to parse internal XML config string\n");
      free(xmlconfig);
      return NULL;
   }
   root_elem = xmlDocGetRootElement(config);
   free(xmlconfig); // done with the stand-in xml config

   // Initialize a posix dal instance
   DAL_location maxloc = { .pod = 0, .block = max_block - 1, .cap = 0, .scatter = 0 };
   DAL dal = init_dal(root_elem, maxloc);
   // free the xmlDoc and any parser global vars
   xmlFreeDoc(config);
   xmlCleanupParser();
   // verify that dal initialization was successful
   if (dal == NULL) {
      LOG(LOG_ERR, "DAL initialization failed\n");
      return NULL;
   }

   // allocate a context struct
   ne_ctxt ctxt = calloc(1,sizeof(struct ne_ctxt_struct));
   if (ctxt == NULL) {
      LOG(LOG_ERR, "Failed to allocate an ne_ctxt struct\n");
      dal->cleanup(dal); // cleanup our DAL context, ignoring errors
      return NULL;
   }

   // fill in context elements
   ctxt->max_block = max_block;
   ctxt->dal = dal;
   // verify or create our erasurelock
   if ( erasurelock ) {
      ctxt->erasurelock = erasurelock;
   }
   else {
      if ( pthread_mutex_init( &(ctxt->locallock), NULL ) ) {
         LOG( LOG_ERR, "failed to intialize internal erasurelock\n" );
         free( ctxt );
         dal->cleanup(dal); // cleanup our DAL context, ignoring errors
         return NULL;
      }
      ctxt->erasurelock = &(ctxt->locallock);
   }

   // return the new ne_ctxt
   return ctxt;
}

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
ne_ctxt ne_init(xmlNode* dal_root, ne_location max_loc, int max_block, pthread_mutex_t* erasurelock) {
   // Initialize a DAL instance
   DAL_location maxdal = { .pod = max_loc.pod, .block = max_block - 1, .cap = max_loc.cap, .scatter = max_loc.scatter };
   DAL dal = init_dal(dal_root, maxdal);
   // Verify that the dal intialized successfully
   if (dal == NULL) {
      LOG(LOG_ERR, "DAL instance failed to properly initialize!\n");
      return NULL;
   }

   // allocate a new context struct
   ne_ctxt ctxt = calloc( 1, sizeof(struct ne_ctxt_struct) );
   if (ctxt == NULL) {
      LOG(LOG_ERR, "failed to allocate memory for a new ne_ctxt struct!\n");
      dal->cleanup(dal); // cleanup our DAL context, ignoring errors
      return NULL;
   }

   // verify or create our erasurelock
   if ( erasurelock ) {
      ctxt->erasurelock = erasurelock;
   }
   else {
      if ( pthread_mutex_init( &(ctxt->locallock), NULL ) ) {
         LOG( LOG_ERR, "failed to intialize internal erasurelock\n" );
         free( ctxt );
         dal->cleanup(dal); // cleanup our DAL context, ignoring errors
         return NULL;
      }
      ctxt->erasurelock = &(ctxt->locallock);
   }

   // fill in context values and return
   ctxt->max_block = max_block;
   ctxt->dal = dal;

   return ctxt;
}

/**
 * Verify an existing ne_ctxt
 * @param ne_ctxt ctxt : Reference to the ne_ctxt to be verified
 * @param char fix : If non-zero, attempt to correct any problems encountered
 * @return int : Zero on a success, and -1 on a failure
 */
int ne_verify(ne_ctxt ctxt, char fix) {
   // Just a wrapper around DAL verification
   return ctxt->dal->verify(ctxt->dal->ctxt, fix);
}

/**
 * Destroys an existing ne_ctxt
 * @param ne_ctxt ctxt : Reference to the ne_ctxt to be destroyed
 * @return int : Zero on a success, and -1 on a failure
 */
int ne_term(ne_ctxt ctxt) {
   // Cleanup the DAL context
   if (ctxt->dal->cleanup(ctxt->dal) != 0) {
      LOG(LOG_ERR, "failed to cleanup DAL context!\n");
      return -1;
   }
   // potentially cleanup our local lock
   if ( ctxt->erasurelock == &(ctxt->locallock) ) {
      pthread_mutex_destroy( ctxt->erasurelock );
   }
   free(ctxt);
   return 0;
}

// ---------------------- PER-OBJECT FUNCTIONS ----------------------

/**
 * Delete a given object
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to be rebuilt
 * @param ne_location loc : Location of the object to be rebuilt
 * @return int : Zero on success and -1 on failure
 */
int ne_delete(ne_ctxt ctxt, const char* objID, ne_location loc) {
   // check for NULL context
   if (ctxt == NULL) {
      LOG(LOG_ERR, "Received NULL context!\n");
      errno = EINVAL;
      return -1;
   }
   int retval = 0;
   DAL_location dalloc = { .pod = loc.pod, .cap = loc.cap, .scatter = loc.scatter };
   LOG(LOG_INFO, "Deleting object %s (%d blocks)\n", objID, ctxt->max_block);

   // loop through and delete all blocks
   int i;
   for (i = 0; i < ctxt->max_block; i++) {
      dalloc.block = i;
      if (ctxt->dal->del(ctxt->dal->ctxt, dalloc, objID)) {
         LOG(LOG_ERR, "Failed to delete block %d of object \"%s\"!\n", i, objID);
         retval = -1;
      }
   }

   return retval;
}

// ---------------------- HANDLE CREATION FUNCTIONS ----------------------

/**
 * Determine the erasure structure of a given object and (optionally) produce a generic handle for it
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to stat
 * @param ne_location loc : Location of the object to stat
 * @return ne_handle : Newly created ne_handle, or NULL if an error occured
 */
ne_handle ne_stat(ne_ctxt ctxt, const char* objID, ne_location loc) {
   // allocate space for temporary error arrays
   char* tmp_meta_errs = calloc(ctxt->max_block * 2, sizeof(char));
   if (tmp_meta_errs == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for temporary error arrays!\n");
      return NULL;
   }
   char* tmp_data_errs = tmp_meta_errs + ctxt->max_block;

   // allocate space for a full set of meta_info structs
   meta_info consensus = {0};
   meta_info* minfo_list = calloc(ctxt->max_block, sizeof(struct meta_info_struct));
   if (minfo_list == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a meta_info_struct list!\n");
      free(tmp_meta_errs);
      return NULL;
   }
   meta_info** minfo_refs = calloc(ctxt->max_block, sizeof(meta_info*));
   if (minfo_refs == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a meta_info refs list!\n");
      free(tmp_meta_errs);
      free(minfo_list);
      return NULL;
   }

   // loop through all blocks, getting meta_info for each
   int curblock = 0;
   int valid_meta = 0;
   int maxblock = ctxt->max_block;
   int match_count = 0;
   for (; curblock < maxblock; curblock++) {

      DAL_location dloc = { .pod = loc.pod, .block = curblock, .cap = loc.cap, .scatter = loc.scatter };

      // first, we need to get a block reference
      BLOCK_CTXT dblock = ctxt->dal->open(ctxt->dal->ctxt, DAL_METAREAD, dloc, objID);
      if (dblock == NULL) {
         LOG(LOG_ERR, "Failed to open a DAL reference for block %d!\n", dloc.block);
         tmp_meta_errs[curblock] = 1;
      }
      else {
         // attempt to retrive meta info
         if (ctxt->dal->get_meta(dblock, &(minfo_list[curblock]))) {
            LOG(LOG_WARNING, "Detected a meta error for block %d\n", curblock);
            tmp_meta_errs[curblock] = 1;
         }
         else {
            // set a reference to the retrieved meta info
            minfo_refs[valid_meta] = &(minfo_list[curblock]);
            valid_meta++;
            // get new consensus values, including this info
            match_count = check_matches(minfo_refs, valid_meta, ctxt->max_block, &consensus);
            // if we have sufficient agreement, update our maxblock value and save us some time
            if (match_count > MIN_MD_CONSENSUS) {
               maxblock = consensus.N + consensus.E;
            }
         }
         // close our block reference
         ctxt->dal->close(dblock);
      }

      // verify that data exists for this block
      if (ctxt->dal->stat(ctxt->dal->ctxt, dloc, objID)) {
         tmp_data_errs[curblock] = 1;
      }
   }

   // we're done with our minfo_refs
   free(minfo_refs);

   // if we've failed to retrieve good meta info values, abort early
   if (valid_meta < 1 || match_count < 1 || consensus.N < 1 || consensus.E < 0 ||
      match_count < consensus.N) {
      errno = ENODATA;
      free(tmp_meta_errs);
      free(minfo_list);
      if (valid_meta == 0) {
         errno = ENOENT;
      }
      LOG(LOG_ERR, "Failed to achieve sufficient meta info consensus across %d blocks (%s)\n", maxblock, strerror(errno));
      fprintf(stderr, "Vmeta=%d, Match=%d, Con.N=%d, Con.E=%d\n", valid_meta, match_count, consensus.N, consensus.E);
      return NULL;
   }

   // create a handle structure
   ne_handle handle = allocate_handle(ctxt, objID, loc, &consensus);
   if (handle == NULL) {
      LOG(LOG_ERR, "Failed to allocate ne_handle struct\n");
      free(tmp_meta_errs);
      free(minfo_list);
      return NULL;
   }

   // perform sanity checks on the values we've gotten
   int modeval = NE_STAT;
   if (consensus.N <= 0) {
      modeval = NE_ERR;
   }
   if (consensus.E < 0) {
      modeval = NE_ERR;
   }
   if (consensus.O < 0 || consensus.O >= (consensus.N + consensus.E)) {
      consensus.O = -1;
      modeval = NE_ERR;
   }
   // at this point, if we have all valid N/E/O values, we need to rearange our errors based on offset
   int i;
   if (modeval == NE_STAT) {
      for (i = 0; i < curblock; i++) {
         int translation = (i + consensus.O) % (consensus.N + consensus.E);
         if (tmp_meta_errs[translation]) {
            LOG(LOG_INFO, "Translating meta error on %d to block %d\n", translation, i);
            handle->thread_states[i].meta_error = 1;
         }
         else if (cmp_minfo(&(minfo_list[translation]), &(consensus))) {
            LOG(LOG_WARNING, "Detected meta value mismatch on block %d\n", i);
            handle->thread_states[i].meta_error = 1;
         }
         if (tmp_data_errs[translation]) {
            handle->thread_states[i].data_error = 1;
         }
      }
   }
   free(tmp_meta_errs);
   if (consensus.partsz <= 0) {
      modeval = NE_ERR;
   }
   if (consensus.versz < 0) {
      modeval = NE_ERR;
   }
   if (consensus.blocksz < 0) {
      modeval = NE_ERR;
   }
   if (consensus.totsz < 0) {
      modeval = NE_ERR;
   }
   // if we have successfully identified all meta values, try to set crcs appropriately
   if (modeval) {
      for (i = 0; i < curblock; i++) {
         if (handle->thread_states[i].meta_error == 0) {
            handle->thread_states[i].minfo.crcsum = minfo_list[(i + consensus.O) % (consensus.N + consensus.E)].crcsum;
         }
      }
   }

   // indicate whether the handle appears usable or not
   handle->mode = modeval;
   free(minfo_list);

   return handle;
}

/**
 * Converts a generic handle (produced by ne_stat()) into a handle for a specific operation
 * @param ne_handle handle : Reference to a generic handle (produced by ne_stat())
 * @param ne_mode mode : Mode to be set for handle (NE_RDONLY || NE_RDALL || NE_WRONLY || NE_WRALL || NE_REBUILD)
 * @return ne_handle : Reference to the modified handle, or NULL if an error occured
 */
ne_handle ne_convert_handle(ne_handle handle, ne_mode mode) {
   // sanity check for NULL value
   if (handle == NULL) {
      LOG(LOG_ERR, "Received NULL ne_handle!\n");
      errno = EINVAL;
      return NULL;
   }

   // check that mode is appropriate
   if (handle->mode != NE_STAT) {
      LOG(LOG_ERR, "Received ne_handle has inappropriate mode value!\n");
      errno = EINVAL;
      return NULL;
   }

   // we need to startup some threads
   TQ_Init_Opts tqopts = {0};
   char* lprefstr = malloc(sizeof(char) * (6 + (handle->ctxt->max_block / 10)));
   if (lprefstr == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for TQ log prefix string!\n");
      return NULL;
   }
   tqopts.log_prefix = lprefstr;
   // create a format string for each thread queue
   char* preffmt = "RQ%d";
   if (mode == NE_REBUILD) {
      preffmt = "RRQ%d";
   }
   else if (mode == NE_WRONLY || mode == NE_WRALL) {
      preffmt = "WQ%d";
   }
   tqopts.init_flags = TQ_HALT; // initialize the threads in a HALTED state (essential for reads, doesn't hurt writes)
   tqopts.max_qdepth = QDEPTH;
   tqopts.num_threads = 1;
   tqopts.num_prod_threads = (mode == NE_WRONLY || mode == NE_WRALL) ? 0 : 1;
   DAL_MODE dmode = DAL_READ;
   if (mode == NE_WRONLY || mode == NE_WRALL) {
      dmode = DAL_WRITE;
      tqopts.thread_init_func = write_init;
      tqopts.thread_consumer_func = write_consume;
      tqopts.thread_producer_func = NULL;
      tqopts.thread_pause_func = write_pause;
      tqopts.thread_resume_func = write_resume;
      tqopts.thread_term_func = write_term;
   }
   else {
      tqopts.thread_init_func = read_init;
      tqopts.thread_consumer_func = NULL;
      tqopts.thread_producer_func = read_produce;
      tqopts.thread_pause_func = read_pause;
      tqopts.thread_resume_func = read_resume;
      tqopts.thread_term_func = read_term;
   }
   // finally, startup the threads
   int i;
   for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
      handle->thread_states[i].dmode = dmode;
      tqopts.global_state = &(handle->thread_states[i]);
      // set a log_prefix value for this queue
      snprintf(lprefstr, 6 + (handle->ctxt->max_block/10), preffmt, i);
      handle->thread_queues[i] = tq_init(&tqopts);
      if (handle->thread_queues[i] == NULL) {
         LOG(LOG_ERR, "Failed to create thread_queue for block %d!\n", i);
         // if we failed to initialize any thread_queue, attempt to abort everything else
         for (i -= 1; i >= 0; i--) {
            tq_set_flags(handle->thread_queues[i], TQ_ABORT);
            tq_next_thread_status(handle->thread_queues[i], NULL);
            tq_close(handle->thread_queues[i]);
         }
         free(lprefstr);
         return NULL;
      }
   }
   free(lprefstr);

   // verify successfull initialization of all threads
   for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
      if ( tq_check_init(handle->thread_queues[i]) ) {
         LOG( LOG_ERR, "Detected initialization failure for thread %d\n", i );
         for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
            tq_set_flags(handle->thread_queues[i], TQ_ABORT);
            tq_next_thread_status(handle->thread_queues[i], NULL);
            tq_close(handle->thread_queues[i]);
         }
         return NULL;
      }
   }

   // For reading handles, we may need to get meta info consensus and correct any outliers
   if (mode != NE_WRONLY && mode != NE_WRALL && handle->totsz == 0) {
      // only get meta info if it hasn't already been set (totsz is a good example value)

      // create a reference array for all of our meta_info structs
      meta_info** minforefs = calloc(handle->epat.N + handle->epat.E, sizeof(meta_info*));
      if (minforefs == NULL) {
         LOG(LOG_ERR, "Failed to allocate space for meta_info references!\n");
         // might as well continue to use 'i'
         for (i -= 1; i >= 0; i--) {
            tq_set_flags(handle->thread_queues[i], TQ_ABORT);
            tq_next_thread_status(handle->thread_queues[i], NULL);
            tq_close(handle->thread_queues[i]);
         }
         return NULL;
      }

      int meta_count = 0;
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         // set references for every thread struct with no meta errors
         if (handle->thread_states[i].meta_error == 0) {
            minforefs[meta_count] = &(handle->thread_states[i].minfo);
            meta_count++;
         }
      }

      // get consensus values across all threads
      meta_info consensus;
      int match_count = check_matches(minforefs, meta_count, handle->epat.N + handle->epat.E, &consensus);
      free(minforefs); // now unneeded
      LOG(LOG_INFO, "Got consensus values (N=%d,E=%d,O=%d,partsz=%zd,versz=%zd,blocksz=%zd,totsz=%zd)\n",
         consensus.N, consensus.E, consensus.O, consensus.partsz, consensus.versz, consensus.blocksz, consensus.totsz);
      if (match_count <= 0) {
         LOG(LOG_ERR, "Insufficient meta info consensus!\n");
         // might as well continue to use 'i'
         for (i -= 1; i >= 0; i--) {
            tq_set_flags(handle->thread_queues[i], TQ_ABORT);
            tq_next_thread_status(handle->thread_queues[i], NULL);
            tq_close(handle->thread_queues[i]);
         }
         errno = ENODATA;
         // special case - report failure to retrieve any meta_info at all as ENOENT
         if (match_count < 0) {
            errno = ENOENT;
         }
         return NULL;
      }
      // check that our erasure pattern matches expected values
      char noopcase = 0;
      if (consensus.N != handle->epat.N || consensus.E != handle->epat.E ||
         consensus.O != handle->epat.O || consensus.partsz != handle->epat.partsz) {
         // special case check for NoOp DAL
         if ( strncmp( handle->ctxt->dal->name, "noop", 5 ) == 0  &&
              ( consensus.N == handle->epat.N && consensus.E == handle->epat.E && consensus.partsz == handle->epat.partsz ) ) {
            LOG( LOG_INFO, "Inserting handle 'offset' value of %d into consensus due to NoOp DAL target\n", handle->epat.O );
            consensus.O = handle->epat.O;
            noopcase = 1;
         }
         else {
            LOG(LOG_ERR, "Read meta values ( N=%d, E=%d, O=%d, partsz=%zd ) disagree with handle values ( N=%d, E=%d, O=%d, partsz=%zd )!\n",
               consensus.N, consensus.E, consensus.O, consensus.partsz, handle->epat.N, handle->epat.E, handle->epat.O, handle->epat.partsz);
            // might as well continue to use 'i'
            for (i -= 1; i >= 0; i--) {
               tq_set_flags(handle->thread_queues[i], TQ_ABORT);
               tq_next_thread_status(handle->thread_queues[i], NULL);
               tq_close(handle->thread_queues[i]);
            }
            errno = EINVAL;
            return NULL;
         }
      }
      // set handle constants based on consensus values
      handle->versz = consensus.versz;
      handle->blocksz = consensus.blocksz;
      handle->totsz = consensus.totsz;

      // confirm and correct meta info values
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         // special case check for NoOp DAL
         if ( noopcase ) {
            // update offset values of all threads, without throwing an error
            handle->thread_states[i].minfo.O = handle->epat.O;
         }
         if (cmp_minfo(&(handle->thread_states[i].minfo), &(consensus))) {
            LOG(LOG_WARNING, "Meta values of thread %d do not match consensus!\n", i);
            handle->thread_states[i].meta_error = 1;
            cpy_minfo(&(handle->thread_states[i].minfo), &(consensus));
         }
      }
   }

   // start with zero erasure threads running only for NE_RDONLY
   handle->ethreads_running = 0;
   if (mode != NE_RDONLY) {
      handle->ethreads_running = handle->epat.E;
   }

   // unpause threads
   for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
      // determine our iosize
      size_t iosz = handle->ctxt->dal->io_size; // need to know our iosz
      if (handle->versz) {
         iosz = handle->versz;
      } // if we already have a versz, use that instead

      // initialize ioqueues
      handle->thread_states[i].ioq = create_ioqueue(iosz, handle->epat.partsz, dmode);
      if (handle->thread_states[i].ioq == NULL) {
         LOG(LOG_ERR, "Failed to create ioqueue for thread %d!\n", i);
         break;
      }
      // remove the PAUSE flag, allowing thread to begin processing
      if (i < handle->epat.N + handle->ethreads_running) {
         if (tq_unset_flags(handle->thread_queues[i], TQ_HALT)) {
            LOG(LOG_ERR, "Failed to unset PAUSE flag for block %d\n", i);
            break;
         }
      }
   }
   // abort if any errors occurred
   if (i != handle->epat.N + handle->epat.E) { // any 'break' condition should trigger this
      for (; i >= 0; i--) {
         if (handle->thread_states[i].ioq) {
            destroy_ioqueue(handle->thread_states[i].ioq);
         }
      }
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         tq_set_flags(handle->thread_queues[i], TQ_ABORT);
         tq_next_thread_status(handle->thread_queues[i], NULL);
         tq_close(handle->thread_queues[i]);
      }
      return NULL;
   }

   // set our mode to the new value
   handle->mode = mode;

   return handle;
}

/**
 * Create a new handle for reading, writing, or rebuilding a specific object
 * @param ne_ctxt ctxt : The ne_ctxt used to access this data stripe
 * @param const char* objID : ID of the object to be rebuilt
 * @param ne_location loc : Location of the object to be rebuilt
 * @param ne_erasure epat : Erasure pattern of the object to be rebuilt
 * @param ne_mode mode : Handle mode (NE_RDONLY || NE_RDALL || NE_WRONLY || NE_WRALL || NE_REBUILD)
 * @return ne_handle : Newly created ne_handle, or NULL if an error occured
 */
ne_handle ne_open(ne_ctxt ctxt, const char* objID, ne_location loc, ne_erasure epat, ne_mode mode) {
   // verify our mode arg and context
   if (ctxt == NULL) {
      LOG(LOG_ERR, "Received a NULL ne_ctxt argument!\n");
      errno = EINVAL;
      return NULL;
   }

   // verify that our mode argument makes sense
   if (mode != NE_RDONLY && mode != NE_RDALL && mode != NE_WRONLY && mode != NE_WRALL && mode != NE_REBUILD) {
      LOG(LOG_ERR, "Recieved an inappropriate mode argument!\n");
      errno = EINVAL;
      return NULL;
   }

   // create a meta_info struct to pass for handle creation
   meta_info minfo;
   minfo.N = epat.N;
   minfo.E = epat.E;
   minfo.O = epat.O;
   minfo.partsz = epat.partsz;
   minfo.versz = (mode == NE_WRONLY || mode == NE_WRALL) ? ctxt->dal->io_size : 0;
   minfo.blocksz = 0;
   minfo.crcsum = 0;
   minfo.totsz = 0;

   // allocate our handle structure
   ne_handle handle = allocate_handle(ctxt, objID, loc, &minfo);
   if (handle == NULL) {
      LOG(LOG_ERR, "Failed to create an ne_handle!\n");
      return NULL;
   }

   // convert our handle to the approprate mode and start threads
   ne_handle converted_handle = ne_convert_handle(handle, mode);
   if (converted_handle == NULL) {
      LOG(LOG_ERR, "Failed to convert handle to appropriate mode!\n");
      free_handle(handle);
      return NULL;
   }

   return handle; // same reference as converted handle
}

/**
 * Close an open ne_handle
 * @param ne_handle handle : The ne_handle reference to close
 * @param ne_erasure* epat : Address of an ne_erasure struct to be populated (ignored, if NULL)
 * @param ne_state* state : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Number of blocks with errors on success, and -1 on a failure.
 */
int ne_close(ne_handle handle, ne_erasure* epat, ne_state* sref) {
   LOG(LOG_INFO, "Closing handle\n");
   // check error conditions
   if (!(handle)) {
      LOG(LOG_ERR, "Received a NULL handle!\n");
      errno = EINVAL;
      return -1;
   }

   ssize_t partsz = handle->epat.partsz;
   size_t stripesz = partsz * handle->epat.N;

   int i;
   if (handle->mode == NE_WRONLY || handle->mode == NE_WRALL) {
      // propagate our current totsz value to all threads
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         handle->thread_states[i].minfo.totsz = handle->totsz;
      }
      // output zero-fill to complete our current stripe
      size_t partstripe = handle->totsz % stripesz;
      if (partstripe) {
         void* zerobuff = calloc(1, (stripesz - partstripe));
         if (zerobuff == NULL) {
            LOG(LOG_ERR, "Failed to allocate space for a zero buffer!\n");
            return -1;
         }
         LOG(LOG_INFO, "Writing %zu bytes of zero-fill to write handle\n", (stripesz - partstripe));
         if (ne_write(handle, zerobuff, stripesz - partstripe) != (stripesz - partstripe)) {
            LOG(LOG_ERR, "Failed to write zero-fill to handle!\n");
            free(zerobuff);
            return -1;
         }
         // correct our totsz value (may not be necessary!)
         handle->totsz -= (stripesz - partstripe);
         free(zerobuff);
      }
   }

   int ret_val = 0;
   //      // make sure to release any remaining ioblocks
   //      if ( handle->iob[i] ) {
   //         if ( handle->mode == NE_WRONLY  ||  handle->mode == NE_WRALL ) {
   //            ioblock* push_block = NULL;
   //            // perform a final reserve call to split any excess data
   //            if ( reserve_ioblock( &(handle->iob[i]), &(push_block), handle->thread_states[i].ioq ) > 0 ) {
   //               LOG( LOG_INFO, "Final ioblock is full, enqueueing it and reserving another\n" );
   //               if ( tq_enqueue( handle->thread_queues[i], TQ_NONE, (void*)(push_block) ) ) {
   //                  LOG( LOG_ERR, "Failed to enqueue semi-final ioblock to thread_queue %d!\n", i );
   //                  ret_val = -1;
   //               }
   //            }
   //            if ( handle->iob[i]->data_size ) {
   //               LOG( LOG_INFO, "Enqueueing final ioblock at position %d\n", i );
   //               // if data remains, push it now
   //               if ( tq_enqueue( handle->thread_queues[i], TQ_NONE, (void*)(handle->iob[i]) ) ) {
   //                  LOG( LOG_ERR, "Failed to enqueue final ioblock to thread_queue %d!\n", i );
   //                  ret_val = -1;
   //               }
   //            }
   //            else {
   //               LOG( LOG_INFO, "Releasing empty final ioblock!\n" );
   //               release_ioblock( handle->thread_states[i].ioq );
   //            }
   //         }
   //         else {
   //            LOG( LOG_INFO, "Releasing final ioblock at position %d\n", i );
   //            release_ioblock( handle->thread_states[i].ioq );
   //         }
   //         handle->iob[i] = NULL;
   //      }
   //      if ( handle->mode != NE_STAT ) {
   //         // signal the thread to finish
   //         LOG( LOG_INFO, "Setting FINISHED state for block %d\n", i );
   //         if ( tq_set_flags( handle->thread_queues[i], TQ_FINISHED ) ) {
   //            LOG( LOG_ERR, "Failed to set a FINISHED state for thread %d!\n", i );
   //            ret_val = -1;
   //            // attempt to abort, ignoring errors
   //            tq_set_flags( handle->thread_queues[i], TQ_ABORT );
   //         }
   //      }

   if (handle->mode != NE_STAT) {
      // set a FINISHED state for all threads
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         LOG(LOG_INFO, "Terminating thread %d\n", i);
         if (terminate_thread(&(handle->iob[i]), handle->thread_queues[i], &(handle->thread_states[i]), handle->mode)) {
            ret_val = -1;
         }
      }
      // verify thread termination and close all queues
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         LOG(LOG_INFO, "Terminating queue %d\n", i);
         if (handle->mode == NE_RDONLY || handle->mode == NE_RDALL) {
            // wait for thread termination
            int waitres = 0;
            while ( (waitres = tq_wait_for_completion( handle->thread_queues[i] )) ) {
               if ( waitres > 0 ) {
                  while ( tq_dequeue(handle->thread_queues[i], TQ_ABORT | TQ_HALT, NULL) > 0 ) {
                     LOG(LOG_INFO, "Releasing unused queue element prior to completion of queue %d\n", i);
                     release_ioblock(handle->thread_states[i].ioq);
                  }
               }
               else {
                  LOG( LOG_ERR, "Failed to wait for thread %d completion\n", i );
                  ret_val = -1;
                  tq_set_flags(handle->thread_queues[i], TQ_ABORT);
               }
            }
            // we need to empty any remaining elements from the queue
            while (tq_dequeue(handle->thread_queues[i], TQ_ABORT | TQ_HALT, NULL) > 0) {
               LOG(LOG_INFO, "Releasing unused queue element for thread %d\n", i);
               release_ioblock(handle->thread_states[i].ioq);
            }
         }
         tq_next_thread_status(handle->thread_queues[i], NULL);
         tq_close(handle->thread_queues[i]);
         destroy_ioqueue(handle->thread_states[i].ioq);
      }
   }

   int numerrs = 0; // for checking write safety
   // check the status of all blocks
   for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
      if (handle->thread_states[i].meta_error || handle->thread_states[i].data_error) {
         LOG(LOG_ERR, "Detected an error for block %d!\n", i);
         numerrs++;
      }
      else if ( handle->blocksz == 0 ) { handle->blocksz = handle->thread_states[i].minfo.blocksz; } // set bsz if unknown
   }
   if (handle->mode == NE_WRONLY || handle->mode == NE_WRALL) {
      // verify that our data meets safetly thresholds
      if (numerrs > 0 && numerrs > (handle->epat.E - MIN_PROTECTION)) {
         LOG(LOG_ERR, "Errors exceed safety threshold!\n");
         ne_delete(handle->ctxt, handle->objID, handle->loc);
         errno = EIO;
         ret_val = -1;
      }
   }

   // populate any info structs
   if (ne_get_info(handle, epat, sref) < 0) {
      LOG(LOG_ERR, "Failed to populate info structs!\n");
      ret_val = -1;
   }

   free_handle(handle);

   // modify our return value to reflect any errors encountered
   if (ret_val == 0) {
      ret_val = numerrs;
   }
   LOG(LOG_INFO, "Close status = %d\n", ret_val);

   return ret_val;
}

/**
    * Abandon a given open WRITE/REBUILD ne_handle. This is roughly equivalent to calling ne_close
    * on the handle; however, NO data changes should be applied to the underlying object (same state
    * as before the handle was opened).
    * @param ne_handle handle : The ne_handle reference to abort
    * @return int : 0 on success, and -1 on a failure.
    */
int ne_abort(ne_handle handle) {
   LOG(LOG_INFO, "Aborting handle\n");
   // check error conditions
   if (!(handle)) {
      LOG(LOG_ERR, "Received a NULL handle!\n");
      errno = EINVAL;
      return -1;
   }

   if (handle->mode != NE_STAT) {
      int i;
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         tq_set_flags(handle->thread_queues[i], TQ_ABORT);
         tq_unset_flags(handle->thread_queues[i], TQ_HALT);
         // we need to empty any remaining elements from the queue
         while (tq_dequeue(handle->thread_queues[i], TQ_ABORT | TQ_HALT, NULL) > 0) {
            LOG(LOG_INFO, "Releasing unused thread queue element\n");
         }
         while (release_ioblock(handle->thread_states[i].ioq) >= 0) {
            LOG(LOG_INFO, "Releasing unused ioblock\n");
         }
         tq_next_thread_status(handle->thread_queues[i], NULL);
         tq_close(handle->thread_queues[i]);
         destroy_ioqueue(handle->thread_states[i].ioq);
      }
   }

   free_handle(handle);

   return 0;
}

// ---------------------- RETRIEVAL/SEEDING OF HANDLE INFO ----------------------

/**
 * Retrieve erasure and status info for a given object handle
 * @param ne_handle handle : Handle to retrieve info for
 * @param ne_erasure* epat : Address of an ne_erasure struct to be populated (ignored, if NULL)
 * @param ne_state* state : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero on success, and -1 on a failure
 */
int ne_get_info(ne_handle handle, ne_erasure* epat, ne_state* sref) {
   // sanity checks
   if (handle == NULL) {
      LOG(LOG_ERR, "Received a NULL ne_handle!\n");
      errno = EINVAL;
      return -1;
   }
   if (handle->mode == 0) {
      LOG(LOG_ERR, "Received an improperly initilized handle!\n");
      errno = EINVAL;
      return -1;
   }

   // populate the epat struct
   if (epat) {
      epat->N = handle->epat.N;
      epat->E = handle->epat.E;
      epat->O = handle->epat.O;
      epat->partsz = handle->epat.partsz;
   }
   // populate the state struct
   if (sref) {
      sref->versz = handle->versz;
      sref->blocksz = handle->blocksz;
      sref->totsz = handle->totsz;
      if (sref->meta_status) {
         int i;
         for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
            if (handle->thread_states[i].meta_error) {
               sref->meta_status[i] = 1;
            }
            else {
               sref->meta_status[i] = 0;
            }
         }
      }
      if (sref->data_status) {
         int i;
         for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
            if (handle->thread_states[i].data_error) {
               sref->data_status[i] = 1;
            }
            else {
               sref->data_status[i] = 0;
            }
         }
      }
      if (sref->csum) {
         int i;
         for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
            sref->csum[i] = handle->thread_states[i].minfo.crcsum;
         }
      }
   }

   return 0;
}

/**
 * Seed error patterns into a given handle (may useful for speeding up ne_rebuild())
 * @param ne_handle handle : Handle for which to set an error pattern
 * @param ne_state* sref : Reference to an ne_state struct containing the error pattern
 * @return int : Zero on success, and -1 on a failure
 */
int ne_seed_status(ne_handle handle, ne_state* sref) {
   // sanity checks
   if (handle == NULL) {
      LOG(LOG_ERR, "Received a NULL ne_handle!\n");
      errno = EINVAL;
      return -1;
   }
   if (handle->mode == 0) {
      LOG(LOG_ERR, "Received an improperly initilized handle!\n");
      errno = EINVAL;
      return -1;
   }
   if (sref == NULL) {
      LOG(LOG_ERR, "Received a null ne_state reference!\n");
      errno = EINVAL;
      return -1;
   }

   // set handle values
   handle->versz = sref->versz;
   handle->blocksz = sref->blocksz;
   handle->totsz = sref->totsz;
   if (sref->meta_status) {
      int i;
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         // only set additional error values, don't ignore any previously encountered
         if (sref->meta_status[i]) {
            handle->thread_states[i].meta_error = 1;
         }
      }
   }
   if (sref->data_status) {
      int i;
      for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
         // only set additional error values, don't ignore any previously encountered
         if (sref->data_status[i]) {
            handle->thread_states[i].data_error = 1;
         }
      }
   }
   // ignore csum values!

   return 0;
}

// ---------------------- READ/WRITE/REBUILD FUNCTIONS ----------------------

/**
 * Verify a given erasure striped object and reconstruct any damaged data, if possible
 * @param ne_handle handle : Handle on which to perform a rebuild
 * @param ne_erasure* epat : Erasure pattern of the object to be rebuilt
 * @param ne_state* sref : Address of an ne_state struct to be populated (ignored, if NULL)
 * @return int : Zero if all errors were repaired, a positive integer count of any remaining UNREPAIRED
 *               errors ( rerun this func ), or a negative value if an unrecoverable failure occurred
 */
int ne_rebuild(ne_handle handle, ne_erasure* epat, ne_state* sref) {
   // check boundary and invalid call conditions
   if (!(handle)) {
      LOG(LOG_ERR, "Received a NULL handle!\n");
      errno = EINVAL;
      return -1;
   }
   if (handle->mode != NE_REBUILD) {
      LOG(LOG_ERR, "Handle is in improper mode for reading!\n");
      errno = EPERM;
      return -1;
   }
   LOG(LOG_INFO, "Attempting rebuild of erasure stripe\n");
   // make sure our handle is set to a zero offset
   if (handle->iob_offset != 0 || handle->sub_offset != 0) {
      LOG(LOG_INFO, "Reseeking to zero prior to rebuild op\n");
      if (ne_seek(handle, 0)) {
         LOG(LOG_ERR, "Failed to reseek handle to zero!\n");
         return -1;
      }
   }

   // get some useful reference values
   int N = handle->epat.N;
   int E = handle->epat.E;
   int O = handle->epat.O;
   ssize_t partsz = handle->epat.partsz;
#ifdef DEBUG
   size_t stripesz = partsz * N;
#endif

   // NOTE -- a REBUILD handle is 'effectively' a READ handle
   //         However, we now need to spin up write threads to output any repaired data
   // prep structs for output threads
   ThreadQueue* OutTQs = calloc(N + E, sizeof(ThreadQueue));
   if (OutTQs == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for Output ThreadQueues!\n");
      return -1;
   }
   gthread_state* outstates = calloc(N + E, sizeof(gthread_state));
   if (outstates == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for Output Thread states!\n");
      free(OutTQs);
      return -1;
   }
   // allocate space for ioblock references
   ioblock** outblocks = calloc(N + E, sizeof(ioblock*));
   if (outblocks == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for ioblock references!\n");
      free(OutTQs);
      free(outstates);
      return -1;
   }

   LOG(LOG_INFO, "Initializing output thread states\n");
   // assign values to thread states
   int i;
   for (i = 0; i < N + E; i++) {
      outstates[i].erasurelock = handle->ctxt->erasurelock;
      // object attributes
      outstates[i].objID = handle->objID;
      outstates[i].location.pod = handle->loc.pod;
      outstates[i].location.block = (i + O) % (N + E);
      outstates[i].location.cap = handle->loc.cap;
      outstates[i].location.scatter = handle->loc.scatter;
      outstates[i].dal = handle->ctxt->dal;
      outstates[i].offset = 0;
      // meta info values
      outstates[i].minfo.N = N;
      outstates[i].minfo.E = E;
      outstates[i].minfo.O = O;
      outstates[i].minfo.partsz = partsz;
      outstates[i].minfo.versz = handle->versz;
      outstates[i].minfo.blocksz = handle->blocksz;
      outstates[i].minfo.crcsum = 0;
      outstates[i].minfo.totsz = 0;
      outstates[i].meta_error = 0;
      outstates[i].data_error = 0;
   }

   TQ_Init_Opts tqopts = {0};
   char* lprefstr = malloc(sizeof(char) * (6 + (handle->ctxt->max_block / 10)));
   if (lprefstr == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for TQ log prefix string!\n");
      return -1;
   }
   tqopts.log_prefix = lprefstr;
   // create a format string for each thread queue
   tqopts.init_flags = TQ_HALT; // initialize the threads in a HALTED state (essential for reads, doesn't hurt writes)
   tqopts.max_qdepth = QDEPTH;
   tqopts.num_threads = 1;
   tqopts.num_prod_threads = 0;
   tqopts.thread_init_func = write_init;
   tqopts.thread_consumer_func = write_consume;
   tqopts.thread_producer_func = NULL;
   tqopts.thread_pause_func = write_pause;
   tqopts.thread_resume_func = write_resume;
   tqopts.thread_term_func = write_term;
   // finally, startup the output threads for each in-error block
   for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
      outstates[i].dmode = DAL_REBUILD;
      tqopts.global_state = &(outstates[i]);
      // only initialize threads for blocks with errors
      if (handle->thread_states[i].data_error || handle->thread_states[i].meta_error) {
         LOG(LOG_INFO, "Starting up output thread %d\n", i);
         // set a log_prefix value for this queue
         snprintf(lprefstr, 6 + (handle->ctxt->max_block/10), "RWQ%d", i);
         OutTQs[i] = tq_init(&tqopts);
         if (OutTQs[i] == NULL) {
            LOG(LOG_ERR, "Failed to create output thread_queue for block %d!\n", i);
            // if we failed to initialize any thread_queue, attempt to abort everything else
            for (i -= 1; i >= 0; i--) {
               if (OutTQs[i] != NULL) {
                  tq_set_flags(OutTQs[i], TQ_ABORT);
                  tq_next_thread_status(OutTQs[i], NULL);
                  tq_close(OutTQs[i]);
               }
            }
            free(lprefstr);
            free(OutTQs);
            free(outstates);
            return -1;
         }
      }
   }
   free(lprefstr);

   // verify thread initialization
   for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
      if (OutTQs[i] != NULL) {
         if( tq_check_init(OutTQs[i]) ) {
            LOG( LOG_ERR, "Detected init failure for OutTQ %d\n", i );
            for (i = 0; i < handle->epat.N + handle->epat.E; i++) {
               // terminate all output threads
               if ( OutTQs[i] != NULL ) {
                  tq_set_flags(OutTQs[i], TQ_ABORT);
                  tq_next_thread_status(OutTQs[i], NULL);
                  tq_close(OutTQs[i]);
               }
            }
            free(OutTQs);
            free(outstates);
            return -1;
         }
      }
   }


   // unpause threads
   for (i = 0; i < N + E; i++) {
      // only bother with thread queues we've initialized
      if (OutTQs[i] != NULL) {
         LOG(LOG_INFO, "Prepping block %d for output\n", i);
         // initialize ioqueues
         outstates[i].ioq = create_ioqueue(handle->versz, handle->epat.partsz, DAL_REBUILD);
         if (outstates[i].ioq == NULL) {
            LOG(LOG_ERR, "Failed to create ioqueue for thread %d!\n", i);
            break;
         }
         // remove the PAUSE flag, allowing thread to begin processing
         if (tq_unset_flags(OutTQs[i], TQ_HALT)) {
            LOG(LOG_ERR, "Failed to unset PAUSE flag for block %d\n", i);
            break;
         }
      }
   }
   // abort if any errors occurred
   if (i != N + E) { // any 'break' condition should trigger this
      for (; i >= 0; i--) {
         if (OutTQs[i] != NULL && outstates[i].ioq) {
            destroy_ioqueue(outstates[i].ioq);
         }
      }
      for (i = 0; i < N + E; i++) {
         if (OutTQs[i] != NULL) {
            tq_set_flags(OutTQs[i], TQ_ABORT);
            tq_next_thread_status(OutTQs[i], NULL);
            tq_close(OutTQs[i]);
         }
      }
      return -1;
   }

   // actually perform the rebuild
   size_t rebuilt = 0;
   while (rebuilt < handle->totsz) {
      LOG(LOG_INFO, "Performing rebuild of stripes %d - %d\n", (int)(rebuilt / stripesz), (int)((rebuilt + handle->iob_datasz) / stripesz));
      // read in new ioblocks, if necessary
      if (handle->sub_offset >= (handle->iob_datasz * N)) {
         LOG(LOG_INFO, "Reading in additional stripes (rebuilt=%zu)\n", rebuilt);
         if (read_stripes(handle)) {
            LOG(LOG_ERR, "Failed to read in additional stripes!\n");
            for (i = 0; i < N + E; i++) {
               if (OutTQs[i] != NULL) {
                  tq_set_flags(OutTQs[i], TQ_ABORT);
                  tq_next_thread_status(OutTQs[i], NULL);
                  tq_close(OutTQs[i]);
               }
            }
            return -1;
         }
      }

      // copy ioblock data off to our writer threads
      for (i = 0; i < N + E; i++) {
         // only copy to running output threads
         if (OutTQs[i] != NULL) {
            size_t block_cpy = 0;
            while (block_cpy < handle->iob_datasz) {

               // check that the current ioblock has room for our data
               int reserved;
               ioblock* push_block;
               if ((reserved = reserve_ioblock(&(outblocks[i]), &(push_block), outstates[i].ioq)) == 0) {
                  void* tgt = ioblock_write_target(outblocks[i]);
                  // copy caller data into our ioblock
                  LOG(LOG_INFO, "Copying %zu bytes out to block %d\n", partsz, i);
                  memcpy(tgt, handle->iob[i]->buff + block_cpy, partsz); // no error check, SEGFAULT or nothing
                  block_cpy += partsz;
                  ioblock_update_fill(outblocks[i], partsz, 0);
               }
               else if (reserved > 0) {
                  LOG(LOG_INFO, "Pushing full ioblock to block %d\n", i);
                  // the block is full and must be pushed to our iothread
                  if (tq_enqueue(OutTQs[i], TQ_NONE, (void*)push_block)) {
                     LOG(LOG_ERR, "Failed to push ioblock to thread_queue %d\n", i);
                     for (i = 0; i < N + E; i++) {
                        if (OutTQs[i] != NULL) {
                           tq_set_flags(OutTQs[i], TQ_ABORT);
                           tq_next_thread_status(OutTQs[i], NULL);
                           tq_close(OutTQs[i]);
                        }
                     }
                     errno = EBADF;
                     return -1;
                  }
               }
               else {
                  LOG(LOG_ERR, "Failed to reserve ioblock for position %d!\n", i);
                  for (i = 0; i < N + E; i++) {
                     if (OutTQs[i] != NULL) {
                        tq_set_flags(OutTQs[i], TQ_ABORT);
                        tq_next_thread_status(OutTQs[i], NULL);
                        tq_close(OutTQs[i]);
                     }
                  }
                  errno = EBADF;
                  return -1;
               }
            }
         }
      } // end of per-block for-loop
      rebuilt += (handle->iob_datasz * N);
      handle->sub_offset += (handle->iob_datasz * N);
   }

   // finally, terminate the output threads
   //
   // propagate our current totsz value to all threads
   for (i = 0; i < N + E; i++) {
      outstates[i].minfo.totsz = handle->totsz;
   }

   // set a FINISHED state for all threads
   int numerrs = 0; // for checking write safety
   for (i = 0; i < N + E; i++) {
      LOG(LOG_INFO, "Terminating output thread %d\n", i);
      if (OutTQs[i] && terminate_thread(&(outblocks[i]), OutTQs[i], &(outstates[i]), NE_WRALL)) {
         LOG(LOG_ERR, "Failed to properly terminate output thread %d!\n", i);
         numerrs++;
      }
      //      // make sure to release any remaining ioblocks
      //      if ( outblocks[i] ) {
      //         ioblock* push_block = NULL;
      //         // perform a final reserve call to split any excess data
      //         if ( reserve_ioblock( &(outblocks[i]), &(push_block), outstates[i].ioq ) > 0 ) {
      //            LOG( LOG_INFO, "Final ioblock is full, enqueueing it and reserving another\n" );
      //            if ( tq_enqueue( OutTQs[i], TQ_NONE, (void*)(push_block) ) ) {
      //               LOG( LOG_ERR, "Failed to enqueue semi-final ioblock to thread_queue %d!\n", i );
      //               ret_val = -1;
      //            }
      //         }
      //         if ( outblocks[i]->data_size ) {
      //            LOG( LOG_INFO, "Enqueueing final ioblock at position %d\n", i );
      //            // if data remains, push it now
      //            if ( tq_enqueue( OutTQs[i], TQ_NONE, (void*)(outblocks[i]) ) ) {
      //               LOG( LOG_ERR, "Failed to enqueue final ioblock to thread_queue %d!\n", i );
      //               ret_val = -1;
      //            }
      //         }
      //         else {
      //            LOG( LOG_INFO, "Releasing empty final ioblock!\n" );
      //            release_ioblock( outstates[i].ioq );
      //         }
      //         outblocks[i] = NULL;
      //      }
      //      // signal the thread to finish
      //      if ( OutTQs[i] ) {
      //         LOG( LOG_INFO, "Setting FINISHED state for block %d\n", i );
      //         if ( ret_val != 0  ||  tq_set_flags( OutTQs[i], TQ_FINISHED ) ) {
      //            LOG( LOG_ERR, "Failed to set a FINISHED state for output thread %d!\n", i );
      //            ret_val = -1;
      //            // attempt to abort, ignoring errors
      //            tq_set_flags( OutTQs[i], TQ_ABORT );
      //         }
      //      }
   }

   int newerrs = 0; // for checking uncorrected errors
   // terminate output threads and close queues
   for (i = 0; i < N + E; i++) {
      if (OutTQs[i]) {
         LOG(LOG_INFO, "Terminating queue %d\n", i);
         tq_next_thread_status(OutTQs[i], NULL);
         tq_close(OutTQs[i]);
         destroy_ioqueue(outstates[i].ioq);
      }
      else if (handle->thread_states[i].meta_error || handle->thread_states[i].data_error) {
         // if errors exist for which we never started output threads, record them
         LOG(LOG_INFO, "Detected errors in block %d during rebuild\n", i);
         newerrs++;
      }
   }


   // populate any info structs BEFORE clearing errors from rebuilt blocks
   if (ne_get_info(handle, epat, sref) < 0) {
      LOG(LOG_ERR, "Failed to populate info structs!\n");
      numerrs++; // easy way to make sure we return a failure for this case
   }


   // cleanup thread states and restart any read threads for newly rebuilt blocks
   for (i = 0; i < N + E; i++) {
      if (OutTQs[i]) {
         // check for any output errors
         if (outstates[i].meta_error || outstates[i].data_error) {
            LOG(LOG_ERR, "Detected error in regenerated block %d!\n");
            numerrs++;
         }
         else {
            // if we successfully reconstructed these, we need to clear any errors
            // stop the thread for any repaired block
            LOG(LOG_INFO, "Terminating input thread %d, pre-restart\n");
            if (terminate_thread(&(handle->iob[i]), handle->thread_queues[i], &(handle->thread_states[i]), NE_REBUILD)) {
               LOG(LOG_ERR, "Failed to terminate input thread %d\n", i);
               numerrs++;
               handle->mode = NE_ERR;
               tq_next_thread_status(handle->thread_queues[i], NULL);
            }
            else {
               tq_next_thread_status(handle->thread_queues[i], NULL);
            }
            // use the meta_info of the output thread (will at least need a new crcsum)
            cpy_minfo(&(handle->thread_states[i].minfo), &(outstates[i].minfo));
            handle->thread_states[i].offset = outstates[i].minfo.blocksz; // set to end of block
            // clean the thread error states
            handle->thread_states[i].meta_error = 0;
            handle->thread_states[i].data_error = 0;
            // populate a tqopts
            TQ_Init_Opts opts = {0};
            opts.log_prefix = malloc(sizeof(char) * (6 + i / 10));
            if (opts.log_prefix == NULL) {
               LOG(LOG_ERR, "Failed to allocate space for a log_prefix string!\n");
               numerrs++;
               handle->mode = NE_ERR;
            }
            else if (tq_get_opts(handle->thread_queues[i], &(opts), (6 + i / 10))) {
               LOG(LOG_ERR, "Failed to populate opts struct for restart of thread %d\n", i);
               numerrs++;
               handle->mode = NE_ERR;
            }
            tq_close(handle->thread_queues[i]);
            opts.global_state = (void*)(&(handle->thread_states[i]));
            // if we've been successful so far, restart this thread
            if (handle->mode != NE_ERR) {
               LOG(LOG_INFO, "Restarting thread %d\n", i);
               handle->thread_queues[i] = tq_init(&opts);
               if (handle->thread_queues[i] == NULL) {
                  LOG(LOG_ERR, "Failed to initialize new thread queue at position %d!\n", i);
                  numerrs++;
                  handle->mode = NE_ERR;
               }
               else if ( tq_check_init(handle->thread_queues[i]) ) {
                  LOG(LOG_ERR, "Detected initialization failure for thread queue at position %d!\n", i);
                  tq_set_flags(handle->thread_queues[i], TQ_ABORT);
                  tq_next_thread_status(handle->thread_queues[i], NULL);
                  tq_close(handle->thread_queues[i]);
                  handle->thread_queues[i] = NULL;
                  numerrs++;
                  handle->mode = NE_ERR;
               }
            }
            free(opts.log_prefix);
         }
      }
   }
   free(outblocks);
   free(OutTQs);
   free(outstates);

   // any output error is a failure
   if (numerrs > 0) {
      LOG(LOG_ERR, "Rebuild failure occurred!\n");
      errno = EIO;
      return -1;
   }

   LOG(LOG_INFO, "Rebuild completed with %d errors remaining\n", newerrs);

   // indicate if uncorrected errors remain
   return newerrs;
}

/**
 * Seek to a new offset on a read ne_handle
 * @param ne_handle handle : Handle on which to seek (must be open for read)
 * @param off_t offset : Offset to seek to ( -1 == EOF )
 * @return off_t : New offset of handle ( negative value, if an error occurred )
 */
off_t ne_seek(ne_handle handle, off_t offset) {
   // check error conditions
   if (!(handle)) {
      LOG(LOG_ERR, "Received a NULL handle!\n");
      errno = EINVAL;
      return -1;
   }

   if (handle->mode != NE_RDONLY && handle->mode != NE_RDALL && (handle->mode == NE_REBUILD && offset != 0)) {
      LOG(LOG_ERR, "Handle is in improper mode for seeking!\n");
      errno = EPERM;
      return -1;
   }

   if (offset > handle->totsz) {
      offset = handle->totsz;
      LOG(LOG_WARNING, "Seek offset extends beyond EOF, resizing read request to %zu\n", offset);
   }

   int N = handle->epat.N;
   ssize_t partsz = handle->epat.partsz;
   size_t stripesz = partsz * N;

   // skip to appropriate stripe

   LOG(LOG_INFO, "Current offset = %zu\n", (handle->iob_offset * N) + handle->sub_offset);
   unsigned int cur_stripe = (unsigned int)(handle->iob_offset / partsz); // I think int truncation actually works out in our favor here
   unsigned int tgt_stripe = (unsigned int)(offset / stripesz);
   ssize_t max_data = ioqueue_maxdata(handle->thread_states[0].ioq);
   if (max_data < 0) {
      max_data = 0;
   } // if we hit an error, just assume no read-ahead

   // if the offset is behind us or so far in front that there is no chance of the work having already been done...
   if ((cur_stripe > tgt_stripe) ||
      ((handle->iob_offset + max_data) < (tgt_stripe * partsz))) {
      LOG(LOG_INFO, "New offset of %zd will require threads to reseek\n", offset);
      //      off_t new_iob_off = -1;
      int i;
      for (i = 0; i < (N + handle->ethreads_running); i++) {
         // first, pause this thread
         if (tq_set_flags(handle->thread_queues[i], TQ_HALT)) {
            LOG(LOG_ERR, "Failed to set HALT state for block %d!\n", i);
            break;
         }
         // next, release any unneeded ioblock reference
         if (handle->iob[i] != NULL) {
            if (release_ioblock(handle->thread_states[i].ioq)) {
               LOG(LOG_ERR, "Failed to release ioblock ref for block %d!\n", i);
               break;
            }
            handle->iob[i] = NULL;
         }
         // make sure that the thread isn't stuck waiting for ioqueue elements
         if (tq_dequeue(handle->thread_queues[i], TQ_HALT, (void**)&(handle->iob[i])) > 0) {
            if (handle->iob[i] != NULL) {
               release_ioblock(handle->thread_states[i].ioq);
               handle->iob[i] = NULL;
            }
         }
         // wait for the thread to pause
         if (tq_wait_for_pause(handle->thread_queues[i])) {
            LOG(LOG_ERR, "Failed to detect thread pause for block %d!\n", i);
            break;
         }
         // empty all remaining queue elements
         int depth = tq_depth(handle->thread_queues[i]);
         while (depth > 0) {
            depth = tq_dequeue(handle->thread_queues[i], TQ_HALT, (void**)&(handle->iob[i]));
            if (depth < 0) {
               LOG(LOG_ERR, "Failed to dequeue from HALTED thread_queue %d!\n", i);
               break;
            }
            if (handle->iob[i] != NULL) {
               release_ioblock(handle->thread_states[i].ioq);
               handle->iob[i] = NULL;
            }
            depth--; // decrement, as dequeue depth includes the returned element
         }
         if (depth != 0) {
            break;
         } // catch any previous error
         // set the thread to our target offset
         handle->thread_states[i].offset = (tgt_stripe * partsz);
         // unpause the thread
         if (tq_unset_flags(handle->thread_queues[i], TQ_HALT)) {
            LOG(LOG_ERR, "Failed to unset HALT state for block %d!\n", i);
            break;
         }
         //         // retrieve a new ioblock
         //         if ( tq_dequeue( handle->thread_queues[i], TQ_HALT, (void**)&(handle->iob[i]) ) < 0 ) {
         //            LOG( LOG_ERR, "Failed to retrieve ioblock for block %d after seek!\n", i );
         //            break;
         //         }
         //         // by now, the thread must have updated our offset
         //         off_t real_iob_off = handle->thread_states[i].offset;
         //         // sanity check this value
         //         if ( real_iob_off < 0  ||  real_iob_off > (tgt_stripe * partsz) ) {
         //            LOG( LOG_ERR, "Real offset of block %d (%zd) is not in expected range!\n", i, real_iob_off );
         //            break;
         //         }
         //         // if this is our first block, remember this value and set ioblock data size
         //         if( new_iob_off < 0 ) {
         //            new_iob_off = real_iob_off;
         //            handle->iob_datasz = handle->iob[i]->data_size;
         //         }
         //         else if ( new_iob_off != real_iob_off ) { //otherwise, further sanity check
         //            LOG( LOG_ERR, "Real offset of block %d (%zd) does not match previous value of %zd!\n", i, real_iob_off, new_iob_off );
         //            break;
         //         }
         //         else if ( handle->iob[i]->data_size != handle->iob_datasz ) {
         //            LOG( LOG_ERR, "Data size of ioblock for position %d (%zd) does not match that of previous ioblocks (%zd)!\n",
         //                           handle->iob[i]->data_size, handle->iob_datasz );
         //            break;
         //         }
      }
      // catch any error conditions by checking our index
      if (i != (N + handle->ethreads_running)) {
         handle->mode = NE_ERR; // make sure that no one tries to reuse this broken handle!
         errno = EBADF;
         return -1;
      }

      handle->sub_offset = 0; // old sub_offset is irrelevant
      handle->iob_datasz = 0;                           // indicate we have to repopulate all ioblocks
      handle->iob_offset = (tgt_stripe * partsz);       //new_iob_off;
                                                        //      int iob_stripe = (int)( new_iob_off / partsz );
      LOG( LOG_INFO, "Reading in additional stripes post-thread-reseek\n", handle->iob_datasz, handle->sub_offset);
      if (read_stripes(handle)) {
         LOG(LOG_ERR, "Failed to read additional stripes!\n");
         return -1;
      }
      handle->sub_offset = offset - (handle->iob_offset * N); //( iob_stripe * stripesz );
   }
   else {
      // reset out sub_offset to the start of the ioblock data
      handle->sub_offset = 0;
      LOG(LOG_INFO, "Offset of %zd is within readable bounds (tgt_stripe=%d / cur_stripe=%d)\n", offset, tgt_stripe, cur_stripe);
      // don't update our sub_offset until we actually have some ioblocks populated
      if ( handle->iob_datasz == 0 ) {
         if (read_stripes(handle)) {
            LOG(LOG_ERR, "Failed to read initial stripe, pre-munch\n");
            return -1;
         }
      }
      // we may still be many stripes behind the given offset.  Calculate how many.
      unsigned int munch_stripes = tgt_stripe - cur_stripe;
      // if we're at all behind...
      if (munch_stripes) {
         LOG(LOG_INFO, "Attempting to 'munch' %d stripes to reach offset of %zu\n", munch_stripes, offset);

         // just chuck buffers off of each queue until we hit the right stripe
         unsigned int thread_munched = 0;
         for (; thread_munched < munch_stripes; thread_munched++) {

            // skip to the start of the next stripe
            handle->sub_offset += stripesz;

            if (handle->sub_offset >= (handle->iob_datasz * N)) {
               LOG( LOG_INFO, "Reading in additional stripes (datasz=%zu/sub_offset=%zu)\n", handle->iob_datasz, handle->sub_offset);
               if (read_stripes(handle)) {
                  LOG(LOG_ERR, "Failed to read additional stripes!\n");
                  return -1;
               }
            }
         }
         LOG(LOG_INFO, "Finished buffer 'munching'\n");
      }
      // add any sub-stripe offset which remains
      handle->sub_offset += offset - (tgt_stripe * stripesz);
   }
   LOG(LOG_INFO, "Post seek: IOBlock_Offset=%zd, Sub_Offset=%zd\n", handle->iob_offset, handle->sub_offset);
   return (handle->iob_offset * N) + handle->sub_offset; // should equal our target offset
}

/**
 * Read from a given NE_RDONLY or NE_RDALL handle
 * @param ne_handle handle : The ne_handle reference to read from
 * @param off_t offset : Offset at which to read
 * @param void* buffer : Reference to a buffer to be filled with read data
 * @param size_t bytes : Number of bytes to be read
 * @return ssize_t : The number of bytes successfully read, or -1 on a failure
 */
ssize_t ne_read(ne_handle handle, void* buffer, size_t bytes) {
   // check boundary and invalid call conditions
   if (!(handle)) {
      LOG(LOG_ERR, "Received a NULL handle!\n");
      errno = EINVAL;
      return -1;
   }
   if (bytes > UINT_MAX) {
      LOG(LOG_ERR, "Not yet validated for write-sizes above %lu\n", UINT_MAX);
      errno = EFBIG; /* sort of */
      return -1;
   }
   if (handle->mode != NE_RDONLY && handle->mode != NE_RDALL) {
      LOG(LOG_ERR, "Handle is in improper mode for reading!\n");
      errno = EPERM;
      return -1;
   }
   size_t offset = (handle->iob_offset * handle->epat.N) + handle->sub_offset;
   LOG(LOG_INFO, "Called to retrieve %zu bytes at offset %zu\n", bytes, offset);
   if ((offset + bytes) > handle->totsz) {
      if (offset >= handle->totsz) {
         LOG(LOG_WARNING, "Read is at EOF, returning 0\n");
         return 0; //EOF
      }
      bytes = handle->totsz - offset;
      LOG(LOG_WARNING, "Read would extend beyond EOF, resizing read request to %zu\n", bytes);
   }

   // get some useful reference values
   int N = handle->epat.N;
   ssize_t partsz = handle->epat.partsz;
   size_t stripesz = partsz * N;

   LOG(LOG_INFO, "Stripe: ( N = %d, E = %d, partsz = %zu )\n", N, handle->epat.E, partsz);

   // ---------------------- BEGIN MAIN READ LOOP ----------------------

   // time to start actually filling this read request
   size_t bytes_read = 0;
   while (bytes_read < bytes) { // while data still remains to be read, loop over each stripe
      // first, check if we need to populate additional data
      if (handle->sub_offset >= (handle->iob_datasz * N)) {
         LOG(LOG_INFO, "Reading in additional stripes (datasz=%zu/sub_offset=%zu)\n", handle->iob_datasz, handle->sub_offset);
         if (read_stripes(handle) < 0) {
            LOG(LOG_ERR, "Failed to populate stripes beyond offset %zu!\n", offset);
            return -1;
         }
      }

#ifdef DEBUG
      int iob_stripe = (int)(handle->iob_offset / stripesz);
#endif
      int cur_stripe = (int)(handle->sub_offset / stripesz);
      off_t off_in_stripe = handle->sub_offset % stripesz;
      size_t to_read_in_stripe = bytes - bytes_read;
      if (to_read_in_stripe > (stripesz - off_in_stripe)) {
         to_read_in_stripe = stripesz - off_in_stripe;
      }
      LOG(LOG_INFO, "Reading %zu bytes from offset %zd of stripe %d (%zu read)\n", to_read_in_stripe, off_in_stripe, cur_stripe + iob_stripe, bytes_read);

      // copy buffers from each block
      int cur_block = off_in_stripe / partsz;
      for (; cur_block < N; cur_block++) {
         ioblock* cur_iob = handle->iob[cur_block];
         // make sure the ioblock has sufficient data
         if ((cur_iob->data_size - (cur_stripe * partsz)) < partsz) {
            LOG(LOG_ERR, "Ioblock at position %d of stripe %d is subsized (%zu)!\n", cur_block, cur_stripe + iob_stripe, cur_iob->data_size);
            errno = EBADF;
            return -1;
         }
         // make sure the ioblock has no errors in this stripe
         if (cur_iob->error_end > (cur_stripe * partsz)) {
            LOG(LOG_ERR, "Ioblock at position %d of stripe %d has an error beyond requested stripe (error_end = %zu)!\n",
               cur_block, cur_stripe + iob_stripe, cur_iob->error_end);
            errno = ENODATA;
            return -1;
         }
         // otherwise, copy this data off to our caller's buffer
         off_t block_off = (off_in_stripe % partsz);
         size_t block_read = (to_read_in_stripe > (partsz - block_off)) ? (partsz - block_off) : to_read_in_stripe;
         if (block_read == 0) {
            break;
         } // if we've completed our reads, stop here
         // check for NULL target buffer
         if (buffer) {
            LOG(LOG_INFO, "   Reading %zu bytes from block %d\n", block_read, cur_block);
            memcpy(buffer + bytes_read, cur_iob->buff + (cur_stripe * partsz) + block_off, block_read);
         }
         else {
            LOG(LOG_INFO, "   Dropping %zu bytes from block %d\n", block_read, cur_block);
         }
         // update all values
         bytes_read += block_read;
         handle->sub_offset += block_read;
         off_in_stripe += block_read;
         to_read_in_stripe -= block_read;
      }
   }

   LOG(LOG_INFO, "Completed read of %zd bytes\n", bytes_read);

   return bytes_read;
}

/**
 * Write to a given NE_WRONLY or NE_WRALL handle
 * @param ne_handle handle : The ne_handle reference to write to
 * @param const void* buffer : Buffer to be written to the handle
 * @param size_t bytes : Number of bytes to be written from the buffer
 * @return ssize_t : The number of bytes successfully written, or -1 on a failure
 */
ssize_t ne_write(ne_handle handle, const void* buffer, size_t bytes) {

   // necessary?
   if (bytes > UINT_MAX) {
      LOG(LOG_ERR, "Not yet validated for write-sizes above %lu!\n", UINT_MAX);
      errno = EFBIG; /* sort of */
      return -1;
   }

   if (!(handle)) {
      LOG(LOG_ERR, "Received a NULL handle!\n");
      errno = EINVAL;
      return -1;
   }

   if (handle->mode != NE_WRONLY && handle->mode != NE_WRALL) {
      LOG(LOG_ERR, "Handle is in improper mode for writing! %d\n", handle->mode);
      errno = EINVAL;
      return -1;
   }

   int N = handle->epat.N;
   int E = handle->epat.E;
   size_t partsz = handle->epat.partsz;
   size_t stripesz = (N * partsz);
   off_t offset = (handle->iob_offset * N) + handle->sub_offset;
#ifdef DEBUG
   unsigned int stripenum = offset / stripesz;
#endif

   // initialize erasure structs (these never change for writes, so we can just check here)
   if (handle->e_ready == 0) {
      LOG(LOG_INFO, "Initializing erasure matricies...\n");
      // critical section : we are now going to call some inlined assembly erasure funcs
      if ( pthread_mutex_lock( handle->ctxt->erasurelock ) ) {
         LOG( LOG_ERR, "Failed to acquire erasurelock prior to encoding prep of stripe %d\n", stripenum );
         return -1;
      }
      // Generate an encoding matrix
      // NOTE: The matrix generated by gf_gen_rs_matrix is not always invertable for N>=6 and E>=5!
      gf_gen_cauchy1_matrix(handle->encode_matrix, N + E, N);
      // Generate g_tbls from encode matrix
      ec_init_tables(N, E, &(handle->encode_matrix[N * N]), handle->g_tbls);
      // exiting critical section
      if ( pthread_mutex_unlock( handle->ctxt->erasurelock ) ) {
         LOG( LOG_ERR, "Failed to relinquish erasurelock after encoding prep of stripe %d\n", stripenum );
         return -1;
      }
      handle->e_ready = 1;
   }

   // allocate space for our buffer references
   void** tgt_refs = calloc(N + E, sizeof(char*));
   if (tgt_refs == NULL) {
      LOG(LOG_ERR, "Failed to allocate space for a target buffer array!\n");
      return -1;
   }

   int outblock = (offset % stripesz) / partsz;  //determine what block we're filling
   size_t to_write = partsz - (offset % partsz); //determine if we need to finish writing a data part

   LOG(LOG_INFO, "Write of %zu bytes at offset %zu\n", bytes, offset);
   LOG(LOG_INFO, "   Init write block = %d\n", outblock);
   LOG(LOG_INFO, "   Init write size = %zu\n", to_write);

   // write out data from the buffer until we have all of it
   // NOTE - the (outblock >= N) check is meant to ensure we don't quit before outputing erasure parts
   ssize_t written = 0;
   while (written < bytes || outblock >= N) {
      ioblock* push_block = NULL;
      int reserved;
      // check that the current ioblock has room for our data
      if ((to_write < partsz) ||
         (reserved = reserve_ioblock(&(handle->iob[outblock]), &(push_block), handle->thread_states[outblock].ioq)) == 0) {
         // if this is a data part, we need to fill it now
         if (outblock < N) {
            // make sure we don't try to store more data than we were given
            char complete_part = 1;
            if (to_write > (bytes - written)) {
               to_write = (bytes - written);
               complete_part = 0;
            }
            void* tgt = ioblock_write_target(handle->iob[outblock]);
            // copy caller data into our ioblock
            LOG(LOG_INFO, "   Writing %zu bytes to block %d\n", to_write, outblock);
            memcpy(tgt, buffer + written, to_write); // no error check, SEGFAULT or nothing
            // update any data tracking values
            ioblock_update_fill(handle->iob[outblock], to_write, 0);
            written += to_write;
            handle->sub_offset += to_write;
            handle->totsz += to_write;
            // check if we have completed a part (always the case for erasure parts)
            if (complete_part) {
               outblock++;
               to_write = partsz;
            }
         }
         else {
            // assume we will successfully generate erasure
            ioblock_update_fill(handle->iob[outblock], partsz, 0);
            outblock++;
         }

         // check if we have completed a stripe
         if (outblock == (N + E)) {
            LOG(LOG_INFO, "Generating erasure parts for stripe %u\n", stripenum);
            // build an array of data/erasure references while reseting outblock to zero
            for (outblock -= 1; outblock >= 0; outblock--) {
               // previously written data will be one partsz behind
               tgt_refs[outblock] = ioblock_write_target(handle->iob[outblock]) - partsz;
            }
            // critical section : we are now going to call some inlined assembly erasure funcs
            if ( pthread_mutex_lock( handle->ctxt->erasurelock ) ) {
               LOG( LOG_ERR, "Failed to acquire erasurelock prior to encoding of stripe %d\n", stripenum );
               free( tgt_refs );
               return -1;
            }
            // generate erasure parts
            ec_encode_data(partsz, N, E, handle->g_tbls, (unsigned char**)tgt_refs, (unsigned char**)&(tgt_refs[N]));
            // exiting critical section
            if ( pthread_mutex_unlock( handle->ctxt->erasurelock ) ) {
               LOG( LOG_ERR, "Failed to relinquish erasurelock after encoding of stripe %d\n", stripenum );
               free( tgt_refs );
               return -1;
            }
            // reset outblock
            outblock = 0;
         }
      }
      else if (reserved > 0) {
         LOG(LOG_INFO, "Pushing full ioblock to thread %d\n", outblock);
         // the block is full and must be pushed to our iothread
         if (tq_enqueue(handle->thread_queues[outblock], TQ_NONE, (void*)push_block)) {
            LOG(LOG_ERR, "Failed to push ioblock to thread_queue %d\n", outblock);
            errno = EBADF;
            free(tgt_refs);
            return -1;
         }
         //NOOOOOOOOO!!!!! outblock++;
      }
      else {
         LOG(LOG_ERR, "Failed to reserve ioblock for position %d!\n", outblock);
         errno = EBADF;
         free(tgt_refs);
         return -1;
      }

      if (outblock == (N + E)) {
         // reset to the beginning of the stripe
         outblock = 0;
      }
   }

   // we have output all data
   free(tgt_refs);
   return written;
}

/* The following function was copied from Intel's ISA-L (https://github.com/intel/isa-l/blob/v2.30.0/examples/ec/ec_simple_example.c).
   The associated Copyright info has been reproduced below */

   /**********************************************************************
     Copyright(c) 2011-2018 Intel Corporation All rights reserved.
     Redistribution and use in source and binary forms, with or without
     modification, are permitted provided that the following conditions
     are met:
       * Redistributions of source code must retain the above copyright
         notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above copyright
         notice, this list of conditions and the following disclaimer in
         the documentation and/or other materials provided with the
         distribution.
       * Neither the name of Intel Corporation nor the names of its
         contributors may be used to endorse or promote products derived
         from this software without specific prior written permission.
     THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
     "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
     LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
     A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
     OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
     SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
     LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
     DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
     THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
     (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
     OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
   **********************************************************************/

   /*
    * Generate decode matrix from encode matrix and erasure list
    *
    */

static int gf_gen_decode_matrix_simple(unsigned char* encode_matrix,
   unsigned char* decode_matrix,
   unsigned char* invert_matrix,
   unsigned char* temp_matrix,
   unsigned char* decode_index, unsigned char* frag_err_list, int nerrs, int k,
   int m) {
   int i, j, p, r;
   int nsrcerrs = 0;
   unsigned char s, * b = temp_matrix;
   unsigned char frag_in_err[MAXPARTS];

   memset(frag_in_err, 0, sizeof(frag_in_err));

   // Order the fragments in erasure for easier sorting
   for (i = 0; i < nerrs; i++) {
      if (frag_err_list[i] < k)
         nsrcerrs++;
      frag_in_err[frag_err_list[i]] = 1;
   }

   // Construct b (matrix that encoded remaining frags) by removing erased rows
   for (i = 0, r = 0; i < k; i++, r++) {
      while (frag_in_err[r])
         r++;
      for (j = 0; j < k; j++)
         b[k * i + j] = encode_matrix[k * r + j];
      decode_index[i] = r;
   }

   // Invert matrix to get recovery matrix
   if (gf_invert_matrix(b, invert_matrix, k) < 0)
      return -1;

   // Get decode matrix with only wanted recovery rows
   for (i = 0; i < nerrs; i++) {
      if (frag_err_list[i] < k) // A src err
         for (j = 0; j < k; j++)
            decode_matrix[k * i + j] =
            invert_matrix[k * frag_err_list[i] + j];
   }

   // For non-src (parity) erasures need to multiply encode matrix * invert
   for (p = 0; p < nerrs; p++) {
      if (frag_err_list[p] >= k) { // A parity err
         for (i = 0; i < k; i++) {
            s = 0;
            for (j = 0; j < k; j++)
               s ^= gf_mul(invert_matrix[j * k + i],
                  encode_matrix[k * frag_err_list[p] + j]);
            decode_matrix[k * p + i] = s;
         }
      }
   }
   return 0;
}
