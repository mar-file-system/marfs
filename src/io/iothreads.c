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
#ifdef DEBUG_IO
#define DEBUG DEBUG_IO
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "iothreads"
#include "logging/logging.h"

#include "io/io.h"
#include "thread_queue/thread_queue.h"
#include "dal/dal.h"
#include "general_include/crc.c"

#include <isa-l.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>


/* ------------------------------   THREAD BEHAVIOR FUNCTIONS   ------------------------------ */

/**
 * Initialize the write thread state and create a DAL BLOCK_CTXT
 * @param unsigned int tID : The ID of this thread
 * @param void* global_state : Reference to a gthread_state struct
 * @param void** state : Reference to be populated with this thread's state info
 * @return int : Zero on success and -1 on failure
 */
int write_init(unsigned int tID, void* global_state, void** state) {
   gthread_state* gstate = (gthread_state*)global_state;
   // sanity check, this code was written to handle only a single thread per-queue
   if (tID > 0) {
      LOG(LOG_ERR, "Block %d has too many threads in a single queue!\n", gstate->location.block);
      return -1;
   }
   // allocate space for a thread state struct
   (*state) = malloc(sizeof(struct thread_state_struct));
   thread_state* tstate = (*state);
   if (tstate == NULL) {
      LOG(LOG_ERR, "Block %d failed to allocate space for a thread state struct!\n", gstate->location.block);
      return -1;
   }

   DAL dal = gstate->dal; // shorthand reference
   // set some gstate values
   gstate->offset = 0;
   gstate->data_error = 0;
   gstate->meta_error = 0;
   gstate->minfo.blocksz = 0;
   gstate->minfo.crcsum = 0;

   // set state fields
   tstate->gstate = gstate;
   tstate->offset = 0;
   tstate->iob = NULL;
   tstate->crcsumchk = 0;
   tstate->continuous = 1;

   // open a handle for this block
   tstate->handle = dal->open(dal->ctxt, gstate->dmode, gstate->location, gstate->objID);
   if (tstate->handle == NULL) {
      LOG(LOG_ERR, "failed to open handle for block %d!\n", gstate->location.block);
      gstate->data_error = 1;
   }

   return 0;
}

/**
 * Initialize the read thread state and create a DAL BLOCK_CTXT
 * @param unsigned int tID : The ID of this thread
 * @param void* global_state : Reference to a gthread_state struct
 * @param void** state : Reference to be populated with this thread's state info
 * @return int : Zero on success and -1 on failure
 */
int read_init(unsigned int tID, void* global_state, void** state) {
   gthread_state* gstate = (gthread_state*)global_state;
   // sanity check, this code was written to handle only a single thread per-queue
   if (tID > 0) {
      LOG(LOG_ERR, "Block %d has too many threads in a single queue!\n", gstate->location.block);
      return -1;
   }
   // allocate space for a thread state struct
   (*state) = malloc(sizeof(struct thread_state_struct));
   thread_state* tstate = (*state);
   if (tstate == NULL) {
      LOG(LOG_ERR, "Block %d failed to allocate space for a thread state struct!\n", gstate->location.block);
      return -1;
   }

   // set some gstate values
   //gstate->offset = 0;
   //gstate->data_error = 0;
   //gstate->meta_error = 0;

   // set state fields
   DAL dal = gstate->dal; // shorthand reference
   tstate->gstate = gstate;
   tstate->offset = gstate->offset;
   tstate->iob = NULL;
   tstate->crcsumchk = 0;
   tstate->continuous = 1;
   if (tstate->offset) {
      tstate->continuous = 0;
   }

   // open a handle for this block
   tstate->handle = dal->open(dal->ctxt, gstate->dmode, gstate->location, gstate->objID);
   if (tstate->handle == NULL) {
      LOG(LOG_WARNING, "failed to open handle for block %d, attempting meta only access\n", gstate->location.block);
      gstate->data_error = 1;
      tstate->handle = dal->open(dal->ctxt, DAL_METAREAD, gstate->location, gstate->objID);
      if (tstate->handle == NULL) {
         LOG(LOG_ERR, "failed to open meta handle for block %d!\n");
         gstate->meta_error = 1;
      }
   }

   // skip setting minfo values if they already appear to be set
   if (gstate->minfo.totsz == 0) {
      // populate our minfo struct with obj meta values
      if (dal->get_meta(tstate->handle, &gstate->minfo) != 0) {
         LOG(LOG_ERR, "Failed to populate all expected meta_info values!\n");
         gstate->meta_error = 1;
      }
   }

   return 0;
}

/**
 * Consume data buffers, generate CRCs for them, and write blocks out to their targets
 * @param void** state : Thread state reference
 * @param void** work_todo : Reference to the data buffer / work package
 * @return int : Integer return code ( -1 on failure, 0 on success )
 */
int write_consume(void** state, void** work_todo) {
   // get a reference to the thread state struct
   thread_state* tstate = (thread_state*)(*state);
   // get a reference to the global state for this block
   gthread_state* gstate = (gthread_state*)(tstate->gstate);
   // get a reference to the ioblock we've recieved to work on
   ioblock* iob = (ioblock*)(*work_todo);

   // determine what and how much data we have
   size_t datasz = 0;
   void* datasrc = ioblock_read_target(iob, &datasz, NULL);
   if (datasrc == NULL) {
      LOG(LOG_ERR, "Block %d received a NULL read target from ioblock!\n", gstate->location.block);
      gstate->data_error = 1;
      release_ioblock(gstate->ioq);
      return -1;
   }

   // sanity check that our data size makes sense
   if (datasz > (gstate->minfo.versz - CRC_BYTES)) {
      LOG(LOG_ERR, "Block %d received unexpectedly large data size: %zd\n", gstate->location.block, datasz);
      gstate->data_error = 1;
      release_ioblock(gstate->ioq);
      return -1;
   }

   if (datasz > 0) {
      // critical section : we are now going to call some inlined assembly erasure funcs
      if ( pthread_mutex_lock( gstate->erasurelock ) ) {
         LOG(LOG_ERR, "Block %d failed to acquire erasurelock\n", gstate->location.block);
         gstate->data_error = 1;
         release_ioblock(gstate->ioq);
         return -1;
      }
      // calculate a CRC for this data and append it to the buffer
      *(uint32_t*)(datasrc + datasz) = crc32_ieee(CRC_SEED, datasrc, datasz);
      // exiting critical section
      if ( pthread_mutex_unlock( gstate->erasurelock ) ) {
         LOG(LOG_ERR, "Block %d failed to release erasurelock\n", gstate->location.block);
         gstate->data_error = 1;
         release_ioblock(gstate->ioq);
         return -1;
      }
      gstate->minfo.crcsum += *((uint32_t*)(datasrc + datasz));
      datasz += CRC_BYTES;
      // increment our block size
      gstate->minfo.blocksz += datasz;

      // write data out via the DAL, but only if we have not yet encoutered a write error
      if ((gstate->data_error == 0) && gstate->dal->put(tstate->handle, datasrc, datasz)) {
         LOG(LOG_ERR, "Failed to write %zu bytes to block %d!\n", datasz, gstate->location.block);
         gstate->data_error = 1;
         // don't bother to abort yet, we'll do that on close
      }
   }

   // regardless of success, we need to free up our ioblock
   if (release_ioblock(gstate->ioq)) {
      LOG(LOG_ERR, "Block %d failed to release ioblock!\n", gstate->location.block);
      gstate->data_error = 1;
      return -1;
   }

   return 0;
}

/**
 * Read data from our target, verify its CRC, and continue until we have a full buffer to push
 * @param void** state : Thread state reference
 * @param void** work_tofill : Reference to be populated with the produced buffer
 * @return int : Integer return code ( -1 on error, 0 on success, and 2 once all buffers have been read )
 */
int read_produce(void** state, void** work_tofill) {
   // get a reference to the thread state struct
   thread_state* tstate = (thread_state*)(*state);
   // get a reference to the global state for this block
   gthread_state* gstate = (gthread_state*)(tstate->gstate);

   // check if our offset is beyond the end of the block
   if (tstate->offset >= gstate->minfo.blocksz) {
      // if we haven't hit any data errors AND we haven't reseeked, verify our global CRC
      if (gstate->data_error == 0 && tstate->continuous && !(gstate->meta_error)) {
         if (tstate->crcsumchk != gstate->minfo.crcsum) {
            LOG(LOG_ERR, "Block %d data CRC sum (%llu) does not match meta CRC sum (%llu)\n",
               gstate->location.block, tstate->crcsumchk, gstate->minfo.crcsum);
            gstate->data_error = 1;
         }
      }
      // we may still have data in our current ioblock
      if (tstate->iob != NULL) {
         if (ioblock_get_fill(tstate->iob)) {
            *work_tofill = tstate->iob;
            tstate->iob = NULL; // NULL this out, so we don't try to release it
            return 0;
         }
      }
      LOG(LOG_INFO, "Thread has reached end of block %d (bsz=%zu)\n", gstate->location.block, gstate->minfo.blocksz);
      *work_tofill = NULL;
      return 2;
   }

   // loop until we have filled an ioblock
   ioblock* push_block = NULL;
   int resres = 0;
   while (1) {
      LOG( LOG_INFO, "Reserving an IOBlock for use in read from block %d\n", gstate->location.block );
      resres = reserve_ioblock(&(tstate->iob), &push_block, gstate->ioq);
      // check for an error condition
      if (resres == -1) {
         LOG(LOG_ERR, "Failed to reserve an ioblock!\n");
         gstate->data_error = 1;
         return -1;
      }
      // check if our ioblock is full, and ready to be pushed
      if (resres > 0) {
         LOG(LOG_INFO, "Pushing full ioblock to work queue\n");
         break;
      }
      // otherwise, perform a read and store data to that block
      ssize_t read_data = 0;
      ssize_t to_read = (gstate->minfo.versz > (gstate->minfo.blocksz - tstate->offset)) ? (gstate->minfo.blocksz - tstate->offset) : gstate->minfo.versz;
      // if we have read all available data, break
      if (to_read == 0) {
         LOG(LOG_INFO, "All data read from block %d, pushing incomplete ioblock\n", gstate->location.block);
         push_block = tstate->iob;
         tstate->iob = NULL;
         break;
      }
      if ( to_read <= CRC_BYTES ) {
         LOG( LOG_ERR, "Remaining data at offset %zu of block %d ( %zd ) is <= CRC_BYTES!\n", tstate->offset, gstate->location.block, to_read );
         gstate->data_error = 1;
         return -1; // force an abort
      }
      void* store_tgt = ioblock_write_target(tstate->iob);
      char data_err = 0;
      LOG(LOG_INFO, "Reading %zd bytes from offset %zu of block %d\n", to_read, tstate->offset, gstate->location.block);
      if ((read_data = gstate->dal->get(tstate->handle, store_tgt, to_read, tstate->offset)) <
         to_read) {
         LOG(LOG_ERR, "Expected read return value of %zd for block %d, but recieved: %zd\n",
            to_read, gstate->location.block, read_data);
         gstate->data_error = 1;
         data_err = 1;
      }
      to_read -= CRC_BYTES;
      // check the crc
      if (data_err == 0) {
         uint32_t crc = 0;
         uint32_t scrc = *((uint32_t*)(store_tgt + to_read));
         tstate->crcsumchk += scrc; // track our global crc, for reference
         // critical section : we are now going to call some inlined assembly erasure funcs
         if ( pthread_mutex_lock( gstate->erasurelock ) ) {
            LOG(LOG_ERR, "Block %d failed to acquire erasurelock\n", gstate->location.block);
            gstate->data_error = 1;
            data_err = 1;
         }
         else {
            crc = crc32_ieee(CRC_SEED, store_tgt, to_read);
            // exiting critical section
            if ( pthread_mutex_unlock( gstate->erasurelock ) ) {
               LOG(LOG_ERR, "Block %d failed to release erasurelock\n", gstate->location.block);
               gstate->data_error = 1;
               return -1; // force an abort
            }
            if (crc != scrc) {
               LOG(LOG_ERR, "Calculated CRC of data (%u) does not match stored CRC: %u\n", crc, scrc);
               gstate->data_error = 1;
               data_err = 1;
            }
         }
      }
      // note how much REAL data (no CRC) we've stored to the ioblock
      ioblock_update_fill(tstate->iob, to_read, data_err);
      // note our increased offset within the data (MUST include the CRC!)
      tstate->offset += (to_read + CRC_BYTES);
   }

   // populate our workpackage with the filled ioblock
   *work_tofill = push_block;
   return 0;
}

/**
 * No-op function, just to fill out the TQ struct
 */
int write_pause(void** state, void** prev_work) {
   return 0; // noop, probably permanently
}

/**
 * No-op function, just to fill out the TQ struct
 */
int read_pause(void** state, void** prev_work) {
   return 0; // noop, probably permanently
}

/**
 * No-op function, just to fill out the TQ struct
 */
int write_resume(void** state, void** prev_work) {
   return 0; // noop, probably permanently
}

/**
 * Create an IOQueue (if not done already), and destory any work package we already produced (reseek possible)
 * @param void** state : Thread state reference
 * @param void** prev_work : Reference to any previously populated buffer
 * @return int : Integer return code ( -1 on error, 0 on success )
 */
int read_resume(void** state, void** prev_work) {
   // get a reference to the thread state struct
   thread_state* tstate = (thread_state*)(*state);
   // get a reference to the global state for this block
   gthread_state* gstate = (gthread_state*)(tstate->gstate);
   LOG(LOG_INFO, "Reader %d is waking up\n", gstate->location.block);

   // check for a NULL ioq and create one if so (TODO: unnecessary?)
   if (gstate->ioq == NULL) {
      LOG(LOG_INFO, "Creating own ioqueue for block %d\n", gstate->location.block);
      gstate->ioq = create_ioqueue(gstate->minfo.versz, gstate->minfo.partsz, gstate->dmode);
      if (gstate->ioq == NULL) {
         LOG(LOG_ERR, "Failed to create ioqueue!\n");
         return -1;
      }
   }
   // check for a NON-NULL work package, and release the block if so
   if (*prev_work != NULL) {
      // attempt to release our previously filled buffer
      // NOTE -- this only works assuming the master / consumer proc has already
      //         consumed all other IOBlock work packages
      LOG(LOG_INFO, "Block %d is releasing previous ioblock\n", gstate->location.block);
      if (release_ioblock(gstate->ioq)) {
         LOG(LOG_ERR, "Failed to release previous ioblock!\n");
         return -1;
      }
      // NULL out our IOBlock reference, causing us to immediately generate another
      *prev_work = NULL;
   }
   // translate our new data offset to an offest in the block (including CRCs) at which we can actually issue I/O
   size_t noffset = gstate->offset;
   unsigned int io_count = (unsigned int)(noffset / (gstate->minfo.versz - CRC_BYTES)); // integer truncation rounds down
   // now, determine if we need to adjust our ioblock alignment to achieve the desired offset
   size_t trim = noffset % (gstate->minfo.versz - CRC_BYTES);
   // check the fill-level of our current ioblock
   size_t curdata = 0;
   if (tstate->iob) {
      curdata = tstate->iob->data_size;
   }
   // if this is a seek, we have some bookkeeping to take care of
   // NOTE -- check if the starting offset of our ioblock ( current offset minus stored data )
   //         matches our target offset value to determine if this is a seek
   if ((tstate->offset - curdata) != (io_count * gstate->minfo.versz) + trim) {
      // zero out our crcsum
      tstate->crcsumchk = 0;
      // a seek to anything besides zero means a non-continous read (can't verify global CRC)
      if (io_count != 0) {
         tstate->continuous = 0;
      }

      LOG(LOG_INFO, "Reseek to %zu (iocnt=%u / trim=%zu / cont=%d)\n", noffset, io_count, trim, tstate->continuous);
      // set our offset to the new value
      tstate->offset = io_count * gstate->minfo.versz;
      // need to cleanup any data lurking in our ioblock reference
      if (curdata != 0) {
         LOG(LOG_INFO, "Releasing current ioblock as it is non-empty\n");
         if (release_ioblock(gstate->ioq)) {
            LOG(LOG_ERR, "Failed to release current ioblock!\n");
            return -1;
         }
         tstate->iob = NULL;
      }

      // check if we need to realign our ioblocks
      if (trim) {
         ioblock* push_block = NULL;
         if (tstate->iob == NULL) {
            // reserve a new ioblock
            LOG(LOG_INFO, "Reserving a new ioblock\n");
            if (reserve_ioblock(&(tstate->iob), &push_block, gstate->ioq)) {
               LOG(LOG_ERR, "Failed to reserve a new ioblock!\n");
               return -1;
            }
         }
         // adjust our ioblock offset to align with the request
         LOG(LOG_INFO, "Aligning ioblock to trim of %zu\n", trim);
         int junk_blocks;
         if ((junk_blocks = align_ioblock(tstate->iob, trim, gstate->ioq)) < 0) {
            LOG(LOG_ERR, "Failed to align ioblock to trim of %zu!\n", trim);
            return -1;
         }
         while (junk_blocks > 0) {
            // now fill this ioblock
            if (read_produce((void**)&(tstate), (void**)&(push_block))) {
               LOG(LOG_ERR, "Failed to produce a junk ioblock!\n");
               return -1;
            }
            // now, just make the current ioblock available again
            LOG(LOG_INFO, "Releasing junk ioblock\n");
            if (release_ioblock(gstate->ioq)) {
               LOG(LOG_ERR, "Failed to release junk ioblock!\n");
               return -1;
            }
            junk_blocks--;
         }
      }
   }
   return 0;
}

/**
 * Write out our meta info and close our target reference
 * @param void** state : Thread state reference
 * @param void** prev_work : Reference to any unused previous buffer
 * @param TQ_Control_Flags flg : Control flags values at thread term
 */
void write_term(void** state, void** prev_work, TQ_Control_Flags flg) {
   // get a reference to the thread state struct
   thread_state* tstate = (thread_state*)(*state);
   // get a reference to the global state for this block
   gthread_state* gstate = (gthread_state*)(tstate->gstate);

   // if we never used an IOBlock reference, we need to release it
   if (*(prev_work) != NULL && release_ioblock(gstate->ioq)) {
      LOG(LOG_ERR, "Failed to release previous IOBlock!\n");
      gstate->data_error = 1;
      // not much to do besides complain
   }

   // attempt to write out meta info
   if (gstate->dal->set_meta(tstate->handle, &(gstate->minfo))) {
      LOG(LOG_ERR, "Failed to set meta value for block %d!\n", gstate->location.block);
      gstate->meta_error = 1;
   }

   // don't leave potentially bad data behind
   // NOTE -- not really a problem of data being corrupt (crcs can catch that)
   //         Rather, completely skipped writes *could* mean our erasure stripes end up
   //         misaligned, something we can't easily detect.
   if (gstate->data_error != 0 || (flg & TQ_ABORT) != 0) {
      LOG(LOG_ERR, "Aborting write of block %d due to previous errors!\n", gstate->location.block);
      // just in case, be CERTAIN to note this as a failure
      gstate->meta_error = 1;
      gstate->data_error = 1;
      if (tstate->handle  &&  gstate->dal->abort(tstate->handle)) {
         LOG(LOG_ERR, "Abort of block %d failed!\n", gstate->location.block);
         // not really much to do besides complain
      }
   }
   else if ( tstate->handle ) { // attempt to close our block
      if ( gstate->dal->close(tstate->handle) ) {
         LOG(LOG_ERR, "Failed to close block %d!\n", gstate->location.block);
         gstate->data_error = 1;
         if (gstate->dal->abort(tstate->handle)) {
            LOG(LOG_ERR, "Abort of block %d failed!\n", gstate->location.block);
            // not really much to do besides complain
         }
      }
   }
   else { gstate->data_error = 1; } // just to be certain we're noting this NULL handle



   // just free and NULL our state, there isn't any useful info in there
   free(tstate);
   *state = NULL;
}

/**
 * Close our target reference
 * @param void** state : Thread state reference
 * @param void** prev_work : Reference to any unused previous buffer
 * @param TQ_Control_Flags flg : Control flags values at thread term
 */
void read_term(void** state, void** prev_work, TQ_Control_Flags flg) {
   // get a reference to the thread state struct
   thread_state* tstate = (thread_state*)(*state);
   // get a reference to the global state for this block
   gthread_state* gstate = (gthread_state*)(tstate->gstate);

   // if we never pushed an IOBlock reference, we need to release it
   if (*(prev_work) != NULL) {
      LOG(LOG_INFO, "Reader %d releasing unused ioblock\n", gstate->location.block);
      if (release_ioblock(gstate->ioq)) {
         LOG(LOG_ERR, "Reader %d failed to release unused IOBlock!\n", gstate->location.block);
         // not much to do besides complain
      }
   }

   // if we were in the process of populating an ioblock, release that as well
   if (tstate->iob != NULL) {
      LOG(LOG_INFO, "Reader %d releasing in-progress ioblock\n", gstate->location.block);
      if (release_ioblock(gstate->ioq)) {
         LOG(LOG_ERR, "Reader %d failed to release in-progress IOBlock!\n", gstate->location.block);
         // not much to do besides complain
      }
   }

   // close our DAL handle
   if (gstate->dal->close(tstate->handle)) {
      // pessimistically call this a data erorr ( may not be necessary )
      gstate->data_error = 1;
      LOG(LOG_ERR, "Failed to close read handle for block %d!\n", gstate->location.block);
      // can only really complain, nothing else to be done
   }

   // just free and NULL our state, there isn't any useful info in there
   free(tstate);
   *state = NULL;

   // NOTE -- it is up to the master / consumer proc to destroy our IOQueue
}

