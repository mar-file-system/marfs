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

// #include "libne_auto_config.h"   /* HAVE_LIBISAL */

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_IO
#define DEBUG DEBUG_IO
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "ioqueue"
#include "logging/logging.h"

#include "io/io.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>




/* ------------------------------   IO QUEUE/BLOCK INTERACTION   ------------------------------ */


/**
 * Creates a new IOQueue
 * @param size_t iosz : Byte size of each IO to be performed
 * @param size_t partsz : Byte size of each erasure part
 * @return ioqueue* : Reference to the newly created IOQueue
 */
ioqueue* create_ioqueue( size_t iosz, size_t partsz, DAL_MODE mode ) {
   LOG( LOG_INFO, "Creating IOQueue with IOSZ=%zu, PARTSZ=%zu, MODE=%s\n", iosz, partsz, ( mode == DAL_READ ) ? "read" : "write" );
   // sanity check that our IO Size is sufficient to at least do something
   if ( iosz <= CRC_BYTES ) {
      LOG( LOG_ERR, "IO Size of %zu is too small for CRC size of %d!\n", iosz, CRC_BYTES );
      return NULL;
   }
   size_t subsz = (mode == DAL_READ) ? (iosz - CRC_BYTES) : partsz;
   int    partcnt = (int) ( (iosz - CRC_BYTES) / partsz); // number of complete parts per IO
   if ( partsz > (iosz - CRC_BYTES) ) {
      partcnt = 1;
   }
   LOG( LOG_INFO, "Subsz = %zu,  and  Partcnt = %d\n", subsz, partcnt );
   // create an ioqueue struct
   ioqueue* ioq = calloc( 1, sizeof( struct ioqueue_struct ) );
   if ( ioq == NULL ) {
      LOG( LOG_ERR, "failed to allocate memory for an ioqueue_struct!\n" );
      return NULL;
   }
   // intialize all ioqueue values
   if ( pthread_mutex_init( &(ioq->qlock), NULL ) ) {
      LOG( LOG_ERR, "failed to initialize the ioqueue lock!\n" );
      free( ioq );
      return NULL;
   }
   // initialize the condition var
   if ( pthread_cond_init( &(ioq->avail_block), NULL ) ) {
      LOG( LOG_ERR, "failed to initialize the ioqueue avail_block conditional var!\n" );
      pthread_mutex_destroy( &(ioq->qlock) );
      free( ioq );
      return NULL;
   }
   // determine fill and split thresholds for these blocks
   //ioq->fill_threshold = ( (iosz - CRC_BYTES) > partsz ) ? (iosz - CRC_BYTES) : partsz;
   // NOTE -- if we're writing, we need to get both a complete part and a complete IO
   ioq->split_threshold = ( mode == DAL_READ ) ? (partcnt * partsz) : (iosz - CRC_BYTES);
   LOG( LOG_INFO, "Using split_treshold=%zu\n", ioq->split_threshold );
   // NOTE -- if we're writing, we need to split blocks on IO alignment to make room for CRCs
   ioq->partsz = partsz;
   ioq->iosz = iosz;
   ioq->partcnt = partcnt;
   ioq->head = 0;
   ioq->depth = SUPER_BLOCK_CNT;
   // calculate the blocksz we must allocate to allways fit written data
   // NOTE -- assuming perfect IOSZ and PARTSZ alignment, we will need space for a full buffer plus
   //         room for trailing CRC bytes.
   //size_t spillage = ioq->fill_threshold - ioq->split_threshold;
   ioq->blocksz = ioq->split_threshold + CRC_BYTES;
   // check for misalignment of IOSZ and PARTSZ values
   //size_t overflow = 0;
   //if ( (overflow = ioq->split_threshold % subsz) ) {
   //   LOG( LOG_INFO, "Split threshold of %zu does not cleanly align with subsz of %zu\n", ioq->split_threshold, subsz );
      //overflow = 1;
      // NOTE -- misalignment means we need space for potentially another subsz worth of overlap
   // NOTE -- ne_seek can result in misalignment regardless of IO/PARTSZ values, so *always* expand this buffer
   ioq->blocksz += subsz;
   //}
   //if ( (ioq->fill_threshold - spillage) % subsz ) {
   //   LOG( LOG_INFO, "Post spillage fill %zu does not cleanly align with subsz of %zu\n", ioq->fill_threshold - spillage, subsz );
   //   overflow = 1;
   //}
   LOG( LOG_INFO, "Using ioblock size of %zu\n", ioq->blocksz );
   int i;
   for ( i = 0; i < SUPER_BLOCK_CNT; i++ ) {
      // initialize state and struct for each ioblock
      int allocres = posix_memalign( &(ioq->block_list[i].buff), 4096, sizeof( char ) * ioq->blocksz );
      if ( allocres  ||  ioq->block_list[i].buff == NULL ) {
         // we've messed up, time to try to clean everything up
         LOG( LOG_ERR, "failed to allocate space for ioblock %d!\n", i );
         for ( i -= 1; i >= 0; i-- ) {
            free( ioq->block_list[i].buff );
         }
         pthread_cond_destroy( &(ioq->avail_block) );
         pthread_mutex_destroy( &(ioq->qlock) );
         free( ioq );
         errno = allocres; // posix_memalign() does not set errno for us
         return NULL;
      }
      ioq->block_list[i].data_size   = 0;
      ioq->block_list[i].error_end   = 0;
   }
   return ioq;
}


/**
 * Destroys an existing IOQueue
 * @param ioqueue* ioq : Reference to the ioqueue struct to be destroyed
 * @return int : Zero on success and a negative value if an error occurred
 */
int destroy_ioqueue( ioqueue* ioq ) {
   if ( ioq == NULL ) {
      LOG( LOG_ERR, "Received NULL ioqueue reference!\n" );
      return -1;
   }
   if ( pthread_mutex_lock(&ioq->qlock) ) { // aquire the queue lock
      LOG( LOG_ERR, "Failed to aquire ioqueue lock!\n" );
      return -1;
   }
   if ( ioq->depth != SUPER_BLOCK_CNT ) {
      LOG( LOG_ERR, "Cannot destroy ioqueue struct while ioblocks are in use!\n" );
      pthread_mutex_unlock(&ioq->qlock);
      return -1;
   }
   int i;
   for ( i = 0; i < SUPER_BLOCK_CNT; i++ ) {
      free( ioq->block_list[i].buff );
   }
   pthread_cond_destroy( &(ioq->avail_block) );
   pthread_mutex_unlock(&ioq->qlock);
   pthread_mutex_destroy( &(ioq->qlock) );
   free( ioq );
   LOG( LOG_INFO, "IOQueue successfully destroyed\n" );
   return 0;
}


/**
 * Calculates the maximum amount of data the queue can contain (useful for determining if seek is necessary)
 * @param ioqueue* ioq : IOQueue for which to calculate the max data value
 * @return size_t : Max data size value
 */
ssize_t ioqueue_maxdata( ioqueue* ioq ) {
   if ( ioq == NULL ) {
      LOG( LOG_ERR, "Received NULL ioqueue reference!\n" );
      return -1;
   }
   return ( SUPER_BLOCK_CNT * ioq->split_threshold );
}


/**
 * Sets ioblock fill level such that a specific data split will occur (used to align ioblock to a specific offset)
 * @param ioblock* iob : Current ioblock
 * @param size_t trim : Amount of data to be 'trimmed' from the final IO to achive the desired offset
 * @param ioqueue* ioq : Reference to the ioqueue struct from which the ioblock was gathered
 * @return int : A positive number of ioblocks to be thrown out or a negative value if an error occurred
 */
int align_ioblock( ioblock* cur_block, size_t trim, ioqueue* ioq ) {
   if ( ioq == NULL ) {
      LOG( LOG_ERR, "Received NULL ioqueue reference!\n" );
      return -1;
   }
   if ( cur_block == NULL ) {
      LOG( LOG_ERR, "Received NULL ioblock reference!\n" );
      return -1;
   }
   // note how many ioblocks will need to be junked to hit this alignment
   int junk_blocks = (int)( trim / ioq->split_threshold) + 1;
   // adjust our trim value, if it exceeds the split_threshold
   trim -= ((junk_blocks - 1) * ioq->split_threshold);
   // calculate how much fake data we need to populate for the desired alignment
   size_t falsefill = ioq->split_threshold - trim;
   // sanity checks
//   if ( falsefill > ioq->fill_threshold ) {
//      LOG( LOG_ERR, "Falsefill value (%zu) exceeds fill_threshold (%zu)\n", falsefill, ioq->fill_threshold );
//      return -1;
//   }
   LOG( LOG_INFO, "Filling %zu fake bytes to achieve alignment\n", falsefill );
   cur_block->data_size = falsefill;
   cur_block->error_end = falsefill;
   return junk_blocks;
}


/**
 * Determines if a new ioblock is necessary to store additional data and, if so, reserves it.  Also, as ioblocks are filled, 
 * populates the 'push_block' reference with the ioblock that should be passed for read/write use.
 * @param ioblock** cur_block : Reference to be popluated with an updated ioblock (usable if return value == 0)
 * @param ioblock** push_block : Reference to be populated with a filled ioblock, ready to be passed for read/write use
 * @param ioqueue* ioq : Reference to the ioqueue struct from which ioblocks should be gathered
 * @return int : A positive value if the passed ioblock is full and push_block has been set (cur_block updated and push_block set),
 *               a value of zero if the current ioblock is now safe to fill (cur_block set to new ioblock reference OR unchanged), 
 *               and a negative value if an error was encountered.
 * 
 * NOTE: It is an error to write data of size != both erasure part size and the IO size to an ioblock.
 * 
 * NOTE -- it is possible, in the case of a full ioblock (return value == 1), for the newly reserved ioblock to ALSO be full.
 *         ONLY a return value of zero implies the current ioblock is safe for use!
 */
int reserve_ioblock( ioblock** cur_block, ioblock** push_block, ioqueue* ioq ) {
   // track any data we need to copy
   void* datacpy = NULL;
   size_t cpysz  = 0;
   ioblock* prev_block = (*cur_block);

   // sanity check that we can dereference the ioq
   if ( ioq == NULL ) {
      LOG( LOG_ERR, "Received NULL ioq!\n" );
      return -1;
   }

   // if we have a previous IO block...
   if ( prev_block != NULL ) {
      // check if that block has room remaining
      if ( prev_block->data_size < ioq->split_threshold ) {
         LOG( LOG_INFO, "Continuing to use current block, as %zu is below split_threshold\n", prev_block->data_size );
         return 0;
      }
      // otherwise, we may need to copy data over to the new ioblock
      if ( prev_block->data_size > ioq->split_threshold ) {
         datacpy = prev_block->buff + ioq->split_threshold;
         cpysz = prev_block->data_size - ioq->split_threshold;
         // sanity check
         if ( prev_block->data_size > ioq->blocksz ) {
            LOG( LOG_ERR, "Detected buffer overrun!  %zu data > %zu blocksz!\n", prev_block->data_size, ioq->blocksz );
            return -1;
         }
      }
      // we will definitely be pushing the previous block
      (*push_block) = prev_block;
   }

   // if the previous block was NULL or did not have sufficient space, we need to reserve a new block
   if ( pthread_mutex_lock(&ioq->qlock) ) { // aquire the queue lock
      LOG( LOG_ERR, "Failed to aquire ioqueue lock!\n" );
      return -1;
   }
   // wait for a block to be available for use
   while ( ioq->depth == 0 ) {
      LOG( LOG_INFO, "Waiting for ioblock to become available\n" );
      pthread_cond_wait( &ioq->avail_block, &ioq->qlock );
   }
   // update the current block to the new reference
   (*cur_block) = &(ioq->block_list[ioq->head]);
   // update queue values to reflect the block being in use
   ioq->depth--;
   ioq->head += 1;
   if ( ioq->head == SUPER_BLOCK_CNT ) { ioq->head = 0; }
   pthread_mutex_unlock(&ioq->qlock);

   // clear any old values in this newly reserved block
   (*cur_block)->data_size   = 0;
   (*cur_block)->error_end   = 0;

   // we have the new block; check if we need to copy data over to it
   if ( datacpy != NULL ) {
      LOG( LOG_INFO, "Copying %zu bytes to next ioblock\n", cpysz );
      (*cur_block)->data_size = cpysz;
      // check for any data errors we need to propagate forward
      if ( prev_block->error_end > ioq->split_threshold ) {
         // this is an oversimplification, but being exact won't actually gain us anything
         (*cur_block)->error_end = cpysz; // assume all copied data is junk
      }
      else {
         // only bother copying good data over to the new block
         memcpy( (*cur_block)->buff, datacpy, cpysz );
      }
      prev_block->data_size = ioq->split_threshold; // update prev block to exclude copied data
   }
   return ( prev_block == NULL ) ? 0 : 1; // if there was a previous block, it should be pushed
}


/**
 * Retrieve a buffer target reference for writing into the given ioblock
 * @param ioblock* block : Reference to the ioblock to retrieve a target for
 * @return void* : Buffer reference to write to
 */
void* ioblock_write_target( ioblock* block ) {
   return block->buff + block->data_size;
}


/**
 * Retrieve a buffer target reference for reading data from the given ioblock
 * @param ioblock* block : Reference to the ioblock to retrieve a target for
 * @param size_t* bytes : Reference to be populated with the data size of the ioblock
 * @param off_t* error_end : Reference to be populated with the offset of the final data error 
 *                           in the buffer ( i.e. data beyond this offset is valid )
 * @return void* : Buffer reference to read from
 */
void* ioblock_read_target( ioblock* block, size_t* bytes, off_t* error_end ) {
   if ( error_end ) {
      *error_end = block->error_end;
   }
   if ( bytes ) {
      (*bytes) = block->data_size;
   }
   return block->buff;
}


/**
 * Update the data_size value of a given ioblock
 * @param ioblock* block : Reference to the ioblock to update
 * @param size_t bytes : Size of data added to the ioblock
 * @param char bad_data : Indicates if the stored data contains errors ( 0 for no errors, 1 for errors )
 */
void ioblock_update_fill( ioblock* block, size_t bytes, char bad_data ) {
   block->data_size += bytes;
   // if we have errors, we need to track them
   if ( bad_data ) {
      block->error_end = block->data_size;
   }
}


/**
 * Overwrites data size and error offset information for a given ioblock
 * @param ioblock* block : Reference to the ioblock to update
 * @param size_t bytes : Data size for this ioblock
 * @param off_t error_end : Offset of the final data error for this block
 */
void ioblock_overwrite_fill( ioblock* block, size_t bytes, off_t error_end ) {
   block->data_size = bytes;
   block->error_end = error_end;
}


/**
 * Get the current data size written to the ioblock
 * @param ioblock* block : Reference to the ioblock to update
 * @return size_t : Data size of the ioblock
 */
size_t ioblock_get_fill( ioblock* block ) {
   return block->data_size;
}


/**
 * Simply makes an ioblock available for use again by increasing ioqueue depth (works due to single producer & consumer assumption)
 * @param ioqueue* ioq : Reference to the ioqueue struct to have depth increased
 * @param int : Zero on success and a negative value if an error occurred
 */
int release_ioblock( ioqueue* ioq ) {
   if ( pthread_mutex_lock(&ioq->qlock) ) { // aquire the queue lock
      LOG( LOG_ERR, "Failed to aquire ioqueue lock!\n" );
      return -1;
   }
   if ( ioq->depth == SUPER_BLOCK_CNT ) {
      LOG( LOG_ERR, "No outstanding ioblocks to be released!\n" );
      pthread_mutex_unlock(&ioq->qlock);
      return -1;
   }
   ioq->depth++;
   LOG( LOG_INFO, "%d out of %d ioblocks available\n", ioq->depth, SUPER_BLOCK_CNT );
   pthread_cond_signal(&ioq->avail_block);
   pthread_mutex_unlock(&ioq->qlock);
   return 0;
}



