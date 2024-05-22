#ifndef __IO_THREADS_H__
#define __IO_THREADS_H__

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

// THIS INTERFACE RELIES ON THE DAL INTERFACE!
#include "dal/dal.h"
#include "thread_queue/thread_queue.h"
#include <pthread.h>
#include <stdint.h>

#define SUPER_BLOCK_CNT 4
#define CRC_BYTES 4 // DO NOT decrease without adjusting CRC gen and block creation code!

/* ------------------------------   IO QUEUE   ------------------------------ */

typedef struct ioblock_struct
{
   size_t data_size; // amount of usable data contained in this buffer
                     //  off_t  error_start;  // offset in buffer at which data errors begin
   off_t error_end;  // offset in buffer at which data errors end
   void *buff;       // buffer for data transfer
} ioblock;

// Queue of IOBlocks for thread communication
typedef struct ioqueue_struct
{
   pthread_mutex_t qlock;               // lock for queue manipulation
   pthread_cond_t avail_block;          // condition for awaiting an available block
   int head;                            // integer indicating location of the next available block
   int depth;                           // current depth of the queue
   ioblock block_list[SUPER_BLOCK_CNT]; // list of ioblocks

   //size_t          fill_threshold;
   size_t split_threshold;

   size_t partsz;  // size of each erasure part
   size_t iosz;    // size of each IO
   int partcnt;    // number of erasure parts each buffer can hold
   size_t blocksz; // size of each ioblock buffer
} ioqueue;

/**
 * Creates a new IOQueue
 * @param size_t iosz : Byte size of each IO to be performed
 * @param size_t partsz : Byte size of each erasure part
 * @return ioqueue* : Reference to the newly created IOQueue
 */
ioqueue *create_ioqueue(size_t iosz, size_t partsz, DAL_MODE mode);

/**
 * Destroys an existing IOQueue
 * @param ioqueue* ioq : Reference to the ioqueue struct to be destroyed
 * @return int : Zero on success and a negative value if an error occurred
 */
int destroy_ioqueue(ioqueue *ioq);

/**
 * Calculates the maximum amount of data the queue can contain (useful for determining if seek is necessary)
 * @param ioqueue* ioq : IOQueue for which to calculate the max data value
 * @return size_t : Max data size value
 */
ssize_t ioqueue_maxdata(ioqueue *ioq);

/**
 * Sets ioblock fill level such that a specific data split will occur (used to align ioblock to a specific offset)
 * @param ioblock* iob : Current ioblock
 * @param size_t trim : Amount of data to be 'trimmed' from the final IO to achive the desired offset
 * @param ioqueue* ioq : Reference to the ioqueue struct from which the ioblock was gathered
 * @return int : A positive number of ioblocks to be thrown out or a negative value if an error occurred
 */
int align_ioblock(ioblock *cur_block, size_t trim, ioqueue *ioq);

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
int reserve_ioblock(ioblock **cur_block, ioblock **push_block, ioqueue *ioq);

/**
 * Retrieve a buffer target reference for writing into the given ioblock
 * @param ioblock* block : Reference to the ioblock to retrieve a target for
 * @return void* : Buffer reference to write to
 */
void *ioblock_write_target(ioblock *block);

/**
 * Retrieve a buffer target reference for reading data from the given ioblock
 * @param ioblock* block : Reference to the ioblock to retrieve a target for
 * @param size_t* bytes : Reference to be populated with the data size of the ioblock
 * @param off_t* error_end : Reference to be populated with the offset of the final data error
 *                           in the buffer ( i.e. data beyond this offset is valid )
 * @return void* : Buffer reference to read from
 */
void *ioblock_read_target(ioblock *block, size_t *bytes, off_t *error_end);

/**
 * Update the data_size value of a given ioblock
 * @param ioblock* block : Reference to the ioblock to update
 * @param size_t bytes : Size of data added to the ioblock
 * @param char bad_data : Indicates if the stored data contains errors ( 0 for no errors, 1 for errors )
 */
void ioblock_update_fill(ioblock *block, size_t bytes, char bad_data);

/**
 * Get the current data size written to the ioblock
 * @param ioblock* block : Reference to the ioblock to update
 * @return size_t : Data size of the ioblock
 */
size_t ioblock_get_fill(ioblock *block);

/**
 * Simply makes an ioblock available for use again by increasing ioqueue depth (works due to single producer & consumer assumption)
 * @param ioqueue* ioq : Reference to the ioqueue struct to have depth increased
 * @param int : Zero on success and a negative value if an error occurred
 */
int release_ioblock(ioqueue *ioq);

/* ------------------------------   THREAD BEHAVIOR   ------------------------------ */

// This struct contains all info read threads should need
// to access their respective data blocks
typedef struct global_state_struct
{
   char *objID;
   DAL_location location;
   DAL_MODE dmode;
   DAL dal;
   off_t offset;
   meta_info minfo;
   char meta_error;
   char data_error;
   ioqueue *ioq;
   pthread_mutex_t* erasurelock;
} gthread_state;

// Write thread internal state struct
typedef struct thread_state_struct
{
   gthread_state *gstate;
   off_t offset;
   BLOCK_CTXT handle;
   ioblock *iob;
   uint64_t crcsumchk;
   char continuous;
} thread_state;

/**
 * Initialize the write thread state and create a DAL BLOCK_CTXT
 * @param unsigned int tID : The ID of this thread
 * @param void* global_state : Reference to a gthread_state struct
 * @param void** state : Reference to be populated with this thread's state info
 * @return int : Zero on success and -1 on failure
 */
int write_init(unsigned int tID, void *global_state, void **state);

/**
 * Initialize the read thread state and create a DAL BLOCK_CTXT
 * @param unsigned int tID : The ID of this thread
 * @param void* global_state : Reference to a gthread_state struct
 * @param void** state : Reference to be populated with this thread's state info
 * @return int : Zero on success and -1 on failure
 */
int read_init(unsigned int tID, void *global_state, void **state);

/**
 * Consume data buffers, generate CRCs for them, and write blocks out to their targets
 * @param void** state : Thread state reference
 * @param void** work_todo : Reference to the data buffer / work package
 * @return int : Integer return code ( -1 on failure, 0 on success )
 */
int write_consume(void **state, void **work_todo);

/**
 * Read data from our target, verify its CRC, and continue until we have a full buffer to push
 * @param void** state : Thread state reference
 * @param void** work_tofill : Reference to be populated with the produced buffer
 * @return int : Integer return code ( -1 on error, 0 on success, and 2 once all buffers have been read )
 */
int read_produce(void **state, void **work_tofill);

/**
 * No-op function, just to fill out the TQ struct
 */
int write_pause(void **state, void **prev_work);

/**
 * No-op function, just to fill out the TQ struct
 */
int read_pause(void **state, void **prev_work);

/**
 * No-op function, just to fill out the TQ struct
 */
int write_resume(void **state, void **prev_work);

/**
 * Create an IOQueue (if not done already), and destory any work package we already produced (reseek possible)
 * @param void** state : Thread state reference
 * @param void** prev_work : Reference to any previously populated buffer
 * @return int : Integer return code ( -1 on error, 0 on success )
 */
int read_resume(void **state, void **prev_work);

/**
 * Write out our meta info and close our target reference
 * @param void** state : Thread state reference
 * @param void** prev_work : Reference to any unused previous buffer
 * @param TQ_Control_Flags flg : Control flags values at thread term
 */
void write_term(void **state, void **prev_work, TQ_Control_Flags flg);

/**
 * Close our target reference
 * @param void** state : Thread state reference
 * @param void** prev_work : Reference to any unused previous buffer
 * @param TQ_Control_Flags flg : Control flags values at thread term
 */
void read_term(void **state, void **prev_work, TQ_Control_Flags flg);

#ifdef __cplusplus
}
#endif

#endif
