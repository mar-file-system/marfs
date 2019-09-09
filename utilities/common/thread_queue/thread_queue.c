/*
This file is part of MarFS, which is released under the BSD license.


Copyright (c) 2015, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANS, LLC added functionality to the original work. The original work plus
LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at <http://www.gnu.org/licenses/>.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2015, Los Alamos National Security, LLC All rights reserved.
Copyright 2015. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

#include "thread_queue.h"
#include "logging.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>



/* -------------------------------------------------------  INTERNAL TYPES  ------------------------------------------------------- */
typedef enum {
   TQ_FINISHED = 0x01 << 0, // signals to threads that all work has been issued and/or completed
   TQ_ABORT    = 0x01 << 1, // signals to threads that an unrecoverable errror requires early termination
   TQ_HALT     = 0x01 << 2  // signals to threads that work should be paused
} TQ_Control_Flags;

typedef enum {
   TQ_READY    = 0x01 << 0, // indicates that this thread has successfully initialized and is ready for work
   TQ_ERROR    = 0x01 << 1, // indicates that this thread encountered an unrecoverable error
   TQ_HALTED   = 0x01 << 2  // indicates that this thread is 'paused'
} TQ_State_Flags;

typedef struct thread_queue_struct {
   // Queue Mechanisms
   pthread_mutex_t    qlock;            /* per-queue lock to prevent simultaneous access */
   void**             workpkg;          /* array of buffers for passing data on the queue */
   unsigned int       qdepth;           /* number of elements in the queue */
   unsigned int       max_qdepth;       /* maximum number of elements in the queue */
   int                head;             /* next full position */  
   int                tail;             /* next empty position */

   // Signaling Mechanisms
   pthread_cond_t     thread_resume;    /* cv signals there is a full slot */
   pthread_cond_t     master_resume;    /* cv signals there is an empty slot */
   TQ_Control_Flags   con_flags;        /* meant for sending thread commands */
   TQ_State_Flags*    state_flags;      /* meant for signaling thread response to commands */

   // Thread Definitions
   unsigned int       num_threads;      /* number of threads associated with this queue */
   unsigned int       threads_running;  /* number of threads that have initialized and not yet returned state info */
   pthread_t*         threads;          /* thread instances */
   void*              global_state;     /* reference to some initial state shared by all threads */
   void* (*thread_init_state) (int tID, void* global_state); /* function pointer defining the initialization behavior of threads */
   int   (*thread_work_func)  (void* state, void* work);     /* function pointer defining behavior of a thread for each work package */
} ThreadQueue;

typedef struct thread_arg_struct {
   ThreadQueue* tq; /* thread queue for this set of threads */
   unsigned int tID;         /* unique integer ID for this thread */
} ThreadArg;



/* -------------------------------------------------------  INTERNAL FUNCTIONS  ------------------------------------------------------- */

// set a TQ control signal and wake all threads
int tq_signal(ThreadQueue* tq, TQ_Control_Flags sig) {
   LOG( LOG_INFO, "Signalling all threads of thread_queue with %s\n", 
                  (sig == TQ_FINISHED) ? "TQ_FINISHED" : ( (sig == TQ_ABORT) ? "TQ_ABORT" : ( (sig == TQ_HALT) ? "TQ_HALT" : "UNKNOWN_SIGNAL!" ) ) );
   if ( pthread_mutex_lock(&tq->qlock) ) { return -1; }
   tq->con_flags |= sig;
   // wake ALL threads
   pthread_cond_broadcast(&tq->thread_resume);
   pthread_mutex_unlock(&tq->qlock);  
   return 0;
}


// free all elements of a TQ that are allocated by tq_init()
void tq_free_all( ThreadQueue* tq ) {
   LOG( LOG_INFO, "Freeing all thread_queue allocations\n" );
   pthread_mutex_destroy( &tq->qlock );
   pthread_cond_destroy( &tq->thread_resume );
   pthread_cond_destroy( &tq->master_resume );
   free(tq->threads);
   free(tq->state_flags);
   free(tq->workpkg);
   free(tq);
}


// defines behavior for all worker threads
void* worker_thread( void* arg ) {
   ThreadArg*   targ = (ThreadArg*) arg;
   ThreadQueue* tq = targ->tq;
   unsigned int tID = targ->tID;
   // 'targ' should never be referenced again by this thread, which will allow the init_tq() func to alter it out from underneath us

   // attempt initialization of state for this thread
   void* tstate = tq->thread_init_state( tID, tq->global_state );
   if ( pthread_mutex_lock( &tq->qlock ) ) {
      // failed to aquire the queue lock, terminate
      LOG( LOG_ERR, "Thread [%u]: Failed to acquire queue lock!\n", tID );
      tq->state_flags[tID] |= TQ_ERROR;
      pthread_cond_signal( &tq->master_resume ); // safe with no lock? TODO
      pthread_exit( tstate );
   }
   if ( tstate == NULL ) { 
      // failed to initialize thread state
      LOG( LOG_ERR, "Thread [%u]: Failed to initialize thread state!\n", tID );
      tq->state_flags[tID] |= TQ_ERROR;
      tq->con_flags |= TQ_ABORT;
      pthread_cond_signal( &tq->master_resume );
      pthread_mutex_unlock( &tq->qlock );
      pthread_exit( tstate );
   }
   // otherwise, let the master know we properly initialized, we'll also signal and block on the first loop iteration
   tq->state_flags[tID] &= TQ_READY;
   LOG( LOG_INFO, "Thread [%u]: Successfully initialized\n", tID );
   // NOTE: tq_init() can only be called by one proc; however, beyond this point, multiple producer procs may be running

   // begin main loop
   while ( 1 ) {
      // This thread should always be holding the queue lock here
      // check for available work or any finished/halt/abort conditions
      while ( ( tq->qdepth == 0  ||  (tq->con_flags & TQ_HALT) )
            &&  !((tq->con_flags & TQ_FINISHED) || (tq->con_flags & TQ_ABORT)) ) {
         // note the halted state if we were asked to pause
         if ( tq->con_flags & TQ_HALT ) {
            LOG( LOG_INFO, "Thread [%u]: Entering HALTED state\n", tID );
            tq->state_flags[tID] |= TQ_HALTED;
            // the producer proc(s) could have been waiting for us to halt, so we must signal
            pthread_cond_broadcast(&tq->master_resume);
         }
         // whatever the reason, we're in a holding pattern until the queue/signal states change
         LOG( LOG_INFO, "Thread [%u]: Sleeping pending wakup signal\n", tID );
         pthread_cond_wait( &tq->thread_resume, &tq->qlock );
         LOG( LOG_INFO, "Thread [%u]: Has awakened and is holding lock\n", tID );
         // if we were halted, make sure we immediately indicate that we aren't any more
         if ( tq->state_flags[tID] & TQ_HALTED ) {
            LOG( LOG_INFO, "Thread [%u]: Clearing HALTED state\n", tID );
            tq->state_flags[tID] &= ~(TQ_HALTED);
         }
      } // end of holding pattern -- this thread has some action to take

      // First, check if we should be quitting
      if( (tq->con_flags & TQ_ABORT)  ||  ( (tq->qdepth == 0) && (tq->con_flags & TQ_FINISHED) ) ) {
         LOG( LOG_INFO, "Thread [%u]: Terminating now\n", tID );
         pthread_mutex_unlock(&tq->qlock);
         break;
      }

      // If not, then we should have work to do...
      void* cur_work = tq->workpkg[ tq->head ];     // get a pointer to our work pkg
      tq->workpkg[ tq->head ] = NULL;               // clear that queue entry
      LOG( LOG_INFO, "Thread [%u]: Retrieved work package from queue position %d\n", tID, tq->head );
      tq->head = ( tq->head + 1 ) % tq->max_qdepth; // adjust the head position to the next work pkg
      tq->qdepth--;                                 // finally, decrement the queue depth
      // if the queue was full, tell the master proc it can now resume
      if ( tq->qdepth == (tq->max_qdepth - 1) ) { pthread_cond_signal( &tq->master_resume ); }
      pthread_mutex_unlock( &tq->qlock );

      // Process our new work pkg
      int work_res = tq->thread_work_func( tstate, cur_work );
      LOG( LOG_INFO, "Thread [%u]: Processed work package\n", tID );
      
      // Reacquire the queue lock before setting flags and going into our next iteration
      if ( pthread_mutex_lock( &tq->qlock ) ) {
         // failed to aquire the queue lock, terminate
         LOG( LOG_ERR, "Thread [%u]: Failed to acquire queue lock within main loop!\n", tID );
         tq->state_flags[tID] |= TQ_ERROR;
         tq->con_flags |= TQ_ABORT; // this is an extrordinary case; try to set the ABORT flag even without a lock
         pthread_cond_broadcast( &tq->master_resume ); // safe with no lock? TODO
         break;
      }
      if ( work_res > 0 ) {
         LOG( LOG_INFO, "Thread [%u]: Setting HALT state due to work processing result\n", tID );
         tq->con_flags |= TQ_HALT;
         pthread_cond_broadcast( &tq->thread_resume );
      }
      else if ( work_res < 0 ) {
         LOG( LOG_ERR, "Thread [%u]: Setting ABORT state due to work processing result\n", tID );
         tq->con_flags |= TQ_ABORT;
         pthread_cond_broadcast( &tq->thread_resume );
         tq->state_flags[tID] |= TQ_ERROR;
      }
   }
   // end of main loop

   pthread_exit( tstate );
}




/* -------------------------------------------------------  EXPOSED FUNCTIONS  ------------------------------------------------------- */

/**
 * Sets the FINISHED state for a given ThreadQueue, allowing thread status info to be collected
 * @param ThreadQueue* tq : ThreadQueue to mark as FINISHED
 * @return int : Zero on success and non-zero on failure
 */
int tq_work_done(ThreadQueue *tq) {
   return tq_signal(tq, TQ_FINISHED);
}


/**
 * Sets a HALT state for the given ThreadQueue and waits for all threads to pause
 * @param ThreadQueue* tq : ThreadQueue to pause
 * @return int : Zero on success and non-zero on failure
 */
int tq_halt( ThreadQueue* tq ) {
   // TODO wait for halt on all threads
   return tq_signal(tq, TQ_HALT);
}


/**
 * Unsets the HALT state for a given ThreadQueue and signals all threads to resume work
 * @param ThreadQueue* tq : ThreadQueue for which to unset the HALT state
 * @return int : Zero on success and non-zero on failure
 */
int tq_resume( ThreadQueue* tq ) {
   if ( pthread_mutex_lock(&tq->qlock) ) { return -1; }
   LOG( LOG_INFO, "Clearing HALT state\n" );
   tq->con_flags &= (~TQ_HALT);
   // wake ALL threads
   pthread_cond_broadcast(&tq->thread_resume);
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}


/**
 * Checks if the HALT flag is set for a given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue to check
 * @return char : 1 if the HALT flag is set, and 0 if not
 */
char tq_halt_set( ThreadQueue* tq ) {
   if ( !(tq->con_flags & TQ_HALT) ) { return 0; }
   return 1;
}


/**
 * Sets an ABORT state for the given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue for which to set the ABORT
 * @return int : Zero on success and non-zero on failure
 */
int tq_abort(ThreadQueue *tq) {
  return tq_signal(tq, TQ_ABORT);
}


/**
 * Checks if the ABORT flag is set for a given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue to check
 * @return char : 1 if the ABORT flag is set, and 0 if not
 */
char tq_abort_set( ThreadQueue* tq ) {
   if ( tq->con_flags & TQ_ABORT ) { return 1; }
   return 0;
}


/**
 * Insert a new element of work into the ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue in which to insert work
 * @param void* workbuff : New element of work to be inserted
 * @return int : Zero on success and -1 on failure (such as, if the queue is FINISHED or ABORTED)
 */
int tq_enqueue( ThreadQueue* tq, void* workbuff ) {
   if ( pthread_mutex_lock( &tq->qlock ) ) { return -1; }

   // wait for an opening in the queue or for work to be canceled
   while ( ( tq->qdepth == tq->max_qdepth )  &&  !(tq->con_flags & (TQ_FINISHED & TQ_ABORT)) ) {
      LOG( LOG_INFO, "Master proc is waiting for an opening to enqueue into\n" );
      pthread_cond_wait( &tq->master_resume, &tq->qlock );
      LOG( LOG_INFO, "Master proc has woken up\n" );
   }
   // check for any oddball conditions which would prevent this work from completing
   if ( tq->con_flags & (TQ_FINISHED & TQ_ABORT) ) {
      LOG( LOG_ERR, "Master proc aborting tq_enqueue() to FINISHED/ABORTED queue!\n" );
      pthread_mutex_unlock( &tq->qlock );
      errno = EINVAL;
      return -1;
   }

   // insert the new work at the tail of the queue
   tq->workpkg[ tq->tail ] = workbuff;          // insert the workbuff into the queue
   tq->tail = (tq->tail + 1 ) % tq->max_qdepth; // move the tail to the next slot
   tq->qdepth++;                                // finally, increment the queue depth
   // if the queue was empty, tell a thread that it can now resume
   if ( tq->qdepth == 1 ) { pthread_cond_signal( &tq->thread_resume ); }
   LOG( LOG_INFO, "Master proc has successfully enqueued work\n" );
   pthread_mutex_unlock( &tq->qlock );

   return 0;
}


/**
 * Returns the status info for the next uncollected thread in a FINISHED or ABORTED ThreadQueue
 *  If no uncollected threads remain in the ThreadQueue, the status info will be set to NULL
 * @param ThreadQueue* tq : ThreadQueue from which to collect status info
 * @param void** tstate : Reference to be populated with thread status info
 * @return int : Zero on success, -1 on failure, and 1 if the collected thread forced an ABORT of this ThreadQueue
 */
int tq_next_thread_status( ThreadQueue* tq, void** tstate ) {
   // make sure the queue has been marked FINISHED/ABORTED
   if ( !( tq->con_flags & (TQ_FINISHED & TQ_ABORT) ) ) { errno = EINVAL; return -1; }
   *tstate = NULL;
   unsigned int tID = tq->num_threads - tq->threads_running; // get thread states in the order they were started
   if ( tID < tq->num_threads ) { // if no threads are running, set state to NULL
      LOG( LOG_INFO, "Attempting to join thread %u\n", tID );
      int ret = pthread_join( tq->threads[tID], tstate );
      if ( ret ) { // indicate a failure if we couldn't join
         LOG( LOG_ERR, "Failed to join thread %u!\n", tID );
         return -1;
      }
      tq->threads_running--;
      LOG( LOG_INFO, "Successfully joined thread %u (%u threads remain)\n", tID, tq->threads_running );
      if ( tq->state_flags[tID] & TQ_ERROR ) { // indicate a thread error occurred
         LOG( LOG_INFO, "Noting that thread %u issued a queue ABORT\n", tID );
         return 1;
      }
   }
   else { LOG( LOG_INFO, "All threads have terminated, returning NULL\n" ); }
   return 0;
}


/**
 * Closes a FINISHED or ABORTED ThreadQueue for which all thread status info has already been collected
 * @param ThreadQueue* tq : ThreadQueue to be closed
 * @return int : Zero on success and -1 on failure
 */
int tq_close( ThreadQueue* tq ) {
   // make sure the queue has been marked FINISHED/ABORTED and that all thread states have been collected
   if ( !( tq->con_flags & (TQ_FINISHED & TQ_ABORT))  ||  tq->threads_running ) { errno = EINVAL; return -1; }
//TODO should perhaps check if work packages remain on the queue and be capable of passing those back
   // free everything and terminate
   tq_free_all(tq);
   return 0;
}


/**
 * Initializes a new ThreadQueue according to the parameters of the passed options struct
 * @param TQ_Init_Opts opts : options struct defining parameters for the created ThreadQueue
 * @return ThreadQueue* : pointer to the created ThreadQueue, or NULL if an error was encountered
 */
ThreadQueue* tq_init( TQ_Init_Opts opts ) {
   LOG( LOG_INFO, "Initializing ThreadQueue with params: Num_threads=%u, Max_qdepth=%u\n", opts.num_threads, opts.max_qdepth );
   // allocate space for a new thread_queue_struct
   ThreadQueue* tq = malloc( sizeof( struct thread_queue_struct ) );
   if ( tq == NULL ) { return NULL; }

   // initialize all fields we received from the caller
   tq->num_threads       = opts.num_threads;
   tq->max_qdepth        = opts.max_qdepth;
   tq->global_state      = opts.global_state;
   tq->thread_init_state = opts.thread_init_state;
   tq->thread_work_func  = opts.thread_work_func;

   // initialize basic queue vars
   tq->qdepth = 0;
   tq->head   = 0;
   tq->tail   = 0;

   // initialize control flags in a HALTED state
   tq->con_flags = TQ_HALT; // this will allow us to wait on the master_resume condition below

   // initialize basic thread vars
   tq->threads_running = 0;

   // initialize pthread control structures
   LOG( LOG_INFO, "Allocating ThreadQueue memory refs\n" );
   if ( pthread_mutex_init( &tq->qlock, NULL ) ) { free( tq ); return NULL; }
   if ( pthread_cond_init( &tq->thread_resume, NULL ) ) { free( tq ); return NULL; }
   if ( pthread_cond_init( &tq->master_resume, NULL ) ) { free( tq ); return NULL; }

   // initialize fields requiring memory allocation
   if ( (tq->state_flags = calloc( sizeof(TQ_State_Flags), opts.num_threads )) == NULL ) { free(tq); return NULL; }
   if ( (tq->workpkg = malloc( sizeof(void*) * opts.max_qdepth )) == NULL ) { free(tq->state_flags); free(tq); return NULL; }
   unsigned int i;
   for ( i = 0; i < opts.max_qdepth; i++ ) {
      tq->workpkg[i] = NULL;
   }
   if ( (tq->threads = malloc( sizeof(pthread_t*) * opts.num_threads )) == NULL ) { free(tq->state_flags); free(tq->workpkg); free(tq); return NULL; }

   // create all threads
   ThreadArg targ;
   targ.tID = 0;
   targ.tq = tq;
   if ( pthread_mutex_lock( &tq->qlock ) ) { i = opts.num_threads + 1; } // if we fail to get the lock, this is an easy way to cleanup
   for ( i = 0; i < opts.num_threads; i++ ) {
      // create each thread
      LOG( LOG_INFO, "Starting thread %u\n", targ.tID );
      if ( pthread_create( &tq->threads[i], NULL, worker_thread, (void*) &targ ) ) { LOG( LOG_ERR, "Failed to create thread %d\n", i ); break; }
      tq->threads_running++;

      // wait for thread to initialize (we're still holding the lock through this entire loop; not too efficient but very conventient)
      while ( !(tq->state_flags[i] & (TQ_READY & TQ_ERROR)) ) { // wait for the thread to succeed or fail
         LOG( LOG_INFO, "Waiting for thread %u to initialize\n", targ.tID );
         pthread_cond_wait( &tq->master_resume, &tq->qlock ); // since TQ_HALT is set, we will be notified when the thread blocks
      }
      if ( tq->state_flags[i] & TQ_ERROR ) {  // if we've hit an error, start terminating threads
         LOG( LOG_ERR, "Thread %u indicates init failed!\n", targ.tID );
         break;
      }
      LOG( LOG_INFO, "Thread %u initialized successfully\n", targ.tID );
      pthread_mutex_unlock( &tq->qlock );
      
      // we await initialization of each thread before altering this struct and lock around passing it in, so it should be safe
      targ.tID = i + 1;
      targ.tq = tq;

      pthread_mutex_lock( &tq->qlock ); //assuming success of acquiring the lock after the first time
   }
   if ( i != opts.num_threads ) { // an error occured while creating threads
      LOG( LOG_ERR, "Failed to init all threads: signaling ABORT!\n" );
      tq->con_flags |= TQ_ABORT; // signal all threads to abort (still holding the lock)
      pthread_cond_broadcast( &tq->thread_resume );
      pthread_mutex_unlock( &tq->qlock );
      for ( i = 0; i < tq->threads_running; i++ ) {
         pthread_join( tq->threads[i], NULL ); // just ignore thread status, we are already aborting
         LOG( LOG_INFO, "Joined with thread %u\n", targ.tID );
      }
      // free everything we allocated and terminate
      tq_free_all( tq );
      return NULL;
   }

   // last step is to remove the TQ_HALT flag (still holding the lock, all threads should be paused)
   LOG( LOG_INFO, "All threads initialized successfully: Silently removing HALT flag\n", targ.tID );
   tq->con_flags &= (~TQ_HALT);
   // As there currently isn't any work to do, we don't want to bother signaling all threads just to have them block again.
   //  Therefore, we'll sneak in and unset the flag without signalling, meaning they'll only wake up when work is enqueued.
   pthread_mutex_unlock( &tq->qlock );

   return tq;
}


