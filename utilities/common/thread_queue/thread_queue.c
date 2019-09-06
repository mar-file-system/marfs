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
#include <stdio.h>
#include <error.h>
#include <pthread.h>


// TODO TMP -- move to thread_queue.h
typedef struct queue_init_struct {
   unsigned int   num_threads;      /* number of threads to initialize */
   unsigned int   max_qdepth;       /* maximum depth of the work queue */
   void*          global_state;     /* reference to some global initial state, passed to the init_thread state func of all threads */
   void* (*thread_init_state) (int tID, void* global_state); /* function pointer defining initilization behavior for each thread
                                                              - This function will be run by only a single thread at a time (non-parallel)
                                                                  Each thread completes a call to thread_init_state before the next thread starts
                                                              - The first argument is an integer ID for the calling thread (0 to (num_threads-1))
                                                              - The second argument is a copy of the global_state pointer for each thread
                                                              - The return from this func will be passed as 'state' to the thread_work_func 
                                                              - A NULL return value will cause all threads to ABORT and tq_init() to fail */
   int   (*thread_work_func)  (void* state, void* work);     /* function pointer defining behavior of a thread for each work package
                                                              - This function may be run by multiple threads in parallel
                                                                  Beware of shared values in the 'state' argument
                                                              - A return value above zero will cause the calling thread to HALT the queue
                                                              - A return value below zero will cause the calling thread to ABORT the queue
                                                              - A return value of zero will be ignored */
} TQ_Init_Opts;




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
   int tID;         /* unique integer ID for this thread */
} ThreadArg;



/* -------------------------------------------------------  INTERNAL FUNCTIONS  ------------------------------------------------------- */

// set a TQ control signal and wake all threads
int tq_signal(ThreadQueue* tq, TQ_Control_Flags sig) {
   if ( pthread_mutex_lock(&tq->qlock) ) { return -1; }
   tq->con_flags |= sig;
   // wake ALL threads
   pthread_cond_broadcast(&tq->thread_resume);
   pthread_mutex_unlock(&tq->qlock);  
}


// free all elements of a TQ that are allocated by tq_init()
void tq_free_all( ThreadQueue* tq ) {
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
   int tID = targ->tID;
   // 'targ' should never be referenced again by this thread, which will allow the init_tq() func to alter it out from underneath us

   // attempt initialization of state for this thread
   void* tstate = tq->thread_init_state( tID, tq->global_state );
   if ( pthread_mutex_lock( &tq->qlock ) ) {
      // failed to aquire the queue lock, terminate
      tq->state_flags[tID] |= TQ_ERROR;
      pthread_cond_signal( &tq->master_resume ); // safe with no lock? TODO
      pthread_exit( tstate );
   }
   if ( tstate == NULL ) { 
      // failed to initialize thread state
      tq->state_flags[tID] |= TQ_ERROR;
      tq->con_flags |= TQ_ABORT;
      pthread_cond_signal( &tq->master_resume );
      pthread_mutex_unlock( &tq->qlock );
      pthread_exit( tstate );
   }
   // otherwise, let the master know we properly initialized, we'll also signal and block on the first loop iteration
   tq->state_flags[tID] &= TQ_READY;
   // NOTE: tq_init() can only be called by one proc; however, beyond this point, multiple producer procs may be running

   // begin main loop
   while ( true ) {
      // This thread should always be holding the queue lock here
      // check for available work or any finished/halt/abort conditions
      while ( ( tq->qdepth == 0  ||  (tq->con_flags & TQ_HALT) )
            &&  !((tq->con_flags & TQ_FINISHED) || (tq->con_flags & TQ_ABORT)) ) {
         // note the halted state if we were asked to pause
         if ( tq->con_flags & TQ_HALT ) {
            tq->state_flags |= TQ_HALTED;
            // the producer proc(s) could have been waiting for us to halt, so we must signal
            pthread_cond_broadcast(&tq->master_resume);
         }
         // whatever the reason, we're in a holding pattern until the queue/signal states change
         pthread_cond_wait( &tq->thread_resume, &tq->qlock );
         // if we were halted, make sure we immediately indicate that we aren't any more
         if ( tq->state_flags & TQ_HALTED ) {
            tq->state_flags &= ~(TQ_HALTED);
         }
      } // end of holding pattern -- this thread has some action to take

      // First, check if we should be quitting
      if( (tq->con_flags & TQ_ABORT)  ||  ( (tq->qdepth == 0) && (tq->con_flags & TQ_FINISHED) ) ) {
         pthread_mutex_unlock(&tq->qlock);
         break;
      }

      // If not, then we should have work to do...
      void* cur_work = tq->workpkg[ tq->head ];     // get a pointer to our work pkg
      tq->workpkg[ tq->head ] = NULL;               // clear that queue entry
      tq->head = ( tq->head + 1 ) % tq->max_qdepth; // adjust the head position to the next work pkg
      tq->qdepth--;                                 // finally, decrement the queue depth
      // if the queue was full, tell the master proc it can now resume
      if ( tq->qdepth == (tq->max_qdepth - 1) ) { pthread_cond_signal( &tq->master_resume ); }
      pthread_mutex_unlock( &tq->qlock );

      // Process our new work pkg
      int work_res = tq->thread_work_func( tstate, cur_work );
      
      // Reacquire the queue lock before setting flags and going into our next iteration
      if ( pthread_mutex_lock( &tq->qlock ) ) {
         // failed to aquire the queue lock, terminate
         tq->state_flags[tID] |= TQ_ERROR;
         tq->con_flags |= TQ_ABORT; // this is an extrordinary case; try to set the ABORT flag even without a lock
         pthread_cond_broadcast( &tq->master_resume ); // safe with no lock? TODO
         break;
      }
      if ( work_res > 0 ) { tq->con_flags |= TQ_HALT; pthread_condition_broadcast( &tq->thread_resume ); }
      else if ( work_res < 0 ) { tq->con_flags |= TQ_ABORT; pthread_condition_broadcast( &tq->thread_resume ); tq->state_flags[tID] |= TQ_ERROR; }
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
   tq->con_flags &= (~TQ_HALT);
   // wake ALL threads
   pthread_cond_broadcast(&tq->thread_resume);
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}


/**
 * Checks if the HALT flag is set for a given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue to check
 * @return bool : true if the HALT flag is set, and false if not
 */
bool tq_halt_set( ThreadQueue* tq ) {
   if ! ( tq->con_flags & TQ_HALT ) { return false; }
   return true;
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
 * @return bool : true if the ABORT flag is set, and false if not
 */
bool tq_abort_set( ThreadQueue* tq ) {
   if ( tq->con_flags & TQ_ABORT ) { return true; }
   return false;
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
      pthread_cond_wait( &tq->master_resume, &tq->qlock );
   }
   // check for any oddball conditions which would prevent this work from completing
   if ( tq->con_flags & (TQ_FINISHED & TQ_ABORT) ) { pthread_mutex_unlock( &tq->qlock ); errno = EINVAL; return -1; }

   // insert the new work at the tail of the queue
   tq->workpkg[ tq->tail ] = workbuff;          // insert the workbuff into the queue
   tq->tail = (tq->tail + 1 ) % tq->max_qdepth; // move the tail to the next slot
   tq->qdepth++;                                // finally, increment the queue depth
   // if the queue was empty, tell a thread that it can now resume
   if ( tq->qdepth == 1 ) { pthread_cond_signal( &tq->thread_resume ); }
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
   int tID = tq->num_threads - tq->threads_running; // get thread states in the order they were started
   if ( tID < tq->num_threads ) { // if no threads are running, set state to NULL
      int ret = pthread_join( tq->threads[tID], tstate );
      if ( ret ) { return -1; } // indicate a failure if we couldn't join
      tq->threads_running--;
      if ( tq->state_flags[tID] & TQ_ERROR ) { return 1; } // indicate a thread error occurred
   }
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
   if ( pthread_mutex_init( &tq->qlock, NULL ) ) { free( tq ); return NULL; }
   if ( pthread_cond_init( &tq->thread_resume, NULL ) ) { free( tq ); return NULL; }
   if ( pthread_cond_init( &tq->master_resume, NULL ) ) { free( tq ); return NULL; }

   // initialize fields requiring memory allocation
   if ( (tq->state_flags = calloc( sizeof(TQ_State_Flags), opts.num_threads )) == NULL ) { free(tq); return NULL; }
   if ( (tq->workpkg = malloc( sizeof(void*) * opts.max_qdepth )) == NULL ) { free(tq->state_flags); free(tq); return NULL; }
   int i;
   for ( i = 0; i < opts.max_qdepth; i++ ) {
      tq->workpkg[i] = NULL;
   }
   if ( (tq->threads = malloc( sizeof(pthread_t*) * opts.num_threads )) == NULL ) { free(tq->state_flags); free(tq->workpkg); free(tq); return NULL; }

   // create all threads
   ThreadArg targ;
   targ.tID = 0;
   targ.tq = tq;
   if ( pthread_mutext_lock( &tq->qlock ) ) { i = opts.num_threads + 1; } // if we fail to get the lock, this is an easy way to cleanup
   for ( i = 0; i < opts.num_threads; i++ ) {
      // create each thread
      if ( pthread_create( &tq->threads[i], NULL, worker_thread, (void*) &targ ) ) { break; }
      tq->threads_running++;

      // wait for thread to initialize (we're still holding the lock through this entire loop; not too efficient but very conventient)
      while ( !(tq->state_flags[i] & (TQ_READY & TQ_ERROR)) ) { // wait for the thread to succeed or fail
         pthread_cond_wait( &tq->master_resume, &tq->qlock ); // since TQ_HALT is set, we will be notified when the thread blocks
      }
      if ( tq->state_flags[i] & TQ_ERROR ) { break; }  // if we've hit an error, start terminating threads
      pthread_mutex_unlock( &tq->qlock );
      
      // we await initialization of each thread before altering this struct and lock around passing it in, so it should be safe
      targ.tID = i + 1;
      targ.tq = tq;

      pthread_mutex_lock( &tq->qlock ); //assuming success of acquiring the lock after the first time
   }
   if ( i != opts.num_threads ) { // an error occured while creating threads
      tq->con_flags |= TQ_ABORT; // signal all threads to abort (still holding the lock)
      pthread_cond_broadcast( &tq->thread_resume );
      pthread_mutex_unlock( &tq->qlock );
      for ( i = 0; i < tq->threads_running; i++ ) {
         pthread_join( tq->threads[i], NULL ); // just ignore thread status, we are already aborting
      }
      // free everything we allocated and terminate
      tq_free_all( tq );
      return NULL;
   }

   // last step is to remove the TQ_HALT flag (still holding the lock, all threads should be paused)
   tq->con_flags &= (~TQ_HALT);
   // As there currently isn't any work to do, we don't want to bother signaling all threads just to have them block again.
   //  Therefore, we'll sneak in and unset the flag without signalling, meaning they'll only wake up when work is enqueued.
   pthread_mutex_unlock( &tq->qlock );

   return tq;
}


