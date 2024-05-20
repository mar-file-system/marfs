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

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_TQ
#define DEBUG DEBUG_TQ
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "thread_queue"
#include "logging/logging.h" //small C file defining MarFS logging format/funcs (it's either this, or link against MarFS)

#include "thread_queue.h"
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <errno.h>
#include <pthread.h>

#define def_queue_pref "ThreadQueue"

/* -------------------------------------------------------  INTERNAL TYPES  ------------------------------------------------------- */

typedef enum
{
   TQ_READY = 0x01 << 0, // indicates that this thread has successfully initialized and is ready for work
   TQ_ERROR = 0x01 << 1, // indicates that this thread encountered an unrecoverable error
   TQ_HALTED = 0x01 << 2 // indicates that this thread is 'paused'
} TQ_State_Flags;

typedef struct thread_queue_worker_pool_struct
{
   // Loggging name
   const char *pname;

   // Thread Definitions
   unsigned int start_tID; /* thread ID value of first thread in this pool ( range from this to this + num_thrds - 1 ) */
   unsigned int num_thrds; /* number of threads associated with this pool */
   unsigned int act_thrds; /* number of threads currently processing work */
   int (*thread_init_func)(unsigned int tID, void *global_state, void **state);
   /* function pointer defining the initialization behavior of threads */
   int (*thread_work_func)(void **state, void **work_tofill);
   /* function pointer defining the behavior of a thread for each work package */
   int (*thread_pause_func)(void **state, void **prev_work);
   /* function pointer defining the behavior of a thread just before entering a HALTED state */
   int (*thread_resume_func)(void **state, void **prev_work);
   /* function pointer defining the behavior of a thread just after exiting a HALTED state */
   void (*thread_term_func)(void **state, void **prev_work, TQ_Control_Flags flg);
   /* function pointer defining the termination behavior of threads */
} * TQWorkerPool;

typedef struct thread_queue_struct
{
   // Logging Prefix
   char *log_prefix;

   // Synchronization Mechanisms
   pthread_mutex_t qlock;          /* per-queue lock to prevent simultaneous access */
   TQ_Control_Flags con_flags;     /* meant for sending thread commands */
   TQ_State_Flags *state_flags;    /* meant for signaling thread response to commands */
   pthread_cond_t state_resume;    /* cv signals master proc to resume */
   pthread_cond_t consumer_resume; /* cv signals any consuming procs to resume */
   pthread_cond_t producer_resume; /* cv signals any producing procs to resume */

   // Queue Mechanisms
   void **workpkg;          /* array of buffers for passing data on the queue */
   unsigned int qdepth;     /* number of elements in the queue */
   unsigned int max_qdepth; /* maximum number of elements in the queue */
   int head;                /* next full position */
   int tail;                /* next empty position */

   // Thread Definitions
   unsigned int uncoll_thrds; /* number of threads that have initialized and not yet returned state info */
   pthread_t *threads;        /* thread instances */
   TQWorkerPool prod_pool;    /* reference to producer thread pool */
   TQWorkerPool cons_pool;    /* reference to consumer thread pool */
} * ThreadQueue;

typedef struct thread_arg_struct
{
   ThreadQueue tq;     /* thread queue for this set of threads */
   unsigned int tID;   /* unique integer ID for this thread */
   void *global_state; /* reference to some initial state shared by all threads */
} ThreadArg;

/* -------------------------------------------------------  INTERNAL FUNCTIONS  ------------------------------------------------------- */

// check that all threads in the given pool have terminated
// NOTE -- expectation is that queue lock is held throughout this func
char tq_threads_terminated(ThreadQueue tq, TQWorkerPool pool) {
   // Queue Lock is held at this point
   if ( pool == NULL ) { return 1; }
   unsigned int curID = pool->start_tID;
   for ( ; curID < (pool->start_tID + pool->num_thrds); curID++ ) {
      if ( tq->state_flags[curID] & TQ_READY ) { return 0; }
   }
   // no READY flags means all threads have terminated
   return 1;
}

// set a TQ control signal and wake all threads
int tq_signal(ThreadQueue tq, TQ_Control_Flags sig)
{
   LOG(LOG_INFO, "%s Signalling with %s\n", tq->log_prefix,
       (sig == TQ_FINISHED) ? "TQ_FINISHED" : ((sig == TQ_ABORT) ? "TQ_ABORT" : ((sig == TQ_HALT) ? "TQ_HALT" : "UNKNOWN_SIGNAL!")));
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s Failed to acquire queue lock!\n", tq->log_prefix);
      return -1;
   }
   tq->con_flags |= sig;
   // wake ALL threads
   pthread_cond_broadcast(&tq->consumer_resume);
   pthread_cond_broadcast(&tq->producer_resume);
   pthread_cond_broadcast(&tq->state_resume);
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

// free all elements of a TQ that are allocated by tq_init()
void tq_free_all(ThreadQueue tq)
{
   LOG(LOG_INFO, "%s Freeing all thread_queue allocations\n", tq->log_prefix);
   pthread_mutex_destroy(&tq->qlock);
   pthread_cond_destroy(&tq->state_resume);
   pthread_cond_destroy(&tq->consumer_resume);
   pthread_cond_destroy(&tq->producer_resume);
   if (tq->cons_pool != NULL)
   {
      free(tq->cons_pool);
   }
   if (tq->prod_pool != NULL)
   {
      free(tq->prod_pool);
   }
   free(tq->threads);
   free(tq->state_flags);
   free(tq->log_prefix);
   free(tq->workpkg);
   free(tq);
}

// call the thread_init_func (if supplied), attempt first lock acquizition, and set a READY state
int general_thread_init_behavior(ThreadQueue tq, TQWorkerPool wp, unsigned int tID, void *global_state, void **tstate)
{
   int init_res = 0;
   if (wp->thread_init_func != NULL)
   {
      init_res = wp->thread_init_func(tID, global_state, tstate);
   }
   if (pthread_mutex_lock(&tq->qlock))
   {
      // failed to acquire the queue lock, terminate
      LOG(LOG_ERR, "%s %s Thread[%u]: Failed to acquire queue lock!\n", tq->log_prefix, wp->pname, tID);
      void *dummyval = NULL;                            // dummy value to satisfy termination func
      wp->thread_term_func(tstate, &dummyval, TQ_NONE); // give the thread a chance to cleanup state
      tq->state_flags[tID] |= TQ_ERROR;
      pthread_cond_signal(&tq->state_resume); // should be safe with no lock (no choice anyway)
      return -1;
   }
   if (init_res != 0)
   {
      // failed to initialize thread state
      LOG(LOG_ERR, "%s %s Thread[%u]: Failed to initialize thread state!\n", tq->log_prefix, wp->pname, tID);
      tq->state_flags[tID] |= TQ_ERROR;
      tq->con_flags |= TQ_ABORT;                    // holding the queue lock, so this should be safe
      pthread_cond_broadcast(&tq->producer_resume); // so other threads check for the ABORT signal
      pthread_cond_broadcast(&tq->consumer_resume); // so other threads check for the ABORT signal
      pthread_cond_signal(&tq->state_resume);       // so our state gets rechecked
      pthread_mutex_unlock(&tq->qlock);             // drop the lock
      return -1;
   }
   // otherwise, let the master know we properly initialized, we'll also signal and block on the first loop iteration
   tq->state_flags[tID] |= TQ_READY;
   LOG(LOG_INFO, "%s %s Thread[%u]: Successfully initialized\n", tq->log_prefix, wp->pname, tID);
   pthread_cond_signal(&tq->state_resume); // master proc may be waiting for us to initialize
   // NOTE: tq_init() can only be called by one proc; however, beyond this point, multiple 'master' threads may be running
   return 0; // still holding lock
}

// call the thread_pause_func (if supplied), set the HALTED state, and signal any master threads to resume
int general_thread_pause_behavior(ThreadQueue tq, TQWorkerPool wp, unsigned int tID, void **tstate, void **prev_work)
{
   // Queue lock MUST be held at this point
   if (tq->con_flags & TQ_HALT)
   {
      // if we have a thread_pause_func(), call it now
      if (wp->thread_pause_func != NULL)
      {
         if (wp->thread_pause_func(tstate, prev_work) != 0)
         {
            // pause func indicates we should abort
            LOG(LOG_ERR, "%s %s Thread[%u]: setting ABORT state due to pause func result\n", tq->log_prefix, wp->pname, tID);
            tq->con_flags |= TQ_ABORT;
            tq->state_flags[tID] |= TQ_ERROR;
            pthread_cond_broadcast(&tq->producer_resume);
            pthread_cond_broadcast(&tq->consumer_resume);
            pthread_cond_broadcast(&tq->state_resume);
            return -1; // still holding lock
         }
         LOG(LOG_INFO, "%s %s Thread[%u]: success of pause function\n", tq->log_prefix, wp->pname, tID);
      }
      LOG(LOG_INFO, "%s %s Thread[%u]: Entering HALTED state\n", tq->log_prefix, wp->pname, tID);
      tq->state_flags[tID] |= TQ_HALTED;
      // the master proc(s) could have been waiting for us to halt, so we must signal
      pthread_cond_broadcast(&tq->state_resume);
   }
   // whatever the reason, we're in a holding pattern until the queue/signal states change
   LOG(LOG_INFO, "%s %s Thread[%u]: Sleeping pending wakup signal\n", tq->log_prefix, wp->pname, tID);
   wp->act_thrds--;
   return 0; // still holding lock
}

int general_thread_resume_behavior(ThreadQueue tq, TQWorkerPool wp, unsigned int tID, void **tstate, void **prev_work)
{
   // Queue lock MUST be held at this point
   wp->act_thrds++;
   LOG(LOG_INFO, "%s %s Thread[%u]: Awake and holding lock\n", tq->log_prefix, wp->pname, tID);

   // if we were halted, but the control flag is gone, make sure we immediately indicate that we aren't any more
   if ((tq->state_flags[tID] & TQ_HALTED) && !(tq->con_flags & TQ_HALT))
   {
      LOG(LOG_INFO, "%s %s Thread[%u]: Clearing HALTED state\n", tq->log_prefix, wp->pname, tID);
      tq->state_flags[tID] &= ~(TQ_HALTED);
      // if we have a thread_resume_func(), call it now
      if (wp->thread_resume_func != NULL)
      {
         if (wp->thread_resume_func(tstate, prev_work) != 0)
         {
            // resume func indicates we should abort
            LOG(LOG_ERR, "%s %s Thread[%u]: setting ABORT state due to resume func result\n", tq->log_prefix, wp->pname, tID);
            tq->con_flags |= TQ_ABORT;
            tq->state_flags[tID] |= TQ_ERROR;
            pthread_cond_broadcast(&tq->producer_resume);
            pthread_cond_broadcast(&tq->consumer_resume);
            pthread_cond_broadcast(&tq->state_resume);
            return -1; // still holding lock
         }
         LOG(LOG_INFO, "%s %s Thread[%u]: success of resume function\n", tq->log_prefix, wp->pname, tID);
      }
   }
   return 0; // still holding lock
}

int general_thread_post_work_behavior(ThreadQueue tq, TQWorkerPool wp, unsigned int tID, void **tstate, void **cur_work, int work_res)
{
   // Queue lock MUST NOT be held at this point
   // Reacquire the queue lock before setting flags and going into our next iteration
   if (pthread_mutex_lock(&tq->qlock))
   {
      // failed to aquire the queue lock, terminate
      LOG(LOG_ERR, "%s %s Thread[%u]: Failed to acquire queue lock within main loop!\n", tq->log_prefix, wp->pname, tID);
      // this is an extraordinary case; try to clean up as best we can, even without the lock
      tq->state_flags[tID] |= TQ_ERROR;
      tq->state_flags[tID] &= (~TQ_READY);
      tq->con_flags |= TQ_ABORT;
      wp->act_thrds--;
      wp->thread_term_func(tstate, cur_work, TQ_NONE); // give the thread a chance to clean up after itself
      pthread_cond_broadcast(&tq->producer_resume);
      pthread_cond_broadcast(&tq->consumer_resume);
      pthread_cond_broadcast(&tq->state_resume); // should be safe with no lock (also, not much choice)
      return -1;                                 // not holding lock
   }
   char setflags = 0;
   if (work_res < 0)
   {
      LOG(LOG_ERR, "%s %s Thread[%u]: Setting ABORT state\n", tq->log_prefix, wp->pname, tID);
      tq->con_flags |= TQ_ABORT;
      tq->state_flags[tID] |= TQ_ERROR;
      setflags = 1;
   }
   else if (work_res == 1)
   {
      if ( tq->con_flags & TQ_ABORT )
      {
         LOG(LOG_INFO, "%s %s Thread[%u]: SKIPPING seting FINISHED state due to presense of ABORT flag\n", tq->log_prefix, wp->pname, tID);
      }
      else
      {
         LOG(LOG_INFO, "%s %s Thread[%u]: Setting FINISHED state\n", tq->log_prefix, wp->pname, tID);
         tq->con_flags |= TQ_FINISHED;
         setflags = 1;
      }
   }
   else if (work_res > 1)
   {
      if ( tq->con_flags & TQ_ABORT  ||  tq->con_flags & TQ_FINISHED )
      {
         LOG(LOG_INFO, "%s %s Thread[%u]: SKIPPING setting HALT state due to existing FINISHED or ABORT flag(s)\n", tq->log_prefix, wp->pname, tID);
      }
      else
      {
         LOG(LOG_INFO, "%s %s Thread[%u]: Setting HALT state\n", tq->log_prefix, wp->pname, tID);
         tq->con_flags |= TQ_HALT;
         setflags = 1;
      }
   }
   if ( setflags )
   {
      pthread_cond_broadcast(&tq->producer_resume);
      pthread_cond_broadcast(&tq->consumer_resume);
      pthread_cond_broadcast(&tq->state_resume);
   }
   return 0; // still holding lock
}

void general_thread_term_behavior(ThreadQueue tq, TQWorkerPool wp, unsigned int tID, void **tstate, void **prev_work)
{
   // Queue lock MUST be held at this point
   LOG(LOG_INFO, "%s %s Thread[%u]: Terminating now\n", tq->log_prefix, wp->pname, tID);
   // unset the READY flag, to indicate that we are no longer processing work
   tq->state_flags[tID] &= (~TQ_READY);
   wp->act_thrds--; // likely won't matter at this point...
   TQ_Control_Flags flg = tq->con_flags;
   pthread_cond_broadcast(&tq->state_resume); // in case master proc(s) are waiting to join
   if ( tq->cons_pool  &&  wp == tq->prod_pool )
   {
      pthread_cond_broadcast(&tq->consumer_resume); // in case consumer threads are waiting for this prod to term
   }
   pthread_mutex_unlock(&tq->qlock);

   // call the termination function
   wp->thread_term_func(tstate, prev_work, flg);

   return; // not holding lock
}

// defines behavior for all consumer threads
void *consumer_thread(void *arg)
{
   ThreadArg *targ = (ThreadArg *)arg;
   ThreadQueue tq = targ->tq;
   TQWorkerPool wp = tq->cons_pool;
   unsigned int tID = targ->tID;
   void *global_state = targ->global_state;
   // 'targ' should never be referenced again by this thread, so we will free it now
   free( targ );

   // attempt initialization of state for this thread
   void *tstate = NULL;
   if (general_thread_init_behavior(tq, wp, tID, global_state, &tstate))
   { // non-zero return means failure to acquire lock or initialize
      pthread_exit(tstate);
   }

   // begin main loop
   void *cur_work = NULL;
   while (1)
   {
      // This thread should always be holding the queue lock here
      // Wait here while there is no work available or the queue is HALTED
      //  but NOT while the queue is FINISHED w/ no producers remaining OR ABORTed
      // NOTE -- For a FINISHED queue, consumers must wait for producers to terminate,
      //         as producers *may* still enqueue additional work.
      while ( (tq->qdepth == 0  ||  (tq->con_flags & TQ_HALT))  &&
             !(tq->con_flags & TQ_ABORT)  &&
             !((tq->con_flags & TQ_FINISHED)  &&  tq_threads_terminated(tq, tq->prod_pool)) )
      {

         if (general_thread_pause_behavior(tq, wp, tID, &tstate, &cur_work) < 0)
         {
            break;
         } // hit standard abort logic
         // if our queue is empty, make sure we have all producers running
         if (tq->qdepth == 0)
         {
            pthread_cond_broadcast(&tq->producer_resume);
         }
         pthread_cond_wait(&tq->consumer_resume, &tq->qlock);
         if (general_thread_resume_behavior(tq, wp, tID, &tstate, &cur_work) < 0)
         {
            break;
         } // hit standard abort logic

      } // end of holding pattern -- this thread has some action to take

      // First, check if we should be quitting
      if ((tq->con_flags & TQ_ABORT)  ||
         ( (tq->qdepth == 0)  &&  (tq->con_flags & TQ_FINISHED)  &&  tq_threads_terminated(tq, tq->prod_pool) ) )
      {
         break;
      }

      // If not, then we should have work to do...
      LOG(LOG_INFO, "%s %s Thread[%u]: Retrieving work package ( pos = %u, depth = %u )\n", tq->log_prefix, wp->pname, tID, tq->head, tq->qdepth);
      cur_work = tq->workpkg[tq->head];           // get a pointer to our work pkg
      tq->workpkg[tq->head] = NULL;               // clear that queue entry
      tq->head = (tq->head + 1) % tq->max_qdepth; // adjust the head position to the next work pkg
      tq->qdepth--;                               // finally, decrement the queue depth

      // if the number of empty queue positions exceeds the number of producers, tell a thread to resume
      if (tq->prod_pool != NULL)
      {
         if ((tq->max_qdepth - tq->qdepth) > tq->prod_pool->act_thrds && tq->prod_pool->act_thrds < tq->prod_pool->num_thrds)
         {
            LOG(LOG_INFO, "%s %s Thread[%u]: signaling %s thread ( running=%u, depth=%u )\n",
                tq->log_prefix, wp->pname, tID, tq->prod_pool->pname, tq->prod_pool->act_thrds, tq->qdepth);
            pthread_cond_signal(&tq->producer_resume);
         }
      }
      else if (tq->qdepth == (tq->max_qdepth - 1))
      { // no producer threads and the queue was full, signal
         LOG(LOG_INFO, "%s %s Thread[%u]: blindly signaling a producer\n", tq->log_prefix, wp->pname, tID);
         pthread_cond_signal(&tq->producer_resume);
      }
      pthread_mutex_unlock(&tq->qlock);

      // Process our new work pkg
      int work_res = wp->thread_work_func(&tstate, &cur_work);
      LOG(LOG_INFO, "%s %s Thread[%u]: Processed work package\n", tq->log_prefix, wp->pname, tID);
      cur_work = NULL; // clear this value to avoid confusion if we can't reacquire the lock
      // acquire lock and set queue flags based on work result
      if (general_thread_post_work_behavior(tq, wp, tID, &tstate, &cur_work, work_res))
      { // non-zero return means failure to acquire lock
         pthread_exit(tstate);
      }
   }
   // end of main loop (still holding lock)

   general_thread_term_behavior(tq, wp, tID, &tstate, &cur_work);
   pthread_exit(tstate);
}

// defines behavior for all producer threads
void *producer_thread(void *arg)
{
   ThreadArg *targ = (ThreadArg *)arg;
   ThreadQueue tq = targ->tq;
   TQWorkerPool wp = tq->prod_pool;
   unsigned int tID = targ->tID;
   void *global_state = targ->global_state;
   // 'targ' should never be referenced again by this thread, so we will free it now
   free( targ );

   // attempt initialization of state for this thread
   void *tstate = NULL;
   if (general_thread_init_behavior(tq, wp, tID, global_state, &tstate))
   { // non-zero return means failure to acquire lock or initialize
      pthread_exit(tstate);
   }

   // define pointer for current work package
   void *cur_work = NULL;

   // special check for HALT flag before producing first work package
   while ((tq->con_flags & TQ_HALT) && !((tq->con_flags & TQ_ABORT) || (tq->con_flags & TQ_FINISHED)))
   {
      if (general_thread_pause_behavior(tq, wp, tID, &tstate, &cur_work) < 0)
      {
         break;
      } // hit standard abort logic
      pthread_cond_wait(&tq->producer_resume, &tq->qlock);
      if (general_thread_resume_behavior(tq, wp, tID, &tstate, &cur_work) < 0)
      {
         break;
      } // hit standard abort logic
   }

   pthread_mutex_unlock(&tq->qlock); // release the lock

   // begin main loop
   while (1)
   {
      // This thread should never be holding the queue lock here
      // Create our new work pkg
      int work_res = wp->thread_work_func(&tstate, &cur_work);
      LOG(LOG_INFO, "%s %s Thread[%u]: Generated work package\n", tq->log_prefix, wp->pname, tID);
      // acquire lock and set queue flags based on work result
      if (general_thread_post_work_behavior(tq, wp, tID, &tstate, &cur_work, work_res))
      { // non-zero return means failure to acquire lock
         pthread_exit(tstate);
      }

      // Wait while there is no space available OR while the queue is both halted and NOT FINISHED
      //  but never wait while the queue is ABORTed
      while ((tq->qdepth == tq->max_qdepth || ((tq->con_flags & TQ_HALT)  &&  !(tq->con_flags & TQ_FINISHED))) &&
             !(tq->con_flags & TQ_ABORT))
      {

         if (general_thread_pause_behavior(tq, wp, tID, &tstate, &cur_work) < 0)
         {
            break;
         } // hit standard abort logic
         // if our queue is full, make sure we have all consumers running
         if (tq->qdepth == tq->max_qdepth)
         {
            pthread_cond_broadcast(&tq->consumer_resume);
         }
         pthread_cond_wait(&tq->producer_resume, &tq->qlock);
         if (general_thread_resume_behavior(tq, wp, tID, &tstate, &cur_work) < 0)
         {
            break;
         } // hit standard abort logic

      } // end of holding pattern -- this thread has some action to take

      // First, check if we should be aborting
      if (tq->con_flags & TQ_ABORT)
      {
         break;
      }

      // check if we have a work package to enqueue
      if (cur_work != NULL)
      {
         // If we got this far, then we have space to enqueue...
         LOG(LOG_INFO, "%s %s Thread[%u]: Storing work package (pos = %d, depth = %d)\n", tq->log_prefix, wp->pname, tID, tq->tail, tq->qdepth);
         tq->workpkg[tq->tail] = cur_work;           // insert our work pkg at the tail
         tq->tail = (tq->tail + 1) % tq->max_qdepth; // adjust the tail position to the next slot
         tq->qdepth++;                               // finally, increment the queue depth

         // if the queue length exceeds the work being processed, tell a thread to resume
         if (tq->cons_pool != NULL)
         {
            if (tq->qdepth > tq->cons_pool->act_thrds && tq->cons_pool->act_thrds < tq->cons_pool->num_thrds)
            {
               LOG(LOG_INFO, "%s %s Thread[%u]: signaling %s Thread ( running=%u, depth=%u )\n",
                   tq->log_prefix, wp->pname, tID, tq->cons_pool->pname, tq->cons_pool->act_thrds, tq->qdepth);
               pthread_cond_signal(&tq->consumer_resume);
            }
         }
         else if (tq->qdepth == 1)
         { // no consumer threads and the queue was empty, signal
            LOG(LOG_INFO, "%s %s Thread[%u]: blindly signaling a consumer\n", tq->log_prefix, wp->pname, tID);
            pthread_cond_signal(&tq->consumer_resume);
         }

         cur_work = NULL; // clear this value to avoid confusion if we exit
      }

      // Now that we've enqueued our (potentially last) work package, check if we should be quitting
      if (tq->con_flags & TQ_FINISHED)
      {
         break;
      }

      pthread_mutex_unlock(&tq->qlock); // release the lock
   }
   // end of main loop (still holding lock)

   general_thread_term_behavior(tq, wp, tID, &tstate, &cur_work);
   pthread_exit(tstate);
}

/* -------------------------------------------------------  EXPOSED FUNCTIONS  ------------------------------------------------------- */

/**
 * Initializes a new ThreadQueue according to the parameters of the passed options struct
 * @param TQ_Init_Opts opts : options struct defining parameters for the created ThreadQueue
 * @return ThreadQueue : pointer to the created ThreadQueue, or NULL if an error was encountered
 */
ThreadQueue tq_init(TQ_Init_Opts *opts)
{
   // allocate space for a new thread_queue_struct
   ThreadQueue tq = malloc(sizeof(struct thread_queue_struct));
   if (tq == NULL)
   {
      LOG(LOG_ERR, "failed to allocate space for ThreadQueue!\n");
      return NULL;
   }

#define FREE_TQP(TQ)     \
   free(TQ->log_prefix); \
   free(TQ);

   // grab our log prefix, if provided
   if (opts->log_prefix != NULL)
   {
      tq->log_prefix = strdup(opts->log_prefix);
      if (tq->log_prefix == NULL)
      {
         LOG(LOG_ERR, "failed to allocate space for TQ log prefix!\n");
         return NULL;
      }
   }
   else
   {
      tq->log_prefix = malloc(sizeof(char) * (strlen(def_queue_pref) + 1));
      if (tq->log_prefix == NULL)
      {
         LOG(LOG_ERR, "failed to allocate space for TQ log prefix!\n");
         return NULL;
      }
      if (tq->log_prefix != strcpy(tq->log_prefix, def_queue_pref))
      {
         LOG(LOG_ERR, "failed to copy default TQ log prefix to newly allocated string!\n");
         FREE_TQP(tq);
         return NULL;
      }
   }

   LOG(LOG_INFO, "%s Initializing ThreadQueue with params: Num_threads=%u, Num_producers=%u, Max_qdepth=%u\n",
       tq->log_prefix, opts->num_threads, opts->num_prod_threads, opts->max_qdepth);

   // sanity check our inputs
   char abort = 0;
   if (opts->max_qdepth == 0)
   {
      LOG(LOG_ERR, "%s Received a zero value for max queue depth\n", tq->log_prefix);
      abort = 1;
   }
   if (opts->num_threads < opts->num_prod_threads)
   {
      abort = 1;
      LOG(LOG_ERR, "%s Received producer thread count (%u) in excess of total thread count (%u)\n",
          tq->log_prefix, opts->num_prod_threads, opts->num_threads);
   }
   else if (opts->num_threads == 0)
   {
      abort = 1;
      LOG(LOG_ERR, "%s Received a zero value for total thread count\n", tq->log_prefix);
   }
   if (opts->thread_producer_func == NULL)
   {
      // check that we aren't asked to start producers with undefinied behavior
      if (opts->num_prod_threads > 0)
      {
         abort = 1;
         LOG(LOG_ERR, "%s Received non-zero producer thread count (%u), but thread_producer_func() is NULL\n",
             tq->log_prefix, opts->num_prod_threads);
      }
      // check that at least consumer threads have defined behavior
      if (opts->thread_consumer_func == NULL)
      {
         abort = 1;
         LOG(LOG_ERR, "%s Received NULL values for thread_producer_func() AND thread_consumer_func()\n", tq->log_prefix);
      }
   }
   else if ((opts->thread_consumer_func == NULL) && (opts->num_threads > opts->num_prod_threads))
   {
      // check that we aren't asked to start consumers with undefinied behavior
      LOG(LOG_ERR, "%s Received thread counts implying %u consumers, but thread_consumer_func() is NULL\n",
          tq->log_prefix, (opts->num_threads - opts->num_prod_threads));
      abort = 1;
   }
   if (abort)
   {
      FREE_TQP(tq);
      return NULL;
   } // abort if any of the above conditions were true

   // initialize all TQ fields we received from the caller
   tq->max_qdepth = opts->max_qdepth;
   // initialize basic queue vars
   tq->qdepth = 0;
   tq->head = 0;
   tq->tail = 0;
   // initialize control flags
   tq->con_flags = opts->init_flags;
   // initialize worker pools to NULL (simplifies cleanup logic)
   tq->prod_pool = NULL;
   tq->cons_pool = NULL;
   // initialize our count of uncollected threads
   tq->uncoll_thrds = 0;
   // initialize pthread control structures
   if (pthread_mutex_init(&tq->qlock, NULL))
   {
      free(tq->log_prefix);
      free(tq);
      return NULL;
   }
   if (pthread_cond_init(&tq->state_resume, NULL))
   {
      pthread_mutex_destroy(&tq->qlock);
      FREE_TQP(tq);
      return NULL;
   }
   if (pthread_cond_init(&tq->producer_resume, NULL))
   {
      pthread_cond_destroy(&tq->state_resume);
      pthread_mutex_destroy(&tq->qlock);
      FREE_TQP(tq);
      return NULL;
   }
   if (pthread_cond_init(&tq->consumer_resume, NULL))
   {
      pthread_cond_destroy(&tq->producer_resume);
      pthread_cond_destroy(&tq->state_resume);
      pthread_mutex_destroy(&tq->qlock);
      FREE_TQP(tq);
      return NULL;
   }

#define FREE_PTHREAD_VALUES(TQ)                \
   pthread_cond_destroy(&TQ->consumer_resume); \
   pthread_cond_destroy(&TQ->producer_resume); \
   pthread_cond_destroy(&TQ->state_resume);    \
   pthread_mutex_destroy(&TQ->qlock);

   // initialize fields requiring memory allocation
   if ((tq->state_flags = calloc(sizeof(TQ_State_Flags), opts->num_threads)) == NULL)
   {
      FREE_PTHREAD_VALUES(tq);
      FREE_TQP(tq);
      return NULL;
   }
   if ((tq->workpkg = malloc(sizeof(void *) * opts->max_qdepth)) == NULL)
   {
      free(tq->state_flags);
      FREE_PTHREAD_VALUES(tq);
      FREE_TQP(tq);
      return NULL;
   }
   unsigned int i;
   for (i = 0; i < opts->max_qdepth; i++)
   {
      tq->workpkg[i] = NULL;
   }
   // allocate space for all thread instances
   tq->threads = malloc(sizeof(pthread_t *) * opts->num_threads);
   if (tq->threads == NULL)
   {
      LOG(LOG_ERR, "%s failed to allocate space for threads!\n", tq->log_prefix);
      free(tq->workpkg);
      free(tq->state_flags);
      FREE_PTHREAD_VALUES(tq);
      FREE_TQP(tq);
      return NULL;
   }

   // allocate space for thread arg structs
   ThreadArg** targs = malloc(sizeof(ThreadArg*) * opts->num_threads);
   if (targs == NULL)
   {
      LOG(LOG_ERR, "%s failed to allocate space for a ThreadArg list!\n", tq->log_prefix);
      tq_free_all(tq);
      return NULL;
   }
   for ( i = 0; i < opts->num_threads; i++ ) {
      targs[i] = malloc(sizeof(struct thread_arg_struct));
      if ( targs[i] == NULL ) {
         LOG(LOG_ERR, "%s failed to allocate space for a ThreadArg struct!\n", tq->log_prefix);
         for ( ; i > 0; i-- ) {
            free( targs[i-1] );
         }
         free( targs );
         tq_free_all(tq);
         return NULL;
      }
   }

   // allocate and initialize producer pool, if necessary
   if (opts->num_prod_threads > 0)
   {
      tq->prod_pool = malloc(sizeof(struct thread_queue_worker_pool_struct));
      if (tq->prod_pool == NULL)
      {
         LOG(LOG_ERR, "%s failed to allocate space for TQ Producer Pool!\n", tq->log_prefix);
         for ( i = 0; i < opts->num_threads; i++ ) {
            free( targs[i] );
         }
         free( targs );
         tq_free_all(tq);
         return NULL;
      }
      TQWorkerPool wp = tq->prod_pool; // shorthand reference
      wp->pname = "Producer";
      wp->start_tID = 0;
      wp->num_thrds = opts->num_prod_threads;
      wp->act_thrds = wp->num_thrds;
      wp->thread_init_func = opts->thread_init_func;
      wp->thread_work_func = opts->thread_producer_func;
      wp->thread_pause_func = opts->thread_pause_func;
      wp->thread_resume_func = opts->thread_resume_func;
      wp->thread_term_func = opts->thread_term_func;
   }
   // allocate and initialize consumer pool, if necessary
   if (opts->num_threads > opts->num_prod_threads)
   {
      tq->cons_pool = malloc(sizeof(struct thread_queue_worker_pool_struct));
      if (tq->cons_pool == NULL)
      {
         LOG(LOG_ERR, "%s failed to allocate space for TQ Producer Pool!\n", tq->log_prefix);
         for ( i  = 0; i < opts->num_threads; i++ ) {
            free( targs[i] );
         }
         free( targs );
         tq_free_all(tq);
         return NULL;
      }
      TQWorkerPool wp = tq->cons_pool; // shorthand reference
      wp->pname = "Consumer";
      wp->start_tID = opts->num_prod_threads;
      wp->num_thrds = (opts->num_threads - opts->num_prod_threads);
      wp->act_thrds = wp->num_thrds;
      wp->thread_init_func = opts->thread_init_func;
      wp->thread_work_func = opts->thread_consumer_func;
      wp->thread_pause_func = opts->thread_pause_func;
      wp->thread_resume_func = opts->thread_resume_func;
      wp->thread_term_func = opts->thread_term_func;
   }

   // initialize all threads
   unsigned int tID = 0;
   // create all producer threads
   for (; tID < opts->num_prod_threads; tID++)
   {
      ThreadArg *targ = targs[tID];
      targ->global_state = opts->global_state;
      targ->tID = tID;
      targ->tq = tq;
      LOG(LOG_INFO, "%s Starting %s Thread %u\n", tq->log_prefix, tq->prod_pool->pname, targ->tID);
      if (pthread_create(&tq->threads[tID], NULL, producer_thread, (void *)targ))
      {
         LOG(LOG_ERR, "%s failed to create thread %d\n", tq->log_prefix, tID);
         break;
      }
      tq->uncoll_thrds++;
   }
   // create all consumer threads
   // NOTE - value of 'tID' persists from producer thread creation
   for (; tID < opts->num_threads; tID++)
   {
      ThreadArg *targ = targs[tID];
      targ->global_state = opts->global_state;
      targ->tID = tID;
      targ->tq = tq;
      LOG(LOG_INFO, "%s Starting %s Thread %u\n", tq->log_prefix, tq->cons_pool->pname, targ->tID);
      if (pthread_create(&tq->threads[tID], NULL, consumer_thread, (void *)targ))
      {
         LOG(LOG_ERR, "%s failed to create thread %d\n", tq->log_prefix, tID);
         for( i = tID; i < opts->num_threads; i++ ) { free( targs[i] ); }
         break;
      }
      tq->uncoll_thrds++;
   }
   // NOTE -- leaving 'targs' subpointers allocated.  These will be freed by the threads themselves
   free( targs );

   // acquire queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s failed to acquire queue lock!\n", tq->log_prefix);
      tID = opts->num_threads + 1; // if we fail to get the lock, this is an easy way to cleanup (skips following loop)
   }

   // still holding lock (so long as that wasn't the reason we're aborting)
   if (tID != opts->num_threads)
   { // an error occured while creating threads
      LOG(LOG_ERR, "%s failed to init all threads: signaling ABORT!\n", tq->log_prefix);
      tq->con_flags |= TQ_ABORT; // signal all threads to abort (potentially redundant)
      pthread_cond_broadcast(&tq->consumer_resume);
      pthread_cond_broadcast(&tq->producer_resume);
      if (tID < opts->num_threads)
      {
         pthread_mutex_unlock(&tq->qlock);
      } //if this wasn't a locking failure, unlock
      for (tID = 0; tID < tq->uncoll_thrds; tID++)
      {
         pthread_join(tq->threads[tID], NULL); // just ignore thread status, we are already aborting
         LOG(LOG_INFO, "%s joined with thread %u\n", tq->log_prefix, tID);
      }
      // free everything we allocated and terminate
      tq_free_all(tq);
      return NULL;
   }
   // release the lock and allow threads to get back to work!
   pthread_mutex_unlock(&tq->qlock);

   return tq;
}

/**
 * Check for successful initialization of all threads of a ThreadQueue
 * @param ThreadQueue tq : ThreadQueue for which to check status
 * @return int : Zero on success, -1 on failure
 */
int tq_check_init(ThreadQueue tq) {
   // acquire queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s failed to acquire queue lock!\n", tq->log_prefix);
      return -1;
   }

   // check for initialization of all threads
   int num_threads = 0;
   if ( tq->cons_pool ) { num_threads += tq->cons_pool->num_thrds; }
   if ( tq->prod_pool ) { num_threads += tq->prod_pool->num_thrds; }
   int tID;
   for (tID = 0; tID < num_threads; tID++)
   {
      // wait for thread to initialize (we're still holding the lock through this entire loop; not too efficient but very conventient)
      while ((tq->state_flags[tID] & (TQ_READY | TQ_ERROR)) == 0)
      { // wait for the thread to succeed or fail
         LOG(LOG_INFO, "%s waiting for thread %u to initialize\n", tq->log_prefix, tID);
         pthread_cond_wait(&tq->state_resume, &tq->qlock); // we will be notified when the thread updates state value
      }
      if (tq->state_flags[tID] & TQ_ERROR)
      { // if we've hit an error, quit out
         LOG(LOG_ERR, "%s thread %u indicates init failed!\n", tq->log_prefix, tID);
         pthread_mutex_unlock(&tq->qlock);
         return -1;
      }
      LOG(LOG_INFO, "%s thread %u initialized successfully\n", tq->log_prefix, tID);
   }

   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Populate a given TQ_Init_Opts struct with the current parameters of a ThreadQueue
 * NOTE -- this will NOT populate the init_flags and global_state values!
 * @param ThreadQueue tq : ThreadQueue from which to gather info
 * @param TQ_Init_Opts* opts : Reference to the TQ_Init_Opts struct to be populated
 * @param int log_strlen : Length of the log_prefix string in the opts struct
 * @return int : Zero on success, -1 on failure
 */
int tq_get_opts(ThreadQueue tq, TQ_Init_Opts *opts, int log_strlen)
{
   if (opts == NULL)
   {
      LOG(LOG_ERR, "Received a NULL opts reference!\n");
      return -1;
   }
   if (tq == NULL)
   {
      LOG(LOG_ERR, "Received a NULL ThreadQueue reference!\n");
      return -1;
   }

   // get some values prepped
   int num_threads = 0;
   int num_prods = 0;
   if (tq->prod_pool)
   {
      num_threads += tq->prod_pool->num_thrds;
      num_prods = num_threads;
   }
   if (tq->cons_pool)
   {
      num_threads += tq->cons_pool->num_thrds;
   }

   // populate all struct fields
   opts->init_flags = TQ_NONE; // just don't bother
   opts->max_qdepth = tq->max_qdepth;
   opts->global_state = NULL; // just don't bother
   opts->num_threads = num_threads;
   opts->num_prod_threads = num_prods;
   opts->thread_consumer_func = NULL; // init to NULL, just in case
   opts->thread_producer_func = NULL; // init to NULL, just in case
   if (tq->prod_pool)
   {
      opts->thread_init_func = tq->prod_pool->thread_init_func; // should be safe from either pool
      opts->thread_producer_func = tq->prod_pool->thread_work_func;
      opts->thread_pause_func = tq->prod_pool->thread_pause_func;
      opts->thread_resume_func = tq->prod_pool->thread_resume_func;
      opts->thread_term_func = tq->prod_pool->thread_term_func;
      if (tq->cons_pool)
      {
         opts->thread_consumer_func = tq->cons_pool->thread_work_func;
      }
   }
   else
   {
      opts->thread_init_func = tq->cons_pool->thread_init_func; // should be safe from either pool
      opts->thread_consumer_func = tq->cons_pool->thread_work_func;
      opts->thread_pause_func = tq->cons_pool->thread_pause_func;
      opts->thread_resume_func = tq->cons_pool->thread_resume_func;
      opts->thread_term_func = tq->cons_pool->thread_term_func;
   }
   if (opts->log_prefix != NULL && log_strlen)
      strncpy(opts->log_prefix, tq->log_prefix, log_strlen);
   return 0;
}

/**
 * Insert a new element of work into the ThreadQueue
 * @param ThreadQueue tq : ThreadQueue in which to insert work
 * @param TQ_Control_Flags ignore_flags : Indicates which queue states should be bypassed during this operation
 *                                        (By default, any queue state will result in a failure)
 * @param void* workbuff : New element of work to be inserted
 * @return int : Zero on success, -1 on failure (such as, if the queue is ABORTED and TQ_ABORT was not specified)
 */
int tq_enqueue(ThreadQueue tq, TQ_Control_Flags ignore_flags, void *workbuff)
{
   if (pthread_mutex_lock(&tq->qlock))
   {
      return -1;
   }

   // wait for an opening in the queue or for work to be canceled
   while ((tq->qdepth == tq->max_qdepth) && !(tq->con_flags & ~(ignore_flags)))
   {
      LOG(LOG_INFO, "%s master proc is waiting for an opening to enqueue into\n", tq->log_prefix);
      pthread_cond_broadcast(&tq->consumer_resume); // our queue is full!  Make sure all consumers are running
      pthread_cond_wait(&tq->producer_resume, &tq->qlock);
      LOG(LOG_INFO, "%s master proc has woken up\n", tq->log_prefix);
   }
   // check for any oddball conditions which would prevent this work from completing
   if (tq->con_flags & ~(ignore_flags))
   {
      LOG(LOG_ERR, "%s queue state prevents enqueueing!\n", tq->log_prefix);
      pthread_mutex_unlock(&tq->qlock);
      errno = EINVAL;
      return -1;
   }

   // insert the new work at the tail of the queue
   tq->workpkg[tq->tail] = workbuff;           // insert the workbuff into the queue
   tq->tail = (tq->tail + 1) % tq->max_qdepth; // move the tail to the next slot
   tq->qdepth++;                               // finally, increment the queue depth
   LOG(LOG_INFO, "%s master proc has successfully enqueued work\n", tq->log_prefix);

   // if the queue length exceeds the work being processed, tell a thread to resume
   if (tq->cons_pool != NULL)
   {
      if (tq->qdepth > tq->cons_pool->act_thrds && tq->cons_pool->act_thrds < tq->cons_pool->num_thrds)
      {
         LOG(LOG_INFO, "%s master signaling an additional %s thread ( running=%u, depth=%u )\n",
             tq->log_prefix, tq->cons_pool->pname, tq->cons_pool->act_thrds, tq->qdepth);
         pthread_cond_signal(&tq->consumer_resume);
      }
   }
   else if (tq->qdepth == 1)
   { // no consumer threads and the queue was empty, signal
      // why are you just using this as a queue?  where are your consumer threads? You're doing it wrong!!!
      LOG(LOG_INFO, "%s master blindly signaling a consumer\n", tq->log_prefix);
      pthread_cond_signal(&tq->consumer_resume);
   }

   pthread_mutex_unlock(&tq->qlock);

   return 0;
}

/**
 * Retrieve a new element of work from the ThreadQueue.
 *  Note that, if the Queue is empty but not FINISHED, this call will block.
 *  However, if the Queue is empty and FINISED, this call will return zero and
 *  populate workbuff with a NULL value.
 * @param ThreadQueue tq : ThreadQueue from which to retrieve work
 * @param TQ_Control_Flags ignore_flags : Indicates which queue states should be bypassed during this operation
 *                                        (By default, only a TQ_FINISHED state will not result in a failure)
 * @param void** workbuff : Reference to be populated with the work element pointer
 * @return int : The depth of the queue (including the retrieved element) on success,
 *               Zero if the queue is both empty and has ANY control flags set (deadlock protection),
 *               and -1 on failure (such as, if the queue is HALTED or ABORTED, and those flags were not ignored)
 */
int tq_dequeue(ThreadQueue tq, TQ_Control_Flags ignore_flags, void **workbuff)
{
   if (pthread_mutex_lock(&tq->qlock))
   {
      return -1;
   }
   ignore_flags |= TQ_FINISHED; // a FINISHED queue can still be dequeued from

   // wait for a queue element or for any state flags which could prevent work from being created
   while ((tq->qdepth == 0 && !(tq->con_flags)))
   {
      LOG(LOG_INFO, "%s master proc is waiting for an element to dequeue\n", tq->log_prefix);
      pthread_cond_broadcast(&tq->producer_resume); // our queue is empty!  Make sure all producers are running
      pthread_cond_wait(&tq->consumer_resume, &tq->qlock);
      LOG(LOG_INFO, "%s master proc has woken up\n", tq->log_prefix);
   }
   // check for any oddball conditions which should prevent this work
   if (tq->con_flags & ~(ignore_flags))
   {
      LOG(LOG_ERR, "%s queue state prevents dequeueing!\n", tq->log_prefix);
      pthread_mutex_unlock(&tq->qlock);
      errno = EINVAL;
      return -1;
   }
   // check for an empty queue
   if (tq->qdepth == 0)
   {
      LOG(LOG_INFO, "%s master proc can't dequeue while queue is empty and has flags: %d\n", tq->log_prefix, tq->con_flags);
      pthread_mutex_unlock(&tq->qlock);
      if (workbuff)
         *workbuff = NULL;
      return 0;
   }

   // note the queue depth before removal, for reporting
   int depth = tq->qdepth;

   // remove a work pkg from the head of the queue
   if (workbuff)
      *workbuff = tq->workpkg[tq->head];
   tq->head = (tq->head + 1) % tq->max_qdepth;
   tq->qdepth--;
   LOG(LOG_INFO, "%s master proc has successfully dequeued work\n", tq->log_prefix);

   // only wake producers if the queue is in a standard state
   if (!(tq->con_flags))
   {
      // if the number of empty queue positions exceeds the number of producers, tell a thread to resume
      if (tq->prod_pool != NULL)
      {
         if ((tq->max_qdepth - tq->qdepth) > tq->prod_pool->act_thrds && tq->prod_pool->act_thrds < tq->prod_pool->num_thrds)
         {
            LOG(LOG_INFO, "%s master signaling an additional %s thread ( running=%u, depth=%u )\n",
                tq->log_prefix, tq->prod_pool->pname, tq->prod_pool->act_thrds, tq->qdepth);
            pthread_cond_signal(&tq->producer_resume);
         }
      }
      else if (tq->qdepth == (tq->max_qdepth - 1))
      { // no producer threads and the queue was full, signal
         // why are you just using this as a queue?  where are your producer threads? You're doing it wrong!!!
         LOG(LOG_INFO, "%s master blindly signaling a producer\n", tq->log_prefix);
         pthread_cond_signal(&tq->producer_resume);
      }
   }

   pthread_mutex_unlock(&tq->qlock);

   return depth;
}

/**
 * Determine the current depth (number of enqueued elements) of the given ThreadQueue
 * @param ThreadQueue tq : ThreadQueue for which to determine depth
 * @return int : Current depth of the ThreadQueue, or -1 on a failure
 */
int tq_depth(ThreadQueue tq)
{
   if (pthread_mutex_lock(&tq->qlock))
   {
      return -1;
   }
   int depth = tq->qdepth;
   pthread_mutex_unlock(&tq->qlock);
   return depth;
}

/**
 * Sets the given control flags on a specified ThreadQueue
 * @param ThreadQueue tq : ThreadQueue on which to set flags
 * @param TQ_Control_Flags flags : Flag values to set
 * @return int : Zero on success and non-zero on failure
 */
int tq_set_flags(ThreadQueue tq, TQ_Control_Flags flags)
{
   // get the queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock\n", tq->log_prefix);
      return -1;
   }
   // set the requested flags
   tq->con_flags |= flags;
   LOG(LOG_INFO, "%s master set flag values: %d\n", tq->log_prefix, (int)flags);
   // wake all threads
   pthread_cond_broadcast(&tq->producer_resume);
   pthread_cond_broadcast(&tq->consumer_resume);
   pthread_cond_broadcast(&tq->state_resume);
   // release the lock
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Unsets (disables) the given control flags on a specified ThreadQueue
 * @param ThreadQueue tq : ThreadQueue on which to unset flags
 * @param TQ_Control_Flags flags : Flag values to remove
 * @return int : Zero on success and non-zero on failure
 */
int tq_unset_flags(ThreadQueue tq, TQ_Control_Flags flags)
{
   // get the queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock\n", tq->log_prefix);
      return -1;
   }
   // unset the requested flags
   tq->con_flags &= ~(flags);
   LOG(LOG_INFO, "%s master removed flag values: %d\n", tq->log_prefix, flags);
   // wake all threads
   pthread_cond_broadcast(&tq->producer_resume);
   pthread_cond_broadcast(&tq->consumer_resume);
   pthread_cond_broadcast(&tq->state_resume);
   // release the lock
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Populates a given reference with the current control flags set on a given ThreadQueue
 * @param ThreadQueue tq : ThreadQueue for which to get flags value
 * @param TQ_Control_Flags* flags : Flag reference to populate
 * @return int : Zero on success and non-zero on failure
 */
int tq_get_flags(ThreadQueue tq, TQ_Control_Flags *flags)
{
   // get the queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock\n", tq->log_prefix);
      return -1;
   }
   // retrieve the flag value
   *flags = tq->con_flags;
   LOG(LOG_INFO, "%s master retrieved queue control flags: %d\n", tq->log_prefix, *flags);
   // release the lock
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Waits for all threads of a given ThreadQueue to pause, then returns
 * @param ThreadQueue tq : ThreadQueue on which to wait
 * @return int : Zero on success and non-zero on failure (such as, if the queue is not HALTED)
 */
int tq_wait_for_pause(ThreadQueue tq)
{
   // get the queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock\n", tq->log_prefix);
      return -1;
   }
   // check that queue is in an expected state
   if (!(tq->con_flags & TQ_HALT))
   {
      LOG(LOG_ERR, "%s master cannot wait on a queue that is not HALTED\n", tq->log_prefix);
      pthread_mutex_unlock(&tq->qlock);
      errno = EINVAL;
      return -1;
   }
   if (tq->con_flags & (TQ_FINISHED | TQ_ABORT))
   {
      LOG(LOG_ERR, "%s master cannont wait on FINISHED or ABORTED queue!\n", tq->log_prefix);
      pthread_mutex_unlock(&tq->qlock);
      errno = EINVAL;
      return -1;
   }
   unsigned int thread = 0;
   // wait for halt of all producer threads
   if (tq->prod_pool != NULL)
   {
      for (; thread < tq->prod_pool->num_thrds; thread++)
      {
         while (!(tq->state_flags[thread] & TQ_HALTED) && !(tq->con_flags & (TQ_FINISHED | TQ_ABORT))
                                                       && (tq->con_flags & TQ_HALT) )
         {
            LOG(LOG_INFO, "%s master is waiting for thread %u to halt\n", tq->log_prefix, thread);
            pthread_cond_wait(&tq->state_resume, &tq->qlock);
         }
         if (!(tq->con_flags & TQ_HALT))
         {
            LOG(LOG_ERR, "%s master cannot wait on a queue that is not HALTED\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         if (tq->con_flags & (TQ_FINISHED | TQ_ABORT))
         {
            LOG(LOG_ERR, "%s master cannont wait on FINISHED or ABORTED queue!\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         LOG(LOG_INFO, "%s master detects that %s Thread %u has halted\n", tq->log_prefix, tq->prod_pool->pname, thread);
      }
   }
   // wait for halt of all consumer threads
   unsigned int con_start = thread;
   if (tq->cons_pool != NULL)
   {
      for (; thread < (tq->cons_pool->num_thrds + con_start); thread++)
      {
         while (!(tq->state_flags[thread] & TQ_HALTED) && !(tq->con_flags & (TQ_FINISHED | TQ_ABORT))
                                                       && (tq->con_flags & TQ_HALT) )
         {
            LOG(LOG_INFO, "%s master is waiting for thread %u to halt\n", tq->log_prefix, thread);
            pthread_cond_wait(&tq->state_resume, &tq->qlock);
         }
         if (!(tq->con_flags & TQ_HALT))
         {
            LOG(LOG_ERR, "%s master cannot wait on a queue that is not HALTED\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         if (tq->con_flags & (TQ_FINISHED | TQ_ABORT))
         {
            LOG(LOG_ERR, "%s master cannont wait on FINISHED or ABORTED queue!\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         LOG(LOG_INFO, "%s master detects that %s Thread %u has halted\n", tq->log_prefix, tq->cons_pool->pname, thread);
      }
   }
   LOG(LOG_INFO, "%s master detects that all threads have halted\n", tq->log_prefix);
   // release the lock and return
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Waits for all threads of a given ThreadQueue to complete, then returns
 * @param ThreadQueue tq : ThreadQueue on which to wait
 * @return int : Zero on success,
 *               < zero on failure (such as, if the queue is not FINISHED),
 *               or > zero if a deadlock condition is possible ( such as, if queue is full and no consumers exist )
 */
int tq_wait_for_completion(ThreadQueue tq)
{
   // get the queue lock
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock\n", tq->log_prefix);
      return -1;
   }
   // check that queue is in an expected state
   if (!(tq->con_flags & TQ_FINISHED))
   {
      LOG(LOG_ERR, "%s master cannot wait on a queue that is not FINISHED\n", tq->log_prefix);
      pthread_mutex_unlock(&tq->qlock);
      errno = EINVAL;
      return -1;
   }
   if (tq->con_flags & TQ_ABORT)
   {
      LOG(LOG_ERR, "%s master cannont wait on an ABORTED queue!\n", tq->log_prefix);
      pthread_mutex_unlock(&tq->qlock);
      errno = EINVAL;
      return -1;
   }
   // wait for completion of all producer threads
   unsigned int thread = 0;
   if (tq->prod_pool != NULL)
   {
      for (; thread < tq->prod_pool->num_thrds; thread++)
      {
         while ( (tq->state_flags[thread] & TQ_READY) && !(tq->con_flags & TQ_ABORT)
                                                      &&  (tq->con_flags & TQ_FINISHED) )
         {
            // special check for possible deadlock
            if ( tq->cons_pool == NULL  &&  tq->qdepth ) {
               LOG( LOG_WARNING, "Possible deadlock condition: Queue is non-empty and no consumer threads exist\n" );
               pthread_mutex_unlock(&tq->qlock);
               return 1;
            }
            LOG(LOG_INFO, "%s master is waiting for thread %u to complete\n", tq->log_prefix, thread);
            pthread_cond_wait(&tq->state_resume, &tq->qlock);
         }
         if (!(tq->con_flags & TQ_FINISHED))
         {
            LOG(LOG_ERR, "%s master cannot wait on a queue that is not FINISHED\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         if (tq->con_flags & TQ_ABORT)
         {
            LOG(LOG_ERR, "%s master cannont wait on an ABORTED queue!\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         LOG(LOG_INFO, "%s master detects that %s Thread %u has completed\n", tq->log_prefix, tq->prod_pool->pname, thread);
      }
   }
   // wait for completion of all consumer threads
   unsigned int con_start = thread;
   if (tq->cons_pool != NULL)
   {
      for (; thread < (tq->cons_pool->num_thrds + con_start); thread++)
      {
         while ( (tq->state_flags[thread] & TQ_READY) && !(tq->con_flags & TQ_ABORT)
                                                      &&  (tq->con_flags & TQ_FINISHED) )
         {
            LOG(LOG_INFO, "%s master is waiting for thread %u to complete\n", tq->log_prefix, thread);
            pthread_cond_wait(&tq->state_resume, &tq->qlock);
         }
         if (!(tq->con_flags & TQ_FINISHED))
         {
            LOG(LOG_ERR, "%s master cannot wait on a queue that is not FINISHED\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         if (tq->con_flags & TQ_ABORT)
         {
            LOG(LOG_ERR, "%s master cannont wait on an ABORTED queue!\n", tq->log_prefix);
            pthread_mutex_unlock(&tq->qlock);
            errno = EINVAL;
            return -1;
         }
         LOG(LOG_INFO, "%s master detects that %s Thread %u has completed\n", tq->log_prefix, tq->cons_pool->pname, thread);
      }
   }
   LOG(LOG_INFO, "%s master detects that all threads have completed\n", tq->log_prefix);
   // release the lock and return
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Waits for the control flags of the given ThreadQueue to be updated.  Afterwards, populates the provided
 *  'flags' reference with the current value of the flags on the queue (same as if tq_get_flags() were called).
 * @param ThreadQueue tq : ThreadQueue on which to wait
 * @param TQ_Control_Flags ignore_flags : Indicates which queue states should be ignored during this operation
 *                                        (This call will continue to block, if only these flags are set)
 * @param TQ_Control_Flags* flags : Flag reference to populate
 * @return int : Zero on success and non-zero on failure (such as, if ignore_flags has all values populated)
 */
int tq_wait_for_flags(ThreadQueue tq, TQ_Control_Flags ignore_flags, TQ_Control_Flags *flags)
{
   // make sure the caller doesn't enter an infinite loop
   if (ignore_flags == (TQ_FINISHED | TQ_ABORT | TQ_HALT))
   {
      LOG(LOG_ERR, "%s master cannont wait while ignoring all flags!\n", tq->log_prefix);
      errno = EINVAL;
      return -1;
   }
   if (pthread_mutex_lock(&tq->qlock))
   {
      return -1;
   }

   // wait for a queue element or for work to be canceled
   while (!(tq->con_flags & ~(ignore_flags)))
   {
      LOG(LOG_INFO, "%s master proc is waiting for queue flags to change\n", tq->log_prefix);
      pthread_cond_wait(&tq->state_resume, &tq->qlock);
      LOG(LOG_INFO, "%s master proc has woken up\n", tq->log_prefix);
   }

   // retrieve the flag value
   *flags = tq->con_flags;
   LOG(LOG_INFO, "%s master retrieved queue control flags: %d\n", tq->log_prefix, *flags);
   // release the lock
   pthread_mutex_unlock(&tq->qlock);
   return 0;
}

/**
 * Populates a reference to the state for the next uncollected thread in a FINISHED or ABORTED ThreadQueue
 * @param ThreadQueue tq : ThreadQueue from which to collect state info
 * @param void** tstate : Reference to a void* be populated with thread state info
 * @return int : The number of thread states to be collected (INCLUDING the state just collected),
 *               zero if all thread states have already been collected, or -1 if a failure occured.
 */
int tq_next_thread_status(ThreadQueue tq, void **tstate)
{
   // theoretically, this function *should* only be called by a single proc at a time after all threads have exited.
   //   However, it seems safer to lock here, just in case.
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock!\n", tq->log_prefix);
      return -1;
   }
   // make sure the queue has been marked FINISHED/ABORTED
   if (!(tq->con_flags & (TQ_FINISHED | TQ_ABORT)))
   {
      LOG(LOG_ERR, "%s cannont retrieve thread states from a non-FINISHED/ABORTED queue!\n", tq->log_prefix);
      errno = EINVAL;
      pthread_mutex_unlock(&tq->qlock);
      return -1;
   }
   unsigned int tcnt = tq->uncoll_thrds;
   if (tq->uncoll_thrds > 0)
   {
      unsigned int tID = 0;
      if (tq->prod_pool != NULL)
      {
         tID += tq->prod_pool->num_thrds;
      }
      if (tq->cons_pool != NULL)
      {
         tID += tq->cons_pool->num_thrds;
      }
      tID -= tq->uncoll_thrds; // get thread states in the order they were started
      // make sure the thread is ready to join (not still trying to process work)
      while ((tq->state_flags[tID] & TQ_READY) != 0)
      {
         LOG(LOG_INFO, "%s master waiting for thread %u to terminate\n", tq->log_prefix, tID);
         // threads should already have been signaled, no need to repeat
         pthread_cond_wait(&tq->state_resume, &tq->qlock);
      }
      LOG(LOG_INFO, "%s master attempting to join thread %u\n", tq->log_prefix, tID);
      int ret = pthread_join(tq->threads[tID], tstate);
      if (ret)
      { // indicate a failure if we couldn't join
         LOG(LOG_ERR, "%s master failed to join thread %u!\n", tq->log_prefix, tID);
         pthread_mutex_unlock(&tq->qlock);
         return -1;
      }
      tq->uncoll_thrds--;
      LOG(LOG_INFO, "%s master successfully joined thread %u (%u threads remain)\n", tq->log_prefix, tID, tq->uncoll_thrds);
   }
   else
   { // if no threads are running, set state to NULL
      LOG(LOG_INFO, "%s all threads collected\n", tq->log_prefix);
      if (tstate != NULL)
      {
         *tstate = NULL;
      }
   }
   pthread_mutex_unlock(&tq->qlock);
   return tcnt;
}

/**
 * Closes a FINISHED or ABORTED ThreadQueue for which all thread status info has already been collected.
 *  However, if the ThreadQueue still has queue elements remaining (such as if the queue ABORTED), this
 *  function will take no action.  In such a case, the tq_dequeue() function must be used to empty the
 *  queue elements before the queue can then be closed by calling this function again.
 * @param ThreadQueue tq : ThreadQueue to be closed
 * @return int : Zero if the queue was successfully closed, -1 on failure, or a positive integer equal to
 *               the number of elements found still on the queue if no action was taken.
 */
int tq_close(ThreadQueue tq)
{
   if (pthread_mutex_lock(&tq->qlock))
   {
      LOG(LOG_ERR, "%s master failed to acquire queue lock!\n", tq->log_prefix);
      return -1;
   }
   // make sure the queue has been marked FINISHED/ABORTED and that all thread states have been collected
   if (!(tq->con_flags & (TQ_FINISHED | TQ_ABORT)) || tq->uncoll_thrds)
   {
      LOG(LOG_ERR, "%s cannont close a non-FINISHED/ABORTED queue or one with running threads!\n", tq->log_prefix);
      errno = EINVAL;
      pthread_mutex_unlock(&tq->qlock);
      return -1;
   }
   if (tq->qdepth != 0)
   {
      LOG(LOG_ERR, "%s cannont close a queue with elements still remaining!\n", tq->log_prefix);
      errno = EINVAL;
      int depth = tq->qdepth;
      pthread_mutex_unlock(&tq->qlock);
      return depth;
   }

   pthread_mutex_unlock(&tq->qlock);
   // free everything and terminate
   tq_free_all(tq);
   //pthread_exit(NULL);
   return 0;
}
