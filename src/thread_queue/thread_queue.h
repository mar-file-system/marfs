#ifndef __THREAD_QUEUE_H__
#define __THREAD_QUEUE_H__

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

typedef enum
{
   TQ_NONE = 0,             // filler value, used to indicate no flags at all
   TQ_HALT = 0x01 << 0,     // Signals to threads that work should be paused
                            //  All threads sleep until the flag is removed
   TQ_FINISHED = 0x01 << 1, // Signals to threads that all work has been issued
                            //  Producers exit after enqueing final work package, consumers exit when the queue is empty
                            //  Takes precedence over TQ_HALT flag ( that flag will be ignored )
   TQ_ABORT = 0x01 << 2     // Signals to threads that an unrecoverable errror requires early termination
                            //  All threads exit immediately, work elements may be left on the queue
                            //  Takes precedence over TQ_HALT and TQ_FINISHED ( those flags will be ignored )
} TQ_Control_Flags;

typedef struct queue_init_struct
{
   // Queue Info
   char *log_prefix;            /* string prefix for all log messages produced by this queue */
   TQ_Control_Flags init_flags; /* state flags to set at the moment of queue creation, before threads initialize */
   unsigned int max_qdepth;     /* maximum depth of the work queue */

   // Thread Info
   void *global_state;            /* reference to some global initial state, passed to the init_thread state func of all threads */
   unsigned int num_threads;      /* number of threads to initialize */
   unsigned int num_prod_threads; /* number of threads to utilize the thread_producer_func() (those with tID < num_prod_threads) */

   /* Please note, the following functions may be run by multiple threads in parallel.
         Beware of placing shared values in the 'state' arguments. */
   /* NULL values for most of these function pointers will cause the corresponding function call(s) to be skipped by all threads.
         This is NOT true for the thread_producer_func() and thread_consumer_func(), which must be defined if producer/consumer threads
         exist, respectively. */

   /*
      function pointer defining the initilization behavior for each thread
      - Arguments
         * The first argument is an integer ID for the calling thread (value from 0 to (num_threads-1))
         * The second argument is a copy of the global_state pointer for each thread
         * The third argument is a reference to the user-defined state pointer for this thread
      - Return Value
         * A non-zero return value will cause the calling thread to ABORT the queue ( this will cause tq_init() to fail )
         * A return value of zero will be ignored
   */
   int (*thread_init_func)(unsigned int tID, void *global_state, void **state);

   /*
      function pointer defining the behavior of a consumer thread for each work package
      - Arguments
         * The first argument is a reference to the user-defined state pointer for this thread
         * The second argument is a reference to a void* work package for this thread (passed in by tq_enqueue())
      - Return Value
         * A return value > 1 will cause the calling thread to HALT the queue
         * A return value == 1 will cause the calling thread to FINISH the queue
         * A return value < 0 will cause the calling thread to ABORT the queue
         * A return value == 0 will be ignored
   */
   int (*thread_consumer_func)(void **state, void **work_todo);

   /*
      function pointer defining the behavior of a producer thread for each work package
      NOTE - It is possible for a thread to terminate before the produced work can be enqueued.  In such a case,
             the thread_term_func() will always be called with the previous work_tofill value as the second arg.
             It is the responsibility of that function to cleanup any resources associated with that unused work
             package.
      - Arguments
         * The first argument is a reference to the user-defined state pointer for this thread
         * The second argument is a reference to a void* work package to be populated by this thread
      - Return Value
         * A return value > 1 will cause the calling thread to HALT the queue
         * A return value == 1 will cause the calling thread to FINISH the queue
         * A return value < 0 will cause the calling thread to ABORT the queue
         * A return value == 0 will be ignored
   */
   int (*thread_producer_func)(void **state, void **work_tofill);

   /*
      function pointer defining the behavior of a thread just before entering a HALTED state
      - Arguments
         * The first/only argument is a reference to the user-defined state pointer for this thread
         * The second argument is a reference to the previous work package produced/consumed by this thread
            ** This reference will be populated with NULL if the previous work package has already been
               processed (passed to the consumer func), enqueued, or no previous work package exists.
      - Return Value
         * A non-zero return value will cause the calling thread to ABORT the queue
         * A return value of zero will be ignored
   */
   int (*thread_pause_func)(void **state, void **prev_work);

   /*
      function pointer defining the behavior of a thread just after exiting a HALTED state
      - Arguments
         * The first/only argument is a reference to the user-defined state pointer for this thread
         * The second argument is a reference to the previous work package produced/consumed by this thread
            ** This reference will be populated with NULL if the previous work package has already been
               processed (passed to the consumer func), enqueued, or no previous work package exists.
      - Return Value
         * A non-zero return value will cause the calling thread to ABORT the queue
         * A return value of zero will be ignored
   */
   int (*thread_resume_func)(void **state, void **prev_work);

   /*
      function pointer defining the termination behavior of a thread
      - Arguments
         * The first argument is a reference to the user-defined state pointer for this thread
         * The second argument is a reference to the previous work package produced/consumed by this thread
            ** This reference will be populated with NULL if the previous work package has already been
               processed (passed to the consumer func), enqueued, or no previous work package exists.
         * The third argument is a copy of the control flags value at the time of thread termination
      - Return Value (NONE)
   */
   void (*thread_term_func)(void **state, void **prev_work, TQ_Control_Flags flg);

} TQ_Init_Opts;

typedef struct thread_queue_struct *ThreadQueue; // forward decl.

/**
 * Initializes a new ThreadQueue according to the parameters of the passed options struct
 * @param TQ_Init_Opts opts : options struct defining parameters for the created ThreadQueue
 * @return ThreadQueue : pointer to the created ThreadQueue, or NULL if an error was encountered
 */
ThreadQueue tq_init(TQ_Init_Opts *opts);

/**
 * Check for successful initialization of all threads of a ThreadQueue
 * @param ThreadQueue tq : ThreadQueue for which to check status
 * @return int : Zero on success, -1 on failure
 */
int tq_check_init(ThreadQueue tq);

/**
 * Populate a given TQ_Init_Opts struct with the current parameters of a ThreadQueue
 * NOTE -- this will NOT populate the init_flags and global_state values!
 * @param ThreadQueue tq : ThreadQueue from which to gather info
 * @param TQ_Init_Opts* opts : Reference to the TQ_Init_Opts struct to be populated
 * @param int log_strlen : Length of the log_prefix string in the opts struct
 * @return int : Zero on success, -1 on failure
 */
int tq_get_opts(ThreadQueue tq, TQ_Init_Opts *opts, int log_strlen);

/**
 * Insert a new element of work into the ThreadQueue
 * @param ThreadQueue tq : ThreadQueue in which to insert work
 * @param TQ_Control_Flags ignore_flags : Indicates which queue states should be bypassed during this operation
 *                                        (By default, any queue state will result in a failure)
 * @param void* workbuff : New element of work to be inserted
 * @return int : Zero on success, -1 on failure (such as, if the queue is ABORTED and TQ_ABORT was not specified)
 */
int tq_enqueue(ThreadQueue tq, TQ_Control_Flags ignore_flags, void *workbuff);

/**
 * Retrieve a new element of work from the ThreadQueue.
 *  Note that, if the Queue is empty and has no state flags set, this call will block.
 *  However, if the Queue is empty and has any state flags set, this call will return zero and
 *  populate workbuff with a NULL value.
 * @param ThreadQueue tq : ThreadQueue from which to retrieve work
 * @param TQ_Control_Flags ignore_flags : Indicates which queue states should be bypassed during this operation
 *                                        (By default, only a TQ_FINISHED state will not result in a failure)
 * @param void** workbuff : Reference to be populated with the work element pointer
 * @return int : The depth of the queue (including the retrieved element) on success,
 *               Zero if the queue is both empty and has ANY control flags set (deadlock protection),
 *               and -1 on failure (such as, if the queue is HALTED or ABORTED, and those flags were not ignored)
 */
int tq_dequeue(ThreadQueue tq, TQ_Control_Flags ignore_flags, void **workbuff);

/**
 * Determine the current depth (number of enqueued elements) of the given ThreadQueue
 * @param ThreadQueue tq : ThreadQueue for which to determine depth
 * @return int : Current depth of the ThreadQueue, or -1 on a failure
 */
int tq_depth(ThreadQueue tq);

/**
 * Sets the given control flags on a specified ThreadQueue
 * @param ThreadQueue tq : ThreadQueue on which to set flags
 * @param TQ_Control_Flags flags : Flag values to set
 * @return int : Zero on success and non-zero on failure
 */
int tq_set_flags(ThreadQueue tq, TQ_Control_Flags flags);

/**
 * Unsets (disables) the given control flags on a specified ThreadQueue
 * @param ThreadQueue tq : ThreadQueue on which to unset flags
 * @param TQ_Control_Flags flags : Flag values to remove
 * @return int : Zero on success and non-zero on failure
 */
int tq_unset_flags(ThreadQueue tq, TQ_Control_Flags flags);

/**
 * Populates a given reference with the current control flags set on a given ThreadQueue
 * @param ThreadQueue tq : ThreadQueue for which to get flags value
 * @param TQ_Control_Flags* flags : Flag reference to populate
 * @return int : Zero on success and non-zero on failure
 */
int tq_get_flags(ThreadQueue tq, TQ_Control_Flags *flags);

/**
 * Waits for all threads of a given ThreadQueue to pause (detect the TQ_HALT flag and stop work), then returns
 * @param ThreadQueue tq : ThreadQueue on which to wait
 * @return int : Zero on success and non-zero on failure (such as, if the queue is not HALTED)
 */
int tq_wait_for_pause(ThreadQueue tq);

/**
 * Waits for the control flags of the given ThreadQueue to be updated.  Afterwards, populates the provided
 *  'flags' reference with the current value of the flags on the queue (same as if tq_get_flags() were called).
 * @param ThreadQueue tq : ThreadQueue on which to wait
 * @param TQ_Control_Flags ignore_flags : Indicates which queue states should be ignored during this operation
 *                                        (This call will continue to block, if only these flags are set)
 * @param TQ_Control_Flags* flags : Flag reference to populate
 * @return int : Zero on success and non-zero on failure (such as, if ignore_flags has all values populated)
 */
int tq_wait_for_flags(ThreadQueue tq, TQ_Control_Flags ignore_flags, TQ_Control_Flags *flags);

/**
 * Waits for all threads of a given ThreadQueue to complete, then returns
 * @param ThreadQueue tq : ThreadQueue on which to wait
 * @return int : Zero on success,
 *               < zero on failure (such as, if the queue is not FINISHED),
 *               or > zero if a deadlock condition is possible ( such as, if queue is full and no consumers exist )
 */
int tq_wait_for_completion(ThreadQueue tq);

/**
 * Populates a reference to the state for the next uncollected thread in a FINISHED or ABORTED ThreadQueue
 * @param ThreadQueue tq : ThreadQueue from which to collect state info
 * @param void** tstate : Reference to a void* be populated with thread state info
 * @return int : The number of uncollected thread states (INCLUDING the state just collected),
 *               zero if all thread states have already been collected, or -1 if a failure occured.
 */
int tq_next_thread_status(ThreadQueue tq, void **tstate);

/**
 * Closes a FINISHED or ABORTED ThreadQueue for which all thread status info has already been collected.
 *  However, if the ThreadQueue still has queue elements remaining (such as if the queue ABORTED), this
 *  function will take no action.  In such a case, the tq_dequeue() function must be used to empty the
 *  queue elements before the queue can then be closed by calling this function again.
 * @param ThreadQueue tq : ThreadQueue to be closed
 * @return int : Zero if the queue was successfully closed, -1 on failure, or a positive integer equal to
 *               the number of elements found still on the queue if no action was taken.
 */
int tq_close(ThreadQueue tq);

#endif
