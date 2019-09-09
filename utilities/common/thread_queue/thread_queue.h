#ifndef THREAD_QUEUE_H
#define THREAD_QUEUE_H

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


typedef struct queue_init_struct {
   unsigned int   num_threads;      /* number of threads to initialize */
   unsigned int   max_qdepth;       /* maximum depth of the work queue */
   void*          global_state;     /* reference to some global initial state, passed to the init_thread state func of all threads */
   int (*thread_init_func) (unsigned int tID, void* global_state, void** state);
         /*
            function pointer defining the initilization behavior for each thread
            - This function will be run by only a single thread at a time (non-parallel), with each thread completing a call to 
               thread_init_func() before the next thread starts
            - Arguments
               * The first argument is an integer ID for the calling thread (0 to (num_threads-1))
               * The second argument is a copy of the global_state pointer for each thread
               * The third argument is a reference to the user-defined state pointer for this thread
            - Return Value
               * A non-zero return value will cause the calling thread to ABORT the queue and fail out of tq_init()
               * A return value of zero will be ignored
         */
   int (*thread_work_func) (void** state, void* work);
         /* 
            function pointer defining the behavior of a thread for each work package
            - This function may be run by multiple threads in parallel
               Beware of shared values in the 'state' argument
            - Arguments
               * The first argument is a reference to the user-defined state pointer for this thread
               * The second argument is a reference to the current work package of the thread (passed in by tq_enqueue())
            - Return Value
               * A return value above zero will cause the calling thread to HALT the queue
               * A return value below zero will cause the calling thread to ABORT the queue
               * A return value of zero will be ignored
         */
   void (*thread_term_func) (void** state);
         /*
            function pointer defining the termination behavior of a thread
            - Arguments
               * The first/only argument is a reference to the user-defined state pointer for this thread
            - Return Value (NONE)
         */
} TQ_Init_Opts;




typedef struct thread_queue_struct* ThreadQueue;

/**
 * Sets the FINISHED state for a given ThreadQueue, allowing thread status info to be collected
 * @param ThreadQueue* tq : ThreadQueue to mark as FINISHED
 * @return int : Zero on success and non-zero on failure
 */
int tq_work_done(ThreadQueue tq);

/**
 * Sets a HALT state for the given ThreadQueue and waits for all threads to pause
 * @param ThreadQueue* tq : ThreadQueue to pause
 * @return int : Zero on success and non-zero on failure
 */
int tq_halt( ThreadQueue tq );

/**
 * Unsets the HALT state for a given ThreadQueue and signals all threads to resume work
 * @param ThreadQueue* tq : ThreadQueue for which to unset the HALT state
 * @return int : Zero on success and non-zero on failure
 */
int tq_resume( ThreadQueue tq );

/**
 * Checks if the HALT flag is set for a given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue to check
 * @return char : 1 if the HALT flag is set, and 0 if not
 */
char tq_halt_set( ThreadQueue tq );

/**
 * Sets an ABORT state for the given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue for which to set the ABORT
 * @return int : Zero on success and non-zero on failure
 */
int tq_abort(ThreadQueue tq);

/**
 * Checks if the ABORT flag is set for a given ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue to check
 * @return char : 1 if the ABORT flag is set, and 0 if not
 */
char tq_abort_set( ThreadQueue tq );

/**
 * Insert a new element of work into the ThreadQueue
 * @param ThreadQueue* tq : ThreadQueue in which to insert work
 * @param void* workbuff : New element of work to be inserted
 * @return int : Zero on success and -1 on failure (such as, if the queue is FINISHED or ABORTED)
 */
int tq_enqueue( ThreadQueue tq, void* workbuff );

/**
 * Returns the status info for the next uncollected thread in a FINISHED or ABORTED ThreadQueue
 *  If no uncollected threads remain in the ThreadQueue, the status info will be set to NULL
 * @param ThreadQueue* tq : ThreadQueue from which to collect status info
 * @param void** tstate : Reference to be populated with thread status info
 * @return int : Zero on success, -1 on failure, and 1 if all threads have already terminated
 */
int tq_next_thread_status( ThreadQueue tq, void** tstate );

/**
 * Closes a FINISHED or ABORTED ThreadQueue for which all thread status info has already been collected
 * @param ThreadQueue* tq : ThreadQueue to be closed
 * @return int : Zero on success and -1 on failure
 */
int tq_close( ThreadQueue tq );

/**
 * Initializes a new ThreadQueue according to the parameters of the passed options struct
 * @param TQ_Init_Opts opts : options struct defining parameters for the created ThreadQueue
 * @return ThreadQueue* : pointer to the created ThreadQueue, or NULL if an error was encountered
 */
ThreadQueue tq_init( TQ_Init_Opts opts );


#endif

