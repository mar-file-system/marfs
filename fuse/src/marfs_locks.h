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
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#ifndef _MARFS_LOCKS_H
#define _MARFS_LOCKS_H

#include <pthread.h>

#ifdef SPINLOCKS
#  include "spinlock.h"
#else
#  include <semaphore.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


// ---------------------------------------------------------------------------
// LOCKING
//
// Depending on whether SPINLOCKS is defined (e.g. compile with
// -DSPINLOCKS), we either use semaphores or our new PoliteSpinLocks, to
// synchronize between a user (e.g. fuse) calling stream_put/get and the
// thread that receives callbacks form curl to read/write more data for the
// user.
//
// With semaphores, and 4 concurrent fuse writers, we're seeing ~300k
// context-switches/sec, on the object-servers.  That drops to ~18k
// context-switches/sec, when using the polite spin-locks.
//
// I had thought this might explain the poor parallel bandwidth through fuse.
// Unfortunately, this isn't causing an improvement, there.
// ---------------------------------------------------------------------------


#ifdef SPINLOCKS

// ...........................................................................
// spinlocks
// ...........................................................................

// return non-zero if we timed-out without getting the lock
# define TIMED_WAIT(SEM_PTR, TIMEOUT_SEC)          \
   PSL_wait_with_timeout((SEM_PTR), (TIMEOUT_SEC))


# define SAFE_WAIT(SEM_PTR, TIMEOUT_SEC, OS_PTR)                        \
   do {                                                                 \
      if (PSL_wait_with_timeout((SEM_PTR), (TIMEOUT_SEC))) {            \
         LOG(LOG_ERR, "PSL_wait_with_timeout failed. (%s)\n", strerror(errno)); \
         (OS_PTR)->flags |= OSF_TIMEOUT;                                \
         return -1;                                                     \
      }                                                                 \
   } while (0)

# define SAFE_WAIT_KILL(SEM_PTR, TIMEOUT_SEC, OS_PTR)                   \
   do {                                                                 \
      if (PSL_wait_with_timeout((SEM_PTR), (TIMEOUT_SEC))) {            \
         LOG(LOG_ERR, "PSL_wait_with_timeout failed. (%s)  Killing thread.\n", strerror(errno)); \
         (OS_PTR)->flags |= OSF_TIMEOUT_K;                              \
         /* pthread_kill((OS_PTR)->op, SIGKILL); */                     \
         pthread_cancel((OS_PTR)->op);                                  \
                                                                        \
         LOG(LOG_INFO, "waiting for terminated op-thread\n");           \
         if (stream_wait(os)) {                                         \
            LOG(LOG_ERR, "err joining op-thread ('%s')\n", strerror(errno)); \
         }                                                              \
         return -1;                                                     \
      }                                                                 \
   } while (0)

# define WAIT(PSL)                        PSL_wait(PSL)
# define POST(PSL)                        PSL_post(PSL)
# define SEM_INIT(PSL, IGNORE, VALUE)     PSL_init((PSL), (VALUE))
# define SEM_DESTROY(PSL)

typedef struct PoliteSpinLock    SEM_T;

#else

// ...........................................................................
// semaphores
// ...........................................................................

int timed_sem_wait(sem_t* sem, size_t timeout_sec);


// return non-zero if we timed-out without getting the lock
# define TIMED_WAIT(SEM_PTR, TIMEOUT_SEC)       \
   timed_sem_wait((SEM_PTR), (TIMEOUT_SEC))


# define SAFE_WAIT(SEM_PTR, TIMEOUT_SEC, OS_PTR)                        \
   do {                                                                 \
      if (timed_sem_wait((SEM_PTR), (TIMEOUT_SEC))) {                   \
         LOG(LOG_ERR, "timed_sem_wait failed. (%s)\n", strerror(errno)); \
         (OS_PTR)->flags |= OSF_TIMEOUT;                                \
         return -1;                                                     \
      }                                                                 \
   } while (0)

# define SAFE_WAIT_KILL(SEM_PTR, TIMEOUT_SEC, OS_PTR)                   \
   do {                                                                 \
      if (timed_sem_wait((SEM_PTR), (TIMEOUT_SEC))) {                   \
         LOG(LOG_ERR, "timed_sem_wait failed. (%s)  Killing thread.\n", strerror(errno)); \
         (OS_PTR)->flags |= OSF_TIMEOUT_K;                              \
         /* pthread_kill((OS_PTR)->op, SIGKILL); */                     \
         pthread_cancel((OS_PTR)->op);                                  \
                                                                        \
         LOG(LOG_INFO, "waiting for terminated op-thread\n");           \
         if (stream_wait(os)) {                                         \
            LOG(LOG_ERR, "err joining op-thread ('%s')\n", strerror(errno)); \
         }                                                              \
         return -1;                                                     \
      }                                                                 \
   } while (0)


# define WAIT(SEM)                        sem_wait(SEM)
# define POST(SEM)                        sem_post(SEM)
# define SEM_INIT(SEM, SHARED, VALUE)     sem_init((SEM), (SHARED), (VALUE))
# define SEM_DESTROY(SEM)                 sem_destroy(SEM)

typedef sem_t  SEM_T;

#endif // SPINLOCKS

#ifdef __cplusplus
}
#endif
   
#endif // _MARFS_LOCKS_H
   
