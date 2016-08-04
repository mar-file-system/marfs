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


// ---------------------------------------------------------------------------
// These handle the guts of MarFS object-interactions for fuse and pftool,
// using libaws4c.  What we're adding on top of libaws4c is "streaming".
// This is needed because a large object must be written (or read) with a
// single PUT (or GET).  We don't want to require pftool to buffer the
// entire contents of an object before writing, nor fuse write() to create
// a complete object per call.
//
// These functions all expect to be wrapped in a TRY0(), in marfs_fuse, so they
// return 0 for success, or an errno for failure.
//
// The streaming support here involves a curl readfunc (or writefunc) which
// moves an incremental amount of data into a PUT (or GET), then waits for
// more.  There is code in test_aws.c in libaws4c that shows a
// proof-of-concept.
//
// The idea is that e.g. marfs_write waits until the readfunc has finished
// reading from the IOBuf that is found in the provided ObjectStream, then
// adds more data to that IOBuf and signals the readfunc that it can
// proceed.  Meanwhile, the readfunc waits until there is an IOBuf
// available, then continues moving data into the stream.  marfs_release
// (aka close()) tells the readfunc that no more data is forthcoming, so
// that the readfunc can properly end the stream with curl (by returning
// 0).  A similar set of operation is done for fuse_read.
//
// TBD: double buffering.  ObjectStream wouldhave 2 IOBufs.  In the writing
//      case, stream_put would signal readfunc to go ahead, and return
//      immediately.  Next write would work on the other IOBuf.  See
//      example-code in test_aws.c, case 12.
//
// TBD: Should ObjectStream.iob be volatile?  Test code worked without that,
//      but future changes here might introduce subtle bugs without it.
//
// TBD: Should we add a mutex to OpenStream?  This would by locked during
//      writes.  Then we'd add a "stream_flush()" function which would try
//      to acquire the lock.  If acquired, it would just unlock again, and
//      return.  The value would be that fuse flush could be assured that
//      pending writes had completed, so that when it called stat on the
//      MDFS file, we would be confident that the result was correct.
//
// ---------------------------------------------------------------------------

#ifndef _MARFS_OBJECTS_H
#define _MARFS_OBJECTS_H

#include "common.h"             // only for MARFS_MAX_URL_SIZE
#include <aws4c.h>
#include <pthread.h>

#ifdef SPINLOCKS
#  include "spinlock.h"
#else
#  include <semaphore.h>
#endif



#  ifdef __cplusplus
extern "C" {
#  endif


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

#endif




// internal flags in an ObjectStream
typedef enum {
   OSF_OPEN       = 0x0001,
   OSF_WRITING    = 0x0002,
   OSF_READING    = 0x0004,
   //   OSF_LENGTH     = 0x0008,
   OSF_RLOCK_INIT = 0x0008,

   OSF_EOB        = 0x0010,
   OSF_EOF        = 0x0020,
   OSF_TIMEOUT    = 0x0040,     // failed SAFE_WAIT() in stream_put/get
   OSF_TIMEOUT_K  = 0x0080,     // SAFE_WAIT_KILL() in stream_put/get

   OSF_ABORT      = 0x0100,     // stream_abort(), or stream_sync()
   OSF_JOINED     = 0x0200,
   OSF_CLOSED     = 0x0400,

   OSF_THREAD_ERR = 0x1000,     // thread returned non-zero (curl err)
} OSFlags;
typedef uint16_t OSFlags_t;

// These are all the OSFlags that imply that errors happened on an open
// stream, so stream_sync()/marfs_release() should take note
#define OSF_ERRORS (OSF_TIMEOUT | OSF_TIMEOUT_K | OSF_THREAD_ERR)


//  For stream_write(), the op-thread runs s3_put(), and acts as a
//  consumer.  It waits for iob_full to be set by stream_write(), and then
//  begins to interact with the streaming_readfunc() to move data to curl
//  [curl is "reading" the data we are writing] Meanwhile stream_write()
//  waits for the thread to set iob_empty, before returning.
//
//  For stream_read(), the op-thread runs s3_get(), and acts as a producer.
//  It waits for iob_empty to be set by stream_read(), and then begins to
//  interact with the streaming_writefunc() to move data from curl [curl is
//  "writing" the data we are reading] Meanwhile stream_read() waits for
//  the thread to set iob_full, before returning.

typedef struct {
   // This comes from libaws4c
   IOBuf               iob;       // should be VOLATILE ?

   SEM_T               iob_empty;
   SEM_T               iob_full;
   SEM_T               read_lock; // concurrent NFS reads

   pthread_t           op;        // GET/PUT
   int                 op_rc;     // typically 0 or -1  (see iob.result, for curl/S3 errors)
   char                url[MARFS_MAX_URL_SIZE]; // WARNING: only valid during open_object()
   size_t              written;   // bytes written-to/read-from stream
   size_t              content_len;
   uint16_t            timeout;   // zero uses hardwired default for GET/PUT timeout

   volatile OSFlags_t  flags;

   // OSOpenFlags       open_flags; // caller's open flags, for when we need to close/repoen
} ObjectStream;


typedef enum {
   OS_GET = 0,
   OS_PUT = 1
} IsPut;


// Initialize os.url, before calling.  Use <preserve_os_written> to prevent
// resetting the count of data written, in os->written
int     stream_open(ObjectStream* os,
                    IsPut         put,
                    curl_off_t    content_length,
                    uint8_t       preserve_os_written,
                    uint16_t      timeout);

int     stream_put(ObjectStream* os, const char* buf, size_t size);
ssize_t stream_get(ObjectStream* os, char* buf,       size_t size);

int     stream_sync(ObjectStream* os);
int     stream_abort(ObjectStream* os);

int     stream_close(ObjectStream* os);





#  ifdef __cplusplus
}
#  endif

#endif // _MARFS_OBJECTS_H
