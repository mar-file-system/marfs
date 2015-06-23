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


// _GNU_SOURCE defines pthread_timedjoin_np(), and pthread_tryjoin_np()
#define _GNU_SOURCE
#include <pthread.h>
#include <signal.h>             // pthread_kill()

#define LOG_PREFIX "obj_stream"
#include "logging.h"
#include "common.h"
#include "object_stream.h"

#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <errno.h>
#include <assert.h>


void stream_reset(ObjectStream* os);


// ---------------------------------------------------------------------------
// TBD
//
// All of the sem_waits should be replaced with something like this.  The
// idea is that the thread doing an S3_put could have returned due to some
// error (e.g. in curl, or S3, or server), and we could wait forever on a
// semaphore that the readfunc is never going to post.

// We should probably iterate in a loop, so that the caller can wait for as
// long as it takes for the server to time-out and return an error code.

// either wait for as long as curl allows the thread to wait, or we can
// impose our own time-out.  If the thread does return, the server has
// probably returned an error-code, or curl has failed for some reason.  If
// so, then return some error-code ourselves.

// ---------------------------------------------------------------------------

#if TBD

#include <time.h>

#define WAIT_SEM(SEM, OS)                       \
  do {                                          \
     int rc = timed_sem_wait((SEM), (OS));     \
     if (rc)                                    \
        return (rc);                            \
  } while (0)

#endif



int timed_sem_wait(sem_t* sem, size_t timeout_sec) {

   struct timespec timeout;
   if (clock_gettime(CLOCK_REALTIME, &timeout))
      return -1;

   // timeout.tv_sec += os->timeout_sec; // TBD
   timeout.tv_sec += timeout_sec;

   // wait for a little while on the semaphore
   while (1) {
      if (! sem_timedwait(sem, &timeout))
         return 0;              // got it

      if (errno == EINTR) {
         LOG(LOG_INFO, "interrupted.  resuming wait ...\n");
         continue;              // interrupted (try again?)
      }

      if (errno == ETIMEDOUT)
         return -1;             // timed-out

      return -1;                // something else went wrong
   }
}

/* #define safe_sem_wait(SEM, TIMEOUT, OS)                              \
   safe_sem_wait_internal((SEM), (TIMEOUT), (OS), __FILE__, __LINE__)
   int safe_sem_wait_internal(sem_t* sem, size_t timeout_sec, ObjectStream* os, char* file, char* line) {
*/

#define SAFE_SEM_WAIT(SEM_PTR, TIMEOUT_SEC)                             \
   do {                                                                 \
      if (timed_sem_wait((SEM_PTR), (TIMEOUT_SEC))) {                   \
         LOG(LOG_ERR, "timed_sem_wait failed to get sem [%s:%d]. (%s)\n", \
             __FILE__, __LINE__, strerror(errno));                      \
         return -1;                                                     \
      }                                                                 \
   } while (0)

#define SAFE_SEM_WAIT_KILL(SEM_PTR, TIMEOUT_SEC, OS)                    \
   do {                                                                 \
      if (timed_sem_wait((SEM_PTR), (TIMEOUT_SEC))) {                   \
         LOG(LOG_ERR, "timed_sem_wait failed to get sem [%s:%d].  (%s) Aborting.\n", \
             __FILE__, __LINE__, strerror(errno));                      \
         pthread_kill((OS)->op, SIGKILL);                               \
         return -1;                                                     \
      }                                                                 \
   } while (0)




// ---------------------------------------------------------------------------
// stream_wait()
//
// Wait until thread completes (and return immediately when that happens).
// Loop is necessary to handle the case where another thread is also trying
// to join (in which case, our join gets EINVAL).  This would happen, I
// think, in the case of duplicated marfs file-handles.
//
// NOTE: This supports fuse flush.  Fuse flush is not the same as fflush().
//       Fuse flush means to wait until the all possible reads/writes are
//       completed, such that no more errors can be generated on this
//       stream.
//
// NOTE: Can't use pthread_timedjoin_np(), without #define _GNU_SOURCE.
//       Not sure I want to do that.
//
// TBD: compare accumulated wait with a max-timeout limit, that could be
//       provided in e.g. OpenStream.timeout_sec, representing a maximum
//       time that a stream was allowed to be unresponsive.
// ---------------------------------------------------------------------------


#if 1

static
int stream_wait(ObjectStream* os) {

   static const size_t  TIMEOUT_SECS = 10;

   struct timespec timeout;
   if (clock_gettime(CLOCK_REALTIME, &timeout)) {
      fprintf(stderr, "stream_wait failed to get timer '%s'\n", strerror(errno));
      return -1;                // errno is set
   }
   timeout.tv_sec += TIMEOUT_SECS;


   void* retval;
   while (1) {

      // check whether thread has returned.  Could mean a curl error, an S3
      // protocol error, or server flaking out.  Successful return will
      // have saved retval in os->op_rc, so we don't have to return it.
      if (! pthread_timedjoin_np(os->op, &retval, &timeout))
         return 0;              // joined

      else if (errno == ETIMEDOUT)
         return -1;             // timed-out

      else if (errno == EINVAL)
         // EINVAL == another thread is also trying to join (?)  timed-wait
         // will just return immediately again, and we'll turn into a
         // busy-wait.  Add an explicit sleep here.  [Alternatively, here's
         // another idea: If multiple threads are waiting on this thread, then
         // that implies the fuse handle was dup'ed.  In that case, if we
         // return success, fuse might still wait until all threads have
         // flushed?  I don't trust my knowledge of fuse enough to depend on
         // that.]
         sleep(1);

      else
         return -1;             // error
   }
}

#else

static
int stream_wait(ObjectStream* os) {

   void* retval = NULL;
   while (1) {

      // check whether thread has returned.  Could mean a curl
      // error, an S3 protocol error, or server flaking out.
      if (! pthread_join(os->op, &retval))
         return 0;              // joined

      else if (errno != EINVAL)
         return -1;             // error

      // EINVAL = another thread trying to join (?)  timed-wait would just
      // return immediately again, and we'd turn into a busy-wait.  Add an
      // explicit sleep here.  [Alternatively, here's another idea: If
      // multiple threads are waiting on this thread, then that implies the
      // fuse handle was dup'ed.  In that case, if we return success, won't
      // fuse still wait until all threads have flushed?  I don't trust my
      // knowledge of fuse enough to depend on that.]
      else
         sleep(1);
   }
}
#endif









// ---------------------------------------------------------------------------
// GET / PUT thread
// ---------------------------------------------------------------------------

// This just allows us to use AWS_CHECK(), returning an int, where s3_op
// has to return return void*.  We return 0 for success, non-zero for
// failure.
//
// Any errors from curl, S3, etc, will be found in os->iob->result.
// If there are any, we'll be returning non-zero.
//
// TBD: If we return non-zero on a GET, writefunc will be stranded waiting for us.
// Now, it will time-out on its sem_wait and report an error.  Therefore,
//      we should return zero on all legitimate cases.

int s3_op_internal(ObjectStream* os) {
   IOBuf*        b  = &os->iob;

   // run the GET or PUT
   int is_get = (os->flags & OSF_READING);
   if (is_get) {
      LOG(LOG_INFO, "GET %lx, '%s'\n", (size_t)b, os->url);
      AWS4C_CHECK1( s3_get(b, os->url) ); /* create empty object with user metadata */
   }
   else {
      LOG(LOG_INFO, "PUT %lx, '%s'\n", (size_t)b, os->url);
      AWS4C_CHECK1( s3_put(b, os->url) ); /* create empty object with user metadata */
   }


   // s3_get with byte-range can leave streaming_writefunc() waiting for
   // a curl callback that never comes.  This happens if there is still writable
   // space in the buffer, when the last bytes in the request are processed.
   // This can happen because caller (e.g. fuse) may ask for more bytes than are present,
   // and provide a buffer big enought o receive them.
   if (is_get && (b->code == 206)) {
      // should we do something with os->iob_full?  set os->flags & EOF?
      LOG(LOG_INFO, "GET complete\n");
      os->flags |= OSF_EOF;
      sem_post(&os->iob_full);
      return 0;
   }
   else if (AWS4C_OK(b) ) {
      LOG(LOG_INFO, "%s complete\n", ((is_get) ? "GET" : "PUT"));
      return 0;
   }
   LOG(LOG_ERR, "CURL ERROR: %lx %d '%s'\n", (size_t)b, b->code, b->result);
   return -1;
}

// this runs as a separate thread, so that stream_open() can return
void* s3_op(void* arg) {
   ObjectStream* os = (ObjectStream*)arg;
   IOBuf*        b  = &os->iob;

   if ((os->op_rc = s3_op_internal(os)))
      LOG(LOG_ERR, "failed (%s) '%s'\n", os->url, b->result);
   else
      LOG(LOG_INFO, "done (%s)\n", os->url);

   return os;
}


// ---------------------------------------------------------------------------
// PUT (write)
//
// This is installed as the "readfunc" which is called by curl, whenever it
// needs more data for a PUT.  (It's a "read" of data from curl's
// perspective.)  This function is invoked in its own thread by curl.  We
// synchronize with stream_put(), where a user has more data to be
// added to a stream.  The user's buffer may be larger than the size of the
// buffer curl gives us to fill, in which case we self-enable, so the next
// call-back can happen immediately.  Finally, stream_close gives us an
// empty buffer, which tells us to signal EOF to curl, which we do by
// returning 0.
// ---------------------------------------------------------------------------

size_t streaming_readfunc(void* ptr, size_t size, size_t nmemb, void* stream) {
   LOG(LOG_INFO, "entry\n");

   IOBuf*        b     = (IOBuf*)stream;
   ObjectStream* os    = (ObjectStream*)b->user_data;
   size_t        total = (size * nmemb);
   LOG(LOG_INFO, "curl buff %ld\n", total);

   // wait for producer to fill buffers
   sem_wait(&os->iob_full);
   LOG(LOG_INFO, "avail-data: %ld\n", b->avail);

   if (b->write_count == 0) {
      LOG(LOG_INFO, "got EOF\n");
      sem_post(&os->iob_empty); // polite
      return 0;
   }

   // move producer's data into curl buffers.
   // (Might take more than one callback)
   size_t move_req = ((total <= b->avail) ? total : b->avail);
   size_t moved    = aws_iobuf_get_raw(b, (char*)ptr, move_req);

   // track total size
   os->written += moved;
   LOG(LOG_INFO, "moved %ld  (total: %ld)\n", moved, os->written);

   if (b->avail) {
      LOG(LOG_INFO, "iterating\n");
      sem_post(&os->iob_full);  // next callback is pre-approved
   }
   else {
      LOG(LOG_INFO, "done with buffer (total written %ld)\n", os->written);
      sem_post(&os->iob_empty); // tell producer that buffer is used
   }

   return moved;
}

// Hand <buf> over to the streaming_readfunc(), so it can be added into
// the ongoing streaming PUT.  You must call stream_open) first.
//
// NOTE: Doing this a little differently from the test_aws.c (case 12)
//       approach.  We're forcing *synchronous* interaction with the
//       readfunc, because we don't want caller's <buf> to go out of scope
//       until the readfunc is finished with it.
//
int stream_put(ObjectStream* os,
               const char*   buf,
               size_t        size) {

   static const int put_timeout_sec = 10; /* totally made up out of thin air */

   LOG(LOG_INFO, "entry\n");
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }
   if (! (os->flags & OSF_WRITING)) {
      LOG(LOG_ERR, "%s isn't open for writing\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }
   IOBuf* b = &os->iob;         // shorthand

   // install buffer into IOBuf
   aws_iobuf_reset(b);          // doesn't affect <user_data>
   aws_iobuf_append_static(b, (char*)buf, size);
   LOG(LOG_INFO, "installed buffer (%ld bytes) for readfn\n", size);

   // let readfunc move data
   sem_post(&os->iob_full);

   LOG(LOG_INFO, "waiting for IOBuf\n"); // readfunc done with IOBuf?
   SAFE_SEM_WAIT(&os->iob_empty, put_timeout_sec);

   LOG(LOG_INFO, "buffer done\n"); // readfunc done with IOBuf?
   return size;
}


// ---------------------------------------------------------------------------
// GET (read)

// curl calls streaming_writefunc with some incoming data on a GET, which
// we are supposed to "write" somewhere.  We interact with stream_get(), to
// write our data into a buffer that a caller provided to stream_get().
//
// streaming_writefunc() is more complex than streaming_readfunc(), because we
// don't return to curl until we have exhausted curl's buffer.  [because
// doc says curl treats anything less than that as an error.  Should test
// that.]  That means is may require multiple calls to stream_get(), before
// we can write all of curl's buffer.
//
// If it so happens that the object is not-bigger-than the buffer, or the
// GET is issued with a byte-range not-bigger-than the buffer, we will fill
// the buffer.
//
// As in the case of stream_put() plus readfunc(), we're guessing that the
// buffer sizes presented to stream_get() by users will tend to be much
// larger than the 16k buffer presented to streaming_writefunc() by curl,
// so it's probably better to use the buffer provided to stream_get() as
// the basis of the shared IOBuf, rather than using the buffer provided by
// curl to streaming_writefunc().
//
// NOTE: I think we're not in danger of deadlocking if someone closes
//       prematurely, because ObjectStream has the pthread that is doing
//       the GET.  It can always just kill that thread, as part of a close,
//       which (I think) ought to end curl's expectaions about the
//       write-function.
//
//       For extra nice-ness, for the case where we are bing closed before
//       the object has been entirely read, we could add a flag to check
//       (e.g. CLOSING).  Then stream_close() could set that (in
//       ObjectStream), and post iob_full, so we'd get a chance to iterate,
//       see the flag, and return 0 to curl.  I expect this would allow the
//       thread to complete naturally (with a curl error-code), instead of
//       requiring being killed.
//
// NOTE: After a little experimentation, it looks like curl never calls us
//       with size 0 to indicate EOF.  Instead, the GET thread will just
//       return.  In the case where the user reads only part of a file,
//       even that won't happen.  Therefore, I think we (or stream_get)
//       will need to count the bytes going past.
//
//       No, it's worse than that.  If the byte-range is beyond EOF, then
//       no write-function gets called
//
// ---------------------------------------------------------------------------

// After EOF, fuse makes a callback to marfs_read() with a byte-range
// beyond the end of the object.  In that case, curl never calls
// streaming_writefunc(), so stream_get() is stuck waiting for iob_full.
// This function can detect that case, because the Content-length returned
// in the header will be zero.
size_t streaming_writeheaderfunc(void* ptr, size_t size, size_t nitems, void* stream) {

   size_t result = aws_headerfunc(ptr, size, nitems, stream);

   // if we've parse content-length from the response-header, and length
   // was zero, then there will be no callback to streaming_writefunc().
   // Therefore, streaming_writefunc() will never post iob_full, and
   // stream_get() will wait forever (or until it times out).  We have
   // knowledge that this is happening, so we can post iob_full ourselves,
   // and let stream_get() proceed.
   if ( !strncmp( ptr, "Content-Length: ", 15 )) {
      IOBuf*        b     = (IOBuf*)stream;
      ObjectStream* os    = (ObjectStream*)b->user_data;
      if (!b->contentLen) {
         LOG(LOG_INFO, "detected EOF\n"); // readfunc done with IOBuf?
         os->flags |= OSF_EOF;                            // (or error)
         sem_post(&os->iob_full);
      }
      else
         LOG(LOG_INFO, "content-length (%ld) is non-zero\n", b->contentLen);
   }
   return result;
}


size_t streaming_writefunc(void* ptr, size_t size, size_t nmemb, void* stream) {
   IOBuf*        b     = (IOBuf*)stream;
   ObjectStream* os    = (ObjectStream*)b->user_data;
   size_t        total = (size * nmemb);
   LOG(LOG_INFO, "curl-buff %ld\n", total);

   // wait for user-buffer, supplied to stream_get()
   sem_wait(&os->iob_empty);
   LOG(LOG_INFO, "user-buff %ld\n", (b->len - b->write_count));

   // check for EOF on the object
   if (! total) {
      os->flags |= OSF_EOF;
      sem_post(&os->iob_full);
      LOG(LOG_INFO, "EOF done\n");
      return 0;
   }

   size_t avail    = total;     /* availble for reading from curl-buffer */
   char*  dst      = ptr;
   size_t writable = (b->len - b->write_count); /* space for writing in user-buffer */
   while (avail) {

      LOG(LOG_INFO, "iterating: writable=%ld, readble=%ld\n", writable, avail);

      // if user-buffer is full, wait for another one
      if (! writable) {
         LOG(LOG_INFO, "user-buff is full\n");
         sem_post(&os->iob_full);
         sem_wait(&os->iob_empty);
         writable = (b->len - b->write_count);
         continue;
      }

      // move data to user's buffer
      size_t move = ((writable < avail) ? writable : avail);
      aws_iobuf_append(b, dst, move);

      avail    -= move;
      writable -= move;
      dst      += move;
   }

   // curl-buffer is exhausted.
   LOG(LOG_INFO, "copied all of curl-buff (writable=%ld)\n", writable);
   if (writable)
      sem_post(&os->iob_empty);    // next callback is pre-approved
   else {
      os->flags |= OSF_EOF;
      sem_post(&os->iob_full);
   }
   return total;                /* to curl */
}



// Accept as much as <size>, from the streaming GET, into caller's <buf>.
// We may discover EOF at any time.  In that case, we'll return however
// much was actually read.  The next call
// will just short-circuit to return 0, signalling EOF to caller.
// 
// return -1 with errno, for failures.
// else return number of chars we get.
//
ssize_t stream_get(ObjectStream* os,
                  char*         buf,
                  size_t        size) {

   static const int get_timeout_sec = 10; /* totally made up out of thin air */

   IOBuf* b = &os->iob;     // shorthand

   LOG(LOG_INFO, "entry\n");
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }
   if (! (os->flags & OSF_READING)) {
      LOG(LOG_ERR, "%s isn't open for reading\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }
   if (os->flags & OSF_EOF)
      return 0; // b->write_count;


   aws_iobuf_reset(b);          // doesn't affect <user_data>
   aws_iobuf_extend_static(b, (char*)buf, size);
   LOG(LOG_INFO, "got %ld-byte buffer for writefn\n", size);

   // let writefn move data
   sem_post(&os->iob_empty);

   // wait for writefn to fill our buffer
   LOG(LOG_INFO, "waiting for writefn\n");
   SAFE_SEM_WAIT(&os->iob_full, get_timeout_sec);

   // 
   if (os->flags & OSF_EOF) {
      LOG(LOG_INFO, "EOF is asserted\n");
   }

   os->written += b->write_count;
   LOG(LOG_INFO, "returning %ld (total=%ld)\n", b->write_count, os->written);
   return (b->write_count);
}



// ---------------------------------------------------------------------------
// OPEN (read/write)
//
// "chunked transfer-encoding" suppresses the header that specifies the
// total size of the object.  Instead, curl implements the chunked
// transfer-concoding, to indicate the size of each individual transfer.
//
// NOTE: We assume OS.url has been initialized
//
// TBD: POSIX allows you to open a file and then seek to an offset.  We
//      could do that here (where open just opens the stream, and then,
//      e.g. stream_get() skips through everything until your offset), but
//      that would probably have truly awful performance, on huge objects.
//      A better plan is to present the offset in the curl header, and let
//      the server skip to our offset, before sending anything.  However,
//      that implies that stream_open wouldn't actually complete until the
//      first stream_get is issued.
//       
// ---------------------------------------------------------------------------

int stream_open(ObjectStream* os, IsPut put) {
   LOG(LOG_INFO, "%s\n", ((put) ? "PUT" : "GET"));

   if (os->flags & OSF_OPEN) {
      LOG(LOG_ERR, "%s is already open\n", os->url);
      errno = EINVAL;
      return -1;                // already open
   }
   if (os->flags) {
      assert(os->flags & OSF_CLOSED);
      LOG(LOG_INFO, "stream being re-opened with %s\n", os->url);
      stream_reset(os);
   }

   os->flags |= OSF_OPEN;
   os->written = 0;             // total read/written through OS
   if (put)
      os->flags |= OSF_WRITING;
   else
      os->flags |= OSF_READING;


   // readfunc/writefunc just get the IOBuf from libaws4c, but they need
   // the ObjectStream.  So IOBuf now has a pointer to allow this.
   os->iob.user_data = os;
   IOBuf* b = &os->iob;         // shorthand

   // install copy of global default-context as per-connection context 
   if (! b->context)
      aws_iobuf_context(b, aws_context_clone());

   AWSContext* ctx = b->context;
   s3_chunked_transfer_encoding_r(1, ctx);

   aws_iobuf_reset(b);          // doesn't affect <user_data> or <context>
   if (put) {
      sem_init(&os->iob_empty, 0, 0);
      sem_init(&os->iob_full,  0, 0);
      aws_iobuf_readfunc(b, &streaming_readfunc);
   }
   else {
      sem_init(&os->iob_empty, 0, 0);
      sem_init(&os->iob_full,  0, 0);
      aws_iobuf_headerfunc(b, &streaming_writeheaderfunc);
      aws_iobuf_writefunc(b, &streaming_writefunc);
   }

   // thread runs the GET/PUT, with the iobuf in <os>
   LOG(LOG_INFO, "starting thread\n");
   if (pthread_create(&os->op, NULL, &s3_op, os)) {
      LOG(LOG_ERR, "pthread_create failed: '%s'\n", strerror(errno));
      return -1;
   }
   return 0;
}


// ---------------------------------------------------------------------------
// SYNC
//
// This is like "flush" in the FUSE sense (i.e. no more I/O errors are
// possible), rather than "fflush" (i.e. wait for current buffers to be
// empty).  When stream_sync() returns, all I/O (ever) is completed on the
// stream.
//
// Fuse may call this before I/O is complete on the stream.  Therefore,
// we shouldn't just assume that failure to join means something is wedged.
//
// NOTE: New approach to reads: each call to marfs_read() will create a
//       distinct GET request.  Therefore, the op should return (because of
//       EOF for the given byte-range), at the end of each call, so we
//       should simply join the thread.
//
//       For marfs_write(), writes start at offset zero, and are expected
//       to be contiguous.  Therefore, we set up a streaming readfunc, and
//       we must signal EOF when closing.
//
//
// NOTE: If there are duplicated handles, fuse will call flush for each of
//       them.  That would imply that our pthread_join() may return EINVAL,
//       because another thread is already trying to join.  We do not currently
//       accomodate that.
// ---------------------------------------------------------------------------



// wait for the S3 GET/PUT to complete
int stream_sync(ObjectStream* os) {

   ///   // TBD: this should be a per-repo config-option
   ///   static const time_t timeout_sec = 5;

   // fuse may call fuse-flush multiple times (one for every open stream).
   // but will not call flush after calling close().
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   ///   IOBuf* b = &os->iob;         // shorthand

#if 0
   void* retval;
   if (! pthread_tryjoin_np(os->op, &retval)) {
      LOG(LOG_INFO, "op-thread joined\n");
      os->flags |= OSF_JOINED;
   }
   else {

      if (os->flags & OSF_WRITING) {
         // signal EOF to readfunc
         LOG(LOG_INFO, "(wr) waiting my turn\n");
         SAFE_SEM_WAIT_KILL(&os->iob_empty, timeout_sec, os);// sem_wait(&os->iob_empty);
         aws_iobuf_reset(b);      // doesn't affect <user_data>

         LOG(LOG_INFO, "(wr) sent EOF\n");
         sem_post(&os->iob_full);
         SAFE_SEM_WAIT_KILL(&os->iob_empty, timeout_sec, os); // sem_wait(&os->iob_empty);
      }
      //      else { // READING
      //         // signal EOF to writefunc
      //         LOG(LOG_INFO, "(rd) waiting my turn\n");
      //         SAFE_SEM_WAIT_KILL(&os->iob_full, timeout_sec, os); // sem_wait(&os->iob_full);
      //         aws_iobuf_reset(b);       // doesn't affect <user_data>
      //
      //         LOG(LOG_INFO, "(rd) sent EOF\n");
      //         sem_post(&os->iob_empty);
      //         SAFE_SEM_WAIT_KILL(&os->iob_full, timeout_sec, os); // sem_wait(&os->iob_full);
      //      }


      // check whether thread has returned.  Could mean a curl
      // error, an S3 protocol error, or server flaking out.
      LOG(LOG_INFO, "waiting for op-thread\n");
      if (stream_wait(os)) {
         LOG(LOG_ERR, "err from op-thread\n");
         return -1;
      }
   }

#elif 0
   // The code above assumes that failure to join means something is
   // wedged, and immediately starts dicking with the stream read/write
   // functions.  But they may be performing just fine, and simply haven't
   // completed yet.  In fact they might not even have started yet!
   //
   // The safer plan is what stream_wait() does: wait for a timeout
   // trying to join, and if that fails (or if user sends signal, e.g. with
   // ctl-C) to force-kill the op-thread.
   LOG(LOG_INFO, "waiting for op-thread\n");
   if (stream_wait(os)) {
      LOG(LOG_ERR, "err from op-thread. Killing.\n");
      pthread_kill(os->op, SIGKILL);
   }

#else
   // See NOTE, above, regarding the difference between reads and writes.
   void* retval;
   if (! pthread_tryjoin_np(os->op, &retval)) {
      LOG(LOG_INFO, "op-thread joined\n");
      os->flags |= OSF_JOINED;
   }
   else {

      if (os->flags & OSF_WRITING) {
         // signal EOF to readfunc
         LOG(LOG_INFO, "(wr) sending empty buffer (%x)\n", os->flags);
         if (stream_put(os, NULL, 0)) {
            LOG(LOG_ERR, "stream_put(0) failed\n");
            pthread_kill(os->op, SIGKILL);
            LOG(LOG_INFO, "killed thread\n");
         }
      }


      // check whether thread has returned.  Could mean a curl
      // error, an S3 protocol error, or server flaking out.
      LOG(LOG_INFO, "waiting for op-thread\n");
      if (stream_wait(os)) {
         LOG(LOG_ERR, "err from op-thread\n");
         return -1;
      }
   }
#endif

   // thread has completed
   os->flags |= OSF_JOINED;
   LOG(LOG_INFO, "op-thread returned %d\n", os->op_rc);
   errno = (os->op_rc ? EINVAL : 0);
   return os->op_rc;
}



// ---------------------------------------------------------------------------
// CLOSE
//
// *** WARNING: This assumes stream_sync() has already been called.
//
// If marfs is implementing fuse-flush, then fuse-flush should invoke
// stream_sync() and fuse-release (aka close) should invoke stream_close().
// If fuse-flush is not implemented, then Gary says fuse falls-back to
// using fuse-release for both tasks, so fuse-close should call
// stream_sync(), then stream_close().  We separate the functions here, to
// allow either approach.
// 
// ---------------------------------------------------------------------------

int stream_close(ObjectStream* os) {

   LOG(LOG_INFO, "entry\n");
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   sem_destroy(&os->iob_empty);
   sem_destroy(&os->iob_full);

   os->flags &= ~(OSF_OPEN);
   os->flags |= OSF_CLOSED;     /* so stream_open() can identify re-opens */

   LOG(LOG_INFO, "done (returning %d)\n", os->op_rc);
   errno = (os->op_rc ? EINVAL : 0);
   return os->op_rc;
}





// Reset everything except URL.  Also, use aws_iob_reset() to reset
// os->iob.
//
// NOTE: This is used when stream_open gets an OS that has been previously
//       opened and closed.  Therefore, we can assume that sems have been
//       destroyed, and thread has bben joined or killed.
//
void stream_reset(ObjectStream* os) {
   if (! (os->flags & OSF_CLOSED)) {
      LOG(LOG_ERR, "We require a stream that was previously opened\n");
      return;
   }

#if 0
   char*  before_ptr  = (char*)os;
   size_t before_size = (char*)os->url - (char*)os;

   char*  after_ptr   = (char*)os->url + MARFS_MAX_URL_SIZE;
   size_t after_size  = (char*)os + sizeof(ObjectStream) - after_ptr;

   memset(before_ptr, 0, before_size);
   memset(after_ptr,  0, after_size);

#else
   aws_iobuf_reset(&os->iob);
   os->op_rc = 0;
   os->written = 0;
   os->flags = 0;
#endif
}
