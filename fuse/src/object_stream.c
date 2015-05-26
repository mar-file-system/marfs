// _GNU_SOURCE defines pthread_timedjoin_np(), and pthread_tryjoin_np()
#define _GNU_SOURCE
#include <pthread.h>

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
     int rc = stream_wait_sem((SEM), (OS));     \
     if (rc)                                    \
        return (rc);                            \
  } while (0)




int stream_wait_sem(sem_t* sem, ObjectStream* os) {
   const struct timespec timeout;
   if (clock_gettime(CLOCK_REALTIME, &timeout))
      return -1;

   timeout.tv_sec += os->timeout_sec; // TBD

   // wait for a little while on the semaphore
   if (! sem_timedwait(sem, &timeout))
      return 0;              // got it

   if (errno != ETIMEDOUT)
      return -1;

   return 1;
}

#endif



// ---------------------------------------------------------------------------
// stream_wait_op()
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


#if 0

static
int stream_wait_op(ObjectStream* os) {
   struct timespec timeout;
   if (clock_gettime(CLOCK_REALTIME, &timeout))
      return -1;                // errno is set

   void* retval;
   while (1) {
      timeout.tv_sec += 1;

      // check whether thread has returned.  Could mean a curl
      // error, an S3 protocol error, or server flaking out.
      if (! pthread_timedjoin_np(os->op, &retval, &timeout))
         return 0;              // joined

      else if (errno == ETIMEDOUT)
         continue;              // timed-out

      else if (errno != EINVAL)
         return -1;             // error

      // EINVAL = another thread trying to join (?)  timed-wait will just
      // return immediately again, and we'll turn into a busy-wait.  Add an
      // explicit sleep here.  [Alternatively, here's another idea: If
      // multiple threads are waiting on this thread, then that implies the
      // fuse handle was dup'ed.  In that case, if we return success, won't
      // fuse still wait until all threads have flushed?  I don't trust my
      // knowledge of fuse enough to depend on that.]
      else
         sleep(1);
   }
}

#else

static
int stream_wait_op(ObjectStream* os) {

   void* retval = NULL;
   while (1) {

      // check whether thread has returned.  Could mean a curl
      // error, an S3 protocol error, or server flaking out.
      if (! pthread_join(os->op, &retval))
         return 0;              // joined

      else if (errno != EINVAL)
         return -1;             // error

      // EINVAL = another thread trying to join (?)  timed-wait will just
      // return immediately again, and we'll turn into a busy-wait.  Add an
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

int s3_op_internal(ObjectStream* os) {
   IOBuf*        b  = &os->iob;

   int is_put = (os->flags & OSF_WRITING);
   if (is_put) {
      LOG(LOG_INFO, "streaming PUT %lx, '%s'\n", b, os->url);
      AWS4C_CHECK1   ( s3_put(b, os->url) ); /* create empty object with user metadata */
   }
   else {
      LOG(LOG_INFO, "streaming GET %lx, '%s'\n", b, os->url);
      AWS4C_CHECK1   ( s3_get(b, os->url) ); /* create empty object with user metadata */
   }

   if (! AWS4C_OK(b)) {
      LOG(LOG_ERR, "CURL ERROR: %d '%s'\n", b->code, b->result);
      return -1;
   }

   LOG(LOG_INFO, "streaming %s complete\n", ((is_put) ? "PUT" : "GET"));
   return 0;
}

// this runs as a separate thread, so that stream_open() can return
void* s3_op(void* arg) {
   ObjectStream* os = (ObjectStream*)arg;
   IOBuf*        b  = &os->iob;

   if ((os->op_rc = s3_op_internal(os)))
      LOG(LOG_ERR, "s3_op(%s) failed '%s'\n", os->url, b->result);
   else
      LOG(LOG_INFO, "s3_op(%s) done\n", os->url);

   return os;
}


// ---------------------------------------------------------------------------
// PUT (write)
//
// This is installed as the "readfunc" which is called by curl, whenever it
// needs more data for a PUT.  (It's a "read" of data from curl's
// perspective.)  This function is invoked in it's own thread by curl.  We
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
   fprintf(stderr, "--- readfn curl buff %ld\n", total);

   // wait for producer to fill buffers
   sem_wait(&os->iob_full);
   fprintf(stderr, "--- readfn avail-data: %ld\n", b->avail);

   if (b->write_count == 0) {
      fprintf(stderr, "--- readfn got EOF\n");
      sem_post(&os->iob_empty); // polite
      return 0;
   }

   // move producer's data into curl buffers.
   // (Might take more than one callback)
   size_t move_req = ((total <= b->avail) ? total : b->avail);
   size_t moved    = aws_iobuf_get_raw(b, (char*)ptr, move_req);

   // track total size
   os->written += moved;

   if (b->avail) {
      fprintf(stderr, "--- readfn iterating\n");
      sem_post(&os->iob_full);  // next callback is pre-approved
   }
   else {
      fprintf(stderr, "--- readfn done with buffer (total written %ld)\n",
              os->written);
      sem_post(&os->iob_empty); // tell producer that buffer is used
   }

   return moved;
}

// Hand <buf> over to the streaming_readfunc(), so it can be added into
// the ongoing streaming PUT.  You must call stream_open() first.
//
// NOTE: Doing this a little differently from the test_aws.c (case 12)
//       approach.  We're forcing *synchronous* interaction with the
//       readfunc, because we don't want caller's <buf> to go out of scope
//       until the readfunc is finished with it.
//
int stream_put(ObjectStream* os,
               const char*   buf,
               size_t        size) {

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
   fprintf(stderr, "--- stream_put appended data for readfn\n");

   // let readfunc move data
   sem_post(&os->iob_full);

   fprintf(stderr, "--- stream_put waiting for IOBuf\n"); // readfunc done with IOBuf?
   sem_wait(&os->iob_empty);

   return 0;
}


// ---------------------------------------------------------------------------
// GET (read)

// curl is calling us with some incoming data on a GET, which we are
// supposed to "write" somewhere.  We interact with stream_get(),
// to write our data into a buffer that a caller provided to
// stream_get().
//
// This function is more complex than streaming_readfunc(), because we
// don't return to curl until we have exhausted curl's buffer.  [because
// doc says curl treats anything less than that as an error.  Should test
// that.]  That means is may require multiple calls to stream_get(), before
// we can write all of curl's buffer.
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
// ---------------------------------------------------------------------------

size_t streaming_writefunc(void* ptr, size_t size, size_t nmemb, void* stream) {
   LOG(LOG_INFO, "entry\n");

   IOBuf*        b     = (IOBuf*)stream;
   ObjectStream* os    = (ObjectStream*)b->user_data;
   size_t        total = (size * nmemb);
   fprintf(stderr, "--- writefn curl-buff %ld\n", total);

   // wait for user-buffer, supplied to stream_get()
   sem_wait(&os->iob_empty);
   fprintf(stderr, "--- writefn user-buff %ld\n", (b->len - b->write_count));

   // check for EOF on the object
   if (! total) {
      os->flags |= EOF;
      sem_post(&os->iob_full);
      fprintf(stderr, "--- writefn EOF done\n");
      return 0;
   }

   size_t avail = total;
   char*  dst   = ptr;
   while (avail) {

      size_t writable = (b->len - b->write_count);
      fprintf(stderr, "--- writefn iterating: writable=%ld, readble=%ld\n",
              writable, avail);

      // if user-buffer is full, wait for another one
      if (! writable) {
         fprintf(stderr, "--- writefn user-buff is full\n");
         sem_post(&os->iob_full);
         sem_wait(&os->iob_empty);
         continue;
      }

      size_t move = ((writable < avail) ? writable : avail);
      aws_iobuf_append(b, dst, move);

      avail -= move;
      dst   += move;
   }

   // curl-buffer is exhausted.  Ready for next callback
   fprintf(stderr, "--- writefn copied all of curl-buff\n");
   sem_post(&os->iob_full);
   return total;
}


// Accept as much as <size>, from the streaming GET, into caller's <buf>.
// We may discover EOF at any time.  In that case, we'll return however
// much was actually read.  The next call
// will just short-circuit to return 0, signalling EOF to caller.
// 
// return -1 with errno, for failures.
ssize_t stream_get(ObjectStream* os,
                  char*         buf,
                  size_t        size) {
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
      return 0;

   IOBuf* b     = &os->iob;     // shorthand

   aws_iobuf_reset(b);          // doesn't affect <user_data>
   aws_iobuf_extend_static(b, (char*)buf, size);
   fprintf(stderr, "--- stream_get got %ld-byte buffer for writefn\n", size);

   // let writefn move data
   sem_post(&os->iob_empty);

   // wait for writefn to fill our buffer
   fprintf(stderr, "--- stream_get waiting for writefn\n");
   sem_wait(&os->iob_full);
   fprintf(stderr, "--- stream_get got %ld bytes\n", b->write_count);

   // 
   if (os->flags & OSF_EOF) {
   fprintf(stderr, "--- stream_get got %ld bytes\n", b->write_count);
   }

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
   LOG(LOG_INFO, "entry\n");

   if (os->flags & OSF_OPEN) {
      LOG(LOG_ERR, "%s is already open\n", os->url);
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

   aws_iobuf_reset(b);          // doesn't affect <user_data>
   aws_iobuf_chunked_transfer_encoding(b, 1);

   if (put) {
      sem_init(&os->iob_empty, 0, 1);
      sem_init(&os->iob_full,  0, 0);
      aws_iobuf_readfunc (b, &streaming_readfunc);
   }
   else {
      sem_init(&os->iob_empty, 0, 0);
      sem_init(&os->iob_full,  0, 0);
      aws_iobuf_writefunc(b, &streaming_writefunc);
   }

   // thread runs the GET/PUT
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
// This is like "flush" in the fuse sense (i.e. no more I/O errors are
// possible), rather than "fflush" (i.e. wait for current buffers to be
// empty).  When stream_sync() returns, all I/O is completed on the stream.
//
// NOTE: If there are duplicated handles, fuse will call flush for each of
//       them.  That would imply that our pthread_join() may return EINVAL,
//       because another thread is already trying to join.  We do not currently
//       accomodate that.
// ---------------------------------------------------------------------------

// wait for the S3 GET/PUT to complete
int stream_sync(ObjectStream* os) {

   // fuse may call fuse-flush multiple times (one for every open stream).
   // but will not call flush after calling close().
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }


#if 1
   IOBuf* b = &os->iob;         // shorthand
   void* retval;

   if (! pthread_tryjoin_np(os->op, &retval)) {
      fprintf(stderr, "--- stream_close op-thread joined\n");
      os->flags |= OSF_JOINED;
   }
   else if (os->flags & OSF_WRITING) {
      // signal EOF to readfunc
      fprintf(stderr, "--- stream_close(wr) waiting my turn\n");
      sem_wait(&os->iob_empty);
      aws_iobuf_reset(b);      // doesn't affect <user_data>

      fprintf(stderr, "--- stream_close(wr) sent EOF\n");
      sem_post(&os->iob_full);
      sem_wait(&os->iob_empty);
   }
   else { // READING
      // signal EOF to writefunc
      fprintf(stderr, "--- stream_close(rd) waiting my turn\n");
      sem_wait(&os->iob_full);
      aws_iobuf_reset(b);       // doesn't affect <user_data>

      fprintf(stderr, "--- stream_close(rd) sent EOF\n");
      sem_post(&os->iob_full);
      sem_wait(&os->iob_empty);
   }

#endif

   // check whether thread has returned.  Could mean a curl
   // error, an S3 protocol error, or server flaking out.
   fprintf(stderr, "--- stream_sync waiting for op-thread\n");
   if (! (os->flags & OSF_JOINED)
       && stream_wait_op(os)) {
      return -1;
   }

   // thread has completed
   os->flags |= OSF_JOINED;
   fprintf(stderr, "--- stream_sync op-thread returned %d\n", os->op_rc);
   errno = (os->op_rc ? EINVAL : 0);
   return os->op_rc;
}



// ---------------------------------------------------------------------------
// CLOSE
//
// This assumes stream_sync() has already been called.  If marfs is
// implementing fuse-flush, then fuse-flush should invoke stream_sync() and
// fuse-release (aka close) should invoke stream_close().  If fuse-flush is
// not implemented, then Gary says fuse falls-back to using fuse-release
// for both tasks, so fuse-close should call stream_sync(), then
// stream_close().  We separate the functions here, to allow either
// approach.
// 
// ---------------------------------------------------------------------------

int stream_close(ObjectStream* os) {

   LOG(LOG_INFO, "entry\n");
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   os->flags &= ~(OSF_OPEN);
   os->flags |= OSF_CLOSED;     /* so stream_open() can identify re-opens */

   fprintf(stderr, "--- stream_close done (returning %d)\n", os->op_rc);
   errno = (os->op_rc ? EINVAL : 0);
   return os->op_rc;
}





// reset everything except URL.  This is used when stream_open gets an OS
// that has been opened and closed.
void stream_reset(ObjectStream* os) {
   char*  before_ptr  = (char*)os;
   size_t before_size = (char*)os->url - (char*)os;

   char*  after_ptr   = (char*)os->url + MARFS_MAX_URL_SIZE;
   size_t after_size  = (char*)os + sizeof(ObjectStream) - after_ptr;

   memset(before_ptr, 0, before_size);
   memset(after_ptr,  0, after_size);
}
