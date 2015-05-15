// #include "object_stream.h"
#include "common.h"

#include <stdlib.h>
#include <errno.h>
#include <string.h>

// This just allows us to use AWS_CHECK(), returning an int, where s3_op
// has to return return void*.  We return 0 for success, non-zero for
// failure.
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
      LOG(LOG_INFO, "CURL ERROR: %d '%s'\n", b->code, b->result);
      return EIO;
   }

   LOG(LOG_INFO, "streaming %s complete\n", ((is_put) ? "PUT" : "GET"));
   return 0;
}

// this runs as a separate thread, so that open_object_stream() can return
void* s3_op(void* arg) {
   ObjectStream* os = (ObjectStream*)arg;
   IOBuf*        b  = &os->iob;
   int rc;
   if ((rc = s3_op_internal(os))) {
      LOG(LOG_INFO, "s3_op(%s) failed '%s'\n", os->url, b->result);
      return os;                // caller already has <os> anyhow.
   }
   LOG(LOG_INFO, "s3_op(%s) done\n", os->url);
   return NULL;                 // (success)
}


// This is installed as the "readfunc" which is called by curl, whenever it
// needs more data for a PUT.  (It's a "read" of data from curl's
// perspective.)  This function is invoked in it's own thread by curl.  We
// synchronize with stream_to_object(), where a user has more data to be
// added to a stream.  The user's buffer may be larger than the size of the
// buffer curl gives us to fill, in which case we self-enable, so the next
// call-back can happen immediately.  Finally, stream_close gives us an
// empty buffer, which tells us to signal EOF to curl, which we do by
// returning 0.
size_t streaming_readfunc(void* ptr, size_t size, size_t nmemb, void* stream) {
   LOG(LOG_INFO, "entry\n");

   IOBuf*        b     = (IOBuf*)stream;
   ObjectStream* os    = (ObjectStream*)b->etc;
   size_t        total = (size * nmemb);
   fprintf(stderr, "--- consumer curl buff %ld\n", total);

   // wait for producer to fill buffers
   sem_wait(&os->iob_full);
   fprintf(stderr, "--- consumer avail-data: %ld\n", b->avail);

   if (b->write_count == 0) {
      fprintf(stderr, "--- consumer got EOF\n");
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
      fprintf(stderr, "--- consumer iterating\n");
      sem_post(&os->iob_full);  // next callback is pre-approved
   }
   else {
      fprintf(stderr, "--- consumer done with buffer\n");
      sem_post(&os->iob_empty); // tell producer that buffer is used
   }

   return moved;
}


size_t streaming_writefunc(void* ptr, size_t size, size_t nmemb, void* stream) {
   LOG(LOG_ERR, "ERR not implemented\n");
   exit(1);
}

// "chunked transfer-encoding" suppresses the header that specifies the
// total size of the object.  Instead, curl implements the
// transfer-concoding, to indicate the size of each individual transfer.
//
// NOTE: We assume OS.url has been initialized
//
int open_object_stream(ObjectStream* os, IsPut put) {
   LOG(LOG_INFO, "entry\n");

   if (os->flags & OSF_OPEN) {
      LOG(LOG_ERR, "%s is already open\n", os->url);
      return -1;                // already open
   }
   os->flags |= OSF_OPEN;
   if (put) {
      os->flags |= OSF_WRITING;
      os->written = 0;
   }


   // readfunc/writefunc just get the IOBuf from libaws4c, but they need
   // the ObjectStream.  So IOBuf now has a pointer to allow this.
   os->iob.etc = os;
   IOBuf* b = &os->iob;         // shorthand

   sem_init(&os->iob_empty, 0, 1);
   sem_init(&os->iob_full,  0, 0);

   aws_iobuf_reset(b);
   aws_iobuf_chunked_transfer_encoding(b, 1);

   if (put)
      aws_iobuf_readfunc (b, &streaming_readfunc);
   else
      aws_iobuf_writefunc(b, &streaming_writefunc);

   // thread runs the GET/PUT
   LOG(LOG_INFO, "starting thread\n");
   if (pthread_create(&os->op, NULL, &s3_op, os)) {
      LOG(LOG_ERR, "pthread_create failed: '%s'\n", strerror(errno));
      return errno;
   }
   return 0;
}

// Hand <buf> over to the streaming_readfunc(), so it can be added into
// the ongoing streaming PUT.  You must call open_object_stream() first.
//
// NOTE: Doing this a little differently from the test_aws.c (case 12)
//       approach.  We're forcing *synchronous* interaction with the
//       readfunc, because we don't want caller's <buf> to go out of scope
//       until the readfunc is finished with it.
int stream_to_object(ObjectStream* os,
                     const char*   buf,
                     size_t        size) {

   LOG(LOG_INFO, "entry\n");
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      return EINVAL;            /* ?? */
   }
   IOBuf* b = &os->iob;         // shorthand

   // install buffer into IOBuf
   aws_iobuf_reset(b);
   b->etc = os;                 // restore value, wiped by aws_iobuf_reset()
   aws_iobuf_append_static(b, (char*)buf, size);
   fprintf(stderr, "--- producer appended data for consumer\n");

   // let readfunc move data
   sem_post(&os->iob_full);

   fprintf(stderr, "--- producer waiting for IOBuf\n"); // readfunc done with IOBuf?
   sem_wait(&os->iob_empty);

   return 0;
}


int stream_from_object(ObjectStream* os,
                       const char*   buf,
                       size_t        size) {
   LOG(LOG_ERR, "TBD: stream_from_object\n");
   return 0;
}

int close_object_stream(ObjectStream* os) {

   LOG(LOG_INFO, "entry\n");
   if (! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      return EINVAL;            /* ?? */
   }

   IOBuf* b = &os->iob;         // shorthand

   // singal EOF to consumer
   sem_wait(&os->iob_empty);
   aws_iobuf_reset(b);
   b->etc = os;                 // restore value, wiped by aws_iobuf_reset()

   fprintf(stderr, "--- producer sent EOF\n");
   sem_post(&os->iob_full);

   sem_wait(&os->iob_empty);
   fprintf(stderr, "--- producer done\n");
   return 0;
}
