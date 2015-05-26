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

//#include "common.h"
#include <aws4c.h>
#include <pthread.h>
#include <semaphore.h>

#  ifdef __cplusplus
extern "C" {
#  endif




typedef enum {
   OSF_OPEN       = 0x01,
   OSF_WRITING    = 0x02,
   OSF_READING    = 0x04,
   OSF_EOF        = 0x08,
   OSF_JOINED     = 0x10,
   OSF_CLOSED     = 0x20,
} OSFlags;


typedef struct {
   // This comes from libaws4c
   IOBuf             iob;       // should be VOLATILE ?
   sem_t             iob_empty; // e.g. stream_to_write() can add data?
   sem_t             iob_full;  // e.g. curl readfunc can copy to PUT-stream
   pthread_t         op;        // GET/PUT
   int               op_rc;     // typically 0 or -1  (see iob.result, for curl/S3 errors)
   char              url[MARFS_MAX_URL_SIZE]; // WARNING: only valid during open_object()
   size_t            written;   // bytes written to stream
   // size_t         req_size;  // 
   volatile OSFlags  flags;
} ObjectStream;


typedef enum {
   OS_GET = 0,
   OS_PUT = 1
} IsPut;



// initialize os.url, before calling
int     stream_open(ObjectStream* os, IsPut put);

int     stream_put(ObjectStream* os, const char* buf, size_t size);
ssize_t stream_get(ObjectStream* os, char* buf,       size_t size);

int     stream_sync(ObjectStream* os);
int     stream_close(ObjectStream* os);





#  ifdef __cplusplus
}
#  endif

#endif // _MARFS_OBJECTS_H
