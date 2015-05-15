// ---------------------------------------------------------------------------
// These handle the guts of MarFS object-interactions for fuse and pftool,
// using libaws4c.  What we're adding on top of libaws4c is "streaming".
// This is needed because a large object must be written (or read) with a
// single PUT (or GET).  We don't want to require pftool to buffer the
// entire contents of an object before writing, nor fuse write() to create
// a complete object per call.
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
//      case, stream_to_obj would signal readfunc to go ahead, and return
//      immediately.  Next write would work on the other IOBuf.  See
//      example-code in test_aws.c, case 12.
//
// TBD: Should ObjectStream.iob be volatile?  Test code worked without that,
//      but future changes here might introduce subtle bugs without it.
//
// ---------------------------------------------------------------------------

#ifndef _MARFS_OBJECTS_H
#define _MARFS_OBJECTS_H

#include "common.h"
#include <aws4c.h>
#include <pthread.h>
#include <semaphore.h>

#  ifdef __cplusplus
extern "C" {
#  endif




typedef enum {
   OSF_OPEN       = 0x01,
   OSF_WRITING    = 0x02,       // else, reading
} OSFlags;


typedef struct {
   // This comes from libaws4c
   IOBuf             iob;       // should be VOLATILE ?
   sem_t             iob_empty; // e.g. stream_to_write() can add data?
   sem_t             iob_full;  // e.g. curl readfunc can copy to PUT-stream
   pthread_t         op;        // GET/PUT
   char              url[MARFS_MAX_URL_SIZE]; // WARNING: only valid during open_object()
   size_t            written;   // bytes written to stream
   volatile OSFlags  flags;
} ObjectStream;


typedef enum {
   OS_GET = 0,
   OS_PUT = 1
} IsPut;



// initialize os.url, before calling
int open_object_stream(ObjectStream* os, IsPut put);

int stream_to_object(ObjectStream* os,
                     const char*   buf,
                     size_t        size);

int stream_from_object(ObjectStream* os,
                       const char*   buf,
                       size_t        size);

int close_object_stream(ObjectStream* os);





#  ifdef __cplusplus
}
#  endif

#endif // _MARFS_OBJECTS_H
