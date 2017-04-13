#ifndef _MARFS_DAL_H
#define _MARFS_DAL_H

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
// MarFS  Data Abstraction Layer (DAL)
//
// This is an abstract interface used for interaction with the Storage part
// of MarFS.  Just as the MDAL provides an abstract interface to the MarFS
// metadata, this abstraction is used when marfs writes actual data to
// storage.  The DAL implementations need only do the simplest part of
// what their interface suggests.
// ---------------------------------------------------------------------------

#include "marfs_configuration.h" // DAL_Type
#include "xdal_common.h"

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <utime.h>

#include <stdio.h>

#if USE_MC
#include "marfs_base.h"  // MARFS_ constants
#include "marfs_locks.h" // SEM_T

// The mc path will be the host field of the repo plus an object id.
// We need a little extra room to account for numbers that will get
// filled in to create the path template, 128 characters should be
// more than enough.
#define MC_MAX_PATH_LEN        (MARFS_MAX_OBJID_SIZE+MARFS_MAX_HOST_SIZE+128)
#define MC_MAX_LOG_LEN         (MC_MAX_PATH_LEN+512)

// The log format is:
// <object-path-template>\t<n>\t<e>\t<start-block>\t<error-pattern>\t<repo-name>\t<pod>\t<capacity-unit>\t\n
#define MC_DEGRADED_LOG_FORMAT "%s\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t\n"
#define MC_LOG_SCATTER_WIDTH   400

#endif // USE_MC

#  ifdef __cplusplus
extern "C" {
#  endif

#if USE_MC
// Need to export the mc_config struct here for the rebuild utility to
// know how many pods and capacity units are in each repo when
// generating statistics about failures.
typedef struct mc_config {
   unsigned int n;
   unsigned int e;
   unsigned int num_pods;
   unsigned int num_cap;
   unsigned int scatter_width;
   char        *degraded_log_path;
   int          degraded_log_fd;
   SEM_T        lock;
} MC_Config;

#endif // USE_MC


// DAL_Context
//
// This is per-file-handle DAL-state.  Think of it as equivalent to a
// file-descriptor. Individual implementations might need extra state, to
// be shared across some or all calls with the same file-handle (e.g. POSIX
// needs a file-descriptor).  Our first cut at this is to provide an extra
// Context argument to all DAL calls.  Individual implementations can
// allocate storage here for whatever state they need.
//
// There is also global-state in each DAL struct (not to be confused with
// DAL_Context).  This can be initialized (if needed) in dal_ctx_init().
// It will initially be NULL.  If desired, one could use dal_ctx_init()
// to associate individual contexts to have a link to the global state.
//
// The MarFS calls to the DAL implementation functions will not touch the
// contents of the Context, except that we call init_dal_context() when
// MarFS file-handles are created, and delete_dal_context() when MarFS
// file-handles are destroyed.
//

typedef struct {
   uint32_t  flags;
   union {
      void*     ptr;
      size_t    sz;
      ssize_t   ssz;
      uint32_t  u;
      int32_t   i;
   } data;
} DAL_Context;


// DAL function signatures



// fwd-decl
struct DAL;



// All DALs must now have a "config" method, which is called at
// read-config-time, passing a vector of xDALConfigOpts, parsed from the
// XML of the config file.
//
// Within the configuration for a repo, you can optionally add a DAL spec,
// which can optionally contain one or more key-value, or value fields.
// What used to be the named DAL-type is now put into a <type> field.  Each
// repo has its own copy of the "master" DAL defined in dal.c, so
// configuration is only done to the local copy owned by a repo.
//
// <repo>
//   ...
//   <dal>
//     <type>SOME_TYPE</type>
//     <opt> <key_val> key1 : value1 </key_val> </opt>
//     <opt> <key_val> key2 : value2 </key_val> </opt>
//     <opt> <value>   value3  </value> </opt>
//   </dal>
// </repo>
//
// The DAL config function is called once in the lifetime of the per-repo
// DAL copy, when it is being installed in a repo, during reading of the
// MarFS configuration.  The options in the config file are generic, to
// allow options to be provided to arbitrary DAL implementations without a
// need for code-changes.  The vector of options are dynamically allocated.
// You can store the ptr (e.g. in DAL.global_state) if you want to keep
// them.  Otherwise, you should free() them.  (See default_dal_config().)

typedef  int     (*dal_config) (struct DAL*     dal,
                                xDALConfigOpt** opts,
                                size_t          opt_count);


// used to check whether an MDAL uses the default-config, so that we can
// reliably print out the options with which it was configured, for
// diagnostics.
extern int   default_dal_config(struct DAL*     dal,
                                xDALConfigOpt** opts,
                                size_t          opt_count);



// initialize/destroy context, if desired.
//
//   -- init      called before any other ops (per file-handle).
//   -- destroy   called after all other ops  (per file-hanfle).
//
#if 0
typedef  int     (*dal_ctx_init)   (DAL_Context* ctx, struct DAL* dal);
typedef  int     (*dal_ctx_destroy)(DAL_Context* ctx, struct DAL* dal);

#else
typedef  int     (*dal_ctx_init)   (DAL_Context* ctx, struct DAL* dal, void* fh);
typedef  int     (*dal_ctx_destroy)(DAL_Context* ctx, struct DAL* dal);
#endif


// --- storage ops

// return NULL from dal_open(), for failure
// This value is only checked for NULL/non-NULL
//
// TBD: This should probably use va_args, to improve generality For now
//      we're just copying the object_stream inteface verbatim.
//
// chunk_offset is the offset within the object to begin a subsequent
// read (or write, if supported?) operation. Will probably not be
// called with non-zero if opening for writing since marfs does not
// currently support writes to arbitratry offests, or appends.
//
// Should set the OSF_OPEN and one of OSF_READING or OSF_WRITING flags
// in ObjectStream->flags for the object stream associated with the
// context.
//
// Additionally the implementation must guard itself against OS
// structures that were previously used and have the OSF_CLOSED flag
// Asserted. In the abscence of OSF_CLOSED, any other flags that are
// asserted (with the exception of OSF_RLOCK_INIT) should be
// considered an error and open should fail with errno=EBADF. If given
// an object stream with the OSF_CLOSED flag then it is also the
// responsibility of the implementation to set ObjectStream->written
// to 0 if preserve_write_count == 0.
typedef int      (*dal_open) (DAL_Context* ctx,
                              int          is_put,
                              size_t       chunk_offset,
                              size_t       content_length,
                              uint8_t      preserve_write_count,
                              uint16_t     timeout);

// Writes the data in the buffer to the object.
//
// Return the number of bytes written, or -1 on error and set errno to
// something appropriate.
//
// MarFS expects the ->written field in the ObjectStream associated
// with the context to be incremented by the number of bytes written.
//
// TODO: check whether stream_put sets any relevant flags in the case
//       of a put failure.
//
// TBD:  return ssize_t, to more-closely support size_t <size> arg.
typedef int      (*dal_put)  (DAL_Context*  ctx,
                              const char*   buf,
                              size_t        size);

// Read from an object stream begining at the offset supplied to
// ->open().
//
// Returns the number of bytes read, or zero to indicate the end of
// the object has been reached. Returns -1 on failure and sets errno.
//
// MarFS expects the ->written field in the ObjectStream associated
// with the context to be incremented by the number of bytes read.  If
// EOF is encountered then this must set OSF_EOF in ->flags for the
// OS.
//
// NOTE: It is possible that other flags are set by the streaming
// functions. for example: OSF_EOF may be used to indicate a read or
// write error (object_stream.c:633)?
typedef ssize_t  (*dal_get)  (DAL_Context*  ctx,
                              char*         buf,
                              size_t        size);

// This is the last chance to return an error. After this is called
// all I/O on the object stream should be done (including any flushes
// that might be needed before close).
typedef int      (*dal_sync) (DAL_Context*  ctx);

// Abort an open object stream. This means we are canceling any
// read/write operations. This is only every called when there is
// nothing yet written to the stream, but theoretically it should be
// able to roll back any data that has been written and close the
// stream without creating a persisten object.
//
// Must set the OSF_ABORT flag in ObjectStream->flags
typedef int      (*dal_abort)(DAL_Context*  ctx);

// Should do whatever is necessary to insure that the object stream is
// closed. If given an OS that is open should fail with errno=EBADF.
//
// Returns 0 on success or -1 for failure.
//
// Must set the OSF_CLOSED flag in ObjectStream->flags for the
// associated OS.
typedef int      (*dal_close)(DAL_Context*  ctx);

// Updates the location of the object on the underlying storage for
// future calls to ->open, ->write, or ->read. Does not mean move the
// object. This should generally be called before any call to ->open
// and after a call to update_pre().
typedef int      (*dal_update_object_location)(DAL_Context* ctx);

// init() is called first, and destroy() after.
typedef int      (*dal_delete)(DAL_Context*  ctx);



// This is a collection of function-ptrs
// They capture a given implementation of interaction with an MDFS.
typedef struct DAL {
   // DAL_Type             type;
   const char*                name;
   size_t                     name_len;

   void*                      global_state;

   dal_config                 config;
   dal_ctx_init               init;
   dal_ctx_destroy            destroy;

   dal_open                   open;
   dal_sync                   sync;
   dal_abort                  abort;
   dal_close                  close;
   dal_put                    put;
   dal_get                    get;
   dal_delete                 del;

   dal_update_object_location update_object_location;

} DAL;


// insert a new DAL, if there are no name-conflicts
int  install_DAL(DAL* dal);

// find a DAL with the given name
DAL* get_DAL(const char* name);




// exported for building custom DAL
int     default_dal_ctx_init   (DAL_Context* ctx, DAL* dal, void* fh);
int     default_dal_ctx_destroy(DAL_Context* ctx, DAL* dal);



#  ifdef __cplusplus
}
#  endif

#endif
