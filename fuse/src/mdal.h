#ifndef _MARFS_MDAL_H
#define _MARFS_MDAL_H

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
// MarFS  MetaData Abstraction Layer (MDAL)
//
// This is an abstract interface used for interaction with the MD part of
// MarFS.  MarFS will already do checking to avoid e.g. calling open() on a
// MarFS file-handle which has already been opened, or calling read/write
// after an open fails.  And so forth.  The MDAL implementations need only
// do the simplest part of what their interface suggests.
// ---------------------------------------------------------------------------


#include "marfs_configuration.h" // MDAL_Type

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include <dirent.h>

#include <stdio.h>


#  ifdef __cplusplus
extern "C" {
#  endif


// MDAL_Context
//
// This is per-file-handle state.  Think of it as equivalent to a
// file-descriptor. Individual implementations might need extra state, to
// be shared across some or all calls with the same file-handle (e.g. POSIX
// needs a file-descriptor).  Our first cut at this is to provide an extra
// Context argument to all MDAL calls.  Individual implementations can
// allocate storage here for whatever state they need.
//
// There is also global-state in each MDAL struct (not to be confused with
// MDAL_Context).  This can be initialized (if needed) in mdal_ctx_init().
// It will initially be NULL.  If desired, one could use mdal_ctx_init()
// to associate individual contexts to have a link to the global state.
//
// The MarFS calls to the MDAL implementation functions will not touch the
// contents of the Context, except that we call init_mdal_context() when
// MarFS file-handles are created, and delete_mdal_context() when MarFS
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
} MDAL_Context;


// MDAL function signatures
//
// Q: Does it really matter if the MDAL_Context is the first argument?
//
// A: There are macros in common.h which assume this.  Of course, they
//    could be extended, if it's *really* that important.



// fwd-decl
struct MDAL;

// used to fill out directory-entries, in marfs_readdir().
typedef int (*marfs_fill_dir_t) (void *buf, const char *name,
                                 const struct stat *stbuf, off_t off);


// initialize/destroy context, if desired.
//
//   -- init    is called before any other ops (per file-handle).
//   -- destroy is called when a file-handle is being destroyed.
//
typedef  int     (*mdal_file_ctx_init)   (MDAL_Context* ctx, struct MDAL* mdal);
typedef  int     (*mdal_dir_ctx_init)    (MDAL_Context* ctx, struct MDAL* mdal);

typedef  int     (*mdal_file_ctx_destroy)(MDAL_Context* ctx, struct MDAL* mdal);
typedef  int     (*mdal_dir_ctx_destroy) (MDAL_Context* ctx, struct MDAL* mdal);


// --- file ops

// return NULL from mdal_open(), for failure 
// This value is only checked for NULL/non-NULL
typedef  void*   (*mdal_open) (MDAL_Context* ctx, const char* path, int flags);
typedef  int     (*mdal_close)(MDAL_Context* ctx);

typedef  ssize_t (*mdal_write)(MDAL_Context* ctx, const void* buf, size_t count);
typedef  ssize_t (*mdal_read) (MDAL_Context* ctx, void*       buf, size_t count);

typedef  int     (*mdal_ftruncate)(MDAL_Context* ctx, off_t length);
typedef  off_t   (*mdal_lseek)(MDAL_Context* ctx, off_t offset, int whence);

// some tests.  Return non-zero for TRUE.
// TBD: gather these into a single function, with a test_type argument?
//      (easier to default/extend?)
typedef  int     (*mdal_is_open) (MDAL_Context* ctx);


// --- file-ops (context-free)
//
// These ops have no MDAL_Context (which means no MarFS file-handle).  They
// just operate on raw pathnames.  The argument will be the full path to
// the MDFS file.

typedef  int     (*mdal_rename)(const char* from, const char* to);

typedef  ssize_t (*mdal_lgetxattr)   (const char* path, const char* name,
                                      void* value, size_t size);
typedef  ssize_t (*mdal_lsetxattr)   (const char* path, const char* name,
                                      const void* value, size_t size, int flags);
typedef  int     (*mdal_lremovexattr)(const char* path, const char* name);
typedef  ssize_t (*mdal_llistxattr)  (const char* path, char* list, size_t size);




// --- directory-ops
// opendir() should return some non-null pointer for success, NULL for failure.
// (It won't be used for anything.).
// The others return 0 for success, -1 (plus errno) for failure.
// Any required state must be maintained in the context.

typedef  int     (*mdal_mkdir)  (MDAL_Context* ctx, const char* path, mode_t mode);
typedef  void*   (*mdal_opendir)(MDAL_Context* ctx, const char* path);
typedef  int     (*mdal_readdir)(MDAL_Context*      ctx,
                                 const char*        path,
                                 void*              buf,
                                 marfs_fill_dir_t   filler,
                                 off_t              offset);
// typedef  int     (*mdal_readdir_r)(MDAL_Context* ctx,
//                                           struct dirent* entry, struct dirent** result);
typedef  int     (*mdal_closedir)(MDAL_Context* ctx);






// This is a collection of function-ptrs
// They capture a given implementation of interaction with an MDFS.
typedef struct MDAL {
   MDAL_Type          type;
   void*              global_state;

   mdal_file_ctx_init    f_init;
   mdal_file_ctx_destroy f_destroy;

   mdal_dir_ctx_init     d_init;
   mdal_dir_ctx_destroy  d_destroy;

   mdal_open          open;
   mdal_close         close;
   mdal_read          read;
   mdal_write         write;
   mdal_ftruncate     ftruncate;
   mdal_lseek         lseek;

   mdal_rename        rename;
   mdal_lgetxattr     lgetxattr;
   mdal_lsetxattr     lsetxattr;
   mdal_lremovexattr  lremovexattr;
   mdal_llistxattr    llistxattr;

   mdal_mkdir         mkdir;
   mdal_opendir       opendir;
   mdal_readdir       readdir;
   // mdal_readdir_r     readdir_r;
   mdal_closedir      closedir;

   mdal_is_open       is_open;
} MDAL;


// find or create an MDAL of the given type
MDAL* get_MDAL(MDAL_Type type);










#  ifdef __cplusplus
}
#  endif

#endif
