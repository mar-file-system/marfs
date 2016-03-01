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


#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <dirent.h>

#include <stdio.h>


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
      uint32_t  i;
   } data;
} MDAL_Context;

// fwd-decl
struct MDAL;


typedef struct {
   MDAL_Context ctx;
   struct MDAL* mdal;
} MDAL_FileHandle;

// used to fill out directory-entries, in marfs_readdir().
typedef int (*marfs_fill_dir_t) (void *buf, const char *name,
                                 const struct stat *stbuf, off_t off);


// initialize/destroy context, if desired.
//
//   -- init    is called before any other ops (per file-handle).
//   -- destroy is called when a file-handle is being destroyed.
//
typedef  int     (*mdal_ctx_init)   (MDAL_Context* ctx, struct MDAL* mdal);
typedef  int     (*mdal_ctx_destroy)(MDAL_Context* ctx, struct MDAL* mdal);


// --- file ops

typedef  int     (*mdal_open) (MDAL_Context* ctx, const char* path, int flags);
typedef  int     (*mdal_close)(MDAL_Context* ctx);

typedef  ssize_t (*mdal_write)(MDAL_Context* ctx, void* buf, size_t count);
typedef  ssize_t (*mdal_read) (MDAL_Context* ctx, void* buf, size_t count);

typedef  ssize_t (*mdal_getxattr)(MDAL_Context* ctx, const char* path,
                                  const char* name, void* value, size_t size);
typedef  ssize_t (*mdal_setxattr)(MDAL_Context* ctx, const char* path,
                                  const char* name, void* value, size_t size,
                                  int flags);

// --- directory-ops
// These all return 0 for success, -1 (plus errno) for failure.
// Any required state must be maintained in the context.

typedef  int     (*mdal_mkdir)  (MDAL_Context* ctx, const char* path, mode_t mode);
typedef  int     (*mdal_opendir)(MDAL_Context* ctx, const char* path);
typedef  int     (*mdal_readdir)(MDAL_Context*      ctx,
                                 const char*        path,
                                 void*              buf,
                                 marfs_fill_dir_t   filler,
                                 off_t              offset);
// typedef  int     (*mdal_readdir_r)(MDAL_Context* ctx,
//                                           struct dirent* entry, struct dirent** result);
typedef  int     (*mdal_closedir)(MDAL_Context* ctx);






typedef enum MDAL_Type {
   MDAL_POSIX  = 0x01,
   MDAL_PVFS2  = 0x02,
   MDAL_IOFSL  = 0x04,
} MDAL_Type;


// This is a collection of function-ptrs
// They capture a given implementation of interaction with an MDFS.
typedef struct MDAL {
   MDAL_Type        type;
   void*            global_state;

   mdal_ctx_init    ctx_init;
   mdal_ctx_destroy ctx_destroy;

   mdal_open        open;
   mdal_close       close;
   mdal_read        read;
   mdal_write       write;
   mdal_getxattr    getxattr;
   mdal_setxattr    setxattr;

   mdal_mkdir       mkdir;
   mdal_opendir     opendir;
   mdal_readdir     readdir;
   // mdal_readdir_r   readdir_r;
   mdal_closedir    closedir;

} MDAL;


// find or create an MDAL of the given type
MDAL* get_MDAL(MDAL_Type type);











#endif
