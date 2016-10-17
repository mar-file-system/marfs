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

typedef  int     (*mdal_config) (struct MDAL*     dal,
                                 xDALConfigOpt**  opts,
                                 size_t           opt_count);

// used to check whether an MDAL uses the default-config, so that we can
// reliably print out the options with which it was configured, for
// diagnostics.
extern int   default_mdal_config(struct MDAL*     mdal,
                                 xDALConfigOpt**  opts,
                                 size_t           opt_count);




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
typedef  void*   (*mdal_open) (MDAL_Context* ctx, const char* path, int flags, ...);
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

typedef  int     (*mdal_access)  (const char* path, int mask);
typedef  int     (*mdal_faccessat)(int fd, const char* path, int mask, int flags);
typedef  int     (*mdal_mknod)   (const char* path, mode_t mode, dev_t dev);
typedef  int     (*mdal_chmod)   (const char* path, mode_t mode);
typedef  int     (*mdal_truncate)(const char* path, off_t size);
typedef  int     (*mdal_lchown)  (const char* path, uid_t owner, gid_t group);
typedef  int     (*mdal_lstat)   (const char* path, struct stat* st);
typedef  int     (*mdal_rename)  (const char* from, const char* to);
typedef  int     (*mdal_readlink)(const char* path, char* buf, size_t size);

typedef  ssize_t (*mdal_lgetxattr)   (const char* path, const char* name,
                                      void* value, size_t size);
typedef  ssize_t (*mdal_lsetxattr)   (const char* path, const char* name,
                                      const void* value, size_t size, int flags);
typedef  int     (*mdal_lremovexattr)(const char* path, const char* name);
typedef  ssize_t (*mdal_llistxattr)  (const char* path, char* list, size_t size);
typedef  int     (*mdal_symlink)     (const char* target, const char* linkname);
typedef  int     (*mdal_unlink)      (const char* path);

typedef  int     (*mdal_utime)    (const char* filename, const struct utimbuf *times);
// NOTE: utimens seems like it should take a MDAL_Context parameter to
//       provide the equivalent of a dirfd; however, anyone calling into the
//       MDAL should be doing so through marfs_utimensat or marfs_utimens
//       which assume that the path is absolute and dirfd should be ignored.
//       Therefore, this is implemented as context free operation.
typedef  int     (*mdal_utimensat)(int dirfd, const char *pathname,
                                   const struct timespec times[2], int flags);



// --- directory-ops
// opendir() should return some non-null pointer for success, NULL for failure.
// (It won't be used for anything.).
// The others return 0 for success, -1 (plus errno) for failure.
// Any required state must be maintained in the context.

typedef  void*   (*mdal_opendir)(MDAL_Context* ctx, const char* path);
typedef  int     (*mdal_readdir)(MDAL_Context*      ctx,
                                 const char*        path,
                                 void*              buf,
                                 marfs_fill_dir_t   filler,
                                 off_t              offset);
// typedef  int     (*mdal_readdir_r)(MDAL_Context* ctx,
//                                           struct dirent* entry, struct dirent** result);
typedef  int     (*mdal_closedir)(MDAL_Context* ctx);

// --- directory-ops (context-free)
typedef  int     (*mdal_mkdir)  (const char* path, mode_t mode);
typedef  int     (*mdal_rmdir)  (const char* path);

typedef  int     (*mdal_statvfs)(const char* path, struct statvfs *buf);


// This is a collection of function-ptrs
// They capture a given implementation of interaction with an MDFS.
typedef struct MDAL {
   const char*        name;
   size_t             name_len;

   void*              global_state;
   mdal_config        config;

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

   mdal_access        access;
   mdal_faccessat     faccessat;
   mdal_mknod         mknod;
   mdal_chmod         chmod;
   mdal_truncate      truncate;
   mdal_lchown        lchown;
   mdal_lstat         lstat;
   mdal_rename        rename;
   mdal_readlink      readlink;
   mdal_lgetxattr     lgetxattr;
   mdal_lsetxattr     lsetxattr;
   mdal_lremovexattr  lremovexattr;
   mdal_llistxattr    llistxattr;
   mdal_symlink       symlink;
   mdal_unlink        unlink;

   mdal_utime         utime;
   mdal_utimensat     utimensat;

   mdal_mkdir         mkdir;
   mdal_rmdir         rmdir;
   mdal_opendir       opendir;
   mdal_readdir       readdir;
   mdal_closedir      closedir;

   mdal_statvfs       statvfs;

   mdal_is_open       is_open;
} MDAL;



// insert a new DAL, if there are no name-conflicts
int  install_MDAL(MDAL* mdal);

// find an MDAL with the given name
MDAL* get_MDAL(const char* name);



// exported for building custom MDAL
int     default_mdal_file_ctx_init(MDAL_Context* ctx, MDAL* mdal);
int     default_mdal_file_ctx_destroy(MDAL_Context* ctx, MDAL* mdal);
int     default_mdal_dir_ctx_init (MDAL_Context* ctx, MDAL* mdal);
int     default_mdal_dir_ctx_destroy (MDAL_Context* ctx, MDAL* mdal);








#  ifdef __cplusplus
}
#  endif

#endif
