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




#include "logging.h"
#include "dal.h"
#include "common.h"

#include <stdlib.h>             // malloc()
#include <errno.h>
#include <stdarg.h>             // va_args for open()
#include <sys/types.h>          // the next three are for open & mode_t
#include <sys/stat.h>
#include <fcntl.h>
#include <utime.h>
#include <unistd.h>
#include <dlfcn.h>
#include <assert.h>


// ===========================================================================
// DEFAULT
// ===========================================================================
//

// Use these if you don't need to do anything special to initialize the
// context before-opening / after-closing for file-oriented or
// directory-oriented operations.
//
// open_data() gives us a ptr to the ObjectStream in the FileHandle.  We
// need this, for now, because MarFS expects stream-ops to have
// side-effects on that OS.


// #define OS(CTX)           ((ObjectStream*)((CTX)->data.ptr))
#define FH(CTX)           ((MarFS_FileHandle*)((CTX)->data.ptr))
#define OS(CTX)           (&FH(CTX)->os)

int     default_dal_ctx_init(DAL_Context* ctx, DAL* dal, void* fh) {
   ctx->flags    = 0;
   ctx->data.ptr = (MarFS_FileHandle*)fh;
   return 0;
}
int     default_dal_ctx_destroy(DAL_Context* ctx, DAL* dal) {
   return 0;
}




// ================================================================
// OBJ
//
// This is just a pass-through to the interfaces in object_stream.h, which
// were the original back-end for MarFS to interact with an object-store.
// ================================================================



#if 0
// NOTE: We need an ObjectStream, to use the functions in object_stream.h
//     There is already a statically-allocated one in the file-handle.  If
//     we just re-used that one (e.g. by having open_data() just assign
//     DAL_Context.data.ptr to point to it), it would save us a malloc/free
//     for every stream.  For now, to avoid refactoring, I'm going to
//     dynamically allocate our own ObjectStream.  We could also just
//     statically define on in DAL_Context.

typedef struct {
   ObjectStream os;
} OBJ_DALContextState;


#define STATE_LVAL(CTX)   (CTX)->data.ptr
#define STATE(CTX)        ((OBJ_DALContextState*)(STATE_LVAL(CTX)))
#define OS(CTX)           &(STATE(ctx)->os)





int     obj_dal_ctx_init(DAL_Context* ctx, DAL* dal) {
   ctx->flags   = 0;

   OBJ_DALContextState* state = (OBJ_DALContextState*)malloc(sizeof(OBJ_DALContextState));
   if (! state) {
      LOG(LOG_ERR, "malloc failed\n");
      errno = ENOMEM;
      return -1;
   }
   memset(state, 0, sizeof(OBJ_DALContextState));

   STATE_LVAL(ctx) = state;
   return 0;
}


int     obj_dal_ctx_destroy(DAL_Context* ctx, DAL* dal) {
   if (STATE(ctx)) {
      free(STATE(ctx));
      STATE_LVAL(ctx) = 0;
   }
   return 0;
}

#endif







int     obj_init(DAL_Context* ctx, DAL* dal, void* fh) {
   default_dal_ctx_init(ctx, dal, fh);
   return stream_init(OS(ctx), NULL, FH(ctx));
}



int     obj_open(DAL_Context* ctx,
                 int          is_put,
                 size_t       chunk_offset,
                 size_t       content_length,
                 uint8_t      preserve_write_count,
                 uint16_t     timeout) {

   return stream_open(OS(ctx), is_put,
                      chunk_offset, content_length,
                      preserve_write_count, timeout);
}

int     obj_put(DAL_Context*  ctx,
                const char*   buf,
                size_t        size) {

   return stream_put(OS(ctx), buf, size);
}

ssize_t obj_get(DAL_Context*  ctx,
                char*         buf,
                size_t        size) {

   return stream_get(OS(ctx), buf, size);
}

int     obj_sync(DAL_Context*  ctx) {

   return stream_sync(OS(ctx));
}

int     obj_abort(DAL_Context*  ctx) {

   return stream_abort(OS(ctx));
}

int     obj_close(DAL_Context*  ctx) {

   return stream_close(OS(ctx));
}

int     obj_delete(DAL_Context*  ctx) {

   // return stream_del(OS(ctx));
   return stream_del_components(OS(ctx),
                                FH(ctx)->info.pre.host,
                                FH(ctx)->info.pre.bucket,
                                FH(ctx)->info.pre.objid);
}

int     obj_update_object_location(DAL_Context* ctx) {

   return update_url(OS(ctx), &FH(ctx)->info);
}


static DAL obj_dal = {
   .name                   = "OBJECT",
   .name_len               = 6, // strlen("OBJECT"),

   .global_state           = NULL,

#if 0
   .init                   = &obj_dal_ctx_init,
   .destroy                = &obj_dal_ctx_destroy,
#elif 0
   .init                   = &default_dal_ctx_init,
   .destroy                = &default_dal_ctx_destroy,
#else
   .init                   = &obj_init,
   .destroy                = &default_dal_ctx_destroy,
#endif

   .open                   = &obj_open,
   .put                    = &obj_put,
   .get                    = &obj_get,
   .sync                   = &obj_sync,
   .abort                  = &obj_abort,
   .close                  = &obj_close,
   .del                    = &obj_delete,
   .update_object_location = &obj_update_object_location
};


// ================================================================
// NO_OP
//
// Like it says, these perform no data action.  The point would be to allow
// benchmarking the cost of meta-data operations alone.
// ================================================================



// #define OS(CTX)         (ObjectStream*)(CTX)->data.ptr





int     nop_open(DAL_Context* ctx,
                 int          is_put,
                 size_t       chunk_offset,
                 size_t       content_length,
                 uint8_t      preserve_write_count,
                 uint16_t     timeout) {

   return 0;
}

int     nop_put(DAL_Context*  ctx,
                const char*   buf,
                size_t        size) {

   OS(ctx)->written += size;
   return size;
}

ssize_t nop_get(DAL_Context*  ctx,
                char*         buf,
                size_t        size) {

   OS(ctx)->written += size;
   return size;
}

int     nop_sync(DAL_Context*  ctx) {

   return 0;
}

int     nop_abort(DAL_Context*  ctx) {

   return 0;
}

int     nop_close(DAL_Context*  ctx) {

   return 0;
}

int     nop_delete(DAL_Context*  ctx) {

   return 0;
}

int     nop_update_object_location(DAL_Context* ctx) {
   return 0;
}


DAL nop_dal = {
   .name                   = "NO_OP",
   .name_len               = 5, // strlen("NO_OP"),

   .global_state           = NULL,

   .init                   = &default_dal_ctx_init,
   .destroy                = &default_dal_ctx_destroy,
   .open                   = &nop_open,
   .put                    = &nop_put,
   .get                    = &nop_get,
   .sync                   = &nop_sync,
   .abort                  = &nop_abort,
   .close                  = &nop_close,
   .del                    = &nop_delete,
   .update_object_location = &nop_update_object_location
};

// ===========================================================================
// POSIX
// ===========================================================================

#define MAX_POSIX_PREFIX_LEN MARFS_MAX_REPO_NAME

#define MAX_OBJECT_PATH_LEN (MAX_POSIX_PREFIX_LEN               \
                             + MARFS_MAX_REPO_NAME + 1          \
                             + MARFS_MAX_NAMESPACE_NAME + 1     \
                             + MARFS_MAX_OBJID_SIZE + 1)

typedef struct posix_dal_ctx {
   MarFS_FileHandle* fh;
   int fd;
   char file_path[MAX_OBJECT_PATH_LEN];
} PosixDal_Context;

enum posix_dal_flags {
   POSIX_DAL_PATH_GENERATED = (1 << 0)
};

#define POSIX_DAL_CONTEXT(CTX) ((PosixDal_Context*)((CTX)->data.ptr))
#define POSIX_DAL_FH(CTX)      POSIX_DAL_CONTEXT(CTX)->fh
#define POSIX_DAL_FD(CTX)      POSIX_DAL_CONTEXT(CTX)->fd
#define POSIX_DAL_OS(CTX)      (&(POSIX_DAL_CONTEXT(CTX)->fh->os))
#define POSIX_DAL_PATH(CTX)    POSIX_DAL_CONTEXT(CTX)->file_path
#define FLAT_OBJID_SEPARATOR '#'

int posix_dal_ctx_init(DAL_Context* ctx, struct DAL* dal, void* fh /* ? */) {
   ENTRY();
   ctx->data.ptr = malloc(sizeof(PosixDal_Context));
   POSIX_DAL_CONTEXT(ctx)->fd = -1;
   POSIX_DAL_CONTEXT(ctx)->fh = (MarFS_FileHandle*)fh;
   ctx->flags = 0;

   // create repo/namespace directory under the posix-repo from config.
   const MarFS_Repo *repo = POSIX_DAL_FH(ctx)->info.pre.repo;
   const MarFS_Namespace *ns = POSIX_DAL_FH(ctx)->info.pre.ns;
   struct stat st;

   char repo_path[MARFS_MAX_REPO_NAME + MARFS_MAX_HOST_SIZE
                  + MARFS_MAX_NAMESPACE_NAME];
   sprintf(repo_path, "%s/%s", repo->host, repo->name);

   // Check that the directory
   // <repo-host>/<repo-name>/<namespace-name> exists
   if(stat(repo_path, &st) == -1) {
      TRY0( mkdir(repo_path, 0755) ); // XXX: an arbitrary mode.
   }
   sprintf(repo_path, "%s/%s/%s", repo->host, repo->name, ns->name);
   if(stat(repo_path, &st) == -1) {
      TRY0( mkdir(repo_path, 0755) );
   }

   memset(POSIX_DAL_PATH(ctx), '\0', MAX_OBJECT_PATH_LEN);
   
   EXIT();
   return 0;
}

int posix_dal_ctx_destroy(DAL_Context* ctx, struct DAL* dal) {
   free(POSIX_DAL_CONTEXT(ctx));
   ctx->flags = 0;
   return 0;
}

// file-ify an object-id.
static void flatten_objid(char* objid) {
   int i;
   for(i = 0; objid[i]; i++) {
      if(objid[i] == '/')
         objid[i] = FLAT_OBJID_SEPARATOR;
   }
}

// Generate the full path the the object in the POSIX repository.
// This will be used as the ->update_object_location interface
// function.
int generate_path(DAL_Context* ctx) {
   ENTRY();
   const MarFS_Repo*      repo        = POSIX_DAL_FH(ctx)->info.pre.repo;
   const MarFS_Namespace* ns          = POSIX_DAL_FH(ctx)->info.pre.ns;
   char*                  object_path = POSIX_DAL_PATH(ctx);

   sprintf(object_path, "%s/%s/%s/", repo->host, repo->name, ns->name);
   LOG(LOG_INFO, "POSIX_DAL Repo top level dir: %s\n", object_path);

   char* object_id_start = object_path + strlen(object_path);
   strncat(object_path, POSIX_DAL_FH(ctx)->info.pre.objid,
           MARFS_MAX_OBJID_SIZE);

   flatten_objid(object_id_start);

   LOG(LOG_INFO, "generated path: %s\n", object_path);
   ctx->flags |= POSIX_DAL_PATH_GENERATED;

   EXIT();
   return 0;
}

int posix_dal_open(DAL_Context* ctx,
                   int          is_put,
                   size_t       chunk_offset,
                   size_t       content_length,
                   uint8_t      preserve_write_count,
                   uint16_t     timeout) {
   ENTRY();

   // fail if the path has not been generated. This should never
   // happen and would be a logical error.
   if(! (ctx->flags & POSIX_DAL_PATH_GENERATED)) {
      LOG(LOG_ERR, "POSIX_DAL: no previous call to "
          "DAL->update_object_location");
      return -1;
   }

   TRY0( stream_cleanup_for_reopen(POSIX_DAL_OS(ctx), preserve_write_count) );

   char*        object_path = POSIX_DAL_PATH(ctx);
   int          object_flags;
   const mode_t mode        = S_IRUSR|S_IWUSR;

   if(is_put) {
      POSIX_DAL_OS(ctx)->flags |= OSF_WRITING;
      object_flags = O_WRONLY|O_CREAT;
   }
   else {
      POSIX_DAL_OS(ctx)->flags |= OSF_READING;
      object_flags = O_RDONLY;
   }

   int fd = open(object_path, object_flags, mode);
   if(fd < 0) {
      LOG(LOG_ERR, "Failed to open file with posix_dal: %s\n", object_path);
      return -1;
   }

   if (chunk_offset) {
      off_t rc = lseek(fd, chunk_offset, SEEK_SET);
      if (rc == (off_t)-1) {
         LOG(LOG_ERR, "lseek(%ld) faild for posix_dal: %s\n",
             chunk_offset, object_path);
         return -1;
      }
   }

   POSIX_DAL_FD(ctx) = fd;
   POSIX_DAL_OS(ctx)->flags |= OSF_OPEN;

   EXIT();
   return 0;
}

int posix_dal_put(DAL_Context* ctx, const char* buf, size_t size) {
   int written = write(POSIX_DAL_FD(ctx), buf, size);

   if(written >= 0)
      POSIX_DAL_OS(ctx)->written += written;

   return written;
}

ssize_t posix_dal_get(DAL_Context* ctx, char* buf, size_t size) {
   ssize_t size_read = 0;

   size_read = read(POSIX_DAL_FD(ctx), buf, size);
   if(size_read == 0) {
      POSIX_DAL_OS(ctx)->flags |= OSF_EOF;
   }
   else if(size_read < 0) {
      return size_read; // relying on errno set by read
   }

   POSIX_DAL_OS(ctx)->written += size_read;

   return size_read;
}

static int close_posix_object(DAL_Context* ctx) {
   TRY_DECLS();

   TRY0( close(POSIX_DAL_FD(ctx)) );
   POSIX_DAL_OS(ctx)->flags &= ~OSF_OPEN;
   POSIX_DAL_OS(ctx)->flags |= OSF_CLOSED;
   // prevent future mistakes by clearing out the file descriptor.
   POSIX_DAL_FD(ctx) = -1;

   return 0;
}

// See comments on stream_sync. When this returns, all I/O (ever) is
// completed. No more I/O errors are possible. There should be no more
// operations done on the stream at this point.
int posix_dal_sync(DAL_Context* ctx) {
   TRY_DECLS();

   // fsync will set errno = EBADF if the fd is not open. In order to
   // be consistent with stream_sync, we detect that case and fail
   // with EINVAL instead.
   if(! (POSIX_DAL_OS(ctx)->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", POSIX_DAL_OS(ctx)->url);
      errno = EINVAL;
      return -1;
   }

   TRY0( fsync(POSIX_DAL_FD(ctx)) );

   // Since close might cause an error we do it here.
   TRY0( close_posix_object(ctx) );

   return 0;
}

// Undo any writes that have been made since the "object" was opened.
//
// NOTE: Based on the use of this function, we are assuming that no
//       data has been written to the object when this is called.
//       In this case, this boils down to a no-op.
int posix_dal_abort(DAL_Context* ctx) {
   if(! (POSIX_DAL_OS(ctx)->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "POSIX_DAL: abort: %s isn't open", POSIX_DAL_OS(ctx)->url);
      errno = EINVAL;
      return -1;
   }

   POSIX_DAL_OS(ctx)->flags |= OSF_ABORT;

   // other than setting flags, this is a no-op.

   return 0;
}

int posix_dal_close(DAL_Context* ctx) {
   TRY_DECLS();

   // the file may have already been closed by _sync
   if(! (POSIX_DAL_OS(ctx)->flags & OSF_OPEN)) {
      return 0;
   }

   TRY0( close_posix_object(ctx) );

   return 0;
}

int posix_dal_delete(DAL_Context* ctx) {
   TRY_DECLS();

   char *object_path = POSIX_DAL_PATH(ctx);

   return unlink(object_path);
}



DAL posix_dal = {
   .name         = "POSIX",
   .name_len     = 5,

   .global_state = NULL,

   .init         = &posix_dal_ctx_init,
   .destroy      = &posix_dal_ctx_destroy,

   .open         = &posix_dal_open,
   .put          = &posix_dal_put,
   .get          = &posix_dal_get,
   .sync         = &posix_dal_sync,
   .abort        = &posix_dal_abort,
   .close        = &posix_dal_close,
   .del          = &posix_dal_delete,

   .update_object_location = &generate_path
};

// ===========================================================================
// MC (Multi-component)
// ===========================================================================

// The mc path will be the host field of the repo plus an object id.
// We need a little extra room to account for numbers that will get
// filled in to create the path template, 128 characters should be
// more than enough.
#define MC_MAX_PATH_LEN (MARFS_MAX_OBJID_SIZE + MARFS_MAX_HOST_SIZE + 128)

#define MC_FH(CTX)      MC_CONTEXT(CTX)->fh
#define MC_OS(CTX)      (&MC_FH(CTX)->os)
#define MC_REPO(CTX)    MC_CONTEXT(CTX)->fh->repo
#define MC_HANDLE(CTX)  MC_CONTEXT(CTX)->mc_handle
#define MC_CONTEXT(CTX) ((MC_Context*)((CTX)->data.ptr))

// XXX: remove all of this.
//      This is scaffolding while we get the ne library up and runnung.
#define NE_WRONLY (1 << 0)
#define NE_RDONLY (1 << 1)
typedef void ne_handle;
#if 0
ne_handle* ne_open(const char* path_template, int mode, int start_block,
                   int n, int e);
int ne_read(ne_handle* handle, void* buf, size_t offset, size_t size);
int ne_write(ne_handle* handle, void buf, size_t size);
int ne_close(ne_handle* handle);
#endif
enum mc_flags {
   MCF_DEFERED_OPEN = 0x1, // we called open, and said we opened the
                           // object, but really we are waiting until
                           // you call put() or get() to do the open.
};

typedef struct mc_context {
   ObjectStream*     os;
   ne_handle*        mc_handle;
   MarFS_FileHandle* fh;
   off_t             chunk_offset;

   // These define the path we will use for the open and are
   // updated/set by ->update_object_location()
   char              path_template[MC_MAX_PATH_LEN];
   unsigned int      start_block;
} MC_Context;

// Computes a good, uniform, hash of the string.
//
// Treats each character in the length n string as a coefficient of a
// degree n polynomial.
//
// f(x) = string[0] + string[1] * x + ... + string[n-1] * x^(n-1)
//
// The hash is computed by evaluating the polynomial for x=31 using
// Horner's rule.
//
// Reference: http://cseweb.ucsd.edu/~kube/cls/100/Lectures/lec16/lec16-14.html
static int polyhash(const char* string) {
   // 31 is apparently what is used by java's String.hashCode()
   // method.  should be good enough for our prototype.
   const int salt = 31;
   char c;
   int h = *string++;
   while((c = *string++))
      h = salt * h + c;
   return h;
}

// Initialize the context for a multi-component backed object.
// Returns 0 on success or -1 on failure (if memory cannot be
// allocated).
int mc_init(DAL_Context* ctx, struct DAL* dal, void* fh) {
   ENTRY();
   
   ctx->data.ptr = malloc(sizeof(MC_Context));
   if(! MC_CONTEXT(ctx)) {
      LOG(LOG_ERR, "failed to allocate memory for MC_Context\n");
      return -1;
   }

   memset(MC_CONTEXT(ctx)->path_template, '\0', MC_MAX_PATH_LEN);   
   MC_FH(ctx) = (MarFS_FileHandle*)fh;
   MC_HANDLE(ctx) = NULL;
   MC_CONTEXT(ctx)->chunk_offset = 0;
   ctx->flags = 0;

   EXIT();
   return 0;
}

// Free the multi-component context stored in the dal context.
// `ctx' should not be used any more after this is called.
int mc_destroy(DAL_Context *ctx, struct DAL* dal) {
   free(MC_CONTEXT(ctx));
   return 0;
}

int mc_update_path(DAL_Context* ctx) {
   // QUESTION: Do we need to prepend the bucket and ns->alias to the
   //           objid? For now We can just flatten the url to make
   //           things easy.

   // shorthand
   PathInfo*              info          = &(MC_FH(ctx)->info);
   MarFS_Repo*            repo          = info->pre.repo;
   char*                  objid         = info->pre.objid;
   char*                  path_template = MC_CONTEXT(ctx)->path_template;
   

   char obj_filename[MARFS_MAX_OBJID_SIZE];
   strncpy(obj_filename, objid, MARFS_MAX_OBJID_SIZE);
   flatten_objid(obj_filename);

   // We will use the hash in multiple places, save it to avoid
   // recomputing.
   //
   // Hash the actual object ID so the hash will remain the same,
   // regadless of changes to the "file-ification" format.
   unsigned int objid_hash = (unsigned int)polyhash(objid);
   
   // TODO: Waiting on code from Jeff, implementing the configurable
   // parameters for MC repos. The variables below will be initialized
   // based on those parameters.
   char *mc_path_format = repo->host;

   // XXX: This all needs to come from the configuration for the
   // DAL/repo.
   int n = 10;
   int e = 2;
   unsigned int num_blocks    = n+e;
   unsigned int num_pods      = 4;
   unsigned int num_cap       = 1;
   unsigned int scatter_width = 128;
   
   unsigned int pod           = objid_hash % num_pods;
   unsigned int capacity_unit = objid_hash % num_cap;
   unsigned int scatter       = objid_hash % scatter_width;

   MC_CONTEXT(ctx)->start_block = objid_hash % num_blocks;
   // fill in path template
   // the mc_path_format is sometheing like:
   //   "<protected-root>/repo10+2/pod%d/block%s/cap%d/scatter%d/"
   snprintf(path_template, MC_MAX_PATH_LEN, mc_path_format,
            pod,
            "%d", // this will be filled in by the ec library
            capacity_unit,
            scatter);

   // be robust to vairation in the config... We could always just add
   // a slash, but that will get ugly in the logs.
   if(path_template[strlen(path_template) - 1] != '/')
      strcat(path_template, "/");
   
   // append the fileified object id
   strncat(path_template, obj_filename, MC_MAX_PATH_LEN);

   return 0;
}

// Actually open an object, don't just defer it and say we did it.
int mc_do_open(DAL_Context* ctx) {
   ENTRY();

   ObjectStream* os            = MC_OS(ctx);
   char*         path_template = MC_CONTEXT(ctx)->path_template;
   // XXX: This needs to come from the configuration for the DAL/repo.
   unsigned int n = 10;
   unsigned int e = 2;

   // QUESTION: Should we test os->flags & OSF_OPEN here? It would be
   // an error to call this except after mc_open(), but... this should
   // only be used internally anyway, so we can pretty much guarantee
   // that won't happen.
   
   int mode = (os->flags & OSF_WRITING) ? NE_WRONLY : NE_RDONLY;
   MC_HANDLE(ctx) = ne_open(path_template, mode,
                            MC_CONTEXT(ctx)->start_block, n, e);
   if(! MC_HANDLE(ctx)) {
      LOG(LOG_ERR, "Failed to open MC Handle %s\n", path_template);
      return -1;
   }

   // open is no longer defered.
   ctx->flags &= ~MCF_DEFERED_OPEN;
   
   EXIT();
   return 0;
}

// We want to use the mc DAL to defer opens until put() or get() are
// called. marfs_open() already implements this for reads, but not
// writes, by not calling DAL->open() until marfs_read() is called. In
// the interest consistency, however, we will defer all opens until a
// MC_DAL operation is invoked that actually requires an open data
// stream (put() or get()). All flags will be set in the object stream
// that is passed to init() to make it look like the stream is opened,
// but an additional flag in the dal context will also be set
// indicating to ->put() or ->get() that the open has been defered.
//
// Could fail if the object stream is being reused and is not in a
// good state.
int mc_open(DAL_Context* ctx,
            int is_put,
            size_t chunk_offset,
            size_t content_length,
            uint8_t preserve_write_count,
            uint16_t timeout) {
   ENTRY();

   ObjectStream* os = MC_OS(ctx);

   // do the generic cleanup stuff like resetting flags.
   TRY0( stream_cleanup_for_reopen(os, preserve_write_count) );

   if(is_put) {
      os->flags |= OSF_WRITING;
   }
   else {
      os->flags |= OSF_READING;
   }

   ctx->flags |= MCF_DEFERED_OPEN;
   os->flags  |= OSF_OPEN;
   MC_CONTEXT(ctx)->chunk_offset = chunk_offset;
   
   EXIT();
   return 0;
}

// Put data into a MC Object. For performance reasons we will need to
// try to align the size of `buf' with the underlying buffers in the
// n+e lib.  This could be hard to achieve, since the buffer size may
// be determined by fuse, or PFTool. Is there some efficient way to
// simply give the buffer to the n+e library and relinquish ownership,
// using a fresh buffer for the next write?
int mc_put(DAL_Context* ctx,
           const char* buf,
           size_t size) {
   ENTRY();

   ne_handle*    handle = MC_HANDLE(ctx);
   ObjectStream* os     = MC_OS(ctx);

   if(! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "Attempted put on OS that is not open.\n");
      errno = EBADF;
      return -1;
   }
   else if(ctx->flags & MCF_DEFERED_OPEN) {
      if(mc_do_open(ctx)) {
         LOG(LOG_ERR, "%s could not be opened for writing\n", os->url);
         return -1; // errno should be set in do_open()
      }
   }
   
   int written = ne_write(handle, buf, size);

   if(written < 0) {
      LOG(LOG_ERR, "ftone_write() failed.\n");
      return -1;
   }

   os->written += written;
   
   EXIT();
   return written;
}

ssize_t mc_get(DAL_Context* ctx, char* buf, size_t size) {
   ENTRY();

   ssize_t       size_read;
   ne_handle*    handle = MC_HANDLE(ctx);
   ObjectStream* os     = MC_OS(ctx);

   if(! (os->flags & OSF_OPEN)) {
      errno = EBADF;
      LOG(LOG_ERR, "Attempted get on OS that is not open.\n");
   }
   else if(ctx->flags & MCF_DEFERED_OPEN) {
      if(mc_do_open(ctx)) {
         LOG(LOG_ERR, "%s could not be opened for reading\n", os->url);
         return -1;
      }
   }

   // Need to figure out how to get the offset from the os.  May be as
   // simple as reading from os->written.  Note that stream_get()
   // truncates requests to fit within os->content_len, thus avoiding
   // reads that return fewer than the number of bytes requested.
   size_read = ne_read(handle, buf, MC_CONTEXT(ctx)->chunk_offset, size);

   if(size_read < 0) {
      LOG(LOG_ERR, "netof_read() failed.\n");
      return -1;
   }
   else if(size_read == 0) { // EOF
      os->flags |= OSF_EOF;
   }

   // update the offset for the next call to read.
   MC_CONTEXT(ctx)->chunk_offset += size_read;
   os->written += size_read;

   EXIT();
   return size_read;
}

// Upon return no more I/O is possible. The stream is closed.  If
// DEFERED_OPEN is asserted, then this is almost no-op only flags are
// changed.
int mc_sync(DAL_Context* ctx) {
   ENTRY();
   
   ObjectStream* os     = MC_OS(ctx);
   ne_handle*    handle = MC_HANDLE(ctx);

   if(! (os->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "%s isn't open\n", os->url);
      errno = EINVAL;
      return -1;
   }
   else if(! (ctx->flags & MCF_DEFERED_OPEN)) {
      // then we actually have work to do.
      TRY0( ne_close(handle) );
   }
   else {
      LOG(LOG_INFO, "%s DEFERED_OPEN asserted. sync is a noop\n", os->url);
      ctx->flags &= ~MCF_DEFERED_OPEN;
   }

   EXIT();
   return 0;
}

// see notes posix_dal_abort().
int mc_abort(DAL_Context* ctx) {
   ENTRY();

   if(! (MC_OS(ctx)->flags & OSF_OPEN)) {
      LOG(LOG_ERR, "abort on not open object stream %s\n", MC_OS(ctx)->url);
      errno = EINVAL;
      return -1;
   }

   // defered open is no longer possible.
   ctx->flags &= ~MCF_DEFERED_OPEN;
   MC_OS(ctx)->flags |= OSF_ABORT;
   
   EXIT();
   return 0;
}

// Marks an OS as closed. All "actual" close work should have been
// done in a previous call to mc_sync(). See notes on stream_sync()
// and stream_close() for more information.
int mc_close(DAL_Context* ctx) {
   ENTRY();

   ObjectStream* os     = MC_OS(ctx);

   if(! (os->flags & OSF_OPEN)) {
      LOG(LOG_INFO, "Close on not-open stream %s\n", os->url);
      return 0;
   }
   // Don't need to worry about defered opens here since that should
   // have been taken care of in mc_sync.

   os->flags &= ~OSF_OPEN;
   os->flags |= OSF_CLOSED;

   EXIT();
   return 0;
}

DAL mc_dal = {
   .name         = "MC_DAL",
   .name_len     = 6,

   .global_state = NULL,

   .init         = &mc_init,
   .destroy      = &mc_destroy,

   .open         = &mc_open,
   .put          = &mc_put,
   .get          = &mc_get,
   .sync         = &mc_sync,
   .abort        = &mc_abort,
   .close        = &mc_close,

   .update_object_location = &mc_update_path
};


// ===========================================================================
// GENERAL
// ===========================================================================

// should be plenty
// static const size_t MAX_DAL = 32; // stupid compiler
#define MAX_DAL 32

static DAL*   dal_list[MAX_DAL];
static size_t dal_count = 0;


# define DL_CHECK(SYM)                                                  \
   if (! dal->SYM) {                                                    \
      LOG(LOG_ERR, "DAL '%s' has no symbol '%s'\n", dal->name, #SYM);   \
      return -1;                                                         \
   }


// add a new DAL to dal_list
int install_DAL(DAL* dal) {

   if (! dal) {
      LOG(LOG_ERR, "NULL arg\n");
      return -1;
   }

   // insure that no DAL with the given name already exists
   int i;
   for (i=0; i<dal_count; ++i) {
      if ((dal->name_len == dal_list[i]->name_len)
          && (! strcmp(dal->name, dal_list[i]->name))) {

         LOG(LOG_ERR, "DAL named '%s' already exists\n", dal->name);
         return -1;
      }
   }

   // validate that DAL has all required members
   DL_CHECK(name);
   DL_CHECK(name_len);
   DL_CHECK(init);
   DL_CHECK(destroy);
   DL_CHECK(open);
   DL_CHECK(put);
   DL_CHECK(get);
   DL_CHECK(sync);
   DL_CHECK(abort);
   DL_CHECK(close);
   DL_CHECK(del);
   DL_CHECK(update_object_location);

   if (dal_count >= MAX_DAL) {
         LOG(LOG_ERR,
             "No room for DAL '%s'.  Increase MAX_DAL_COUNT and rebuild.\n",
             dal->name);
         return -1;
   }

   // install
   LOG(LOG_INFO, "Installing DAL '%s'\n", dal->name);
   dal_list[dal_count] = dal;
   ++ dal_count;

   return 0;
}

# undef DL_CHECK



// Untested support for dynamically-loaded DAL. This is not a link-time
// thing, but a run-time thing.  The name in the configuration is something
// like: "DYNAMIC /path/to/my/lib", and we go look for all the DAL symbols
// (e.g. dal_open()) in that module, and install a corresponding DAL.
// *All* DAL symbols must be defined in the library.

static
DAL* dynamic_DAL(const char* name) {

   DAL* dal = (DAL*)calloc(1, sizeof(DAL));
   if (! dal) {
      LOG(LOG_ERR, "no memory for new DAL '%s'\n", name);
      return NULL;
   }

   // zero everything, so if we forget to update something it will be obvious
   memset(dal, 0, sizeof(DAL));

   dal->name     = name;
   dal->name_len = strlen(name);

   if (! strcmp(name, "DYNAMIC")) {
      return NULL;

      // second token is library-name
      const char* delims = " \t";
      char* delim = strpbrk(name, delims);
      if (! delim)
         return NULL;
      char* lib_name = delim + strspn(delim, delims);
      if (! *lib_name)
         return NULL;

      // dig out symbols
      void* lib = dlopen(lib_name, RTLD_LAZY);
      if (! lib) {
         LOG(LOG_ERR, "Couldn't open dynamic lib '%s'\n", lib_name);
         return NULL;
      }

      dal->global_state = NULL;

      dal->init         = (dal_ctx_init)    dlsym(lib, "dal_init");
      dal->destroy      = (dal_ctx_destroy) dlsym(lib, "dal_destroy");

      dal->open         = (dal_open)    dlsym(lib, "dal_open");
      dal->put          = (dal_put)     dlsym(lib, "dal_put");
      dal->get          = (dal_get)     dlsym(lib, "dal_get");
      dal->sync         = (dal_sync)    dlsym(lib, "dal_sync");
      dal->abort        = (dal_abort)   dlsym(lib, "dal_abort");
      dal->close        = (dal_close)   dlsym(lib, "dal_close");

      dal->update_object_location =
         (dal_update_object_location) dlsym(lib, "dal_update_object_location");

      dlclose(lib);

   }
   else {
      // unknown name
      return NULL;
   }


   return dal;
}



// Applications can push private custom DALs before calling
// read_configuration() [or, at least, before calling
// validate_configuration()].  They should just build a struct like the
// ones used below, and call install_DAL() with their struct.  Then those
// DALs will be found, when referenced in the configuration.

DAL* get_DAL(const char* name) {
   static int needs_init = 1;
   if (needs_init) {

      // one-time initialization of dal_list
      assert(! install_DAL(&obj_dal)   );
      assert(! install_DAL(&nop_dal)   );
      assert(! install_DAL(&posix_dal) );

      needs_init = 0;
   }

   // look up <name> in known DALs
   size_t name_len = strlen(name);
   int i;
   for (i=0; i<dal_count; ++i) {
      if ((name_len == dal_list[i]->name_len)
          && (! strcmp(name, dal_list[i]->name))) {

         return dal_list[i];
      }
   }

   // not found.  Maybe it was dynamic?
   if (! strcmp(name, "DYNAMIC")) {
      DAL* dynamic = dynamic_DAL(name);
      assert(! install_DAL(dynamic) );
      return dynamic;
   }

   return NULL;
}
