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
#include "mdal.h"

#include <stdlib.h>             // malloc()
#include <errno.h>


// ===========================================================================
// DEFAULT
// ===========================================================================
// 

// Use these if you don't need to do anything special to initialize the
// context before-opening / after-closing for file-oriented or
// directory-oriented operations.
int     default_mdal_file_ctx_init(MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}
int     default_mdal_file_ctx_destroy(MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}



int     default_mdal_dir_ctx_init (MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}
int     default_mdal_dir_ctx_destroy (MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}



// ================================================================
// POSIX
// ================================================================


// POSIX needs a file-descriptor.  We'll just use ctx->data.i for that.

// typedef struct {
//    int fd
// } MDAL_Context_Posix;

#define POSIX_FD(CTX)   (CTX)->data.i
#define POSIX_DIRP(CTX) (CTX)->data.ptr




void*   posix_mdal_open(MDAL_Context* ctx, const char* path, int flags) {
   POSIX_FD(ctx) = open(path, flags);
   return ctx;
}

int     posix_mdal_is_open(MDAL_Context* ctx) {
   return POSIX_FD(ctx);
}

int     posix_mdal_close(MDAL_Context* ctx) {
   int retval = close(POSIX_FD(ctx));
   POSIX_FD(ctx) = 0;
   return retval;
}


ssize_t posix_mdal_read (MDAL_Context* ctx, void* buf, size_t count) {
   return read(POSIX_FD(ctx), buf, count);
}

ssize_t posix_mdal_write(MDAL_Context* ctx, const void* buf, size_t count) {
   return write(POSIX_FD(ctx), buf, count);
}


ssize_t posix_mdal_getxattr(MDAL_Context* ctx, const char* path,
                            const char* name, void* value, size_t size) {
   return getxattr(path, name, value, size);
}


ssize_t posix_mdal_setxattr(MDAL_Context* ctx, const char* path,
                            const char* name, void* value, size_t size, int flags) {
   return setxattr(path, name, value, size, flags);
}

int     posix_mdal_ftruncate(MDAL_Context* ctx, off_t length) {
   return ftruncate(POSIX_FD(ctx), length);
}

off_t   posix_mdal_lseek(MDAL_Context* ctx, off_t offset, int whence) {
   return lseek(POSIX_FD(ctx), offset, whence);
}
int     posix_mdal_rename(const char* from, const char* to) {
   return rename(from, to);
}





int     posix_mdal_mkdir (MDAL_Context* ctx, const char* path, mode_t mode) {
   return mkdir(path, mode);   
}


void*   posix_mdal_opendir (MDAL_Context* ctx, const char* path) {
   POSIX_DIRP(ctx) = opendir(path);
   return POSIX_DIRP(ctx);
}


// lifted from marfs_readdir()
int     posix_mdal_readdir (MDAL_Context*      ctx,
                            const char*        path,
                            void*              buf,
                            marfs_fill_dir_t   filler,
                            off_t              offset) {

   DIR*           dirp = POSIX_DIRP(ctx);
   struct dirent* dent;
   ssize_t        rc_ssize;

   while (1) {
      // #if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
      //      struct dirent* dent_r;       /* for readdir_r() */
      //      TRY0( readdir_r(dirp, dent, &dent_r) );
      //      if (! dent_r)
      //         break;                 /* EOF */
      //      if (filler(buf, dent_r->d_name, NULL, 0))
      //         break;                 /* no more room in <buf>*/

      // #else
      errno = 0;
      rc_ssize = (ssize_t)readdir(dirp);
      if (! rc_ssize) {
         if (errno)
            return -1;       /* error */
         break;              /* EOF */
      }
      dent = (struct dirent*)rc_ssize;
      if (filler(buf, dent->d_name, NULL, 0))
         break;                 /* no more room in <buf>*/
      // #endif
      
   }

   return 0;
}


// int            posix_mdal_readdir_r(MDAL_Context* ctx, DIR* dirp,
//                                     struct dirent* entry, struct dirent** result) {
// #if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
//    return readdir_r(POSIX_DIRP(dirp), entry, result);
// #else
// # error "No support for readdir_r()"
//    LOG(LOG_ERR, "No support for readdir_r()\n");
// #endif
// }


int     posix_mdal_closedir (MDAL_Context* ctx) {
   return closedir((DIR*)POSIX_DIRP(ctx));
}





// ===========================================================================
// GENERAL
// ===========================================================================



static
int mdal_init(MDAL* mdal, MDAL_Type type) {

   // zero everything, so if we forget to update something it will be obvious
   memset(mdal, 0, sizeof(MDAL));

   mdal->type = type;
   switch (type) {

   case MDAL_POSIX:
      mdal->global_state = NULL;

      mdal->f_init       = &default_mdal_file_ctx_init;
      mdal->f_destroy    = &default_mdal_file_ctx_destroy;

      mdal->d_init       = &default_mdal_dir_ctx_init;
      mdal->d_destroy    = &default_mdal_dir_ctx_destroy;

      mdal->open         = &posix_mdal_open;
      mdal->close        = &posix_mdal_close;
      mdal->write        = &posix_mdal_write;
      mdal->read         = &posix_mdal_read;
      mdal->getxattr     = &posix_mdal_getxattr;
      mdal->setxattr     = &posix_mdal_setxattr;
      mdal->ftruncate    = &posix_mdal_ftruncate;
      mdal->lseek        = &posix_mdal_lseek;
      mdal->rename       = &posix_mdal_rename;

      mdal->mkdir        = &posix_mdal_mkdir;
      mdal->opendir      = &posix_mdal_opendir;
      mdal->readdir      = &posix_mdal_readdir;
      // mdal->readdir_r    = &posix_mdal_readdir_r;
      mdal->closedir     = &posix_mdal_closedir;

      mdal->is_open      = &posix_mdal_is_open;
      break;

      // TBD ...
   case MDAL_PVFS2:
   case MDAL_IOFSL:
   default:
      return -1;
   };

   return 0;
}



// should be plenty
// static const size_t MAX_MDAL = 32; // stupid compiler
#define MAX_MDAL 32

static MDAL*  mdal_vec[MAX_MDAL];
static size_t mdal_count = 0;


MDAL* get_MDAL(MDAL_Type type) {

   int i;
   for (i=0; i<mdal_count; ++i) {
      if (mdal_vec[i] && (mdal_vec[i]->type == type))
         return mdal_vec[i];
   }

   if (i >= MAX_MDAL -1) {
      LOG(LOG_ERR, "out of room for new MDAL structs.  Increase MAX_MDAL and rebuild\n");
      return NULL;
   }

   MDAL* new_mdal = (MDAL*)calloc(1, sizeof(MDAL));
   if (! new_mdal) {
      LOG(LOG_ERR, "out of memory for new MDAL (0x%02x)\n", (unsigned)type);
      return NULL;
   }
   
   if (mdal_init(new_mdal, type)) {
      LOG(LOG_ERR, "Couldn't initialize MDAL (0x%02x)\n", (unsigned)type);
      return NULL;
   }

   mdal_vec[mdal_count] = new_mdal;
   ++ mdal_count;
   return new_mdal;
}
