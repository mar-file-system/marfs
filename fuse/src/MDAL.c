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
#include "common.h"
#include "MDAL.h"

#include <stdlib.h>             // malloc()


// ================================================================
// POSIX
// ================================================================


int     mdal_posix_ctx_init(MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}

int     mdal_posix_ctx_destroy(MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}


int     mdal_posix_open(MDAL_Context* ctx, const char* path, int flags) {
   return open(path, flags);
   
}

int     mdal_posix_close(MDAL_Context* ctx, int fd) {
   return close(fd);
}


ssize_t mdal_posix_read(MDAL_Context* ctx, int fd, void* buf, size_t count) {
   return read(fd, buf, count);
}

ssize_t mdal_posix_write(MDAL_Context* ctx, int fd, void* buf, size_t count) {
   return write(fd, buf, count);
}


ssize_t mdal_posix_getxattr(MDAL_Context* ctx, const char* path,
                      const char* name, void* value, size_t size) {
   return getxattr(path, name, value, size);
}


ssize_t mdal_posix_setxattr(MDAL_Context* ctx, const char* path,
                            const char* name, void* value, size_t size, int flags) {
   return setxattr(path, name, value, size, flags);
}


int            mdal_posix_mkdir (MDAL_Context* ctx, const char* path, mode_t mode) {
   return mkdir(path, mode);
}


DIR*           mdal_posix_opendir (MDAL_Context* ctx, const char* path) {
   return opendir(path);
}


struct dirent* mdal_posix_readdir (MDAL_Context* ctx, DIR* dirp) {
   return readdir(dirp);
}


int            mdal_posix_readdir_r(MDAL_Context* ctx, DIR* dirp,
                                    struct dirent* entry, struct dirent** result) {
#if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
   return readdir_r(dirp, entry, result);
#else
# error "No support for readdir_r()"
   LOG(LOG_ERR, "No support for readdir_r()\n");
#endif
}


int            mdal_posix_closedir (MDAL_Context* ctx, DIR* dirp) {
   return closedir(dirp);
}





// ===========================================================================
// GENERAL
// ===========================================================================



static
int mdal_init(MDAL* mdal, MDAL_Type type) {

   mdal->type         = type;
   mdal->global_state = NULL;

   switch (type) {
   case MDAL_POSIX:
      mdal->ctx_init     = &mdal_posix_ctx_init;
      mdal->ctx_destroy  = &mdal_posix_ctx_destroy;
      mdal->open         = &mdal_posix_open;
      mdal->close        = &mdal_posix_close;
      mdal->write        = &mdal_posix_write;
      mdal->read         = &mdal_posix_read;
      mdal->getxattr     = &mdal_posix_getxattr;
      mdal->setxattr     = &mdal_posix_setxattr;
      mdal->mkdir        = &mdal_posix_mkdir;
      mdal->opendir      = &mdal_posix_opendir;
      mdal->readdir      = &mdal_posix_readdir;
      mdal->readdir_r    = &mdal_posix_readdir_r;
      mdal->closedir     = &mdal_posix_closedir;
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


MDAL* find_mdal(MDAL_Type type) {

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
   
   mdal_init(new_mdal, type);

   mdal_vec[mdal_count] = new_mdal;
   ++ mdal_count;
   return new_mdal;
}
