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

#include "marfs_auto_config.h"

#ifdef HAVE_RENAMEAT2
#include <sys/syscall.h>
#include <linux/fs.h>
#endif

#include "logging.h"
#include "mdal.h"

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


int   default_mdal_config(struct MDAL*     mdal,
                          xDALConfigOpt**  opts,
                          size_t           opt_count) {
   mdal->global_state = opts;
   return 0;
}


// Use these if you don't need to do anything special to initialize the
// context before-opening / after-closing for file-oriented or
// directory-oriented operations.
int     default_mdal_file_ctx_init(MDAL_Context* ctx, MDAL* mdal) {
   ctx->flags   = 0;
   ctx->data.sz = 0;
   return 0;
}
int     default_mdal_file_ctx_destroy(MDAL_Context* ctx, MDAL* mdal) {
   return 0;
}



int     default_mdal_dir_ctx_init (MDAL_Context* ctx, MDAL* mdal) {
   ctx->flags   = 0;
   ctx->data.sz = 0;
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



// --- file-ops

void*   posix_open(MDAL_Context* ctx, const char* path, int flags, ...) {
   va_list ap;
   va_start(ap, flags);
   
   if(flags & O_CREAT) {
      mode_t mode = va_arg(ap, mode_t);
      POSIX_FD(ctx) = open(path, flags, mode);
   }
   else {
      POSIX_FD(ctx) = open(path, flags);
   }
   // clean up
   va_end(ap);
   return ctx;
}

int     posix_is_open(MDAL_Context* ctx) {
   return (POSIX_FD(ctx) > 0);
}

int     posix_close(MDAL_Context* ctx) {
   int retval = close(POSIX_FD(ctx));
   POSIX_FD(ctx) = 0;
   return retval;
}


ssize_t posix_read (MDAL_Context* ctx, void* buf, size_t count) {
   return read(POSIX_FD(ctx), buf, count);
}

ssize_t posix_write(MDAL_Context* ctx, const void* buf, size_t count) {
   return write(POSIX_FD(ctx), buf, count);
}


int     posix_ftruncate(MDAL_Context* ctx, off_t length) {
   return ftruncate(POSIX_FD(ctx), length);
}

off_t   posix_lseek(MDAL_Context* ctx, off_t offset, int whence) {
   return lseek(POSIX_FD(ctx), offset, whence);
}


// --- file-ops (context-free)

int     posix_euidaccess(const char* path, int mask) {
   #ifdef _GNU_SOURCE
      return euidaccess(path, mask);
   #else
      #warning "Missing definition of _GNU_SOURCE: posix_euidaccess() is falling back to use of the access() syscall"
      return access(path, mask);
   #endif
}

int     posix_faccessat(int fd, const char* path, int mask, int flags) {
   return faccessat(fd, path, mask, flags);
}

int     posix_rename(const char* from, const char* to) {
   return rename( from, to );
}

int     posix_link( const char* oldpath, const char* newpath ) {
   return link( oldpath, newpath );
}

int     posix_readlink(const char* path, char* buf, size_t size) {
   return readlink(path, buf, size);
}

int     posix_mknod(const char* path, mode_t mode, dev_t dev) {
   return mknod(path, mode, dev);
}

int     posix_chmod(const char* path, mode_t mode) {
   return chmod(path, mode);
}

int     posix_truncate(const char* path, off_t length) {
   return truncate(path, length);
}

int     posix_lchown(const char* path, uid_t owner, gid_t group)
{
   return lchown(path, owner, group);
}

int     posix_lstat(const char* path, struct stat* st) {
   return lstat(path, st);
}


ssize_t posix_lgetxattr(const char* path, const char* name,
                             void* value, size_t size) {
   return lgetxattr(path, name, value, size);
}

ssize_t posix_lsetxattr(const char* path, const char* name,
                             const void* value, size_t size, int flags) {
   return lsetxattr(path, name, value, size, flags);
}

int     posix_lremovexattr(const char* path, const char* name) {
   return lremovexattr(path, name);
}

ssize_t posix_llistxattr(const char* path, char* list, size_t size) {
   return llistxattr(path, list, size);
}

int     posix_symlink(const char* target, const char* linkname) {
   return symlink(target, linkname);
}

int     posix_unlink(const char* path) {
   return unlink(path);
}

int     posix_utime(const char* filename, const struct utimbuf* times) {
   return utime(filename, times);
}

int     posix_utimensat(int dirfd, const char* pathname,
                        const struct timespec times[2], int flags)
{
   return utimensat(dirfd, pathname, times, flags);
}

// --- directory-ops

int     posix_mkdir (const char* path, mode_t mode) {
   return mkdir(path, mode);   
}

int     posix_rmdir(const char* path) {
   return rmdir(path);
}

void*   posix_opendir (MDAL_Context* ctx, const char* path) {
   POSIX_DIRP(ctx) = opendir(path);
   return POSIX_DIRP(ctx);
}


// lifted from marfs_readdir()
int     posix_readdir (MDAL_Context*      ctx,
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


// int            posix_readdir_r(MDAL_Context* ctx, DIR* dirp,
//                                     struct dirent* entry, struct dirent** result) {
// #if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
//    return readdir_r(POSIX_DIRP(dirp), entry, result);
// #else
// # error "No support for readdir_r()"
//    LOG(LOG_ERR, "No support for readdir_r()\n");
// #endif
// }


int     posix_closedir (MDAL_Context* ctx) {
   return closedir((DIR*)POSIX_DIRP(ctx));
}


int     posix_statvfs(const char* path, struct statvfs* statbuf) {
   return statvfs(path, statbuf);
}



MDAL posix_mdal = {
   .name         = "POSIX",
   .name_len     = 5, // strlen("POSIX"),

   .global_state = NULL,
   .config       = &default_mdal_config,

   .f_init       = &default_mdal_file_ctx_init,
   .f_destroy    = &default_mdal_file_ctx_destroy,

   .d_init       = &default_mdal_dir_ctx_init,
   .d_destroy    = &default_mdal_dir_ctx_destroy,

   .open         = &posix_open,
   .close        = &posix_close,
   .write        = &posix_write,
   .read         = &posix_read,
   .ftruncate    = &posix_ftruncate,
   .lseek        = &posix_lseek,

   .euidaccess   = &posix_euidaccess,
   .faccessat    = &posix_faccessat,
   .mknod        = &posix_mknod,
   .chmod        = &posix_chmod,
   .truncate     = &posix_truncate,
   .lchown       = &posix_lchown,
   .lstat        = &posix_lstat,
   .rename       = &posix_rename,
   .link         = &posix_link,
   .readlink     = &posix_readlink,
   .lgetxattr    = &posix_lgetxattr,
   .lsetxattr    = &posix_lsetxattr,
   .lremovexattr = &posix_lremovexattr,
   .llistxattr   = &posix_llistxattr,
   .symlink      = &posix_symlink,
   .unlink       = &posix_unlink,

   .utime        = &posix_utime,
   .utimensat    = &posix_utimensat,

   .mkdir        = &posix_mkdir,
   .rmdir        = &posix_rmdir,
   .opendir      = &posix_opendir,
   .readdir      = &posix_readdir,
   .closedir     = &posix_closedir,

   .statvfs      = &posix_statvfs,

   .is_open      = &posix_is_open
};





// ===========================================================================
// GENERAL
// ===========================================================================



// should be plenty
// static const size_t MAX_MDAL = 32; // stupid compiler
#define MAX_MDAL 32

static MDAL*  mdal_list[MAX_MDAL];
static size_t mdal_count = 0;



# define DL_CHECK(SYM)                                                  \
   if (! mdal->SYM) {                                                    \
      LOG(LOG_ERR, "MDAL '%s' has no symbol '%s'\n", mdal->name, #SYM);   \
      return -1;                                                         \
   }

// add a new MDAL to mdal_list
int install_MDAL(MDAL* mdal) {

   if (! mdal) {
      LOG(LOG_ERR, "NULL arg\n");
      return -1;
   }

   // insure that no MDAL with the given name already exists
   int i;
   for (i=0; i<mdal_count; ++i) {
      if ((mdal->name_len == mdal_list[i]->name_len)
          && (! strcmp(mdal->name, mdal_list[i]->name))) {

         LOG(LOG_ERR, "MDAL named '%s' already exists\n", mdal->name);
         return -1;
      }
   }

   // validate that MDAL has all required members
   DL_CHECK(name);
   DL_CHECK(name_len);

   DL_CHECK(config);
   
   DL_CHECK(f_init);
   DL_CHECK(f_destroy);

   DL_CHECK(d_init);
   DL_CHECK(d_destroy);

   DL_CHECK(open);
   DL_CHECK(close);
   DL_CHECK(read);
   DL_CHECK(write);
   DL_CHECK(ftruncate);
   DL_CHECK(lseek);

   DL_CHECK(euidaccess);
   DL_CHECK(faccessat);
   DL_CHECK(mknod);
   DL_CHECK(chmod);
   DL_CHECK(truncate);
   DL_CHECK(lchown);
   DL_CHECK(lstat);
   DL_CHECK(rename);
   DL_CHECK(link);
   DL_CHECK(readlink);
   DL_CHECK(lgetxattr);
   DL_CHECK(lsetxattr);
   DL_CHECK(lremovexattr);
   DL_CHECK(llistxattr);
   DL_CHECK(symlink);
   DL_CHECK(unlink);

   DL_CHECK(utime);
   DL_CHECK(utimensat);

   DL_CHECK(mkdir);
   DL_CHECK(rmdir);
   DL_CHECK(opendir);
   DL_CHECK(readdir);
   DL_CHECK(closedir);

   DL_CHECK(statvfs);
   DL_CHECK(is_open);

   if (mdal_count >= MAX_MDAL) {
         LOG(LOG_ERR,
             "No room for MDAL '%s'.  Increase MAX_MDAL_COUNT and rebuild.\n",
             mdal->name);
         return -1;
   }

   // install
   LOG(LOG_INFO, "Installing MDAL '%s'\n", mdal->name);
   mdal_list[mdal_count] = mdal;
   ++ mdal_count;

   return 0;
}

# undef DL_CHECK



// Untested support for dynamically-loaded MDAL. This is not a link-time
// thing, but a run-time thing.  The name in the configuration is something
// like: "DYNAMIC /path/to/my/lib", and we go look for all the MDAL symbols
// (e.g. mdal_open()) in that module, and install a corresponding MDAL.
// *All* MDAL symbols must be defined in the library.

static
MDAL* dynamic_MDAL(const char* name) {

   MDAL* mdal = (MDAL*)calloc(1, sizeof(MDAL));
   if (! mdal) {
      LOG(LOG_ERR, "no memory for new MDAL '%s'\n", name);
      return NULL;
   }
   
   // zero everything, so if we forget to update something it will be obvious
   memset(mdal, 0, sizeof(MDAL));

   mdal->name     = name;
   mdal->name_len = strlen(name);

   if (! strcmp(name, "DYNAMIC")) {

      // second token is library-path
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

      mdal->global_state = NULL;
      mdal->config       = (mdal_config)     dlsym(lib, "config");

      mdal->f_init       = (mdal_file_ctx_init)    dlsym(lib, "f_init");
      mdal->f_destroy    = (mdal_file_ctx_destroy) dlsym(lib, "f_destroy");

      mdal->d_init       = (mdal_dir_ctx_init)     dlsym(lib, "d_init");
      mdal->d_destroy    = (mdal_dir_ctx_destroy)  dlsym(lib, "d_destroy");

      mdal->open         = (mdal_open)       dlsym(lib, "mdal_open");
      mdal->close        = (mdal_close)      dlsym(lib, "mdal_close");
      mdal->write        = (mdal_write)      dlsym(lib, "mdal_write");
      mdal->read         = (mdal_read)       dlsym(lib, "mdal_read");
      mdal->ftruncate    = (mdal_ftruncate)  dlsym(lib, "mdal_ftruncate");
      mdal->lseek        = (mdal_lseek)      dlsym(lib, "mdal_lseek");

      mdal->euidaccess   = (mdal_euidaccess) dlsym(lib, "mdal_euidaccess");
      mdal->faccessat    = (mdal_faccessat)  dlsym(lib, "mdal_faccessat");
      mdal->mknod        = (mdal_mknod)      dlsym(lib, "mdal_mknod");
      mdal->chmod        = (mdal_chmod)      dlsym(lib, "mdal_chmod");
      mdal->truncate     = (mdal_truncate)   dlsym(lib, "mdal_truncate");
      mdal->lchown       = (mdal_lchown)     dlsym(lib, "mdal_lchown");
      mdal->lstat        = (mdal_lstat)      dlsym(lib, "mdal_lstat");
      mdal->rename       = (mdal_rename)     dlsym(lib, "mdal_rename");
      mdal->link         = (mdal_link)       dlsym(lib, "mdal_link");
      mdal->readlink     = (mdal_readlink)   dlsym(lib, "mdal_readlink");
      mdal->lgetxattr    = (mdal_lgetxattr)  dlsym(lib, "mdal_lgetxattr");
      mdal->lsetxattr    = (mdal_lsetxattr)  dlsym(lib, "mdal_lsetxattr");
      mdal->lremovexattr = (mdal_lremovexattr)  dlsym(lib, "mdal_lremovexattr");
      mdal->llistxattr   = (mdal_llistxattr) dlsym(lib, "mdal_llistxattr");
      mdal->symlink      = (mdal_symlink)    dlsym(lib, "mdal_symlink");
      mdal->unlink       = (mdal_unlink)     dlsym(lib, "mdal_unlink");

      mdal->utime        = (mdal_utime)      dlsym(lib, "mdal_utime");
      mdal->utimensat    = (mdal_utimensat)  dlsym(lib, "mdal_utimensat");

      mdal->mkdir        = (mdal_mkdir)      dlsym(lib, "mdal_mkdir");
      mdal->rmdir        = (mdal_rmdir)      dlsym(lib, "mdal_rmdir");
      mdal->opendir      = (mdal_opendir)    dlsym(lib, "mdal_opendir");
      mdal->readdir      = (mdal_readdir)    dlsym(lib, "mdal_readdir");
      mdal->closedir     = (mdal_closedir)   dlsym(lib, "mdal_closedir");

      mdal->statvfs      = (mdal_statvfs)    dlsym(lib, "mdal_statvfs");

      mdal->is_open      = (mdal_is_open)    dlsym(lib, "mdal_is_open");

      // done
      dlclose(lib);

   }
   else {

      // unknown name
      return NULL;
   }

   return mdal;
}




MDAL* get_MDAL(const char* name) {
   static int needs_init = 1;
   if (needs_init) {

      // one-time initialization of mdal_list
      assert(! install_MDAL(&posix_mdal) );
      needs_init = 0;
   }

   // look up <name> in known MDALs
   size_t name_len = strlen(name);
   int i;
   for (i=0; i<mdal_count; ++i) {
      if ((name_len == mdal_list[i]->name_len)
          && (! strcmp(name, mdal_list[i]->name))) {

         return mdal_list[i];
      }
   }

   // not found.  Maybe it was dynamic?
   if (! strcmp(name, "DYNAMIC")) {
      MDAL* dynamic = dynamic_MDAL(name);
      assert(! install_MDAL(dynamic) );
      return dynamic;
   }

   return NULL;
}

