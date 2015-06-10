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

#include "common.h"

#include <sys/types.h>          /* uid_t */
#include <unistd.h>
#include <attr/xattr.h>
#include <errno.h>
#include <stdlib.h>             /* calloc() */
#include <string.h>
#include <stdio.h>              /* rename() */
#include <assert.h>
#include <stdarg.h>


// ---------------------------------------------------------------------------
// COMMON
//
// These are functions to support the MarFS fuse implementation (and pftool
// TBD).  These should generally return zero for true, and non-zero for
// errors.  They should not invert the values to negative, for fuse.  The
// fuse impl takes care of that.
// ---------------------------------------------------------------------------



// push_user()
//
//   Save current user info from syscall into saved_user
//   Set userid to requesting user (in fuse request structure)
//   return 0/negative for success/error
//
// NOTE: If seteuid() fails, and the problem is that we lack privs to call
//       seteuid(), this means *we* are running unpriviledged.  This could
//       happen if we're testing the FUSE mount as a non-root user.  In
//       this case, the <uid> argument should be the same as the <uid> of
//       the FUSE process, which we can extract from the fuse_context.
//       [We try the seteuid() first, for speed]
//
// NOTE: setuid() doesn't allow returning to priviledged user, from
//       unpriviledged-user.  For that, we apparently need the (BSD)
//       seteuid().
//       
int push_user(uid_t* saved_euid) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
   //   fuse_context* ctx = fuse_get_context();
   //   if (ctx->flags & PUSHED_USER) {
   //      LOG(LOG_ERR, "push_user -- already pushed!\n");
   //      return;
   //   }
   *saved_euid = geteuid();
   uid_t new_uid = fuse_get_context()->uid;
   LOG(LOG_INFO, "user %ld (euid %ld) -> (euid %ld) ...\n",
       (size_t)getuid(), (size_t)*saved_euid, (size_t)new_uid);
   int rc = seteuid(new_uid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_uid == getuid())) {
         LOG(LOG_INFO, "failed (but okay)\n");
         return 0;              /* okay [see NOTE] */
      }
      else {
         LOG(LOG_ERR, "failed!\n");
         return -1;
      }
   }
   LOG(LOG_INFO, "success\n");
   return 0;
#else
#  error "No support for seteuid()"
#endif
}


//  push_user() changes the effective UID.  Here, we revert to the
//  "real" UID.
int pop_user(uid_t* saved_euid) {
#if (_BSD_SOURCE || _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
   uid_t  new_uid = *saved_euid;
   int rc = seteuid(new_uid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_uid == getuid()))
         return 0;              /* okay [see NOTE] */
      else {
         LOG(LOG_ERR,
             "pop_user -- user %ld (euid %ld) failed seteuid(%ld)!\n",
             (size_t)getuid(), (size_t)geteuid(), (size_t)new_uid);
         return -1;
      }
   }
   return 0;
#else
#  error "No support for seteuid()"
#endif
}



// NOTE: The Attractive Chaos tools actually include a suffix-tree data-structure.
//       (Actually a suffix array.)  
int expand_path_info(PathInfo*   info, /* side-effect */
                     const char* path) {
   LOG(LOG_INFO, "path %s\n", path);

   // (pass path, address to stuff, batch/interactive, working with existing file)
   if (! info) {
      errno = ENOENT;
      return -1;            /* no such file or directory */
   }

#if 0
   // NOTE: It's not always the same path that is being expanded.
   //       Suppressing this test (for now) so that we will always do the
   //       expansion.  However, we do want to set the flag, so,
   //       e.g. has_any_xattrs() can make sure it is being called with an
   //       expanded PathInfo.
   if (info->flags & PI_EXPANDED)
      return 0;
#endif

   // Take user supplied path in fuse request structure to look up info,
   // using MAR_mnttop and look up in MAR_namespace array, fill in all
   // MAR_namespace and MAR_repo info into a big structure to hold
   // everything we know about that path.  We also compute the full path of
   // the corresponding entry in the MDFS (see Namespace.md_path).
   info->ns = find_namespace(path);
   if (! info->ns) {
      errno = ENOENT;
      return -1;            /* no such file or directory */
   }

   LOG(LOG_INFO, "namespace %s\n", info->ns->mnt_suffix);

   const char* sub_path = path + info->ns->mnt_suffix_len; /* below fuse mount */
   snprintf(info->md_path, MARFS_MAX_MD_PATH,
            "%s%s", info->ns->md_path, sub_path);

   LOG(LOG_INFO, "sub-path %s\n", sub_path);
   LOG(LOG_INFO, "md-path  %s\n", info->md_path);

   // don't let users into the trash
   if (! strcmp(info->md_path, info->trash_path)) {
      LOG(LOG_ERR, "users can't access trash_path (%s)\n", info->md_path);
      errno = EPERM;
      return -1;
   }

   // you need to pass in is this interactive (fuse) or batch
   // so you can use iperms or bperms as the perms to use)
   //
   // [jti: we now handle this by having callers pass whichever perms they
   //       want to test, calling CHECK_PERMS() within fuse routines.]

   // if this is an existing file operation (you need to pass in if this
   // may be a existing file op) and if it is you need to pull in the
   // Xattrs from the existing file (use stat_xattrs() and put them into the
   // structure as well, so you have everything needed to deal with this
   // operation, whatever it might be
   //
   // the reason you need to get the xattrs from the existing file for some
   // ops is that you need that info for how to do the subsequent read op
   //
   // [jti: we now have fuse routines call stat_xattrs() explicitly, when needed.]


   info->flags |= PI_EXPANDED;
   return 0;
}


// expand_path_info() deferred computing the trash-path name until needed.
// Now's the time.
// Trash-path gets a time-stamp added, so that if the same file is
// trashed many times, we can find all the versions. (?)
int expand_trash_info(PathInfo*    info,
                      const char*  path) {
   size_t rc;                   // __TRY() assumes this exists

   // won't hurt (much), if it's already been done.
   __TRY0(expand_path_info, info, path);

   if (! (info->flags & PI_TRASH_PATH)) {
      const char* sub_path = path + info->ns->mnt_suffix_len; /* below fuse mount */

      // construct date-time string in standard format
      char       date_string[MARFS_DATE_STRING_MAX];
      time_t     now = time(NULL);
      if (now == (time_t)-1)
         return -1;

      __TRY0(epoch_to_str, date_string, MARFS_DATE_STRING_MAX, &now);

      // construct trash-path
      if (snprintf(info->trash_path, MARFS_MAX_MD_PATH,
                   "%s/%s.trash_%s",
                   info->ns->trash_path, sub_path, date_string) < 0)
         return -1;

      // subsequent calls to expand_path_info() are NOP.
      info->flags |= PI_TRASH_PATH;
   }

   return 0;
}




int stat_regular(PathInfo* info) {

   size_t rc;

   if (info->flags & PI_STAT_QUERY)
      return 0;                 /* already called stat_regular() */

   memset(&(info->st), 0, sizeof(struct stat));
   __TRY0(lstat, info->md_path, &info->st);

   info->flags |= PI_STAT_QUERY;
   return 0;
}


// return non-zero if info->md_path exists
int md_exists(PathInfo* info) {
   assert(info->flags & PI_EXPANDED); /* expand_path_info() was called? */
   stat_regular(info);                /* no-op, if already done */
   return (info->st.st_ino != 0);
}




XattrSpec*  MarFS_xattr_specs = NULL;

int init_xattr_specs() {

   // these are used by a parser (e.g. stat_xattrs())
   // The string in MarFS_XattrPrefix is appended to all of them
   // TBD: free this in clean-up
   MarFS_xattr_specs = (XattrSpec*) calloc(4, sizeof(XattrSpec));

   MarFS_xattr_specs[0] = (XattrSpec) { XVT_PRE,     MarFS_XattrPrefix "objid" };
   MarFS_xattr_specs[1] = (XattrSpec) { XVT_POST,    MarFS_XattrPrefix "post" };
   MarFS_xattr_specs[2] = (XattrSpec) { XVT_RESTART, MarFS_XattrPrefix "restart" };

   MarFS_xattr_specs[3] = (XattrSpec) { XVT_NONE,    NULL };

   return 0;
}



// stat_xattrs()
//
// Find all the reserved xattrs on <path>.  These key-values are all parsed
// and stored into specific fields of a MarFS_ReservedXattr struct.  You
// must have called expand_path_info, first, so that PathInfo.md_path has
// been initialized.  Quick-and-dirty parser.
//
// NOTE: It should not be an error to fail to find xattrs, or to fail to
//       find all of the ones needed to fill out the Reserved struct.
//       Caller can check flags, afterwards to see that.



// Attempt to read all MarFS system-xattrs from the file.  All values are
// assumed to be ascii text.  Parse these values to populate fields in the
// corresponding structs, in PathInfo.
//
// For each found xattr, we set the corresponding flag (XattrValueType) in
// PathInfo.xattrs.  Then, you can use has_any_xattrs() to test wheter
// specific xattrs, or groups of xattrs, were found.
// 
//  Need to call stat() first, so caller will know (by failing return) that
//  failure to get xattrs means either an existing file without xattrs
//  (i.e. Repo.access_proto=DIRECT), or a non-existent file.  We also need
//  the stat info in order to construct new xattr field-values (e.g. MD
//  path-name.)
//
//  if no objtype xattr then its just stat
//  if objtype exists get entire H2O_reserved_xattr list
//
// *** NOTE: IT IS NOT AN ERROR FOR THERE TO BE NO XATTRS!  We should only
//       return non-zero when something goes wrong.  Caller can call
//       has_marfs_xattrs(info, mask) to see whether we found given ones.
//
int stat_xattrs(PathInfo* info) {

   size_t rc;

   if (info->flags & PI_XATTR_QUERY)
      return 0;                 // already did this


   // call stat_regular().
   __TRY0(stat_regular, info);

   // go through the list of reserved Xattrs, and install string values into
   // fields of the corresponding structs, in PathInfo.
   char       xattr_value_str[MARFS_MAX_XATTR_SIZE];
   XattrSpec* spec;
   for (spec=MarFS_xattr_specs; spec->value_type!=XVT_NONE; ++spec) {

      switch (spec->value_type) {

      case XVT_PRE: {
         // object-IDs encode data that fills out an XattrPre struct
         // NOTE: If obj doesn't exist, its md_ctime will match the
         //       ctime currently found in info->st, as a result of
         //       the call to stat_regular(), above.

         if (lgetxattr(info->md_path, spec->key_name,
                       xattr_value_str, MARFS_MAX_XATTR_SIZE) != -1) {
            // got the xattr-value.  Parse it into info->pre
            LOG(LOG_INFO, "XVT_PRE %s\n", xattr_value_str);
            __TRY0(str_2_pre, &info->pre, xattr_value_str, &info->st);
            LOG(LOG_INFO, "md_ctime: %016lx, obj_ctime: %016lx\n",
                info->pre.md_ctime, info->pre.obj_ctime);
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if (errno == ENOATTR) {
            // ENOATTR means no attr, or no access.  Treat as the former.
            __TRY0(init_pre, &info->pre,
                   OBJ_FUSE, info->ns, info->ns->iwrite_repo, &info->st);
            info->flags |= PI_PRE_INIT;
         }
         else {
            LOG(LOG_INFO, "lgetxattr -> err (%d) %s\n", errno, strerror(errno));
            return -1;
         }
         break;
      }

      case XVT_POST: {
         if (lgetxattr(info->md_path, spec->key_name,
                       xattr_value_str, MARFS_MAX_XATTR_SIZE) != -1) {
            // got the xattr-value.  Parse it into info->pre
            LOG(LOG_INFO, "XVT_POST %s\n", xattr_value_str);
            __TRY0(str_2_post, &info->post, xattr_value_str);
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if (errno == ENOATTR) {
            // ENOATTR means no attr, or no access.  Treat as the former.
            __TRY0(init_post, &info->post, info->ns, info->ns->iwrite_repo);
            info->flags |= PI_POST_INIT;
         }
         else {
            LOG(LOG_INFO, "lgetxattr -> err (%d) %s\n", errno, strerror(errno));
            return -1;
         }
         break;
      }

      case XVT_RESTART: {
         info->flags &= ~(PI_RESTART); /* default = NOT in restart mode */
         ssize_t val_size = lgetxattr(info->md_path, spec->key_name,
                                      &xattr_value_str, 2);
         if (val_size < 0) {
            if (errno == ENOATTR)
               break;           /* treat ENOATTR as restart=0 */
            LOG(LOG_INFO, "lgetxattr -> err (%d) %s\n", errno, strerror(errno));
            return -1;
         }
         LOG(LOG_INFO, "XVT_RESTART\n");
         info->xattrs |= spec->value_type; /* found this one */
         if (val_size && (xattr_value_str[0] & ~'0')) /* value is not '0' */
            info->flags |= PI_RESTART;
         break;
      }

      case XVT_SLAVE: {
         // TBD ...
         LOG(LOG_ERR, "slave xattr TBD\n");
         break;
      }


      default:
         // a key was added to MarFS_attr_specs, but stat_xattrs() wasn't updated
         LOG(LOG_ERR, "unknown xattr %d = '%s'\n", spec->value_type, spec->key_name);
         assert(0);
      };
   }

   // subsequent calls can skip processing
   info->flags |= PI_XATTR_QUERY;


   // if you have ANY of the MarFS xattrs, you should have ALL of them
   // NOTE: These will call stat_xattrs(), but skip out because of PI_XATTR_QUERY
   if (has_any_xattrs(info, MARFS_MD_XATTRS)
       && ! has_all_xattrs(info, MARFS_MD_XATTRS)) {
      LOG(LOG_ERR, "%s -- incomplete MD xattrs\n", info->md_path);
      errno = EINVAL;            /* ?? */
      return -1;
   }


   return 0;                    /* "success" */
}



// Return non-zero if info->md_path has ALL/ANY of the reserved xattrs
// indicated in <mask>.  Else, zero.
//
// NOTE: Having these reserved xattrs indicates that the data-contents are
//       stored in object(s), described in the meta-data.  Otherwise, data
//       is stored directly in the md_path.

int has_all_xattrs(PathInfo* info, XattrMaskType mask) {
   assert(info->flags & PI_EXPANDED); /* expand_path_info() was called? */
   stat_xattrs(info);                  /* no-op, if already done */
   return ((info->xattrs & mask) == mask);
}
int has_any_xattrs(PathInfo* info, XattrMaskType mask) {
   assert(info->flags & PI_EXPANDED); /* expand_path_info() was called? */
   stat_xattrs(info);                  /* no-op, if already done */
   return (info->xattrs & mask);
}



// For all the attributes in <mask>, convert info xattrs to stringified values, and save
// on info->md_path.
int save_xattrs(PathInfo* info, XattrMaskType mask) {

   ENTRY();

   // call stat_regular().
   __TRY0(stat_regular, info);


   // go through the list of reserved Xattrs, and install string values into
   // fields of the corresponding structs, in PathInfo.
   char       xattr_value_str[MARFS_MAX_XATTR_SIZE];
   XattrSpec* spec;
   for (spec=MarFS_xattr_specs; spec->value_type!=XVT_NONE; ++spec) {

      // only save the xattrs selected by <mask>
      if (! (mask & spec->value_type)) {
         LOG(LOG_INFO, "skipping xattr %s ...\n", spec->key_name);
         continue;
      }
         
      LOG(LOG_INFO, "xattr %s ...\n", spec->key_name);
      switch (spec->value_type) {

      case XVT_PRE: {
         // object-IDs encode data that fills out an XattrPre struct
         // NOTE: If obj doesn't exist, its md_ctime will match the
         //       ctime currently found in info->st, as a result of
         //       the call to stat_regular(), above.

         // create the new xattr-value from info->pre
         __TRY0(pre_2_str, xattr_value_str, MARFS_MAX_XATTR_SIZE, &info->pre);
         LOG(LOG_INFO, "XVT_PRE %s\n", xattr_value_str);
         __TRY0(lsetxattr, info->md_path,
                spec->key_name, xattr_value_str, strlen(xattr_value_str)+1, 0);
         break;
      }

      case XVT_POST: {
         __TRY0(post_2_str, xattr_value_str, MARFS_MAX_XATTR_SIZE, &info->post);
         LOG(LOG_INFO, "XVT_POST %s\n", xattr_value_str);
         __TRY0(lsetxattr, info->md_path,
                spec->key_name, xattr_value_str, strlen(xattr_value_str)+1, 0);
         break;
      }

      case XVT_RESTART: {

         // TBD: Other flags could be combined into a single value to be
         //      stored as "flags" rather than just "restart".  Then the
         //      scan for files to restart (when restarting pftool) would
         //      just be "find inodes that have xattr with key 'flags'
         //      having value matching a given bit-pattern", rather than
         //      "find inodes that have xattr with key 'restart'"
         //
         //      [However, you'd want to be sure that no two processes
         //      would ever be racing to read/modify/write such an xattr.]

         // If the flag isn't set, then don't install (or remove) the xattr.
         if (info->flags & (PI_RESTART)) {
            LOG(LOG_INFO, "XVT_RESTART\n");
            xattr_value_str[0] = 1;
            xattr_value_str[1] = 0; // in case someone tries strlen
            __TRY0(lsetxattr, info->md_path,
                   spec->key_name, xattr_value_str, 2, 0);
         }
         else {
            ssize_t val_size = lremovexattr(info->md_path, spec->key_name);
            if (val_size < 0) {
               if (errno == ENOATTR)
                  break;           /* not a problem */
               LOG(LOG_INFO, "ERR removexattr(%s, %s) (%d) %s\n",
                   info->md_path, spec->key_name, errno, strerror(errno));
               return -1;
            }
         }
      }

      case XVT_SLAVE: {
         // TBD ...
         LOG(LOG_INFO, "slave xattr TBD\n");
         break;
      }


      default:
         // a key was added to MarFS_attr_specs, but stat_xattrs() wasn't updated
         LOG(LOG_ERR, "unknown xattr %d = '%s'\n", spec->value_type, spec->key_name);
      };
   }

   LOG(LOG_INFO, "exit\n");
   return 0;                    /* "success" */
}





// [trash_file]
// This is used to implement unlink().
//
// Rename MD file to trashfile, keeping all attrs.
// original is gone.
// Object-storage is untouched.
//
// NOTE: Should we do something to make this thread-safe (like unlink()) ?
//
int  trash_unlink(PathInfo*   info,
                const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   //    uniqueify name somehow with time perhaps == trashname, 
   //    rename file to trashname 
   //    trash_name()   record the full path in a related file in trash

   size_t rc;
   __TRY0(expand_trash_info, info, path); /* initialize info->trash_path */
   __TRY0(rename, info->md_path, info->trash_path);
   return 0;
}


// [trash_dup_file]
// This is used to implement truncate/ftruncate
//
// Copy trashed MD file into trash area, does NOT unlink original.
// Does NOT do anything with object-storage.
// Wipes all xattrs on original, and truncs to zero.
//
int  trash_truncate(PathInfo*   info,
                    const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   assert(info->flags & PI_EXPANDED);

   //    If this has no xattrs (its just a normal file using the md file
   //    for data) just trunc the file and return – we have nothing to
   //    clean up, too bad for the user as we aren’t going to keep the
   //    trunc’d file.

   size_t rc;
   __TRY0(stat_xattrs, info);
   if (! has_all_xattrs(info, MARFS_MD_XATTRS)) {
      __TRY0(truncate, info->md_path, 0);
      return 0;
   }

   //   uniqueify name somehow with time perhaps == trashname, 
   //   stat_xattrs()    to get all file attrs/xattrs
   //   open trashdir/trashname
   //
   //   if no xattr objtype [jti: i.e. info->xattr.obj_type == OBJ_UNI]
   //        copy file data to file
   //   if xattr objtype is multipart [jti: i.e. == OBJ_MULTI]
   //        copy file data until end of objlist marker
   //
   //   update trash file mtime to original mtime
   //   trunc trash file to same length as original
   //   set all reserved xattrs like original
   //
   //   close


   // capture mode-bits, etc.  Destination has all the same mode-bits,
   // for permissions and file-type bits only. [No, just permissions.]
   __TRY0(stat_regular, info);
   // mode_t new_mode = info->st.st_mode & (ACCESSPERMS); // ACCESSPERMS is only BSD
   mode_t new_mode = info->st.st_mode & (S_IRWXU|S_IRWXG|S_IRWXO); // more-portable

   // read from md_file
   int in = open(info->md_path, O_RDONLY);
   if (in == -1) {
      LOG(LOG_ERR, "open(%s, O_RDONLY) [oct]%o failed\n",
          info->md_path, new_mode);
      return -1;
   }

   // write to trash_file
   __TRY0(expand_trash_info, info, path);
   int out = open(info->trash_path, (O_CREAT | O_WRONLY), new_mode);
   if (out == -1) {
      LOG(LOG_ERR, "open(%s, (O_CREAT|O_WRONLY), [oct]%o) failed\n",
          info->trash_path, new_mode);
      return -1;
   }

   // MD files (except Packed) are trunc'ed to their "logical" size (the
   // size of the data they represent.  They may also contain some
   // "physical" data (blobs we have tucked inside to track object-storage.
   // Move the physical data, then trunc to logical size.
   off_t log_size = info->st.st_size;
   off_t phy_size = info->post.chunk_info_bytes;

   if (phy_size) {

      // buf used for data-transfer
      const size_t BUF_SIZE = 64 * 1024 * 1024; /* 64 MB */
      char         buf[BUF_SIZE];

      size_t read_size = ((phy_size < BUF_SIZE) ? phy_size : BUF_SIZE);
      size_t wr_total = 0;

      // copy phy-data from md_file to trash_file, one buf at a time
      size_t rd_count;
      for (rd_count = read(in, buf, read_size);
           rd_count > 0;
           rd_count = read(in, buf, read_size)) {

         char*  buf_ptr = buf;
         size_t remain  = rd_count;
         while (remain) {
            size_t wr_count = write(out, buf_ptr, remain);
            if (wr_count < 0) {
               LOG(LOG_ERR, "err writing %s (byte %ld)\n",
                   info->trash_path, wr_total);
               return -1;
            }
            remain   -= wr_count;
            wr_total += wr_count;
            buf_ptr  += wr_count;
         }

         size_t phy_remain = phy_size - wr_total;
         read_size = ((phy_remain < BUF_SIZE) ? phy_remain : BUF_SIZE);
      }
      if (rd_count < 0) {
         LOG(LOG_ERR, "err reading %s (byte %ld)\n",
             info->trash_path, wr_total);
         return -1;
      }

      __TRY0(close, in);
      __TRY0(close, out);

   }

   // trunc trash-file to size
   __TRY0(truncate, info->trash_path, log_size);

   // copy xattrs to the trash-file.
   // ugly-but-simple: make a duplicate PathInfo, but with md_path
   // set to our trash_path.  Then save_xattrs() will just work on the
   // trash-file.
   {  PathInfo trash_info = *info;
      memcpy(trash_info.md_path, trash_info.trash_path, MARFS_MAX_MD_PATH);

      // tweak the Post xattr to indicate to garbage-collector that this
      // file is in the trash.  The latest trick is to indicate this by
      // storing the full path to the trash-file inside the Post.gc_path
      // field.
      memcpy(trash_info.post.gc_path, info->trash_path, MARFS_MAX_MD_PATH);

      __TRY0(save_xattrs, &trash_info, MARFS_ALL_XATTRS);
   }

   // clean out everything on the original
   __TRY0(trunc_xattr, info);
   __TRY0(trash_name, info, path);

   return 0;
}



//   trunc file to zero
//   remove (not just reset but remove) all reserved xattrs
int trunc_xattr(PathInfo* info) {
   XattrSpec*  spec;
   for (spec=MarFS_xattr_specs; spec->value_type!=XVT_NONE; ++spec) {
      lremovexattr(info->md_path, spec->key_name);
   }   
   return 0;
}


int trash_name(PathInfo* info, const char* path) {
   // (pass in expanded_path_info_structure and file name and trashname)

   // make file in trash that has the full path of the file name and the
   // inode number in the file data (having inode helps you not have to
   // walk the tree for gpfs to do garbage collection/reclaim, you know
   // object id, object type (uni, multi, packed, striped), file name, and
   // inode.  Gpfs has ilm that will list all but the file path very
   // quickly.)

   // and call it $trashname.path
   return 0;
}





// return non-zero if there's no more space (according to user's quota).
//
// Namespace.fsinfo has a path to a file where info about overall
// space-usage is maintained in a custom way.  The idea is that a batch
// process will periodically crawl the MDFS to collect the amount of
// storage used, and it will store this information in the fsinfo file.
// Then mknod()/create() can look there to see whether an attempt to create
// a new object should be allowed to succeed.
//
// The first approach might be to store the contents of a fake statvfs
// struct in the file, maybe in some human-readable form, for convenience.
// However, the types of values in that struct may have some scaling
// problems: Our equivalent of statvfs.f_frsize might want to be as large
// as 10 GB, which couldn't fit into 32-bits, assuming an 'unsigned long'
// could be 32 bits.
//
// We could improve access time by making two fsinfo files, one having
// size trunc'ed to be multiplier, and the other having size trunc'ed to be
// a multiplicand.  Then we wouldn't have to do an open/read/write to check
// quotes, for every call to mknod().
//
// For 0.1, I'll do something even cheaper (and quicker): assume fsinfo is
// trunc'ed to the space-limit, and ignore name-limits.
//
// NOTE: We could possibly save ourselves some work by setting a flag
//       (e.g. PI_STATVFS) in info->flags, to avoid redundantly updating
//       info->stvfs, but probably we should be responsive to potential
//       ongoing updates to fsinfo, and just always read it.
//
// NOTE: During testing, I sometimes forget to create a dummy version of
//       this file.  That shouldn't happen in production, but if we're
//       testing this, and you're seeing an error in the log because this
//       file doesn't exist, just 'touch' it.

int check_quotas(PathInfo* info) {

   //   size_t rc;

   //   if (! (info->flags & PI_STATVFS))
   //      __TRY0(statvfs, info->ns->fsinfo, &info->stvfs);


   uint64_t  space_limit = ((uint64_t)info->ns->quota_space_units *
                            (uint64_t)info->ns->quota_space);
#if TBD
   uint64_t  names_limit = ((uint64_t)info->ns->quota_name_units *
                            (uint64_t)info->ns->quota_names);
#endif

   struct stat st;
   if (stat(info->ns->fsinfo_path, &st)) {
      LOG(LOG_ERR, "couldn't stat fsinfo at '%s': %s\n",
          info->ns->fsinfo_path, strerror(errno));
      errno = EINVAL;
      return -1;
   }
   return (st.st_size >= space_limit); /* 0 = OK,  1 = no-more-space */
}

