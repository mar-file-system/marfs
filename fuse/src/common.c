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
// NOTE: setuid() doesn't allow returning to priviledged user, from
//       unpriviledged-user.  For that, we apparently need the (BSD)
//       seteuid().
//
// NOTE: If seteuid() fails, and the problem is that we lack privs to call
//       seteuid(), this means *we* are running unpriviledged.  This could
//       happen if we're testing the FUSE mount as a non-root user.  In
//       this case, the <uid> argument should be the same as the <uid> of
//       the FUSE process, which we can extract from the fuse_context.
//       [We try the seteuid() first, for speed]
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
//
int expand_path_info(PathInfo*   info, /* side-effect */
                     const char* path) {
   LOG(LOG_INFO, "path        %s\n", path);

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
   // the corresponding entry in the MDFS (into PathInfo.pre.md_path).
   info->ns = find_namespace_by_path(path);
   if (! info->ns) {
      LOG(LOG_INFO, "no namespace for path %s\n", path);
      errno = ENOENT;
      return -1;            /* no such file or directory */
   }

   LOG(LOG_INFO, "namespace   %s\n", info->ns->name);
   LOG(LOG_INFO, "mnt_path    %s\n", info->ns->mnt_suffix);

   const char* sub_path = path + info->ns->mnt_suffix_len; /* below NS */
   int prt_count = snprintf(info->post.md_path, MARFS_MAX_MD_PATH,
                            "%s%s", info->ns->md_path, sub_path);
   if (prt_count < 0) {
      LOG(LOG_ERR, "snprintf(..., %s, %s) failed\n",
          info->ns->md_path,
          sub_path);
      return -1;
   }
   else if (prt_count >= MARFS_MAX_MD_PATH) {
      LOG(LOG_ERR, "snprintf(..., %s, %s) truncated\n",
          info->ns->md_path,
          sub_path);
      errno = EIO;
      return -1;
   }
   //   else
   //      // saves us a strlen(), later
   //      info->post.md_path_len = prt_count;

   LOG(LOG_INFO, "sub-path    %s\n", sub_path);
   LOG(LOG_INFO, "md-path     %s\n", info->post.md_path);


#if 0
   // Should be impossible, as long as the trash-dir is not below the mdfs-dir

   // don't let users into the trash
   if (! strcmp(info->post.md_path, info->trash_path)) {
      LOG(LOG_ERR, "users can't access trash_path (%s)\n", info->post.md_path);
      errno = EPERM;
      return -1;
   }
#endif


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
//
// Info in the trash supports three kinds of operations: (a) a Garbage
// Collection task, that may skim through the trash, deleting "trashed"
// files with age greater than some threshold, etc, (b) a Quota task, which
// skims through all the inodes in the MDFS (including trash), counting
// totals used per project, per-user, etc, but discounting storage taken up
// by trash, and (c) an "undelete" admin function, that can restore a
// trashed file to its original location.
//
// Files that are moved to the trash keep only the basename (i.e. the name
// of the file in the bottom-most directory of the full-path).  There may
// be multiple files in different directories with the same name, and a
// file in the same place may be deleted and created anew many times, so we
// "unique-ify" the file-name by appending the inode, and a timestamp.
// 
// We actually create two files in the trash, one that has all the xattrs
// of the original (so that we can find the corresponding object,
// object-type, etc) and another with contents that hold the full path of
// the original.  We considered achieving this latter objective by adding
// more xattr info, but part of the goal here is to facilitate the GC batch
// process, which may be doing a fast tree-walk using ILM, in GPFS.  In
// this latter case, not all of the xattr info is part of the inode
// data. We want to make sure the GC task can get all the info it needs
// from the fast-access list of inodes it will see, without further calls
// to lgetxattr(), etc.  So, we want to keep the amount of xattr info
// small.
//
// Each of the two files use the same path produced by expand trash_info,
// and one of them just adds a suffix ".path", with the contents as
// described above.

int expand_trash_info(PathInfo*    info,
                      const char*  path) {
   size_t rc;                   // __TRY() assumes this exists

   // won't hurt (much), if it's already been done.
   __TRY0(expand_path_info, info, path);

   if (! (info->flags & PI_TRASH_PATH)) {
      const char* sub_path  = path + info->ns->mnt_suffix_len; /* below fuse mount */
      char*       base_name = strrchr(sub_path, '/');
      base_name = (base_name ? base_name +1 : (char*)sub_path);

      // construct date-time string in standard format
      char       date_string[MARFS_DATE_STRING_MAX];
      time_t     now = time(NULL);
      if (now == (time_t)-1) {
         LOG(LOG_ERR, "time() failed\n");
         return -1;
      }
      __TRY0(epoch_to_str, date_string, MARFS_DATE_STRING_MAX, &now);

      // Won't hurt (much) if it's already been done.
      __TRY0(stat_regular, info);

      // these are the last 3 digits (base 10), of the inode
      ino_t   inode = info->st.st_ino; // shorthand
      uint8_t lo  = (inode % 10);
      uint8_t med = (inode % 100) / 10;
      uint8_t hi  = (inode % 1000) / 100;

      // FUTURE: find the actual shard to use
      const uint32_t shard = 0;

      // construct trash-path
      int prt_count = snprintf(info->trash_path, MARFS_MAX_MD_PATH,
                               "%s/%s.%d/%d/%d/%d/%s.trash_%010ld_%s",
                               info->ns->trash_path,
                               info->ns->name, shard,
                               hi, med, lo,
                               base_name,
                               info->st.st_ino,
                               date_string);
      if (prt_count < 0) {
         LOG(LOG_ERR, "snprintf(..., %s, %s, %010ld, %s) failed\n",
             info->ns->trash_path,
             base_name,
             info->st.st_ino,
             date_string);
         return -1;
      }
      else if (prt_count >= MARFS_MAX_MD_PATH) {
         LOG(LOG_ERR, "snprintf(..., %s, %s, %010ld, %s) truncated\n",
             info->ns->trash_path,
             base_name,
             info->st.st_ino,
             date_string);
         errno = EIO;
         return -1;
      }
      else if (prt_count + strlen(MARFS_TRASH_COMPANION_SUFFIX)
               >= MARFS_MAX_MD_PATH) {
         LOG(LOG_ERR, "no room for '%s' after trash_path '%s'\n",
             MARFS_TRASH_COMPANION_SUFFIX,
             info->trash_path);
         errno = EIO;
         return -1;
      }
      //      else
      //         // saves us a strlen(), later
      //         info->trash_path_len = prt_count;

      // subsequent calls to expand_trash_info() are NOP.
      info->flags |= PI_TRASH_PATH;
   }

   LOG(LOG_INFO, "trash_path  %s\n", info->trash_path);
   return 0;
}




int stat_regular(PathInfo* info) {

   size_t rc;

   if (info->flags & PI_STAT_QUERY)
      return 0;                 /* already called stat_regular() */

   memset(&(info->st), 0, sizeof(struct stat));
   __TRY0(lstat, info->post.md_path, &info->st);

   info->flags |= PI_STAT_QUERY;
   return 0;
}


// return non-zero if info->post.md_path exists
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
// must have called expand_path_info, first, so that PathInfo.post.md_path has
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

   size_t  rc;                  /* for __TRY() */
   ssize_t str_size;

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

         str_size = lgetxattr(info->post.md_path, spec->key_name,
                              xattr_value_str, MARFS_MAX_XATTR_SIZE);
         if (str_size != -1) {
            // got the xattr-value.  Parse it into info->pre
            xattr_value_str[str_size] = 0;
            LOG(LOG_INFO, "XVT_PRE %s\n", xattr_value_str);
            __TRY0(str_2_pre, &info->pre, xattr_value_str, &info->st);
            LOG(LOG_INFO, "md_ctime: %016lx, obj_ctime: %016lx\n",
                info->pre.md_ctime, info->pre.obj_ctime);
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if ((errno == ENOATTR)
                  || ((errno == EPERM) && S_ISLNK(info->st.st_mode))) {
            // (a) ENOATTR means no attr, or no access.  Treat as the former.
            // (b) GPFS returns EPERM for lgetxattr on symlinks.
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
         str_size = lgetxattr(info->post.md_path, spec->key_name,
                              xattr_value_str, MARFS_MAX_XATTR_SIZE);
         if (str_size != -1) {
            // got the xattr-value.  Parse it into info->pre
            xattr_value_str[str_size] = 0;
            LOG(LOG_INFO, "XVT_POST %s\n", xattr_value_str);
            __TRY0(str_2_post, &info->post, xattr_value_str);
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if ((errno == ENOATTR)
                  || ((errno == EPERM) && S_ISLNK(info->st.st_mode))) {
            // (a) ENOATTR means no attr, or no access.  Treat as the former.
            // (b) GPFS returns EPERM for lgetxattr on symlinks.
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
         ssize_t val_size = lgetxattr(info->post.md_path, spec->key_name,
                                      &xattr_value_str, 2);
         if (val_size < 0) {
            if ((errno == ENOATTR)
                || ((errno == EPERM) && S_ISLNK(info->st.st_mode)))
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

      case XVT_SHARD: {
         // TBD ...
         LOG(LOG_ERR, "shard xattr TBD\n");
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
      LOG(LOG_ERR, "%s -- incomplete MD xattrs\n", info->post.md_path);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // initialize the object-ID fields
   __TRY0(update_pre, &info->pre);

   return 0;                    /* "success" */
}



// Return non-zero if info->post.md_path has ALL/ANY of the reserved xattrs
// indicated in <mask>.  Else, zero.
//
// NOTE: Having these reserved xattrs indicates that the data-contents are
//       stored in object(s), described in the meta-data.  Otherwise, data
//       is stored directly in the post.md_path.

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
// on info->post.md_path.
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
         __TRY0(lsetxattr, info->post.md_path,
                spec->key_name, xattr_value_str, strlen(xattr_value_str)+1, 0);
         break;
      }

      case XVT_POST: {
         __TRY0(post_2_str, xattr_value_str, MARFS_MAX_XATTR_SIZE,
                &info->post, info->ns->iwrite_repo);
         LOG(LOG_INFO, "XVT_POST %s\n", xattr_value_str);
         __TRY0(lsetxattr, info->post.md_path,
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
            __TRY0(lsetxattr, info->post.md_path,
                   spec->key_name, xattr_value_str, 2, 0);
         }
         else {
            ssize_t val_size = lremovexattr(info->post.md_path, spec->key_name);
            if (val_size < 0) {
               if (errno == ENOATTR)
                  break;           /* not a problem */
               LOG(LOG_INFO, "ERR removexattr(%s, %s) (%d) %s\n",
                   info->post.md_path, spec->key_name, errno, strerror(errno));
               return -1;
            }
         }
      }

      case XVT_SHARD: {
         // TBD ...
         LOG(LOG_INFO, "shard xattr TBD\n");
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



// This is only used internally.  We just assume expand_trash_info() has
// already been called.
//
// In addition to moving the original to the trash, the two trash functions
// (trash_unlink() and trash_truncate()) also write the full-path of the
// original (MarFS) file into "<trash_path>.path".  [NOTE: This is not the
// same as the path to the file, where it now resides in trash, which is
// installed into the POST xattr by trash_unlink/trash_truncate.]
//
// QUESTION: Do we want to write the MDFS path into the companion trash
//     file, or should we write the MarFS path (i.e. the path the user
//     would've known it by)?  The latter would allow undelete to deal with
//     the case where a namespace now uses an MDFS located somewhere else,
//     but that could also be handled by moving trash (in such rare cases), and
//     relocating the stored paths.  The former is probably what's generally most
//     useful for undelete, allowing immediate movement of the file.
//
// If <utim> is non-NULL, then it holds mtime/atime values for us to
// install onto the file.  This is to allow "undelete" to also restore the
// original atime of a file.

static
int write_trash_companion_file(PathInfo*             info,
                               const char*           path,
                               const struct utimbuf* utim) {
   size_t  rc;
   ssize_t rc_ssize;

   __TRY0(expand_trash_info, info, path); /* initialize info->trash_path */

   // expand_trash_info() assures us there's room in MARFS_MAX_MD_PATH to
   // add MARFS_TRASH_COMPANION_SUFFIX, so no need to check.
   char companion_fname[MARFS_MAX_MD_PATH];
   __TRY_GE0(snprintf, companion_fname, MARFS_MAX_MD_PATH, "%s%s",
             info->trash_path,
             MARFS_TRASH_COMPANION_SUFFIX);

   // TBD: Don't want to depend on support for open(... (O_CREAT|O_EXCL)).
   //      Should just stat() the companion-file, before opening, to assure
   //      it doesn't already exist.
   LOG(LOG_INFO, "companion:  %s\n", companion_fname);
   __TRY_GE0(open, companion_fname, (O_WRONLY|O_CREAT), info->st.st_mode);
   int fd = rc_ssize;

#if 1
   // write MDFS path into the trash companion
   __TRY_GE0(write, fd, info->post.md_path, strlen(info->post.md_path));
#else
   // write MarFS path into the trash companion
   __TRY_GE0(write, fd, MarFS_mnt_top, MarFS_mnt_top_len);
   __TRY_GE0(write, fd, path, strlen(path));
#endif

   __TRY0(close, fd);

   // maybe install ctime/atime to support "undelete"
   if (utim)
      __TRY0(utime, companion_fname, utim);

   return 0;
}


// [trash_file]
// This is used to implement unlink().
//
// Rename MD file to trashfile, keeping all attrs.
// original is gone.
// Object-storage is untouched.
//
// NEW APPROACH: Because we want to allow trash directories to live outside
//     the file-system/file-set where the gpfs metadata is stored (e.g. so
//     there can be fewer trash directories than filesets), we can no
//     longer expect rename(2) to work.  We could *try* rename first, and
//     then fail-over to moving the data, but we're not sure whether that
//     could add considerable overhead in the case where the rename is
//     going to fail.  [NOTE: could we just compute this once, up front,
//     and store it as a flag in the Repo or Namespace structs?]  For now,
//     we just always do a copy + unlink.
//
//     On second thought, we want the inode of the truncated file to remain
//     the same (?) Therefore, rename(2) would always be wrong.

// NOTE: Should we do something to make this thread-safe (like unlink()) ?
//
int  trash_unlink(PathInfo*   info,
                  const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   assert(info->flags & PI_EXPANDED);

   //    If this has no xattrs (its just a normal file using the md file
   //    for data) just unlink the file and return we have nothing to
   //    clean up, too bad for the user as we aren't going to keep the
   //    unlinked file in the trash.
   //
   // NOTE: The has_all_xattrs test treats any files that don't have both
   //    POST and OBJID as though they were DIRECT, and just deletes them.
   //    Such files are malformed, lacking sufficient info to be cleaned-up
   //    when we take out the trash.
   //
   // NOTE: We don't put xattrs on symlinks, so they just get deleted.
   //
   size_t rc;
   __TRY0(stat_xattrs, info);
   if (! has_all_xattrs(info, MARFS_MD_XATTRS)) {
      LOG(LOG_INFO, "no xattrs\n");
      __TRY0(unlink, info->post.md_path);
      return 0;
   }

#if 0
   //    uniqueify name somehow with time perhaps == trashname, 

   __TRY0(expand_trash_info, info, path); /* initialize info->trash_path */
   LOG(LOG_INFO, "md_path:    '%s'\n", info->post.md_path);

   //    rename file to trashname 
   __TRY0(rename, info->post.md_path, info->trash_path);

   // copy xattrs to the trash-file.
   // ugly-but-simple: make a duplicate PathInfo, but with post.md_path
   // set to our trash_path.  Then save_xattrs() will just work on the
   // trash-file.
   {  PathInfo trash_info = *info;
      memcpy(trash_info.post.md_path, trash_info.trash_path, MARFS_MAX_MD_PATH);

      trash_info.post.flags |= POST_TRASH;

      __TRY0(save_xattrs, &trash_info, MARFS_ALL_XATTRS);
   }

   // write full-MDFS-path of original-file into similarly-named file
   __TRY0(write_trash_companion_file, info, path);

#else

   // we no longer assume that a simple rename into the trash will always
   // be possible (e.g. because trash will be in a different fileset, or
   // filesystem).  It was thought we shouldn't even *try* the rename
   // first.  Instead, we'll copy to the trash, then unlink the original.
   __TRY0(trash_truncate, info, path);
   __TRY0(unlink, info->post.md_path);

#endif

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
   assert(info->flags & PI_EXPANDED); // could just expand it ...

   //    If this has no xattrs (its just a normal file using the md file
   //    for data) just trunc the file and return we have nothing to
   //    clean up, too bad for the user as we aren't going to keep the
   //    trunc’d file.

   size_t rc;
   __TRY0(stat_xattrs, info);
   if (! has_all_xattrs(info, MARFS_MD_XATTRS)) {
      LOG(LOG_INFO, "no xattrs\n");
      __TRY0(truncate, info->post.md_path, 0);
      return 0;
   }

   //   uniqueify name somehow with time perhaps == trashname, 
   //   stat_xattrs()    to get all file attrs/xattrs
   //   open trashdir/trashname
   //
   //   if no xattr objtype [jti: i.e. OBJ_UNI]
   //        copy file data to file
   //   if xattr objtype is multipart [jti: i.e. == OBJ_MULTI]
   //        copy file data until end of objlist marker
   //
   //   update trash file mtime to original mtime
   //   trunc trash file to same length as original
   //   set all reserved xattrs like original
   //
   //   close

   __TRY0(stat_regular, info);

   // capture atime of the original, so this can be saved as the ctime of
   // the trash file.  This will allow "undelete" to restore the original
   // atime, while also letting us see the correct atime of the trash file.
   struct utimbuf trash_time;
   trash_time.modtime = info->st.st_atime; // trash mtime = orig atime

   time_t     now = time(NULL);
   if (now == (time_t)-1) {
      LOG(LOG_ERR, "time() failed\n");
      return -1;
   }
   trash_time.actime  = now;               // trash atime = now

   // capture mode-bits, etc.  Destination has all the same mode-bits,
   // for permissions and file-type bits only. [No, just permissions.]
   //
   // mode_t new_mode = info->st.st_mode & (ACCESSPERMS); // ACCESSPERMS is only BSD
   mode_t new_mode = info->st.st_mode & (S_IRWXU|S_IRWXG|S_IRWXO); // more-portable


   // we'll read from md_file
   int in = open(info->post.md_path, O_RDONLY);
   if (in == -1) {
      LOG(LOG_ERR, "open(%s, O_RDONLY) [oct]%o failed\n",
          info->post.md_path, new_mode);
      return -1;
   }

   // we'll write to trash_file
   __TRY0(expand_trash_info, info, path);
   int out = open(info->trash_path, (O_CREAT | O_WRONLY), new_mode);
   if (out == -1) {
      LOG(LOG_ERR, "open(%s, (O_CREAT|O_WRONLY), [oct]%o) failed\n",
          info->trash_path, new_mode);
      __TRY0(close, in);
      return -1;
   }

   // MD files are trunc'ed to their "logical" size (the size of the data
   // they represent.  They may also contain some "system" data (blobs we
   // have tucked inside, to track object-storage.  Move the physical data,
   // then trunc to logical size.
   off_t log_size = info->st.st_size;
   off_t phy_size = info->post.chunk_info_bytes;

   if (phy_size) {

      // buf used for data-transfer
      const size_t BUF_SIZE = 32 * 1024 * 1024; /* 32 MB */
      char* buf = malloc(BUF_SIZE);
      if (!buf) {
         LOG(LOG_ERR, "malloc %ld bytes failed\n", BUF_SIZE);

         // clean-up
         __TRY0(close, in);
         __TRY0(close, out);
         return -1;
      }

      size_t read_size = ((phy_size < BUF_SIZE) ? phy_size : BUF_SIZE);
      size_t wr_total = 0;

      // copy phy-data from md_file to trash_file, one buf at a time
      ssize_t rd_count;
      for (rd_count = read(in, (void*)buf, read_size);
           (read_size && (rd_count > 0));
           rd_count = read(in, (void*)buf, read_size)) {

         char*  buf_ptr = buf;
         size_t remain  = rd_count;
         while (remain) {
            size_t wr_count = write(out, buf_ptr, remain);
            if (wr_count < 0) {
               LOG(LOG_ERR, "err writing %s (byte %ld)\n",
                   info->trash_path, wr_total);

               // clean-up
               __TRY0(close, in);
               __TRY0(close, out);
               free(buf);
               return -1;
            }
            remain   -= wr_count;
            wr_total += wr_count;
            buf_ptr  += wr_count;
         }

         size_t phy_remain = phy_size - wr_total;
         read_size = ((phy_remain < BUF_SIZE) ? phy_remain : BUF_SIZE);
      }
      free(buf);
      if (rd_count < 0) {
         LOG(LOG_ERR, "err reading %s (byte %ld)\n",
             info->trash_path, wr_total);

         // clean-up
         __TRY0(close, in);
         __TRY0(close, out);
         return -1;
      }
   }

   // clean-up
   __TRY0(close, in);
   __TRY0(close, out);

   // trunc trash-file to size
   __TRY0(truncate, info->trash_path, log_size);

   // copy xattrs to the trash-file.
   // ugly-but-simple: make a duplicate PathInfo, but with post.md_path
   // set to our trash_path.  Then save_xattrs() will just work on the
   // trash-file.
   PathInfo trash_info = *info;
   memcpy(trash_info.post.md_path, trash_info.trash_path, MARFS_MAX_MD_PATH);
   trash_info.post.flags |= POST_TRASH;
   __TRY0(save_xattrs, &trash_info, MARFS_ALL_XATTRS);


   // write full-MDFS-path of original-file into trash-companion file
   __TRY0(write_trash_companion_file, info, path, &trash_time);

   // update trash-file atime/mtime to support "undelete"
   __TRY0(utime, info->trash_path, &trash_time);

   // clean out everything on the original
   __TRY0(trunc_xattr, info);

   // old stat-info and xattr-info is obsolete.  Generate new obj-ID, etc.
   info->flags &= ~(PI_STAT_QUERY | PI_XATTR_QUERY | PI_PRE_INIT | PI_POST_INIT);
   __TRY0(stat_xattrs, info);   // has none, so initialize from scratch

   // NOTE: Unique-ness of Object-IDs currently comes from inode, plus
   //     obj-ctime, plus MD-file ctime.  It's possible the trashed file
   //     was created in the same second as this one, in which case, we'd
   //     be setting up to overwrite the object used by the trashed file.
   //     Adding pre.unique to the object-ID, which we increment from the
   //     version in the trash
   if (!strcmp(info->pre.objid, trash_info.pre.objid)) {
      info->pre.unique = trash_info.pre.unique +1;
      __TRY0(update_pre, &info->pre);
      LOG(LOG_INFO, "unique: '%s'\n", info->pre.objid);
   }

   return 0;
}



//   trunc file to zero
//   remove (not just reset but remove) all reserved xattrs

int trunc_xattr(PathInfo* info) {
   XattrSpec*  spec;
   for (spec=MarFS_xattr_specs; spec->value_type!=XVT_NONE; ++spec) {
      lremovexattr(info->post.md_path, spec->key_name);

      info->xattrs &= ~(spec->value_type);
      if (spec->value_type == XVT_RESTART)
         info->flags &= ~(PI_RESTART);
   }
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
//
//
// TBD: New approach.  We should periodically parse the results of Alfred's
//     quota-scan, and maybe store the info into respective NS structs, and
//     set ourselves a timestamp.  If we are called and the timestamp is
//     older than gettimeofday(), then we reparse the quota-scan output.
//     Otherwise, we use the cached results of the previous parse.  How do
//     we cache the results?  Well, if all we care about is size, then we
//     take the usage reported by the scan, minus the trash-usage reported
//     by the scan, and say they have that much space.
//
//     Is it worth the trouble to truncate the fsinfo file to the new size,
//     at marfs_release()?

int check_quotas(PathInfo* info) {

   //   size_t rc;

   //   if (! (info->flags & PI_STATVFS))
   //      __TRY0(statvfs, info->ns->fsinfo, &info->stvfs);

#if TBD
   uint64_t  names_limit = (uint64_t)info->ns->quota_names;
#endif

   uint64_t space_limit = (uint64_t)info->ns->quota_space;

   // value of -1 for ns->quota_space implies unlimited
   if (space_limit >= 0) {
      struct stat st;
      if (stat(info->ns->fsinfo_path, &st)) {
         LOG(LOG_ERR, "couldn't stat fsinfo at '%s': %s\n",
             info->ns->fsinfo_path, strerror(errno));
         errno = EINVAL;
         return -1;
      }
      if (st.st_size >= space_limit) /* 0 = OK,  1 = no-more-space */
         return -1;
   }

   // not over quota
   return 0;
}



// write MultiChunkInfo (as binary data in network-byte-order), into file
//
// <total_written> includes all user-data (but not system-data,
// e.g. RecoveryInfo) written to the object.  When called by
// marfs_release(), we assume the final RecoveryInfo is included in the
// total_written.  And that only the final chunk can be less than 100%
// full.
//
// We use "sizeof(RecoveryInfo) +8" everywhere, because the final 8 bytes
// in an object hold the index of the begining of the RecoveryInfo, within
// the object.

int write_chunkinfo(int                   md_fd,
                    const PathInfo* const info,
                    const size_t          total_written) {

   const size_t chunk_info_len = sizeof(MultiChunkInfo);
   char         str[chunk_info_len];

   const size_t recovery             = sizeof(RecoveryInfo) +8;
   const size_t user_data_per_chunk  = info->pre.chunk_size - recovery;
   const size_t log_offset           = info->pre.chunk_no * user_data_per_chunk;
   const size_t user_data_this_chunk = total_written - log_offset;

   MultiChunkInfo chunk_info = (MultiChunkInfo) {
      .config_vers      = MarFS_config_vers,
      .chunk_no         = info->pre.chunk_no,
      .logical_offset   = log_offset,
      .chunk_data_bytes = user_data_this_chunk,
      .correct_info     = info->post.correct_info,
      .encrypt_info     = info->post.encrypt_info,
   };

   // convert struct to portable binary
   ssize_t str_count = chunkinfo_2_str(str, chunk_info_len, &chunk_info);
   if (str_count < 0) {
      LOG(LOG_ERR, "error preparing chunk-info (%ld < 0)\n",
          str_count);
      errno = EIO;
      return -1;
   }
   if (str_count < chunk_info_len) {
      // pad with zeros, if nec
      memset(str + str_count, 0, (chunk_info_len - str_count));
   }
   else if (str_count > chunk_info_len) {
      LOG(LOG_ERR, "error preparing chunk-info (%ld != %ld)\n",
          str_count, chunk_info_len);
      errno = EIO;
      return -1;
   }

   // write portable binary to MD file
   ssize_t wr_count = write(md_fd, str, chunk_info_len);
   if (wr_count < 0) {
      LOG(LOG_ERR, "error writing chunk-info (%s)\n",
          strerror(errno));
      return -1;
   }
   if (wr_count != chunk_info_len) {
      LOG(LOG_ERR, "error writing chunk-info (%ld != %ld)\n",
          wr_count, chunk_info_len);
      errno = EIO;
      return -1;
   }


   return 0;
}

// read MultiChunkInfo for given chunk, from file
int read_chunkinfo(int md_fd, MultiChunkInfo* chnk) {
   static const size_t chunk_info_len = sizeof(MultiChunkInfo);

   char str[chunk_info_len];
   ssize_t rd_count = read(md_fd, str, chunk_info_len);
   if (rd_count < 0) {
      LOG(LOG_ERR, "error reading chunk-info (%s)\n",
          strerror(errno));
      return -1;
   }
   if (rd_count != chunk_info_len) {
      LOG(LOG_ERR, "error reading chunk-info (%ld != %ld)\n",
          rd_count, chunk_info_len);
      errno = EIO;
      return -1;
   }

   ssize_t str_count = str_2_chunkinfo(chnk, str, rd_count);
   if (str_count < 0) {
      LOG(LOG_ERR, "error preparing chunk-info (%ld < 0)\n",
          str_count);
      errno = EIO;
      return -1;
   }
   if (str_count != chunk_info_len) {
      LOG(LOG_ERR, "error preparing chunk-info (%ld != %ld)\n",
          str_count, chunk_info_len);
      errno = EIO;
      return -1;
   }

   return 0;
}






// write appropriate RecoveryInfo into an object.  Moved this here so it
// could be shared by marfs_write() and marfs_release()
//
// NOTE: We actually write "sizeof(RecoveryInfo)+8", because the final 8
//     bytes are a value that indicate the size of the RecoveryInfo.  This
//     allows RecoveryInfo written by earlier versions of the software
//     (e.g. when RecoveryInfo was different) to be properly located and
//     parsed.

ssize_t write_recoveryinfo(ObjectStream* os, const PathInfo* const info) {
   const size_t recovery   = sizeof(RecoveryInfo) +8; // written in tail of object
   ssize_t rc_ssize;

#if TBD
   // add recovery-info, at the tail of the object
   LOG(LOG_INFO, "writing recovery-info of size %ld\n", recovery);

   //      char objid[MARFS_MAX_OBJID_SIZE];
   //      TRY0(pre_2_str, objid, MARFS_MAX_OBJID_SIZE, &info->pre);
   //      TRY_GE0(write, fh->md_fd, objid, MARFS_MAX_OBJID_SIZE);
   //      info->post.chunk_info_bytes += MARFS_MAX_OBJID_SIZE;
   //      info->post.chunk_info_bytes += MARFS_MAX_OBJID_SIZE;

   char file_info[...];
   __TRY_GE0(file_info_2_str, fh->write_status.file_info);
   __TRY_GE0(stream_put, os, file_info, recovery);

#else
   LOG(LOG_WARNING, "writing fake recovery-info of size %ld\n", recovery);

   // static char rec[recovery];
   static char rec[sizeof(RecoveryInfo) +8]; // stupid compiler ...

   ///   static int  needs_init=1;
   ///   if (needs_init) {
   ///      memset(rec, 1, recovery); // stands out in objects written from /dev/zero
   ///      needs_init = 0;
   ///   }
   static uint8_t dbg=1;
   memset(rec, dbg, recovery); // stands out in objects written from /dev/zero
   dbg += 1;

   LOG(LOG_WARNING, "first part of fake rec-info: 0x%02x,%02x,%02x,%02x\n",
       rec[0], rec[1], rec[2], rec[3]);
   __TRY_GE0(stream_put, os, rec, recovery);

#endif

   return rc_ssize;
}


// Make sure the hierarchical trash directory tree exists, for all namespaces.
//
// The configuration-file specifies a root trash-directory for each
// namespace, as well as a namespace-name, and shard info (for future use).
// Supposing the trash-path in the config is "/my_trash", and the
// namespace-name is "ns1", then the initialized trash directory looks
// like:
//
//    /my_trash/ns1.0/a/b/c
//
// Where "a/b/c" represents 1000 different leaf-subdirectories, with each
// character (a, b, or c) having all the values 0-9.
//
// When a file with inode = "xxxxx762" (base 10) is trashed, it appears in
// the subdir:
//
//    /my_trash/ns1.0/7/6/2
//
// The files are copied, rather than renamed, to the trash.  Therefore, the
// file in the trash will not actually have an inode matching the
// subdir-name.
//
// NOTE: We do not create the top-level "trash" directory, if it doesn't
//       exist.  It was felt this should be done by some admin script that
//       sets up all directories etc, associated with new namespaces and
//       repos.  That way, we know what user should own them.
//
// NOTE: We just iterate through all the namespaces doing this, but I think
//     we will soon have namespaces that we don't want to initialize by
//     default (e.g. because they are offline, and it is
//     expensive/impossible to bring them online).  Therefore, we will
//     probably want config to mark those namespaces that should be
//     initialized like this.
//

int init_scatter_tree(const char*    root_dir,
                      const char*    ns_name,
                      const uint32_t shard,
                      const mode_t   mode) {
   size_t      rc;          // for TRY0
   // ssize_t     rc_ssize;    // for TRY_GE0

   struct stat st;

   LOG(LOG_INFO, "scatter_tree %s/%s.%d\n", root_dir, ns_name, shard);

   // --- assure that top-level trash-dir (from the config) exists
   LOG(LOG_INFO, " maybe create %s\n", root_dir);
   rc = mkdir(root_dir, mode);
   if ((rc < 0) && (errno != EEXIST)) {
      LOG(LOG_ERR, "mkdir(%s) failed\n", root_dir);
      return -1;
   }

   // --- create 'namespace.shard' directory-tree under <root_dir> (if needed).
   //     Make sure there's room for inode-based subdirs, so we don't have to
   //     check that for each one.
   char dir_path[MARFS_MAX_MD_PATH];

   // generate the name of the 'namespace.shard' dir
   int prt_count = snprintf(dir_path, MARFS_MAX_MD_PATH,
                            "%s/%s.%d", root_dir, ns_name, shard);
   if (prt_count < 0) {
      LOG(LOG_ERR, "snprintf(..., %s, %s) failed\n",
          root_dir,
          ns_name);
      return -1;
   }
   else if (prt_count >= MARFS_MAX_MD_PATH) {
      LOG(LOG_ERR, "snprintf(..., %s, %s) truncated\n",
          root_dir,
          ns_name);
      errno = EIO;
      return -1;
   }
   else if ((prt_count + strlen("/x/x/x")) >= MARFS_MAX_MD_PATH) {
      LOG(LOG_ERR, "no room for inode-subdirs after trash_path '%s'\n",
          dir_path);
      errno = EIO;
      return -1;
   }

   // create the 'namespace.shard' dir
   LOG(LOG_INFO, " maybe create %s\n", dir_path);
   rc = mkdir(dir_path, mode);
   if ((rc < 0) && (errno != EEXIST)) {
      LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
      return -1;
   }

   // --- create the inode-based '.../ns.shard/a/b/c' subdirs (if needed)
   //     (We're assuming they have 6 characters max: "/x/x/x")
   //
   //     If the last directory (in would-be create-order) doesn't
   //     exist, then we'll assume we need to create all of them.  If it
   //     does exist, we'll asume they all do.


   // mark where the 'ns.shard' path ends.
   char*        sub_dir = dir_path + strlen(dir_path);
   // const size_t MAX_SUBDIR_SIZE = strlen("/xxx") + 1;

   // check whether the last subdir exists
   memcpy(sub_dir, "/9/9/9", 7); // incl final '/0'

   LOG(LOG_INFO, " checking %s\n", dir_path);
   if (! lstat(dir_path, &st))
      return 0;           // skip the subdir-create loop
   else if (errno != ENOENT) {
      LOG(LOG_ERR, "lstat(%s) failed\n", dir_path);
      return -1;
   }

   // subdir "/9/9/9" didn't exist: try to create all of them
   LOG(LOG_INFO, " creating inode-subdirs\n");
   int i, j, k;
   for (i=0; i<=9; ++i) {

      // cheaper than sprintf()
      sub_dir[0] = '/';
      sub_dir[1] = '0' + i;
      sub_dir[2] = 0;

      rc = mkdir(dir_path, mode);
      if ((rc < 0) && (errno != EEXIST)) {
         LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
         return -1;
      }

      LOG(LOG_INFO, " creating inode-subdirs %s/*\n", dir_path);
      for (j=0; j<=9; ++j) {

         // cheaper than sprintf()
         sub_dir[2] = '/';
         sub_dir[3] = '0' + j;
         sub_dir[4] = 0;

         rc = mkdir(dir_path, mode);
         if ((rc < 0) && (errno != EEXIST)) {
            LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
            return -1;
         }

         for (k=0; k<=9; ++k) {

            // cheaper than sprintf()
            sub_dir[4] = '/';
            sub_dir[5] = '0' + k;
            sub_dir[6] = 0;

            // make the '.../trash/namespace.shard//a/b/c' subdir
            rc = mkdir(dir_path, mode);
            if ((rc < 0) && (errno != EEXIST)) {
               LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
               return -1;
            }
         }
      }
   }

   return 0;                    // success
}


// NOTE: for now, all the marfs directories are chown root:root, cmod 770
int init_mdfs() {
   size_t  rc;                  // for TRY0
   /// ssize_t rc_ssize;            // for TRY_GE0

   struct stat st;

   NSIterator       it = namespace_iterator();
   MarFS_Namespace* ns;
   for (ns = namespace_next(&it);
        ns;
        ns = namespace_next(&it)) {

      const uint32_t shard = 0;   // FUTURE: make scatter-tree for each shard?
#if TBD
      MarFS_Repo*    repo  = ns->iwrite_repo; // for fuse
#endif
      mode_t         mode  = (S_IRWXU | S_IRWXG ); // default 'chmod 770'

      // "root" namespace is not backed by real MD or storage, it is just
      // so that calls to list '/' can be answered.
      if (ns->is_root) {
         LOG(LOG_INFO, "skipping root NS: %s\n", ns->name);
         continue;
      }

      //      // only risk screwing up "jti", while debugging
      //      if (strcmp(ns->name, "jti")) {
      //         LOG(LOG_INFO, "skipping NS %s\n", ns->name);
      //         continue;
      //      }

      LOG(LOG_INFO, "\n");
      LOG(LOG_INFO, "NS %s\n", ns->name);




      // check whether "trash" dir exists (and create sub-dirs, if needed)
      LOG(LOG_INFO, "top-level trash dir   %s\n", ns->trash_path);
      rc = lstat(ns->trash_path, &st);
      if (! rc) {
         if (! S_ISDIR(st.st_mode)) {
            LOG(LOG_ERR, "not a directory %s\n", ns->fsinfo_path);
            return -1;
         }
      }
      else if (errno == ENOENT) {
         // LOG(LOG_ERR, "creating %s\n", ns->fsinfo_path);
         // rc = mkdir(ns->trash_path, mode);
         // if ((rc < 0) && (errno != EEXIST)) {
         //   LOG(LOG_ERR, "mkdir(%s) failed\n", ns->trash_path);
         //   return -1;
         // }
         LOG(LOG_ERR, "doesn't exist %s\n", ns->fsinfo_path);
         return -1;
      }
      else {
         LOG(LOG_ERR, "stat failed %s (%s)\n", ns->fsinfo_path, strerror(errno));
         return -1;
      }


      // create the scatter-tree for trash, if needed
      __TRY0(init_scatter_tree, ns->trash_path, ns->name, shard, mode);






      // check whether mdfs top-level dir exists
      LOG(LOG_INFO, "top-level MDFS dir    %s\n", ns->md_path);
      rc = lstat(ns->md_path, &st);
      if (! rc) {
         if (! S_ISDIR(st.st_mode)) {
            LOG(LOG_ERR, "not a directory %s\n", ns->md_path);
            return -1;
         }
      }
      else if (errno == ENOENT) {
         //      rc = mkdir(ns->md_path, mode);
         //      if ((rc < 0) && (errno != EEXIST)) {
         //         LOG(LOG_ERR, "mkdir(%s) failed\n", ns->md_path);
         //         return -1;
         //      }
         LOG(LOG_ERR, "doesn't exist %s\n", ns->md_path);
         return -1;
      }
      else {
         LOG(LOG_ERR, "stat failed %s (%s)\n", ns->md_path, strerror(errno));
         return -1;
      }




      // check whether fsinfo-file exists.  Currently, we truncate to size
      // to represent the amount of storage used by this namespace.  This
      // value is compared with the configured maximum, to see whether use
      // can open a new file for writing.
      //
      // TBD: We could parse Alfred's quota-log, and store per-namespace
      //     quota info into the NS structures.  We would then use these
      //     until the next timeout, at which point fuse would reparse the
      //     quota info into NS structs (on the next call to
      //     check_quotas()).
      LOG(LOG_INFO, "top-level fsinfo file %s\n", ns->fsinfo_path);
      rc = lstat(ns->fsinfo_path, &st);
      if (! rc) {
         if (! S_ISREG(st.st_mode)) {
            LOG(LOG_ERR, "not a regular file %s\n", ns->fsinfo_path);
            return -1;
         }
      }
      else if (errno == ENOENT) {
         // __TRY0(truncate, ns->fsinfo_path, 0); // infinite quota, for now
         LOG(LOG_ERR, "doesn't exist %s\n", ns->fsinfo_path);
         return -1;
      }
      else {
         LOG(LOG_ERR, "stat failed %s (%s)\n", ns->fsinfo_path, strerror(errno));
         return -1;
      }



#if TBD
      // COMMENTED OUT.  Turns out there are issues with POSIX permissions
      // in this setup, because "who owns the directory into which the
      // user's data-files are stored"?  It was felt that, even if the
      // storage file-system is unshared, the fact that the parent dir
      // (i.e. leaf dir in the scatter-tree) would have to be
      // world-writable was not good enough protection.

      // create a scatter-tree for semi-direct fuse repos, if any
      if (repo->access_proto == PROTO_SEMI_DIRECT) {
         __TRY0(init_scatter_tree, repo->host, ns->name, shard, mode);
      }
#endif

   }

   return 0;
}
