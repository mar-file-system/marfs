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
#include <stdarg.h>
#include <regex.h>              // canonicalize()
#include <assert.h>

// ---------------------------------------------------------------------------
// COMMON
//
// These are functions to support the MarFS fuse implementation (and pftool
// TBD).  These should generally return zero for true, and non-zero for
// errors.  They should not invert return-values to negative, for fuse.
// The fuse impl takes care of that.
// ---------------------------------------------------------------------------


// Fuse calls the callback-functions with that part of the path that is
// below the mount-point.  Thus, the internal support-functions (find namespace, etc)
// and configuration settings (namespace-names, etc), are defined in terms of these
// "sub-paths".
//
// But pftool gets absolute paths.  It needs to know (a) is this path on a
// marfs mount-point, and (b) what is the part of the path below the
// mount-point, so I can call the internal support-functions?  This
// function answers both questions.
//
// Return the part of the path below the configured marfs <mnt_top", or
// return NULL if the path is not below there.
// 
const char* marfs_sub_path(const char* path) {
   if (strncmp(path, marfs_config->mnt_top, marfs_config->mnt_top_len))
      return NULL;
   else if (! path[marfs_config->mnt_top_len])
      return "/";
   else if (path[marfs_config->mnt_top_len] != '/')
      return NULL;
   else
      return path + marfs_config->mnt_top_len;
}


// Given:
//  <path> is a user-perspective "marfs path", minus the mnt_top portion
// 
// Result:
//  info->ns               has NS corresponding to <path>
//  info->post.md_path     has corresponding MD path
//
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
   // using marfs_config->mnt_top and look up in MAR_namespace array, fill in all
   // MAR_namespace and MAR_repo info into a big structure to hold
   // everything we know about that path.  We also compute the full path of
   // the corresponding entry in the MDFS (into PathInfo.post.md_path).
   info->ns = find_namespace_by_mnt_path(path);
   if (! info->ns) {
      LOG(LOG_INFO, "no namespace for path %s\n", path);
      errno = ENOENT;
      return -1;            /* no such file or directory */
   }

   LOG(LOG_INFO, "namespace   %s\n", info->ns->name);
   LOG(LOG_INFO, "mnt_path    %s\n", info->ns->mnt_path);

   const char* sub_path = path + info->ns->mnt_path_len; /* below NS */
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
   if (! strcmp(info->post.md_path, info->trash_md_path)) {
      LOG(LOG_ERR, "users can't access trash_md_path (%s)\n", info->post.md_path);
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
   TRY_DECLS();

   // won't hurt (much), if it's already been done.
   __TRY0( expand_path_info(info, path) );

   if (! (info->flags & PI_TRASH_PATH)) {
      const char* sub_path  = path + info->ns->mnt_path_len; /* below fuse mount */
      char*       base_name = strrchr(sub_path, '/');
      base_name = (base_name ? base_name +1 : (char*)sub_path);

      // construct date-time string in standard format
      char       date_string[MARFS_DATE_STRING_MAX];
      time_t     now = time(NULL);
      if (now == (time_t)-1) {
         LOG(LOG_ERR, "time() failed\n");
         return -1;
      }
      __TRY0( epoch_to_str(date_string, MARFS_DATE_STRING_MAX, &now) );

      // Won't hurt (much) if it's already been done.
      __TRY0( stat_regular(info) );

      // these are the last 3 digits (base 10), of the inode
      ino_t   inode = info->st.st_ino; // shorthand
      uint8_t lo  = (inode % 10);
      uint8_t med = (inode % 100) / 10;
      uint8_t hi  = (inode % 1000) / 100;

      // FUTURE: find the actual shard to use
      const uint32_t shard = 0;

      // construct trash-path
      int prt_count = snprintf(info->trash_md_path, MARFS_MAX_MD_PATH,
                               "%s/%s.%d/%d/%d/%d/%s.trash_%010ld_%s",
                               info->ns->trash_md_path,
                               info->ns->name, shard,
                               hi, med, lo,
                               base_name,
                               info->st.st_ino,
                               date_string);
      if (prt_count < 0) {
         LOG(LOG_ERR, "snprintf(..., %s, %s, %010ld, %s) failed\n",
             info->ns->trash_md_path,
             base_name,
             info->st.st_ino,
             date_string);
         return -1;
      }
      else if (prt_count >= MARFS_MAX_MD_PATH) {
         LOG(LOG_ERR, "snprintf(..., %s, %s, %010ld, %s) truncated\n",
             info->ns->trash_md_path,
             base_name,
             info->st.st_ino,
             date_string);
         errno = EIO;
         return -1;
      }
      else if (prt_count + strlen(MARFS_TRASH_COMPANION_SUFFIX)
               >= MARFS_MAX_MD_PATH) {
         LOG(LOG_ERR, "no room for '%s' after trash_md_path '%s'\n",
             MARFS_TRASH_COMPANION_SUFFIX,
             info->trash_md_path);
         errno = EIO;
         return -1;
      }
      //      else
      //         // saves us a strlen(), later
      //         info->trash_md_path_len = prt_count;

      // subsequent calls to expand_trash_info() are NOP.
      info->flags |= PI_TRASH_PATH;
   }

   LOG(LOG_INFO, "trash_md_path  %s\n", info->trash_md_path);
   return 0;
}




int stat_regular(PathInfo* info) {
   TRY_DECLS();

   if (info->flags & PI_STAT_QUERY)
      return 0;                 /* already called stat_regular() */

   memset(&(info->st), 0, sizeof(struct stat));
   __TRY0( MD_PATH_OP(lstat, info->ns, info->post.md_path, &info->st) );

   info->flags |= PI_STAT_QUERY;
   return 0;
}


// return non-zero if info->post.md_path exists
int md_exists(PathInfo* info) {
   if (! (info->flags & PI_EXPANDED)) {
      // NOTE: we could just expand it ...
      LOG(LOG_ERR, "caller should already have called expand_path_info()\n");
      errno = EINVAL;
      return -1;
   }

   stat_regular(info);                /* no-op, if already done */
   return (info->st.st_ino != 0);
}




XattrSpec*  MarFS_xattr_specs = NULL;

int init_xattr_specs() {

   if (! MarFS_xattr_specs) {

      // these are used by a parser (e.g. stat_xattrs())
      // The string in MarFS_XattrPrefix is appended to all of them
      // TBD: free this in clean-up
      MarFS_xattr_specs = (XattrSpec*) calloc(4, sizeof(XattrSpec));
      assert(MarFS_xattr_specs);

      MarFS_xattr_specs[0] = (XattrSpec) { XVT_PRE,     MarFS_XattrPrefix "objid" };
      MarFS_xattr_specs[1] = (XattrSpec) { XVT_POST,    MarFS_XattrPrefix "post" };
      MarFS_xattr_specs[2] = (XattrSpec) { XVT_RESTART, MarFS_XattrPrefix "restart" };

      MarFS_xattr_specs[3] = (XattrSpec) { XVT_NONE,    NULL };
   }

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
//  (i.e. Repo.access_method=DIRECT), or a non-existent file.  We also need
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

// NOTE: if pftool is calling, it will already have called
//       batch_pre_process(), which will have initialized xattrs
//       (e.g. correct repo, etc).
//
// NOTE: This isn't intended for (and won't work for) files in the trash.
//       The problem is that expand_path_info() won't find the namespace
//       for such files, so default initializations of PRE and POST will
//       segfault.  If you want to look at a file in the trash, you should
//       first read the original path out of the trash "companion" file
//       (*.path), then expand_path_info() on that, then change
//       post.md_path to the trash file, then stat_xattrs().

int stat_xattrs(PathInfo* info) {
   TRY_DECLS();
   ssize_t str_size;

   if (info->flags & PI_XATTR_QUERY)
      return 0;                 // already did this

   // call stat_regular().
   __TRY0( stat_regular(info) );

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
         str_size = MD_PATH_OP(lgetxattr, info->ns, info->post.md_path,
                               spec->key_name, xattr_value_str,
                               MARFS_MAX_XATTR_SIZE);

         if (str_size != -1) {
            // got the xattr-value.  Parse it into info->pre
            xattr_value_str[str_size] = 0;
            LOG(LOG_INFO, "XVT_PRE %s\n", xattr_value_str);
            __TRY0( str_2_pre(&info->pre, xattr_value_str, &info->st) );
            LOG(LOG_INFO, "md_ctime: %016lx, obj_ctime: %016lx\n",
                info->pre.md_ctime, info->pre.obj_ctime);
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if ((errno == ENOATTR)
                  || ((errno == EPERM) && S_ISLNK(info->st.st_mode))) {
            // (a) ENOATTR means no attr, or no access.  Treat as the former.
            // (b) GPFS returns EPERM for lgetxattr on symlinks.
            __TRY0( init_pre(&info->pre, OBJ_FUSE, info->ns,
                             info->ns->iwrite_repo, &info->st) );
            info->xattr_inits |= spec->value_type; /* initialized this one */
         }
         else {
            LOG(LOG_INFO, "lgetxattr -> err (%d) %s\n", errno, strerror(errno));
            return -1;
         }
         break;
      }

      case XVT_POST: {
         str_size = MD_PATH_OP(lgetxattr, info->ns, info->post.md_path,
                               spec->key_name, xattr_value_str,
                               MARFS_MAX_XATTR_SIZE);

         if (str_size != -1) {
            // got the xattr-value.  Parse it into info->pre
            xattr_value_str[str_size] = 0;
            LOG(LOG_INFO, "XVT_POST %s\n", xattr_value_str);
            __TRY0( str_2_post(&info->post, xattr_value_str, 0) );
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if ((errno == ENOATTR)
                  || ((errno == EPERM) && S_ISLNK(info->st.st_mode))) {
            // (a) ENOATTR means no attr, or no access.  Treat as the former.
            // (b) GPFS returns EPERM for lgetxattr on symlinks.
            __TRY0( init_post(&info->post, info->ns, info->ns->iwrite_repo) );
            info->xattr_inits |= spec->value_type; /* initialized this one */
         }
         else {
            LOG(LOG_INFO, "lgetxattr -> err (%d) %s\n", errno, strerror(errno));
            return -1;
         }
         break;
      }

      case XVT_RESTART: {
         str_size = MD_PATH_OP(lgetxattr, info->ns, info->post.md_path,
                               spec->key_name, xattr_value_str,
                               MARFS_MAX_XATTR_SIZE);

         if (str_size != -1) {
            // got the xattr-value.  Parse it into info->pre
            xattr_value_str[str_size] = 0;
            LOG(LOG_INFO, "XVT_RESTART %s\n", xattr_value_str);
            __TRY0( str_2_restart(&info->restart, xattr_value_str) );
            info->xattrs |= spec->value_type; /* found this one */
         }
         else if ((errno == ENOATTR)
                  || ((errno == EPERM) && S_ISLNK(info->st.st_mode))) {
            // (a) ENOATTR means no attr, or no access.  Treat as the former.
            // (b) GPFS returns EPERM for lgetxattr on symlinks.
            __TRY0( init_restart(&info->restart) );
            info->xattr_inits |= spec->value_type; /* initialized this one */
         }
         else {
            LOG(LOG_INFO, "lgetxattr -> err (%d) %s\n", errno, strerror(errno));
            return -1;
         }
         break;
      }

      case XVT_SHARD: {
         // TBD ...
         LOG(LOG_INFO, "shard xattr TBD\n");
         break;
      }


      default:
         // a key was added to MarFS_attr_specs, but stat_xattrs() wasn't updated
         LOG(LOG_ERR, "unknown xattr %d = '%s'\n", spec->value_type, spec->key_name);
         errno = EINVAL;
         return -1;
      };
   }

   // subsequent calls can skip processing
   info->flags |= PI_XATTR_QUERY;


   // initialize the object-ID fields
   __TRY0( update_pre(&info->pre) );

   return 0;                    /* "success" */
}


// return non-zero for "true"
int under_mdfs_top(const char* path) {
   return (! strncmp((const char*)path, marfs_config->mdfs_top,
                     marfs_config->mdfs_top_len)
           && (! path[marfs_config->mdfs_top_len]
               || (path[marfs_config->mdfs_top_len] == '/')));
}

// "canonical" paths do not have "../", "./", or "//" in them.
// follow_some_links() is called by marfs_readlink(), perhaps from fuse.
// It wants to canonicalize a path, but, if it indirectly invokes fuse to
// do a readlink on a marfs-path, fuse will deadlock.  realpath() and
// canonicalize_file_name() both risk that, because they follow links.
// Unlike realpath() and canonicalize_file_name(), this function
// canonicalizes the path *without* following symlinks.  After return,
// <path> may be smaller, but will not be larger.

int canonicalize(char* path) {
   static regex_t preg;
   static int     needs_init = 1;

   // compile the regex on the first call (only)
   if (needs_init) {
      // all matches can be replaced with a single slash
      const char* regex = "(//|/[.]/|/[^./]+[^/]*/[.][.]/)";
      int         rc;
      if( (rc = regcomp(&preg, regex, (REG_EXTENDED | REG_ICASE))) ) {
#define       ERRBUF_SIZE 1024
         char errbuf[ERRBUF_SIZE];
         regerror(rc, &preg, errbuf, ERRBUF_SIZE);
         LOG(LOG_ERR, "Compiling '%s': %s\n", regex, errbuf);
         return -1;
      }
      needs_init = 0;
   }

   // need to run against entire string, because later condensations
   // might bring ".." after a directory-component.
#define        NMATCH 1
   regmatch_t  pmatch[NMATCH];
   size_t      len = strlen(path) +1;
   while (! regexec(&preg, path, NMATCH, pmatch, 0)) {

      // keep the 1st matched '/', and remove everything else in the match.
      // this moves the terminal NULL also.
      memmove(&path[pmatch[0].rm_so +1],
              &path[pmatch[0].rm_eo],
              len - pmatch[0].rm_eo);
      len -= (pmatch[0].rm_eo - pmatch[0].rm_so -1);
   }

   return 0;
}


// We are chasing links to prevent "unsupervised" access to the MDFS.
// e.g. /marfs/jti/link -> /dev/shm/redirect -> /gpfs/jti/mdfs/foo In fuse,
// if fuse returns the first link-target, then the kernel follows the
// remaining link without going through fuse (because that link isn't under
// the fuse mount).
//
// UPDATE: This is no longer a problem, as long as we protect the fuse
//   mount by 'unshare'ing the mdfs mount-point.  We can just let
//   marfs_readlink() return whatever is inside the symlink.  If we return
//   a direct path to the mdfs, it should be impossible for an external
//   user to access that, because of the unshare.  If we return a relative
//   path (i.e. because that's what's in the link), then the caller will
//   canonicalize that against the *marfs* path they called us with, and,
//   again, we're safe.
//
//   And pftool doesn't follow symlinks, so we're safe there.
//
//
// We must canonicalize and follow chains of symlinks that lead outside of
// marfs, until either (a) they lead back inside marfs (forbidden), or (b)
// they reach an end (either non-link or no-such-path).  We don't have to
// chase link-targets that are within marfs, even if they are themselves
// symlinks, because fuse will give them back to us as it follows the chain
// itself.  (Chasing these links would be complicated.)  pftool only copies
// links, without following, so that seems safe.
//
// Canonicalization must be done on MDFS paths, in order to avoid locking
// up fuse.  For link chains that stay within marfs, we need to convert the
// canonicalized MDFS path back into the corresponding marfs-path.
//
// Even though we chase the links, we should return the first link-target,
// after confirming that it's okay.
//
// NOTE: We don't have to forbid access to links that lead out of marfs,
//    through an indirection, but we do have to follow such links
//    ourselves.
//
// NOTE: You might think that it would be reasonable to allow chains to go
//    outside a marfs mount, then come back in, provided they come back in
//    to a marfs-path, rather than to an mdfs path ... and you'd be right.
//    For this first cut, it's awkward to tell whether the path was a
//    marfs-path or not, when we must convert to mdfs to do the readlink()
//    and realpath().
//
// When we are called: info.post.md_path has the marfs-path.  buf and size
// were already filled in by marfs_readlink().  If the target (in buf is
// not another symlink, then there's nothing for us to do.

int follow_some_links(PathInfo* info, char* buf, size_t size) {
   TRY_DECLS();

   char  temp[PATH_MAX];
   char  work[3][PATH_MAX];

   int   retval = 0;
   int   relative = 0;
   //   int   out = 0;               // links ever went outside mdfs?

   const char* sub_path;

   //   // generate a copy of the user's-view marfs-path (under mnt_top).
   //   char  marfs_path[PATH_MAX];
   //   int count = snprintf(marfs_path, PATH_MAX, "%s%s",
   //                        marfs_config->mnt_top, path);
   //   if ((count < 0) || (count >= PATH_MAX)) {
   //      LOG(LOG_ERR, "couldn't generate absolute path: %s%s\n",
   //          marfs_config->mnt_top, path, strerror(errno));
   //      errno = EINVAL;
   //      return -1;
   //   }

   //   char* src = marfs_path;
   char* src    = info->post.md_path;
   char* target = buf;

   char* abs    = (char*)work[0];
   __attribute__ ((unused)) char* spare0 = (char*)work[1];
   __attribute__ ((unused)) char* spare1 = (char*)work[2];

   __attribute__ ((unused)) int iteration=0;
   while (1) {

      TRY_GE0( readlink(src, target, size) );
      size_t link_len = rc_ssize;
      if (link_len >= size) {
         LOG(LOG_ERR, "no room for '\\0'\n");
         errno = ENAMETOOLONG;
         retval = -1;
         break;
      }
      target[link_len] = '\0';
      LOG(LOG_INFO, "readlink '%s' -> '%s' = (%ld)\n", src, target, link_len);


      // if the link-target is relative, then append it onto the (MDFS)
      // directory where the link was found.
      if (target[0] != '/') {

         // find the directory-component of <src>
         char*  last_slash = strrchr(src, '/');
         size_t src_dir_len = (last_slash
                               ? (last_slash - src)
                               : strlen(src));

         size_t new_path_len = src_dir_len + 1 + link_len + 1;
         if (new_path_len >= size) {
            LOG(LOG_ERR, "no room for new relative-path\n");
            errno = ENAMETOOLONG;
            retval = -1;
            break;
         }
         if (link_len >= PATH_MAX) {
            LOG(LOG_ERR, "no room for temp buffering of link\n");
            errno = ENAMETOOLONG;
            retval = -1;
            break;
         }

         // append relative-link onto source-directory
         strncpy(temp,                 src, src_dir_len);
         strncpy(temp +src_dir_len,    "/", 1);
         strncpy(temp +src_dir_len +1, target, link_len);
         temp[new_path_len -1] = 0;

         target = temp;
         relative = 1;
      }

      // canonicalize the link-target without chasing links, so we can
      // assure that it doesn't refer to an illegal spot in the MDFS.  (A
      // path might need canonicalization, even if it was not "relative",
      // above.)  Can't use realpath() or canonicalize_file_name(), because
      // those follow links, and if we invoke fuse readlink recursively
      // (i.e. on a marfs-path) fuse will deadlock.
      //
      ///      if (! realpath(target, abs))
      ///         LOG(LOG_ERR, "realpath(%s) failed: %s\n", target, strerror(errno));
      ///         retval = -1;
      ///         break;
      ///      }
      strcpy(abs, target);
      if (canonicalize(abs)) {
         LOG(LOG_ERR, "canonicalize(%s) failed: %s\n", target, strerror(errno));
         retval = -1;
         break;
      }

      // Even if <target> wasn't a marfs path, <abs> may be one, now.
      sub_path = marfs_sub_path(abs);
      if (sub_path) {
         if (! find_namespace_by_mnt_path(sub_path)) {
            LOG(LOG_ERR, "symlink target '%s' is not in any known NS\n", abs);
            errno = EPERM;
            retval = -1;
            break;
         }

         // legitimate MarFS path
         break;
      }

      // detect whether absolute path is under configured mdfs_top
      int in_mdfs = under_mdfs_top(abs);

      // There is no legal way (?) to follow links, and obtain path that is
      // under the MDFS, even after performing canonicalization.  However,
      // we will have converted a relative path (i.e. in a marfs namespace)
      // into an MDFS-path in order to allow canonicalization.  Therefore,
      // a relative-path will *look* like it's in the mdfs.  Our goal with
      // a relative path is just to confirm that it stayed in a legitimate
      // marfs namespace.  In order to avoid having to check through all of
      // them, we just require that it stay in the same one.
      if (relative) {
         if (strncmp(abs, info->ns->md_path, info->ns->md_path_len)) {
            LOG(LOG_ERR, "canonicalized rel target '%s' not in NS '%s'\n",
                abs, info->ns->name);
            errno = EPERM;
            retval = -1;
            break;
         }
         // canonicalized path in the same marfs NS
         break;
      }
      else if (in_mdfs) {
         LOG(LOG_ERR, "explicit link (%s) to MDFS path\n", target);
         errno = EPERM;
         retval = -1;
         break;
      }


#if CHASE_SYMLINKS
      // -----------------------------------------------------------------
      // *** SECURITY ISSUE.
      //
      // We run fuse in an unshare, such that mdfs_top is hidden everywhere
      // except inside fuse.  That protects us from the following
      // scenarios:
      //
      // (a)  /marfs/ns/foo.link -> /externalfs/sneaky.lnk -> /mdfs/ns/foo
      //
      // (b)  /marfs/ns/dir.link -> /externalfs/sneaky2.link -> /mdfs/ns
      //
      // In either of these secenarios, a user could modify marfs metadata
      // on a file they have access to (e.g. changing the obj-ID).  In that
      // environment, this code chases links to assure that neither
      // scenario above can happen.
      // -----------------------------------------------------------------


      // stat the target, to see whether it's also a link.
      struct stat  st;
      memset(&st, 0, sizeof(struct stat));

      //      if (in_mdfs) {
      //#if USE_MDAL
      //         TRY0( F_OP_NOCTX(lstat, info->ns, abs, &st) );
      //#else
      //         TRY0( lstat(abs, &st) );
      //#endif
      //      }
      //      else {
      //         // This is not a MarFS path, so do not use the MDAL
      //         TRY0( lstat(abs, &st) );
      //      }
      //
      // This is not a MarFS path, so do not use the MDAL
      TRY0( lstat(abs, &st) );

      // if target is a directory, and is outside of MarFS, we forbid
      // access in order to prevent a potential vulnerability, in which the
      // remote directory *contains* symlinks that point back to something
      // under mdfs_top.  If we allow fuse to chase this link over there,
      // then we lose control of what is done from there.
      if (S_ISDIR(st.st_mode)) {
         LOG(LOG_ERR, "target '%s' is a remote directory\n", abs);
         errno = EPERM;
         retval = -1;
         break;
      }

      // if target is not a link then we're done chasing
      if (! S_ISLNK(st.st_mode)) {
         LOG(LOG_INFO, "target '%s' is NOT a symlink\n", abs);
         break;
      }

      // prepare for next iteration, to chase the symlink.
      // Even though we are chasing links, our actual
      LOG(LOG_INFO, "target '%s' is also a symlink\n", abs);
      size_t buf_path_len = strlen(abs);
      if (buf_path_len >= PATH_MAX) {
         LOG(LOG_ERR, "size %ld too small for temp-path (%d)\n",
             buf_path_len, PATH_MAX);
         errno = ENAMETOOLONG;
         retval = -1;
         break;
      }

      // pointer-swaps, instead of str-copies
      if (! iteration) {
         src    = abs;
         target = spare0;
         abs    = spare1;
      }
      else {
         char* temp_ptr = src;
         src    = abs;
         abs    = target;
         target = temp_ptr;
      }

      relative = 0;
      ++ iteration;

#else
      break;
#endif

   }


   return retval;
}


// When pftool is calling, we're in batch mode.  Find the appropriate repo,
// based on total file-size that will be written here.  (This operation is
// done at stat-time.)  Any subsequent pftool tasks that are writing chunks
// will get the repo from the parsed xattrs.  File is not necessarily open
// at this time, so we have to make a costly call to stat_xattrs().
//
// The xattrs will be initialized (e.g. via init_pre()), in the call to
// stat_xattrs().
int batch_pre_process(const char* path, size_t file_size) {
   TRY_DECLS();
   LOG(LOG_INFO, "%s, %ld\n", path, file_size);

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));

   EXPAND_PATH_INFO(&info, path);
   STAT_XATTRS(&info);

   // update the Pre.repo to match the batch-repo corresponding to file-size,
   // in the namespace for this path.
   MarFS_Repo* repo = find_repo_by_range(info.ns, file_size);
   if (! repo) {
      LOG(LOG_ERR, "no batch-repo for ns '%s', size=%ld\n",
          info.ns->name, file_size);
      return -1;
   }
   LOG(LOG_INFO, "batch-repo for ns '%s', size=%ld -> '%s'\n",
       info.ns->name, file_size, repo->name);

   // xattrs always have the raw chunksize (including recovery-info)
   info.pre.repo       = repo;
   info.pre.chunk_size = repo->chunk_size;
   info.pre.obj_type   = OBJ_Nto1;

   TRY0( update_pre(&info.pre) );        // ?

   // POST is also required by stat_xattrs().
   // we have enough info to fill it
   const size_t recovery   = MARFS_REC_UNI_SIZE; // sys bytes, per chunk
   size_t       chunk_size = repo->chunk_size - recovery;
   size_t       n_chunks   = file_size / chunk_size;
   if ((n_chunks * chunk_size) < file_size)
      ++ n_chunks;              // round up

   info.post.obj_type         = ((n_chunks > 1) ? OBJ_MULTI : OBJ_UNI);
   info.post.chunks           = n_chunks;
   // chunk_info_bytes should be 0 if n_chunks == 1
   info.post.chunk_info_bytes = (n_chunks > 1 ?
                                 n_chunks * sizeof(MultiChunkInfo) :
                                 0);

   // save Pre and Post
   SAVE_XATTRS(&info, (XVT_PRE | XVT_POST));

   LOG(LOG_INFO, "done.\n");
   return 0;
}


// Called single-threaded from pftool in update_stats(), after all parallel
// activity to create <path> has completed.
//
// NOTE: the truncate() changes mtime, so pftool must not call utime()
//     until after this.
//
// NOTE: In the case of pftool (the only caller of
//     batch_pre/post_process()), we install PRE and POST in the
//     pre-process call, and pre and post won't have changed by the time we
//     get to post-process (in the case of pftool), so we only need to
//     remove RESTART.  RESTART indicates, among other things that a file
//     wasn't completely written, so MarFS will prevent reads to it.
int batch_post_process(const char* path, size_t file_size) {
   TRY_DECLS();
   LOG(LOG_INFO, "%s, %ld\n", path, file_size);

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   STAT_XATTRS(&info);
   if (has_all_xattrs(&info, MARFS_MD_XATTRS)) {

      // truncate to final size
      TRY0( MD_PATH_OP(truncate, info.ns, info.post.md_path, file_size) );

      // As in release(), we take note of whether RESTART was saved with
      // a restrictive mode, which we couldn't originally install because
      // would've prevented manipulating xattrs.  If so, then (after removing
      // the RESTART xattr) install the more-restrictive mode.
      int    install_new_mode = 0;
      mode_t new_mode = 0; // suppress wrong gcc warning: maybe used w/out init
      if (has_all_xattrs(&info, XVT_RESTART)
          && (info.restart.flags & RESTART_MODE_VALID)) {

         install_new_mode = 1;
         new_mode = info.restart.mode;
      }

      // remove "restart" xattr.  [Can't do this at close-time, in the case
      // of N:1 files, so we do it here, for them.]
      if( !(OBJ_PACKED == info.post.obj_type) ) {
         info.xattrs &= ~(XVT_RESTART);
      }
      SAVE_XATTRS(&info, (XVT_RESTART));

      // install more-restrictive mode, if needed.
      if (install_new_mode) {
         TRY0( MD_PATH_OP(chmod, info.ns, info.post.md_path, new_mode) );
      }
   }
   else
      LOG(LOG_INFO, "no xattrs\n");

   return 0;
}

// Return non-zero if info->post.md_path has ALL/ANY of the reserved xattrs
// indicated in <mask>.  Else, zero.
//
// NOTE: Having these reserved xattrs indicates that the data-contents are
//       stored in object(s), described in the meta-data.  Otherwise, data
//       is stored directly in the post.md_path.

int has_all_xattrs(PathInfo* info, XattrMaskType mask) {
   if (! (info->flags & PI_EXPANDED)) {
      // NOTE: we could just expand it ...
      LOG(LOG_ERR, "caller should already have called expand_path_info()\n");
      errno = EINVAL;
      return -1;
   }
   stat_xattrs(info);                  /* no-op, if already done */
   return ((info->xattrs & mask) == mask);
}
int has_any_xattrs(PathInfo* info, XattrMaskType mask) {
   if (! (info->flags & PI_EXPANDED)) {
      // NOTE: we could just expand it ...
      LOG(LOG_ERR, "caller should already have called expand_path_info()\n");
      errno = EINVAL;
      return -1;
   }
   stat_xattrs(info);                  /* no-op, if already done */
   return (info->xattrs & mask);
}



// For all the attributes in <mask>, convert info xattrs to stringified values, and save
// on info->post.md_path.
int save_xattrs(PathInfo* info, XattrMaskType mask) {
   TRY_DECLS();

   // call stat_regular().
   __TRY0( stat_regular(info) );


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
         __TRY0( pre_2_str(xattr_value_str, MARFS_MAX_XATTR_SIZE, &info->pre) );
         LOG(LOG_INFO, "XVT_PRE %s\n", xattr_value_str);
         __TRY0( MD_PATH_OP(lsetxattr, info->ns,
                            info->post.md_path,
                            spec->key_name, xattr_value_str,
                            strlen(xattr_value_str)+1, 0) );
         break;
      }

      case XVT_POST: {
         __TRY0( post_2_str(xattr_value_str, MARFS_MAX_XATTR_SIZE,
                            &info->post, info->ns->iwrite_repo,
                            (info->post.flags & POST_TRASH)) );
         LOG(LOG_INFO, "XVT_POST %s\n", xattr_value_str);
         __TRY0( MD_PATH_OP(lsetxattr, info->ns, info->post.md_path,
                            spec->key_name, xattr_value_str,
                            strlen(xattr_value_str)+1, 0) );
         break;
      }

      case XVT_RESTART: {

         // TBD: Other flags could be combined into a single value to be
         //      stored as "flags" rather than just "restart".  Then the
         //      scan for files to restart (when restarting pftool) would
         //      just be "find inodes that have xattr with key 'flags'
         //      having value matching a given bit-pattern", rather than
         //      "find inodes that have the following xattrs"
         //
         //      [However, you'd want to be sure that no two processes
         //      would ever be racing to read/modify/write the flags
         //      xattr.]

         // If the flag isn't set, then remove the xattr.
         if (info->xattrs & XVT_RESTART) {
            __TRY0( restart_2_str(xattr_value_str, MARFS_MAX_XATTR_SIZE,
                                  &info->restart) );
            LOG(LOG_INFO, "XVT_RESTART: %s\n", xattr_value_str);
            __TRY0( MD_PATH_OP(lsetxattr, info->ns, info->post.md_path,
                               spec->key_name, xattr_value_str,
                               strlen(xattr_value_str)+1, 0) );
         }
         else {
            ssize_t val_size = MD_PATH_OP(lremovexattr, info->ns,
                                          info->post.md_path, spec->key_name);
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

   return 0;                    /* "success" */
}

static
void init_filehandle(MarFS_FileHandle* fh, PathInfo* info) {
   memset((char*)fh, 0, sizeof(MarFS_FileHandle));
   fh->info = *info;
#if USE_MDAL
   F_MDAL(fh) = info->pre.ns->file_MDAL;
   LOG(LOG_INFO, "file-MDAL: %s\n", F_MDAL(fh)->name);
   F_OP(f_init, fh, F_MDAL(fh));
#endif
}

// This should only be used internally.
// Open an arbitrary path in the MDFS storing the relevant state (file
// descriptor, etc.) in the file handle.
static
int open_md_path(MarFS_FileHandle* fh, const char* path, int flags, ...) {
   __attribute__ ((unused)) PathInfo* info = &fh->info;
   va_list ap;

   va_start(ap, flags);
   
   // initialize the file handle MDAL
   // As this is used now, the file handle mdal will already be initialized
   // whenever this is called... Could maybe clean this up, but it might
   // make it less flexible.
#if USE_MDAL
   if(! F_MDAL(fh)) {
      F_MDAL(fh) = info->pre.ns->file_MDAL;
      LOG(LOG_INFO, "file-MDAL: %s\n", F_MDAL(fh)->name);

      // allow MDAL implementation to do custom initializations
      F_OP(f_init, fh, F_MDAL(fh));
   }
#else
   LOG(LOG_INFO, "ignoring file-MDAL\n");
#endif

   // if we haven't already opened the MD file, do it now.
#if USE_MDAL
   if (! F_OP(is_open, fh)) {
      if(flags & O_CREAT) {
         mode_t mode = va_arg(ap, mode_t);
         F_OP(open, fh, path, flags, mode);
      }
      else {
         F_OP(open, fh, path, flags);
      }
      
      if(! F_OP(is_open, fh) ) {
         LOG(LOG_ERR, "open_md_path(%s) failed: %s\n",
             path, strerror(errno));
         return -1;
      }
   }
#else
   if (! fh->md_fd) { // not already open.
      if(flags & O_CREAT) {
         mode_t mode = va_arg(ap, mode_t);
         fh->md_fd = open(path, flags, mode);
      }
      else {
         fh->md_fd = open(path, flags);
      }
      
      if (fh->md_fd < 0) {
         LOG(LOG_ERR, "open_md_path(%s) failed: %s\n",
             info->post.md_path, strerror(errno));
         fh->md_fd = 0;
         return -1;
      }
   }
#endif

   va_end(ap);
   
   LOG(LOG_INFO, "open_md_path(%s) ok\n", path);
   return 0;
}

// This is only used internally.  We just assume expand_trash_info() has
// already been called.
//
// In addition to moving the original to the trash, the two trash functions
// (trash_unlink() and trash_truncate()) also write the full-path of the
// original (MarFS) file into "<trash_md_path>.path".  [NOTE: This is not the
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
   TRY_DECLS();

   /* initialize info->trash_md_path */
   __TRY0( expand_trash_info(info, path) );

   // expand_trash_info() assures us there's room in MARFS_MAX_MD_PATH to
   // add MARFS_TRASH_COMPANION_SUFFIX, so no need to check.
   char companion_fname[MARFS_MAX_MD_PATH];
   __TRY_GE0( snprintf(companion_fname, MARFS_MAX_MD_PATH, "%s%s",
                       info->trash_md_path,
                       MARFS_TRASH_COMPANION_SUFFIX) );

   // TBD: Don't want to depend on support for open(... (O_CREAT|O_EXCL)).
   //      Should just stat() the companion-file, before opening, to assure
   //      it doesn't already exist.
   LOG(LOG_INFO, "companion:  %s\n", companion_fname);

   // Set up a file handle for the trash companion file
   MarFS_FileHandle companion_fh;
   init_filehandle(&companion_fh, info);

   // Open the file
   open_md_path(&companion_fh, companion_fname,
                 (O_WRONLY|O_CREAT), info->st.st_mode);
   
   __TRY_GE0( is_open_md(&companion_fh) );

#if 1
   // write MDFS path into the trash companion
   __TRY_GE0( MD_FILE_OP(write, &companion_fh,
                         info->post.md_path, strlen(info->post.md_path)));
#else // never executes
   // write MarFS path into the trash companion
   __TRY_GE0( write(fd, marfs_config->mnt_top, marfs_config->mnt_top_len) );
   __TRY_GE0( write(fd, path, strlen(path)) );
#endif

   __TRY0( close_md(&companion_fh) );

   // maybe install ctime/atime to support "undelete"
   if (utim)
      __TRY0( MD_PATH_OP(utime, info->ns, companion_fname, utim) );

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
//
// UPDATE: Fuse always installs RESTART on new files.  Previously, it only
//    installed PRE and POST when closing files.  It still installs POST at
//    close of all files, and also installs PRE at close of Uni files.
//    However, for Multi files, fuse now installs PRE and POST, at the time
//    when a file first becomes N:1.  At that point, there is at least one
//    object associated with the file, and the MD file has chunk-info about
//    which objects have been written.  If fuse were then to crash, and
//    this file were to later be trashed, GC could still reclaim the
//    storage for any objects that had been written, by crawling the MD
//    file with read_chunkinfo(), and using that plus the object-ID to
//    generate objids for the chunks.  So, if the file only has RESTART
//    (can only happen if fuse crashed while writing a Uni, [or if another
//    fuse is still writing the file!]), then no associated object was
//    successfully written, and we just delete it outright, here, as though
//    it were DIRECT.
//
// NOTE: Should we do something to make this thread-safe (like unlink()) ?
//
int  trash_unlink(PathInfo*   info,
                  const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   if (! (info->flags & PI_EXPANDED)) {
      // NOTE: we could just expand it ...
      LOG(LOG_ERR, "caller should already have called expand_path_info()\n");
      errno = EINVAL;
      return -1;
   }

   //    If this has no xattrs (its just a normal file using the md file
   //    for data) just unlink the file and return we have nothing to
   //    clean up, too bad for the user as we aren't going to keep the
   //    unlinked file in the trash.
   //
   // NOTE: See "UPDATE", above
   //
   // NOTE: We don't put xattrs on symlinks, so they also get deleted here.
   //
   TRY_DECLS();
   __TRY0( stat_xattrs(info) );
   if (! has_all_xattrs(info, XVT_PRE)) {
      LOG(LOG_INFO, "incomplete xattrs\n"); // not enough to reclaim objs
      __TRY0( MD_PATH_OP(unlink, info->ns, info->post.md_path) );
      return 0;
   }

   // we no longer assume that a simple rename into the trash will always
   // be possible (e.g. because trash will be in a different fileset, or
   // filesystem).  It was thought we shouldn't even *try* the rename
   // first.  Instead, we'll copy to the trash, then unlink the original.
   __TRY0( trash_truncate(info, path) );
   __TRY0( MD_PATH_OP(unlink, info->ns, info->post.md_path) );

   return 0;
}


// [trash_dup_file]
// This is used to implement truncate/ftruncate
//
// Copy trashed MD file into trash area, plus all its xattrs.
// Does NOT unlink original.
// Does NOT do anything with object-storage.
// Wipes all xattrs on original, and truncs to zero.
//
int  trash_truncate(PathInfo*   info,
                    const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   if (! (info->flags & PI_EXPANDED)) {
      // NOTE: we could just expand it ...
      LOG(LOG_ERR, "caller should already have called expand_path_info()\n");
      errno = EINVAL;
      return -1;
   }

   //    If this has no xattrs (its just a normal file using the md file
   //    for data) just trunc the file and return we have nothing to
   //    clean up, too bad for the user as we aren't going to keep the
   //    trunc’d file.
   //
   // NOTE: See "UPDATE", above trash_unlink()

   TRY_DECLS();
   __TRY0( stat_xattrs(info) );
   if (! has_all_xattrs(info, XVT_PRE)) {
      LOG(LOG_INFO, "incomplete xattrs\n"); // see "UPDATE" in trash_unlink().

      __TRY0( MD_PATH_OP(truncate, info->ns, info->post.md_path, 0) );

      __TRY0( trunc_xattrs(info) ); // didn't have all, but might have had some

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

   __TRY0( stat_regular(info) );

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

   // we'll write to trash_file
   __TRY0( expand_trash_info(info, path) );

   // This is untested code to move symlinks to the named trash-file.
   // Because links also don't have (MarFS) xattrs, they will already have
   // been simply unlinked above.
   if (S_ISLNK(info->st.st_mode)) {
      char target[MARFS_MAX_MD_PATH];
      __TRY_GE0( MD_PATH_OP(readlink, info->ns, info->post.md_path,
                            target, MARFS_MAX_MD_PATH) );
      __TRY0( MD_PATH_OP(symlink, info->ns, target, info->trash_md_path) );
      __TRY0( MD_PATH_OP(chmod, info->ns, info->trash_md_path, new_mode) );
   }

   else {
      // read from md_file
      // file handle for the metadata file
      MarFS_FileHandle md_fh;
      init_filehandle(&md_fh, info);
      // open_md will initialize the file handle for us.
      open_md(&md_fh, 0 /* open for reading */);
      if(! is_open_md(&md_fh)) {
         LOG(LOG_ERR, "open(%s, O_RDONLY) [oct]%o failed\n",
             info->post.md_path, new_mode);
         return -1;
      }
      // write to trash file
      MarFS_FileHandle trash_fh;
      init_filehandle(&trash_fh, info);
      open_md_path(&trash_fh, info->trash_md_path,
                   (O_CREAT|O_WRONLY), new_mode);
      if(! is_open_md(&trash_fh)) {
         LOG(LOG_ERR, "open(%s, (O_CREAT|O_WRONLY), [oct]%o) failed\n",
             info->trash_md_path, new_mode);
         __TRY0( close_md(&md_fh) );
         return -1;
      }

      // MD files are trunc'ed to their "logical" size (the size of the data
      // they represent).  They may also contain some "system" data (blobs we
      // have tucked inside, to track object-storage.  Move the physical data,
      // then trunc to logical size.
      off_t log_size = info->st.st_size;
      off_t phy_size = (has_all_xattrs(info, XVT_POST)
                        ? info->post.chunk_info_bytes
                        : info->st.st_size); // fuse crashed?  Can still GC.

      if (phy_size) {

         // buf used for data-transfer
         const size_t BUF_SIZE = 32 * 1024 * 1024; /* 32 MB */
         char* buf = malloc(BUF_SIZE);
         if (!buf) {
            LOG(LOG_ERR, "malloc %ld bytes failed\n", BUF_SIZE);

            // clean-up
            __TRY0( close_md(&md_fh) );
            __TRY0( close_md(&trash_fh) );
            return -1;
         }

         size_t read_size = ((phy_size < BUF_SIZE) ? phy_size : BUF_SIZE);
         size_t wr_total = 0;

         // copy phy-data from md_file to trash_file, one buf at a time
         ssize_t rd_count;
         for (rd_count = MD_FILE_OP(read, &md_fh, (void*)buf, read_size);
              (read_size && (rd_count > 0));
              rd_count = MD_FILE_OP(read, &md_fh, (void*)buf, read_size)) {
            char*  buf_ptr = buf;
            size_t remain  = rd_count;
            while (remain) {
               size_t wr_count = MD_FILE_OP(write, &trash_fh, buf_ptr, remain);
               if (wr_count < 0) {
                  LOG(LOG_ERR, "err writing %s (byte %ld)\n",
                      info->trash_md_path, wr_total);

                  // clean-up
                  __TRY0( close_md(&md_fh) );
                  __TRY0( close_md(&trash_fh) );
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
                info->trash_md_path, wr_total);

            // clean-up
            __TRY0( close_md(&md_fh) );
            __TRY0( close_md(&trash_fh) );
            return -1;
         }
      }

      // clean-up
      __TRY0( close_md(&md_fh) );
      __TRY0( close_md(&trash_fh) );

      // trunc trash-file to size
      __TRY0( MD_PATH_OP(truncate, info->ns, info->trash_md_path, log_size) );
   }

   // copy any xattrs to the trash-file.
   //
   // ugly-but-simple: make a duplicate PathInfo, but with post.md_path set
   // to our trash_md_path.  Then save_xattrs() will just work on the
   // trash-file.
   PathInfo trash_info = *info;
   memcpy(trash_info.post.md_path, trash_info.trash_md_path, MARFS_MAX_MD_PATH);
   trash_info.post.flags |= POST_TRASH;

   //   // Save only those xattrs that were non-default in the original
   //   // PathInfo.  (Does that make sense?)
   //   __TRY0( save_xattrs(&trash_info, info->xattrs) );
   //
   // we know it has at least one chunk, because it has PRE.
   // Help GC to delete objects.
   int orig_has_restart = has_all_xattrs(info, XVT_RESTART);
   if ( orig_has_restart  &&  (info->pre.obj_type == OBJ_FUSE) ) {
      trash_info.post.obj_type         = OBJ_MULTI;
      trash_info.post.chunks           = info->st.st_size / sizeof(MultiChunkInfo);
      trash_info.post.chunk_info_bytes = trash_info.post.chunks * sizeof(MultiChunkInfo);
   }
   __TRY0( save_xattrs(&trash_info, (info->xattrs | XVT_POST)) ); // GC needs POST

   // update trash-file atime/mtime to support "undelete"
   __TRY0( MD_PATH_OP(utime, info->ns, info->trash_md_path, &trash_time) );

   // if not already present, set a RESTART xattr on the original MD-file
   if ( ! orig_has_restart ) {
      info->xattrs |= XVT_RESTART;
      __TRY0( init_restart( &(info->restart) ) );
      __TRY0( save_xattrs( info, XVT_RESTART ) );
   }

   // write full-MDFS-path of original-file into trash-companion file
   __TRY0( write_trash_companion_file(info, path, &trash_time) );

   // clean out marfs xattrs on the original
   __TRY0( trunc_xattrs(info) );

   // old stat-info and xattr-info is obsolete.  Generate new obj-ID, etc.
   info->flags &= ~(PI_STAT_QUERY | PI_XATTR_QUERY);
   info->xattr_inits = 0;
   __TRY0( stat_xattrs(info) );   // has none, so initialize from scratch

   // NOTE: Unique-ness of Object-IDs currently comes from inode, plus
   //     obj-ctime, plus MD-file ctime.  It's possible the trashed file
   //     was created in the same second as this one, in which case, we'd
   //     be setting up to overwrite the object used by the trashed file.
   //     Adding pre.unique to the object-ID, which we increment from the
   //     version in the trash
   if (!strcmp(info->pre.objid, trash_info.pre.objid)) {
      info->pre.unique = trash_info.pre.unique +1;
      __TRY0( update_pre(&info->pre) );
      LOG(LOG_INFO, "unique: '%s'\n", info->pre.objid);
   }

   return 0;
}



// remove (not just reset but remove) all reserved xattrs

int trunc_xattrs(PathInfo* info) {
   XattrSpec*  spec;
   for (spec=MarFS_xattr_specs; spec->value_type!=XVT_NONE; ++spec) {
      MD_PATH_OP(lremovexattr, info->ns, info->post.md_path, spec->key_name);
      info->xattrs &= ~(spec->value_type);
   }
   return 0;
}




// return 0    if the quota is not exceeded
// return > 0  if there's no more space (according to user's quota).
// return < 0  for errors  (with errno)
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

   //   TRY_DECLS();
   //   if (! (info->flags & PI_STATVFS))
   //      __TRY0( statvfs(info->ns->fsinfo, &info->stvfs) );

#if TBD
   uint64_t  names_limit = (uint64_t)info->ns->quota_names;
#endif

   uint64_t space_limit = (uint64_t)info->ns->quota_space;

   // value of -1 for ns->quota_space implies unlimited
   if (info->ns->quota_space >= 0) {
      struct stat st;
      if (MD_PATH_OP(lstat, info->ns, info->ns->fsinfo_path, &st)) {
         LOG(LOG_ERR, "couldn't stat fsinfo at '%s': %s\n",
             info->ns->fsinfo_path, strerror(errno));
         errno = EINVAL;
         return -1;
      }
      if (st.st_size >= space_limit) { /* 0 = OK,  1 = no-more-space */
         LOG(LOG_INFO, "quota (%lu) exceeded by %lu\n",
             space_limit, (st.st_size - space_limit));
         return 1;
      }
   }

   // not over quota
   return 0;
}


// update the URL in the ObjectStream, in our FileHandle
int update_url(ObjectStream* os, PathInfo* info) {
   //   TRY_DECLS();
   //   __TRY0( update_pre(&info->pre) );
   strncpy(os->url, info->pre.objid, MARFS_MAX_URL_SIZE);

   // This can't go here anymore since we have DALS that do not use
   // the iobuf.

   // log the full URL, if possible:
   /* IOBuf*        b  = &os->iob; */
   /* __attribute__ ((unused)) AWSContext* ctx = ((b) */
   /*                                             ? b->context */
   /*                                             : aws_context_clone()); */

   /* LOG(LOG_INFO, "generated URL %s %s/%s/%s\n", */
   /*     ((b) ? "" : "(defaults)"), */
   /*     ctx->S3Host, ctx->Bucket, os->url); */

   return 0;
}


// Intended for internal consumption only (e.g. open_data() and
// delete_data().)  You probably don't need to call this directly.
//
// Assure that there is an initialized DAL, if needed.  We assume the
// filehandle is setup as it would be after expand_path_info(), with the MD
// path in fh->post.md_path, and info->flags |= PI_EXPANDED.

static
int init_data(MarFS_FileHandle* fh) {

   __attribute__((unused)) PathInfo* info = &fh->info;

   // if we haven't already initialized the FileHandle DAL, do it now.
#if USE_DAL
   if (! FH_DAL(fh)) {

      // copy static DAL ptr from NS to FileHandle
      FH_DAL(fh) = info->pre.repo->dal;
      LOG(LOG_INFO, "DAL: %s\n", FH_DAL(fh)->name);
   }
#else
   LOG(LOG_INFO, "ignoring file-DAL\n");
#endif

   // allow DAL implementation to do custom initializations
   //
   // MarFS expects fh->os.written to be side-effected by streaming
   // operations (i.e. any DAL I/O).  We give the DAL init-function
   // access to the ObjectStream member of the file-handle, so the
   // implementation can have this effect.
   //
   // In fact, we now pass the entire file-handle to the DAL init
   // function.  Why not?  DAL implementations should have the power to
   // royally eff everything up, just like we do.  The fh.dal_handle.ctx
   // is just extra generic state that is reserved for them.  This new
   // approach lets us clean some of the aws4c-specific inits out of
   // marfs_open() and into stream_init(), which in turn allows them to
   // be used in a DAL that supports a delete function, which will not
   // go through marfs_open(), but can go through DAL init.
   DAL_OP(init, fh, FH_DAL(fh), fh);

   return 0;
}


// DAL clean-up, after file-handle is completely closed/done/finis.  It
// doesn't mean "destroy the data", but, rather, invoke the "destroy"
// function for the "data" aspect of a file-handle, as opposed to the "md"
// aspect.
int destroy_data(MarFS_FileHandle* fh) {
   int rc = 0;

#if USE_DAL
   // clean-up DAL state
   rc = DAL_OP(destroy, fh, FH_DAL(fh));
   FH_DAL(fh) = NULL; // need to make sure that if we call open_data
                      // again, it will actually reinitialize the dal
#endif

   return rc;
}


int open_data(MarFS_FileHandle* fh,
              int               writing_p,
              size_t            chunk_offset,
              size_t            content_length,
              uint8_t           preserve_wr_count,
              uint16_t          timeout) {
   ENTRY();
   int         flags;
   const char* flags_str;

   if (writing_p) {
      flags     =  O_WRONLY;
      flags_str = "O_WRONLY";
   }
   else {
      flags     =  O_RDONLY;
      flags_str = "O_RDONLY";
   }

   // if we haven't already initialized the FileHandle DAL, do it now.
   init_data(fh);

#if USE_DAL
   TRY0( DAL_OP(update_object_location, fh) );
#else
   TRY0( update_url(&fh->os, &fh->info) );
#endif
   // if we haven't already opened the data-stream, do it now.
   //
   // TBD: is_open().  Should assimilate FH->os_init, which should become a flag
   //
   // if (! DAL_OP(is_open, fh))
   {
      if (DAL_OP(open, fh, writing_p,
                 chunk_offset, content_length, preserve_wr_count, timeout) ) {
         LOG(LOG_ERR, "open_data() failed: %s\n", strerror(errno));
         return -1;
      }
   }

   LOG(LOG_INFO, "open_data() ok\n");
   EXIT();
   return 0;
}


// Standard idiom for closing data-streams is something like
//    DAL_OP(sync, fh);
//    DAL_OP(close, fh);
//    DAL_OP(destroy, fh);
//
// <abort> means use DAL_OP(abort, fh), instead of sync.
// <force> acts as though they were all wrapped in TRY0()

int close_data(MarFS_FileHandle* fh,
               int               abort,
               int               force) {
   int rc;

   // try the abort/sync
   if (abort)
      rc = DAL_OP(abort, fh);
   else
      rc = DAL_OP(sync, fh);

   if (rc && !force) {
      LOG(LOG_ERR, "%s failed\n", (abort ? "abort" : "sync"));
      return rc;
   }

   // try the close
   rc = DAL_OP(close, fh);
   if (rc && !force) {
      LOG(LOG_ERR, "close failed\n");
      return rc;
   }

   rc = destroy_data(fh);
   if (rc)
      LOG(LOG_ERR, "destroy failed\n");
   return rc;
}



// DELETE_DATA
//
// delete the named object.  The only client that currently does true
// deletes of objects is Garbage Collection.  For everyone else, "delete"
// means "move marfs file to the trash".
//
// TBD: We could consider moving the logic that covers all of deletion
//     including deleting the MD file, from GC to here.



// There's a complication.  We want GC deletes of objects to be abstracted
// by going through the DAL, so that the deletes will "just work" with any
// kind of backing store.  However, the DAL is oriented toward
// file-handles.  MarFS file-handles are initialized from marfs files, and
// have a relationship with the marfs xattrs on an MD file, which are
// parsed into the Pre and Post structs in the FH.  To go through the DAL,
// we'll want to create a dummy file-handle that looks like a real
// file-handle in any ways the DAL will care about, which, luckily enough,
// are probably few.
//
// One issue with using file-handles is that, in this "context free" usage,
// there is no "open" or "close" to be done.
//
//    <objid> -- an object-ID, including bucket, but not host.
//
//    <md_path> -- is this the original MD path before the file was
//       deleted, or is it the new MD path of the file, where it currently
//       sits in the trash?  We probably don't care, because we are *only*
//       deleting the object, not the MD file.
//

int fake_filehandle_for_delete(MarFS_FileHandle* fh,
                               const char*       objid,
                               const char*       md_path) {
   TRY_DECLS();
   MarFS_XattrPre*  pre  = &fh->info.pre;
   PathInfo*        info = &fh->info;
   struct stat*     unused = NULL;

   memset(fh, 0, sizeof(MarFS_FileHandle));
   TRY0( str_2_pre(pre, objid, unused) );

   strncpy(info->post.md_path, md_path, MARFS_MAX_MD_PATH);
   info->post.md_path[MARFS_MAX_MD_PATH -1] = 0;

   return fake_filehandle_for_delete_inits(fh);
}


int fake_filehandle_for_delete_inits(MarFS_FileHandle* fh) {

   MarFS_XattrPre*  pre  = &fh->info.pre;
   PathInfo*        info = &fh->info;

   // fake expand_path_info()
   info->ns      = (MarFS_Namespace*)pre->ns;
   info->flags  |= PI_EXPANDED;

   // fake stat_xattrs()
   info->xattrs |= (XVT_PRE | XVT_POST);
   info->flags  |= PI_XATTR_QUERY;

   // don't let anyone call stat_regular().
   info->flags |= PI_STAT_QUERY;

   return 0;
}


// The GC utility uses this to do DAL-agnostic deletion of objects.
// 
int delete_data(MarFS_FileHandle* fh) {
   TRY_DECLS();
   size_t res;

   TRY0( init_data(fh) );
   LOG(LOG_INFO, "ATTEMPTING(%s)\n", "DAL_OP(update_object_location, fh)");
   if( (res = (size_t)DAL_OP(update_object_location, fh)) == 0 ) {
      LOG(LOG_INFO, "ATTEMPTING(%s)\n", "DAL_OP(del, fh)");
      res = (size_t)DAL_OP(del, fh);
      if(res) {
         PRE_RETURN();
         LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n", "DAL_OP(del, fh)", res, errno, strerror(errno));
      }
   }
   else {
      PRE_RETURN();
      LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n", "DAL_OP(update_object_location, fh)", res, errno, strerror(errno));
   }
   TRY0( destroy_data(fh) );
   if( res )
      RETURN(-1);
   return 0;
}


// Assure MD is open.
//
// NOTE: It would be simpler to just look at fh->flags.  If they include
//     FH_WRITING, then open for writing, or if FH_READING, then open for
//     reading.  However, this needs to be called from pftools MARFS_Path,
//     in some rank which may not have "opened" the FileHandle, and so
//     there would be no flags in fh->flags.  For example,
//     write_chunkinfo() is now called from a rank that is distinct from
//     the rank that opens the destination file-handle for writing.
//
//     The issue is that by taking a read/write argument, open_md() is
//     allowing someone to screw up the direction in which the MD fd is
//     opened.  (Should be easy to notice, though.)

int open_md(MarFS_FileHandle* fh, int writing_p) {

   __attribute__((unused)) PathInfo* info = &fh->info;

   int         flags;
   const char* flags_str;

   if (writing_p) {
      flags     =  O_WRONLY;
      flags_str = "O_WRONLY";
   }
   else {
      flags     =  O_RDONLY;
      flags_str = "O_RDONLY";
   }

   // if we haven't already initialized the FileHandle MD, do it now.
#if USE_MDAL
   if (! F_MDAL(fh)) {
      // copy static MDAL ptr from NS to FileHandle
      F_MDAL(fh) = info->pre.ns->file_MDAL;
      LOG(LOG_INFO, "file-MDAL: %s\n", F_MDAL(fh)->name);

      // allow MDAL implementation to do custom initializations
      F_OP(f_init, fh, F_MDAL(fh));
   }
#else
   LOG(LOG_INFO, "ignoring file-MDAL\n");
#endif


   // if we haven't already opened the MD file, do it now.
#if USE_MDAL
   if (! F_OP(is_open, fh)) {
      if (! F_OP(open, fh, info->post.md_path, flags) ) {
         LOG(LOG_ERR, "open_md(%s,%s) failed: %s\n",
             info->post.md_path, flags_str, strerror(errno));
         return -1;
      }
   }
#else
   if (! fh->md_fd) {
      fh->md_fd = open(info->post.md_path, flags);
      if (fh->md_fd < 0) {
         LOG(LOG_ERR, "open_md(%s,%s) failed: %s\n",
             info->post.md_path, flags_str, strerror(errno));
         fh->md_fd = 0;
         return -1;
      }
   }
#endif

   LOG(LOG_INFO, "open_md(%s,%s) ok\n", info->post.md_path, flags_str);
   return 0;
}


// non-zero = true, 0 = false
//
// NOTE: A case where <fh> doesn't have an MDAL is in pftool, in
//     Pool<MARFS_Path>::get(), supporting factory creation, where the
//     initial object is default-constructed, and is being replaced by a
//     new object.  The destructor of the old object calls here.
int is_open_md(MarFS_FileHandle* fh) {
#if USE_MDAL
   return (F_MDAL(fh) ? F_OP(is_open, fh) : 0);
#else
   return (fh->md_fd > 0);
#endif
}


int close_md(MarFS_FileHandle* fh) {
   TRY_DECLS();
#if USE_MDAL
   TRY0( F_OP(close, fh) );
   TRY0( F_OP(f_destroy, fh, F_MDAL(fh)) );
#else
   TRY0( close(fh->md_fd) );
   fh->md_fd = 0;
#endif
   return 0;
}

// Open a metadata directory
int opendir_md(MarFS_DirHandle *dh, PathInfo* info) {
   TRY_DECLS();
#if USE_MDAL
   D_MDAL(dh) = info->ns->dir_MDAL;
   LOG(LOG_INFO, "dir-MDAL: %s\n", D_MDAL(dh)->name);

   // allow MDAL implementation to do any initializations necessary
   D_OP(d_init, dh, D_MDAL(dh));

   TRY_GT0( D_OP(opendir, dh, info->post.md_path) );
   // XXX: there is potential for a memory leak here if opendir fails
   //      for a mdal that allocates memory in d_init. In that case, it
   //      is not clear to me whether d_destroy will ever be called...
#else
   TRY_GT0( opendir(info->post.md_path) );
   dh->internal.dirp = (DIR*)rc_ssize;
#endif
   return 1; // must be > 0
}

// Close a metadata directory
int closedir_md(MarFS_DirHandle* dh) {
   TRY_DECLS();
#if USE_MDAL
   TRY0( D_OP(closedir, dh) );
   TRY0( D_OP(d_destroy, dh, D_MDAL(dh)) );
#else
   DIR* dirp = dh->internal.dirp;
   TRY0( closedir(dirp) );
#endif
   return 0;
}


// Seek to the location of a given chunk.
// [e.g. Then call read_chunkinfo().]
// We assume the MD fd is already open
//
int seek_chunkinfo(MarFS_FileHandle* fh, size_t chunk_no) {
   TRY_DECLS();
   const size_t chunk_info_len    = sizeof(MultiChunkInfo);
   off_t        chunk_info_offset = chunk_no * chunk_info_len;

   TRY_GE0( MD_FILE_OP(lseek, fh, chunk_info_offset, SEEK_SET) );

   LOG(LOG_INFO, "chunk=%ld, sizeof chunkinfo=%ld, chunkinfo_offset=%ld\n",
       chunk_no, chunk_info_len, chunk_info_offset);

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
// NOTE: When writing through fuse, there may be a long sequence of writes
//    that are arbitrarily-sized with respect to the size of a marfs-chunk.
//    Therefore, the total data written can be tracked in the ObjectStream
//    in a MarFS_FileHandle, and any system-writes (recovery-info) can be
//    subtracted.  In this case, <user_data_written> is the total amount of
//    user-data written, so far.
//
//    On the other hand, pftool may have many tasks writing to the same
//    file (see OBJ_Nto1).  Pftool assures that they all start writing at
//    logical chunk-boundaries, and that they all (except possibly the
//    last) write full-sized chunks.  In this case, <user_data_written>
//    is just the amount written to this chunk.
//
//    <size_is_per_chunk> is true from pftool, and false from fuse.
//
//    //    In both cases, if we have the logical offset that was used at
//    //    open-time (i.e. 0 for fuse, or some block-boundary for pftool),
//    //    then we can fill out the chunk-info.  And, in both cases,
//    //    OS.written should be preserved.
//
int write_chunkinfo(MarFS_FileHandle*     fh,
                    // size_t                open_offset,
                    size_t                user_data_written,
                    int                   size_is_per_chunk) {
   TRY_DECLS();

   PathInfo* info = &fh->info;

   const size_t recovery             = MARFS_REC_UNI_SIZE;
   const size_t user_data_per_chunk  = info->pre.chunk_size - recovery;
   const size_t log_offset           = info->pre.chunk_no * user_data_per_chunk;
   const size_t user_data_this_chunk = (size_is_per_chunk
                                        ? user_data_written
                                        : (user_data_written - log_offset));

   const size_t chunk_info_len    = sizeof(MultiChunkInfo);
   char         str[chunk_info_len];

   MultiChunkInfo chunk_info = (MultiChunkInfo) {
      .config_vers_maj  = MARFS_CONFIG_MAJOR, // marfs_config->version_major,
      .config_vers_min  = MARFS_CONFIG_MINOR, // marfs_config->version_minor,
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

   TRY0( open_md(fh, 1) );

   // seek to offset for this chunk, in MD file
   TRY0(seek_chunkinfo(fh, info->pre.chunk_no));

   // write portable binary to MD file
   LOG(LOG_INFO, "chunk=%ld, open_offset=%ld, data_length=%ld\n",
       info->pre.chunk_no, fh->open_offset, user_data_this_chunk);

   ssize_t wr_count = MD_FILE_OP(write, fh, str, chunk_info_len);

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

// read MultiChunkInfo for the next chunk, from file
int read_chunkinfo(MarFS_FileHandle* fh, MultiChunkInfo* chnk) {
   static const size_t chunk_info_len = sizeof(MultiChunkInfo);
   char                str[chunk_info_len];
   TRY_DECLS();

   TRY0( open_md(fh, 0) );

   ssize_t rd_count = MD_FILE_OP(read, fh, str, chunk_info_len);

   if (rd_count < 0) {
      LOG(LOG_ERR, "error reading chunk-info (%s)\n",
          strerror(errno));
      return -1;
   }
   else if (rd_count != chunk_info_len) {
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
   // NOTE: Value can legitimately be less than sizeof(MultiChunkInfo),
   //     because chunkinfo_2_str() and str_2_chunkinfo() do not write/read
   //     the implicit padding that may exist in the MultiChunkInfo struct.
   else if (str_count > chunk_info_len) {
      LOG(LOG_ERR, "error preparing chunk-info (%ld != %ld)\n",
          str_count, chunk_info_len);
      errno = EIO;
      return -1;
   }

   return 0;
}




// This is intended to count the number of valid MultiChunkInfo records in
// a file.  It was used from pftool, in the case of N:1 writes, where the
// writers can't be sure who wrote last, so they can't maintain the correct
// file-size.  But, then, I realized we have the source-file handy, so we
// can just stat it to get the proper destination-size.
//
// However, I want to keep this around, because it could be used to check
// for whether a file was completely copied, in a restart situation.
//
// return count of chunks, or -1 if there's an error.  <chnk> gets the
// final MultiChunkInfo.
//
// NOTE: We don't want to parse as deeply as read_chunkinfo() does, just
//     count valid chunks.
//
// caller could do something like this:
//
//       STAT_XATTRS(&info); // to get xattrs
//       if ((has_all_xattrs(&info, XVT_PRE))
//           && (info.pre.obj_type == OBJ_Nto1)
//           && (info.post.obj_type == OBJ_MULTI)) {
//    
//          const size_t recovery = MARFS_REC_UNI_SIZE; // written in tail of object
//    
//          // find the MarFS logical file-size by counting MultiChunkInfo
//          // objects, written into the metadata file.  There will always be at
//          // least one, even if file received no writes (?)
//       #if USE_MDAL
//          if (! F_OP(open, fh, info.post.md_path, (O_RDONLY)))  // no O_BINARY in Linux.
//             return -1;
//       #else
//          int md_fd = open(info.post.md_path, (O_RDONLY));  // no O_BINARY in Linux.
//          if (md_fd < 0) {
//             // fh->md_fd = 0;
//             return -1;
//          }
//       #endif
//          MultiChunkInfo final_chunk;      // gets final chunk-contents
//          ssize_t n_chunks = count_chunkinfo(fh, &final_chunk);
//       #if USE_MDAL
//          close(fh->md_fd);
//       #else
//          F_OP(close, fh);
//       #endif
//          if (n_chunks < 0) {
//             return -1;
//          }
//    
//          size_t logical_size = final_chunk.chunk_data_bytes;
//          if (n_chunks)
//              logical_size += ((n_chunks -1) * (info.pre.repo->chunk_size - recovery));
//    
//          // truncate file to logical-size indicated in chunk-data
//       #if USE_MDAL
//          TRY0( F_OP_NOCTX(truncate, info.ns, info.post.md_path, logical_size) );
//       #else
//          TRY0( truncate(info.post.md_path, logical_size) );
//       #endif
//    
//          // Some xattr fields were unknown until now
//          info.post.obj_type         = ((n_chunks > 1) ? OBJ_MULTI : OBJ_UNI);
//          info.post.chunks           = ((n_chunks > 1) ? n_chunks : 0);
//          info.post.chunk_info_bytes = ((n_chunks > 1)
//                                        ? (n_chunks * sizeof(MultiChunkInfo))
//                                        : 0);
//          info.xattrs &= ~(XVT_RESTART);
//    
//          SAVE_XATTRS(&info, MARFS_ALL_XATTRS);
//       }

ssize_t count_chunkinfo(MarFS_FileHandle* fh) {
   const size_t  chunk_info_len = sizeof(MultiChunkInfo);
   char          str[chunk_info_len];
   ssize_t       result = 0;

   while (1) {
      ssize_t rd_count = MD_FILE_OP(read, fh, str, chunk_info_len);

      if (rd_count < 0) {
         LOG(LOG_ERR, "error reading chunk-info (%s)\n",
             strerror(errno));
         return -1;
      }
      else if (rd_count == 0) {
         return result;         // EOF
      }
      else if (rd_count != chunk_info_len) {
         LOG(LOG_ERR, "error reading chunk-info #%ld (%ld != %ld)\n",
             result, rd_count, chunk_info_len);
         errno = EIO;
         return -1;
      }
      else {
         // Got a full-sized MultiChunkInfo.  However, it might be
         // all-zeros, if we're reading from a part of the MD file that is
         // sparse (e.g. because several pftool workers are doing N:1
         // writes to different regions of the file).
         int empty = 1;
         int i;
         for (i=0; i<chunk_info_len; ++i) {
            if (str[i]) {
               empty = 0;       // found non-zero
               break;
            }
         }
         if (empty)
            return result;      // chunkinfo was all-zeros
      }

      // one more valid MultiChunkInfo ...
      ++ result;
   }
}



// write appropriate recovery-info into an object.  Moved this here so it
// could be shared by marfs_write() and marfs_release()
//
// NOTE: We write several distinct null-terminated strings, instead of a
//     single string.  That's because each of these strings has its own
//     parser, e.g. str_2_pre(), and I'm not positive that any given
//     delimiter between them would be guaranteed easy to find.

ssize_t write_recoveryinfo(ObjectStream* os, PathInfo* info, MarFS_FileHandle* fh) {

   // compute the amount of user-data written into this object This will be
   // the total amount of user-data written to this file-handle, minus the
   // total-amount written as of the provious write to this file-handle.
   // (This should work for pftool, as well.)
   size_t user_data          = (os->written - fh->write_status.sys_writes);
   size_t user_data_this_obj = (user_data - fh->write_status.rec_info_mark);

#if 0
   // This version just writes some fake data, so we can exercise
   // fuse/pftool to (a) get the mechanics of writing recovery-info, and
   // (b) assure we don't return recovery-info to users as part of a read.

   TRY_DECLS();
   const size_t recovery = MARFS_REC_UNI_SIZE; // written in tail of object
   LOG(LOG_WARNING, "writing fake recovery-info of size %ld\n", recovery);

   // static char rec[recovery];  // stupid compiler can't handle this
   static char  rec[MARFS_REC_UNI_SIZE];

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
   __TRY_GE0( DAL_OP(put, fh, rec, recovery) );
   ssize_t wrote = rc_ssize;

#else

   TRY_DECLS();
   char  rec[MARFS_REC_BODY_SIZE + MARFS_REC_TAIL_SIZE];
   //   char* pos = &rec[0];


   //   // convert mtime to date-time string in our standard format
   //   char       date_string[MARFS_DATE_STRING_MAX];
   //   __TRY0( epoch_to_str(date_string, MARFS_DATE_STRING_MAX, &info->st.st_mtime) );


   // write the HEAD, with leading recovery-info.  These values include
   // size of the record, version info, and some stat-info (that which is
   // not redundant with info in the pre/post string, coming later)
   int write_count;
   write_count = snprintf(rec, MARFS_REC_HEAD_SIZE,
                          MARFS_REC_HEAD_FORMAT,
                          info->pre.config_vers_maj,
                          info->pre.config_vers_min,
                          MARFS_REC_UNI_SIZE,
                          user_data_this_obj,
                          info->st.st_mode, // initialized in mknod/open ?
                          info->st.st_uid,  // mknod/open ?
                          info->st.st_gid,  // mknod/open ?
                          (uint64_t)info->st.st_mtime); // mknod/open ?

   if (write_count < 0)
      return -1;
   if (write_count == MARFS_REC_HEAD_SIZE) { /* overflow */
      LOG(LOG_ERR, "problem writing HEAD\n");
      errno = EIO;
      return -1;
   }
   size_t  idx    = write_count +1;
   ssize_t remain = MARFS_REC_UNI_SIZE - idx;


   // write the PRE xattr string
   TRY0( pre_2_str(&rec[idx], remain, &info->pre) );

   idx += strlen(&rec[idx]) +1;
   remain = MARFS_REC_UNI_SIZE - idx;
   if (remain < 0) {
      LOG(LOG_ERR, "out of room for recovery-info\n");
      errno = EIO;
      return -1;
   }

   // write the POST xattr string (including the MDFS path)
   TRY0( post_2_str(&rec[idx], remain, &info->post, info->pre.repo, 0) );

   idx += strlen(&rec[idx]) +1;
   remain = MARFS_REC_UNI_SIZE - idx;
   if (remain < 0) {
      LOG(LOG_ERR, "out of room for recovery-info\n");
      errno = EIO;
      return -1;
   }

   // write MarFS namespace path. (This not the MDFS path.)
   write_count = snprintf(&rec[idx], MARFS_MAX_NS_PATH,
                          "PATH:%s",
                          fh->ns_path);
   if (write_count < 0)
      return -1;
   if (write_count == MARFS_MAX_NS_PATH) { /* overflow */
      LOG(LOG_ERR, "problem writing NS path\n");
      errno = EIO;
      return -1;
   }

   idx += write_count +1;
   remain = MARFS_REC_UNI_SIZE - idx;
   if (remain < 0) {
      LOG(LOG_ERR, "out of room for recovery-info\n");
      errno = EIO;
      return -1;
   }
   

   // add padding, leaving room for the final 2 64-bit values
   ssize_t padding = remain - MARFS_REC_TAIL_SIZE;
   if (padding < 0) {
      LOG(LOG_ERR, "out of room for recovery-info\n");
      errno = EIO;
      return -1;
   }
   memset(&rec[idx], 0, padding);

   idx += padding;
   remain = MARFS_REC_UNI_SIZE - idx;

   // write the TAIL.  After all the recovery-infos in an object, we print
   // two values, so that the recovery-info can be parsed out during
   // recovery, when we might have no knowledge of the structure of the
   // object.  (1) The next-to-last number says how many files have user
   // data in this object.  (This will also be the number of recovery-info
   // records).  (2) The last number indicates the length of all the
   // recovery-info (including the tail!).  Given this, a recovery-tool can
   // set about parsing out the recovery-info, and regenerating the MDFS as
   // it existed at the time the individual objects were created.
   //
   // Q: Why not just put the *offset* of the start of recovery-info into
   //    the final value?
   //
   // A: Putting the length here allows the packer to work in two different
   //    ways.  (a) The original spec said we would combine data at the
   //    beginning of the packed object, and segregate recovery-info at the
   //    end.  This is easy on the recovery, but less efficient for the
   //    packer (which is, we hope, the much more common operation).  (b)
   //    lay the small objects with their contiguous rec-info (and tail)
   //    right into the packed object.  (i.e. data1, rec1, tail1, data2,
   //    rec2, tail2, ...)  For recovery to work in this case, without
   //    doing any adjustment of the objects being packed, the rec-info
   //    tail should have length, rather than offset, of the recovery-info.

   write_count = snprintf(&rec[idx], MARFS_REC_TAIL_SIZE,
                          MARFS_REC_TAIL_FORMAT,
                          (uint64_t)1,                          // N files in this object
                          // (uint64_t)user_data_this_obj);     // offset of rec-info
                          (uint64_t)idx + MARFS_REC_TAIL_SIZE); // length of rec-info
   if (write_count < 0)
      return -1;
   if (write_count == MARFS_REC_TAIL_SIZE) { /* overflow */
      LOG(LOG_ERR, "problem writing TAIL\n");
      errno = EIO;
      return -1;
   }

   // write the buffer we have generated into the tail of the object
   TRY_GE0( DAL_OP(put, fh, rec, MARFS_REC_UNI_SIZE) );
   ssize_t wrote = rc_ssize;

   if (wrote != MARFS_REC_UNI_SIZE) {
      LOG(LOG_ERR, "wrote %ld bytes, instead of %ld\n", wrote, (uint64_t)MARFS_REC_UNI_SIZE);
      errno = EIO;
      return -1;
   }

#endif


   if (wrote > (ssize_t)(int)-1) {
      
   }

   // Maintain file-handle
   fh->write_status.sys_writes    += wrote;     // track non-user-data written
   fh->write_status.rec_info_mark  = user_data; // note user-data written as of now


   return wrote;
}


// For N:1 writes, pftool needs the marfs chunksize, minus recovery-info
// size, so that it can allocate chunks to tasks such that they will write
// an integral number of marfs chunks (except possibly for the last one),
// into a MULTI object.
//
// This adjusted size may depend on the size of the file being written, as
// well. (MarFS supports using different repos for different file-sizes,
// but, in practice, this might not tend to apply to the ranges of files
// that would be chunked by pftool.)
// 
// TBD: Allow writing multiple chunks per pftool task, by rounding the return
//     value to be a multiple of logical chunks.
ssize_t get_chunksize(const char* path,
                      size_t      file_size,
                      size_t      desired_chunk_size,
                      uint8_t     subtract_recovery_info) {
   TRY_DECLS();

   PathInfo info;
   memset((char*)&info, 0, sizeof(PathInfo));
   EXPAND_PATH_INFO(&info, path);

   return get_chunksize_with_info(&info, file_size, desired_chunk_size, subtract_recovery_info);
}

ssize_t get_chunksize_with_info(PathInfo*   info,
                                size_t      file_size,
                                size_t      desired_chunk_size,
                                uint8_t     subtract_recovery_info) {

   MarFS_Repo* repo = find_repo_by_range(info->ns, file_size);

   if (subtract_recovery_info)
      return repo->chunk_size - MARFS_REC_UNI_SIZE;
   else
      return repo->chunk_size;
}

// The returns the value that will ultimately become a "Content-Length"
// header in the PUT request.  If it is zero, then no header will be used,
// and instead the PUT will use chunked-transfer-encoding.
//
// See discussion above the WriteStatus decl, in common.h
//
// NOTE: FH.sys_req is fixed at open-time.
//
size_t get_stream_wr_open_size(MarFS_FileHandle* fh, uint8_t decrement) {
   PathInfo* info      = &fh->info;
   size_t    log_chunk = info->pre.repo->chunk_size - MARFS_REC_UNI_SIZE;

   if (decrement) // previous request completed ...
      fh->write_status.data_remain -= fh->write_status.user_req;

   fh->write_status.user_req = fh->write_status.data_remain;
   if (fh->write_status.user_req > log_chunk) {
      fh->write_status.user_req = log_chunk;
   }

   LOG(LOG_INFO, "returning %ld+%ld rem=%ld, lchnk=%ld, decr=%d\n",
       fh->write_status.user_req,
       fh->write_status.sys_req,
       fh->write_status.data_remain,
       log_chunk,
       decrement);
   return fh->write_status.user_req + fh->write_status.sys_req;
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
// character (a, b, or c) having all the values 0-9.  (We think of "a" and
// "b" as "branch" directories, and "c" as a "leaf" directory.)
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


// call init_scatter_tree(), not this.
int init_scatter_tree_internal(const char*    root_dir,
                               const MarFS_Namespace* ns,
                               const uint32_t shard,
                               const mode_t   branch_mode,
                               const mode_t   leaf_mode) {
   TRY_DECLS();
   struct stat st;
   const char* ns_name = ns->name;

   LOG(LOG_INFO, "scatter_tree %s/%s.%d\n", root_dir, ns_name, shard);


   // --- assure that top-level trash-dir (from the config) exists
   rc = MD_PATH_OP(lstat, ns, root_dir, &st);
   if (! rc) {
      if (! S_ISDIR(st.st_mode)) {
         LOG(LOG_ERR, "not a directory %s\n", root_dir);
         return -1;
      }
   }
   else if (errno == ENOENT) {
      // LOG(LOG_ERR, "creating %s\n", root_dir);
      // rc = mkdir(root_dir, mode);
      // if ((rc < 0) && (errno != EEXIST)) {
      //   LOG(LOG_ERR, "mkdir(%s) failed\n", root_dir);
      //   return -1;
      // }
      LOG(LOG_ERR, "doesn't exist %s\n", root_dir);
      return -1;
   }
   else {
      LOG(LOG_ERR, "stat failed %s (%s)\n", root_dir, strerror(errno));
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
      LOG(LOG_ERR, "no room for inode-subdirs after trash_md_path '%s'\n",
          dir_path);
      errno = EIO;
      return -1;
   }

   // create the 'namespace.shard' dir
   LOG(LOG_INFO, " maybe create %s\n", dir_path);
   rc = MD_D_PATH_OP(mkdir, ns, dir_path, branch_mode);
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
   // if not, we'll build/rebuild the entire tree.
   memcpy(sub_dir, "/9/9/9", 7); // incl final '/0'

   LOG(LOG_INFO, " checking %s\n", dir_path);
   if (! MD_PATH_OP(lstat, ns, dir_path, &st))
      return 0;           // skip the subdir-create loop
   else if (errno != ENOENT) {
      LOG(LOG_ERR, "lstat(%s) failed\n", dir_path);
      return -1;
   }

   // subdir "/9/9/9" didn't exist: try to create all of them
   // initialize "/i"
   LOG(LOG_INFO, " creating inode-subdirs\n");
   int i, j, k;
   for (i=0; i<=9; ++i) {

      sub_dir[0] = '/';
      sub_dir[1] = '0' + i;
      sub_dir[2] = 0;
      rc = MD_D_PATH_OP(mkdir, ns, dir_path, branch_mode);
      if ((rc < 0) && (errno != EEXIST)) {
         LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
         return -1;
      }

      // initialize "/i/j"
      LOG(LOG_INFO, " creating inode-subdirs %s/*\n", dir_path);
      for (j=0; j<=9; ++j) {

         sub_dir[2] = '/';
         sub_dir[3] = '0' + j;
         sub_dir[4] = 0;

         rc = MD_D_PATH_OP(mkdir, ns, dir_path, branch_mode);
         if ((rc < 0) && (errno != EEXIST)) {
            LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
            return -1;
         }

         // initialize "/i/j/k"
         for (k=0; k<=9; ++k) {

            sub_dir[4] = '/';
            sub_dir[5] = '0' + k;
            sub_dir[6] = 0;

            // make the '.../trash/namespace.shard//a/b/c' subdir
            rc = MD_D_PATH_OP(mkdir, ns, dir_path, leaf_mode);
            if ((rc < 0) && (errno != EEXIST)) {
               LOG(LOG_ERR, "mkdir(%s) failed\n", dir_path);
               return -1;
            }
         }
      }
   }

   return 0;                    // success
}

int init_scatter_tree(const char*    root_dir,
                      const MarFS_Namespace* ns,
                      const uint32_t shard,
                      const mode_t   branch_mode,
                      const mode_t   leaf_mode) {

   // temporarily suppress original umask, so we can avoid interfering with
   // desired mode-args given in the arguments.
   mode_t umask_orig = umask(0);
   int rc = init_scatter_tree_internal(root_dir, ns, shard, branch_mode, leaf_mode);
   umask(umask_orig);

   return rc;
}



// NOTE: for now, all the marfs directories are chown root:root, cmod 770
int init_mdfs() {
   TRY_DECLS();
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
      mode_t         branch_mode  = (S_IRWXU | S_IXOTH );     // 'chmod 701'
      mode_t         leaf_mode    = (branch_mode | S_IWOTH ); // 'chmod 703'


      LOG(LOG_INFO, "\n");
      LOG(LOG_INFO, "NS %s\n", ns->name);

      //      // only risk screwing up "jti", while debugging
      //      if (strcmp(ns->name, "jti")) {
      //         LOG(LOG_INFO, "skipping NS %s\n", ns->name);
      //         continue;
      //      }

      // "root" namespace is not backed by real MD or storage, it is just
      // so that calls to list '/' can be answered.
      if (IS_ROOT_NS(ns)) {
         LOG(LOG_INFO, "skipping root NS: %s\n", ns->name);
         continue;
      }




      // check whether "trash" dir exists (and create sub-dirs, if needed)
      LOG(LOG_INFO, "top-level trash dir   %s\n", ns->trash_md_path);

      rc = MD_PATH_OP(lstat, ns, ns->trash_md_path, &st);
      if (! rc) {
         if (! S_ISDIR(st.st_mode)) {
            LOG(LOG_ERR, "not a directory %s\n", ns->fsinfo_path);
            return -1;
         }
      }
      else if (errno == ENOENT) {
         // LOG(LOG_ERR, "creating %s\n", ns->fsinfo_path);
         // rc = mkdir(ns->trash_md_path, mode);
         // if ((rc < 0) && (errno != EEXIST)) {
         //   LOG(LOG_ERR, "mkdir(%s) failed\n", ns->trash_md_path);
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
      __TRY0( init_scatter_tree(ns->trash_md_path, ns, shard, branch_mode, leaf_mode) );






      // check whether mdfs top-level dir exists
      LOG(LOG_INFO, "top-level MDFS dir    %s\n", ns->md_path);

      rc = MD_PATH_OP(lstat, ns, ns->md_path, &st);
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

      rc = MD_PATH_OP(lstat, ns, ns->fsinfo_path, &st);
      if (! rc) {
         if (! S_ISREG(st.st_mode)) {
            LOG(LOG_ERR, "not a regular file %s\n", ns->fsinfo_path);
            return -1;
         }
      }
      else if (errno == ENOENT) {
         // __TRY0( truncate(ns->fsinfo_path, 0) ); // infinite quota, for now
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
      if (repo->access_method == ACCESSMETHOD_SEMI_DIRECT) {
         __TRY0( init_scatter_tree(repo->host, ns->name, shard, branch_mode, leaf_mode) );
      }
#endif

   }

   return 0;
}




// see marfs_read().  This is the new scheme to allow multiple NFS read
// threads on the same file-handle to avoid doing any close/reopens on the
// object-stream.  Caller is holding FH.OS.read_lock, so we're thread-safe.
// Insert caller's offset into the queue.  Each queue-element includes a
// lock that that reader is waiting on.  Return them a pointer to their
// lock, so they can wait on it.

volatile ReadQueueElt* enqueue_reader(off_t offset, MarFS_FileHandle* fh) {

   volatile ReadQueueElt* elt    = (ReadQueueElt*)calloc(1, sizeof(ReadQueueElt));
   if (! elt) {
      LOG(LOG_ERR, "couldn't allocate ReadQueueElt for offset %lu\n", offset);
      return NULL;
   }
   elt->offset = offset;
   SEM_INIT((SEM_T*)&elt->lock, 0, 0); // compiler is worried about "volatile"
   elt->next   = NULL;

   volatile ReadQueueElt* q      = fh->read_status.read_queue;
   volatile ReadQueueElt* q_next = (q ? q->next : NULL);

   if (q && q->rewinding)
     elt->rewinding = 1;

   while (q
          && q_next
          && (offset > q_next->offset)) {

      q      = q->next;
      q_next = q_next->next;
   }

   if (! q) {
      fh->read_status.read_queue = elt;
   }
   else if (offset < q->offset) {
      fh->read_status.read_queue = elt;
      elt->next = q;
   }
   else {
      q->next = elt;
      elt->next = q_next;

      if (q->rewinding)
         elt->rewinding = 1;
   }


   return elt;
}


// When any reader finishes, it checks to see whether there is an enqueued
// reader that is now ready to go.  If so, let him go.
void check_read_queue(MarFS_FileHandle* fh) {
   volatile ReadQueueElt* q = fh->read_status.read_queue;

   if (q && q->rewinding) {
     volatile ReadQueueElt* q2;
     for (q2=q; q2; q2=q2->next)
       q2->rewinding = 0;
   }

   if (q && (fh->read_status.log_offset == q->offset)) {
      LOG(LOG_INFO, "releasing reader at %lu\n", q->offset);
      POST((SEM_T*)&q->lock);   // volatile RQE* worries compiler
   }
}

// The queue is maintained in (ascending) order of offsets, so the only
// reader that should ever be dequeued is the one at the front of the
// queue.  That reader has now finished waiting, and just wants us to clean
// up the queue.
void dequeue_reader(off_t offset, MarFS_FileHandle* fh) {

   volatile ReadQueueElt* q = fh->read_status.read_queue;
   if (! q) {
      LOG(LOG_ERR, "No queue!\n");
      return;
   }
   else if (offset != q->offset) {
      LOG(LOG_INFO, "offset %lu doesn't match front of queue %lu\n",
          offset, q->offset);
      return;
   }

   fh->read_status.read_queue = q->next;

   SEM_DESTROY((SEM_T*)&q->lock); // volatile RQE* worries compiler
   free((void*)q);
}

// Give marfs_release() a way to clean up all pending readers
void terminate_all_readers(MarFS_FileHandle* fh) {

   if (! (fh->flags & FH_MULTI_THR))
      return;

   ObjectStream*     os   = &fh->os; // shorthand

   WAIT(&os->read_lock);
   fh->flags |= FH_RELEASING;   // message to waking sleepers

   volatile ReadQueueElt* q;
   for (q = fh->read_status.read_queue;
        q;
        q = q->next) {

      // unlock head-of-queue
      LOG(LOG_INFO, "releasing reader at %lu\n", q->offset);
      POST((SEM_T*)&q->lock);   // volatile RQE* worries compiler

      // wait for him to dequeue himself
      while (q == fh->read_status.read_queue) {
         POST(&os->read_lock);
         sched_yield();
         WAIT(&os->read_lock);
      }
   }

   POST(&os->read_lock);
}
