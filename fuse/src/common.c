#include "common.h"

#include <sys/types.h>          /* uid_t */
#include <unistd.h>
#include <attr/xattr.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>              /* rename() */
#include <assert.h>
#include <syslog.h>

#define FUSE_USE_VERSION 26
#include <fuse.h>



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
   //      syslog(LOG_ERR, "push_user -- already pushed!\n");
   //      return;
   //   }
   *saved_euid = geteuid();
   uid_t new_uid = fuse_get_context()->uid;
   int rc = seteuid(new_uid);
   if (rc == -1) {
      if ((errno == EACCES) && (new_uid == getuid()))
         return 0;              /* okay [see NOTE] */
      else {
         syslog(LOG_ERR, "push_user -- user %ld (euid %ld) failed seteuid(%ld)!\n",
                (size_t)getuid(), (size_t)geteuid(), (size_t)new_uid);
         return errno;
      }
   }
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
         syslog(LOG_ERR,
                "pop_user -- user %ld (euid %ld) failed seteuid(%ld)!\n",
                (size_t)getuid(), (size_t)geteuid(), (size_t)new_uid);
         return errno;
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
   // (pass path, address to stuff, batch/interactive, working with existing file)
   if (! info)
      return ENOENT;            /* no such file or directory */
   if (info->flags & PI_EXPANDED)
      return 0;

   // Take user supplied path in fuse request structure to look up info,
   // using MAR_mnttop and look up in MAR_namespace array, fill in all
   // MAR_namespace and MAR_repo info into a big structure to hold
   // everything we know about that path.  We also compute the full path of
   // the corresponding entry in the MDFS (see Namespace.md_path).
   info->ns = find_namespace(path);
   if (! info->ns)
      return ENOENT;            /* no such file or directory */

   char* sub_path = path + info->ns->mnt_suffix_len; /* below fuse mount */
   snprintf(info->md_path, "%s/%s", ns->md_path, sub_path);

   // you need to pass in is this interactive (fuse) or batch
   // so you can use iperms or bperms as the perms to use)
   //
   // [jti: we now handle this by having callers pass whichever perms they
   //       want to test.]

   // if this is an existing file operation (you need to pass in if this
   // may be a existing file op) and if it is you need to pull in the
   // Xattrs from the existing file (use stat_xattr() and put them into the
   // structure as well, so you have everything needed to deal with this
   // operation, whatever it might be
   //
   // the reason you need to get the xattrs from the existing file for some
   // ops is that you need that info for how to do the subsequent read op
   //
   // [jti: we now have fuse routines call stat_xattr() explicitly, when needed.]

   info->flags |= PI_EXPANDED;
   return 0;
}


// we defer computing the trash-path name until it is needed
// Trash-path gets a time-stamp added, so that if the same file is
// trashed many times, we can find all the versions. (?)
int expand_trash_info(PathInfo*    info,
                      const char*  path) {
   if (! info->flags & PI_TRASH_PATH) {
      char* sub_path = path + info->ns->mnt_suffix_len; /* below fuse mount */

      char       date_string[128];
      time_t     now = time(NULL);
      if (now == (time_t)-1)
         return errno;

      struct tm* t = localtime(&now);
      if (! t)
         return -1;

      if (! strftime(date_string, sizeof(date_string)-1, "%Y%m%d%_H%M%S", t))
         return -1;

      if (snprintf(info->md_trash_path,
                   "%s/%s.trash_%s",
                   ns->md_trash_path, sub_path, date_string) < 0)
         return -1;

      info->flags |= PI_TRASH_PATH;
   }
}



// stat_xattr()
//
// Find all the reserved xattrs on <path>.  These key-values are all parsed
// and stored into specific fields of a MarFS_ReservedXattr struct.  You
// must have called expand_path_info, first, so that PathInfo.md_path has
// been initialized.  Quick-and-dirty parser.
//
// NOTE: It should not be an error to fail to find xattrs, or to fail to
//       find all of the ones needed to fill out the Reserved struct.
//       Caller can check flags, afterwards to see that.



// most fields can be parsed like this
#define PARSE_XATTR(FIELD)                                              \
   do {                                                                 \
      if (lgetxaattr(md_path, spec->key_name,                           \
                     resv->(FIELD),                                     \
                     sizeof(resv->(FIELD))) < 0)                        \
         return errno;                                                  \
   } while (0)


//  if no objtype xattr then its just stat  [jti: leaving out plain stat()]
//  if objtype exists get entire H2O_reserved_xattr list
int stat_xattr(PathInfo* info) {

   if (info.flags & PI_XATTR_QUERY)
      return 0;                 /* already called stat_xattr() */

   MarFS_ReservedXattr* resv = &info->xattr;
   const char*          md_path = info->md_path;

   // go through the list of reserved Xattrs, and install values into
   // fields of the MarFS_ReservedXattr struct, in PathInfo.  Set the
   // RESV_INTIALIZED only if we found ALL reserved xattrs
   XattrSpec* spec;
   for (spec=MarFS_xattr_specs; spec.value_type!=XVT_NONE; ++spec) {

      switch (spec->value_type) {

      case XVT_REPO_NAME: {
         // read the repo-name, find the repo, and store a pointer
         char repo_name[MARFS_MAX_REPO_NAME];
         if (lgetxaattr(md_path, spec->key_name,
                        repo_name, MARFS_MAX_REPO_NAME) < 0)
            return errno;

         MarFS_Repo* repo = lookup_repo(repo_name);
         if (! repo)
            return ENOKEY;      /* ?? */

         resv->repo = repo;
      }
         break;

      case XVT_OBJID:
         if (lgetxaattr(md_path, spec->key_name,
                        resv->obj_id, MARFS_MAX_OBJ_ID) < 0)
            return errno;
         break;


         // remaining fields can be parsed in a standard way
      case XVT_OBJTYPE:       PARSE_XATTR(obj_type);       break;
      case XVT_OBJOFFSET:     PARSE_XATTR(obj_offset);     break;
      case XVT_CHUNK_SIZE:    PARSE_XATTR(chnksz);         break;
      case XVT_CONF_VERS:     PARSE_XATTR(conf_vers);      break;
      case XVT_COMPRESS:      PARSE_XATTR(compress);       break;
      case XVT_CORRECT:       PARSE_XATTR(correct);        break;
      case XVT_CORRECT_INFO:  PARSE_XATTR(correct_info);   break;
      case XVT_FLAGS:         PARSE_XATTR(flags);          break;
      case XVT_SECURITY:      PARSE_XATTR(security);       break;

      default:
         return ENOKEY;         /* ?? */
      };
   }

   // found ALL fields in MarFS_xattr_specs
   resv->flags |= RESV_INITIALIZED;
   

   return 0;                    /* "success" */
}



int stat_regular(PathInfo* info) {

   if (info.flags & PI_STAT_QUERY)
      return 0;                 /* already called stat_xattr() */

   memset(&(info->st), 0, sizeof(struct stat));

   int rc = lstat(info->md_path, &info->st);
   info->flags |= PI_STAT_QUERY;
   return rc;
}


// return non-zero if info->md_path exists
int exists(PathInfo* info) {
   assert(info->flags & PI_EXPANDED); /* expand_path_info() was called? */
   stat_regular(info);                /* no-op, if already done */
   return (info.st.st_ino != 0);
}

// Return non-zero if info->md_path has ALL the reserved xattrs needed to
// fill out the PathInfo.xattr struct.  Else, zero.
//
// NOTE: Having these reserved xattrs indicates that the data-contents are
//       stored directly in an object, described in the meta-data.
//       Otherwise, data is stored directly in the md_path.
int has_resv_xattrs(PathInfo* info) {
   assert(info->flags & PI_EXPANDED); /* expand_path_info() was called? */
   stat_xattr(info);                  /* no-op, if already done */
   return (info.xattr.flags & RESV_INITIALIZED);
}


// Rename MD file to trashfile, keeping all attrs.
// original is gone.
// Object-storage is untouched.
//
// NOTE: This is used to implement unlink().  Should we do something to
//       make this thread-safe (like unlink()) ?
int  trash_file(PathInfo*   info,
                const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   //    uniqueify name somehow with time perhaps == trashname, 
   //    rename file to trashname 
   //    trash_name()   record the full path in a related file in trash

   __TRY0(expand_trash_info, info, path); /* initialize info->trash_path */
   __TRY0(rename, info->md_path, info->trash_path);
   return 0;
}


// Copy trashed MD file into trash area, does NOT unlink original.
// Does NOT do anything with object-storage.
int  trash_dup_file(PathInfo*   info,
                    const char* path) {

   //    pass in expanded_path_info_structure and file name to be trashed
   //    rename mdfile (with all xattrs) into trashmdnamepath,
   assert(info->flags & PI_EXPANDED);

   //    If this has no xattrs (its just a normal file using the md file
   //    for data) just trunc the file and return – we have nothing to
   //    clean up, too bad for the user as we aren’t going to keep the
   //    trunc’d file.
   __TRY0(stat_xattr);
   if (! has_resv_xattrs(info)) {
      __TRY0(truncate, info->md_path, 0);
      return 0;
   }

   //   uniqueify name somehow with time perhaps == trashname, 
   //   stat_xattr()    to get all file attrs/xattrs
   //   open trashdir/trashname
   //
   //   if no xattr objtype [jti: i.e. info->xattr.obj_type == MARFS_UNI]
   //        copy file data to file
   //   if xattr objtype is multipart [jti: i.e. == MARFS_MULTI]
   //        copy file data until end of objlist marker
   //
   //   update trash file mtime to original mtime
   //   trunc trash file to same length as original
   //   set all reserved xattrs like original
   //
   //   close


   // capture mode-bits, etc.  Destination has all the same mode-bits,
   // for permissions and file-type bits only.  No, just permissions.
   __TRY0(stat_regular, info);
   mode_t new_mode = info->st & (ACCESSPERMS);

   // write to trash_file
   __TRY0(expand_trash_info, info, path);
   int out = open(info->trash_path, (O_CREAT | O_WRONLY), new_mode);
   if (out == -1)
      return errno;


   // MD file for "uni" storage contains ... what?
   // copy contents to the trash-version
   if (info->xattr.obj_type == MARFS_UNI) {

      // read from md_file
      int in = open(info->md_path, O_RDONLY));
      if (in == -1)
         return errno;

      // buf used for data-transfer
      const size_t BUF_SIZE = 64 * 1024 * 1024; /* 64 MB */
      char buf[BUF_SIZE];

      // copy everything from md_file to trash_file, one buf at a time
      size_t rd_count;
      for (rd_count = read(in, buf, BUF_SIZE);
           rd_count > 0;
           rd_count = read(in, buf, BUF_SIZE)) {

         size_t remain = rd_count;
         while (remain) {
            size_t wr_count = write(out, buf, remain);
            if (wr_count < 0)
               return errno;
            remain -= wr_count;
         }
      }

      if (rd_count < 0)
         return errno;
   }

   // MD file for "multi" storage contains ... what?
   // copy contents to the trash-version
   else if (info->xattr.obj_type == MARFS_MULTI) {
   }
   else if (info->xattr.obj_type == MARFS_PACKED) {
      assert(0); // TBD
   }
   else {
      assert(0);
   }
   
   __TRY0(trunc_xattr, info);
   __TRY0(trash_name, info, path);

   return 0;
}



int trash_name(const char* path, PathInfo* info) {
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

  

//   trunc file to zero
//   remove (not just reset but remove) all reserved xattrs
int trunc_xattr(PathInfo* info) {
   const char* path = info.md_path;

   XattrSpec*  spec;
   for (spec=MarFS_xattr_specs; spec->value_type!=XVT_NONE; ++spec) {
      lremovexattr(path, spec->key_name);
   }   
   return 0;
}
