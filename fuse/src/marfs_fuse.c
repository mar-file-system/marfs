

/* ___________________________________________________________________________
from Gary's email:

I was trying to update the fuse specs
I saw this about linux fuse
------------------------------------------------------
int(* fuse_operations::mknod)(const char *, mode_t, dev_t)
Create a file node
This is called for creation of all non-directory, non-symlink nodes. If the filesystem defines a create() method, then for regular files that will be called instead.

int(* fuse_operations::open)(const char *, struct fuse_file_info *)
File open operation
No creation (O_CREAT, O_EXCL) and by default also no truncation (O_TRUNC) flags will be passed to open(). If an application specifies O_TRUNC, fuse first calls truncate() and then open(). Only if 'atomic_o_trunc' has been specified and kernel version is 2.6.24 or later, O_TRUNC is passed on to open.
Unless the 'default_permissions' mount option is given, open should check if the operation is permitted for the given flags. Optionally open may also return an arbitrary filehandle in the fuse_file_info structure, which will be passed to all file operations.
-Changed in version 2.2 
------------------------------------------------------
This changes the spec slightly simplifying open a bunch
Looks like open with create is really mknod then open r/rw/w  no create/no trunc :-)
Looks like open with trunc is really trunc then open rw/w  :-)
Wow, fuse makes things easier.
___________________________________________________________________________ */

#include "common.h"
#include "marfs_fuse.h"

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <attr/xattr.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <utime.h>              /* for deprecated marfs_utime() */
#include <stdio.h>



void* marfs_init(struct fuse_conn_info* conn) {
   conn->max_write = MARFS_WRITEBUF_MAX;
   conn->want      = FUSE_CAP_BIG_WRITES;

   return conn;
}

// called when fuse file system exits
void marfs_destroy (void* private_data) {
   // nothing for us to do here, we wont have dirty data when the fuse
   // daemon exits. I suppose they wait for all threads to finish before
   // leaving, so this should be ok.
}





// ---------------------------------------------------------------------------
// Fuse routines in alpha order (so you can actually find them)
// Unimplmented functions are gathered at the bottom
// ---------------------------------------------------------------------------


int marfs_access (const char* path,
                  int         mask) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  access call filling in fuse structure 
   //
   // jti: which fuse structure?
   TRY0(access, info.md_path, mask);
 
   POP_USER();
   return 0;
}

int marfs_chmod(const char* path,
                mode_t      mode) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  chmod call filling in fuse structure
   // WARNING: No lchmod() on rrz.
   //          chmod() always follows links.
   TRY0(chmod, info.md_path, mode);

   POP_USER();
   return 0;
}

int marfs_chown (const char* path,
                 uid_t       uid,
                 gid_t       gid) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  chown call filling in fuse structure
   TRY0(lchown, info.md_path, uid, gid);

   POP_USER();
   return 0;
}


int marfs_flush (const char*            path,
                 struct fuse_file_info* ffi) {
   // I don’t think we will have dirty data that we can control
   // I guess we could call flush on the filehandle  that is being written
   // But the only data we will write is multi-part objects, 
   // All other data would be to some object interface
   return 0;
}

int marfs_fsync (const char*            path,
                 int                    isdatasync,
                 struct fuse_file_info* ffi) {
   // I don’t know if we do anything here, I don’t think so, we will be in
   // sync at the end of each thread end
   return 0; // Just return
}

int marfs_fsyncdir (const char*            path,
                    int                    isdatasync,
                    struct fuse_file_info* ffi) {
   // don’t think there is anything to do here, we wont have dirty data
   // unless its trash
   LOG(LOG_INFO, "fsyncdir: skipping '%s'\n", path);

   return 0; // just return
}


int marfs_ftruncate(const char*            path,
                    off_t                  length,
                    struct fuse_file_info* ffi) {

   // *** this may not be needed until we implement write in the fuse daemon ***
   // *** may not be needed for the kind of support we want to provide ***

   // Check/act on truncate-to-zero only.
   if (length)
      return -EPERM;

   PUSH_USER();

   // resolve the full path to use to expand
   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // Call access() syscall to check/act if allowed to truncate for this user
   ACCESS(info.md_path, (W_OK));        /* for truncate? */

   // stat_xattr – or look up info stuffed into memory pointed at in fuse
   // open table if this is not just a normal [object-storage case?], use
   // the md for file data
   STAT_XATTR(&info);
   if (! has_any_xattrs(&info, MD_MARFS_XATTRS))
      assert(0); // TBD.  (data stored directly in file)

   //***** this may or may not work – may need a trash_dup_file() that uses
   //***** ftruncate since the file is already open (may need to modify the
   //***** trash_dup_file to use trunc or ftrunc depending on if file is
   //***** open or not

   // [jti: I think I read that FUSE will never call open() with O_TRUNC,
   // but will instead call truncate first, then open.  However, a user
   // might still call truncate() or ftruncate() explicitly.  For these
   // cases, I guess we assume the file is already open, and the filehandle
   // is good.]

   // copy metadata to trash, resets original file zero len and no xattr
   TRASH_DUP_FILE(&info, path);

   POP_USER();
   return 0;
}


int marfs_getattr (const char*  path,
                   struct stat* stbuf) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);
   LOG(LOG_INFO, "expanded %s -> %s\n", path, info.md_path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, R_META);

   // No need for access check, just try the op
   // appropriate statlike call filling in fuse structure (dont mess with xattrs here etc.)
   LOG(LOG_INFO, "lstat %s\n", info.md_path);
   TRY_GE0(lstat, info.md_path, stbuf);

   POP_USER();
   return 0;
}


int marfs_getxattr (const char* path,
                    const char* name,
                    char*       value,
                    size_t      size) {
   // *** this may not be needed until we implement user xattrs in the fuse daemon ***
   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // *** make sure they aren’t getting a reserved xattr***
   if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize))
      return EPERM;

   // No need for access check, just try the op
   // Appropriate  getxattr call filling in fuse structure
   TRY0(lgetxattr, info.md_path, name, (void*)value, size);

   POP_USER();
   return 0;
}


int marfs_ioctl(const char*            path,
                int                    cmd,
                void*                  arg,
                struct fuse_file_info* ffi,
                unsigned int           flags,
                void*                  data) {
   // if we need an ioctl for something or other
   // ***Maybe a way to get fuse deamon to read up new config file
   // *** we need a way for daemon to read up new config file without stopping
   return 0;
}




// NOTE: Even though we remove reserved xattrs, user can call with empty
//       buffer and receive back length of xattr names.  Then, when we
//       remove reserved xattrs (in a subsequent call), user will see a
//       different list length than the previous call lead him to expect.
int marfs_listxattr (const char* path,
                     char*       list,
                     size_t      size) {
   // *** this may not be needed until we implement user xattrs in the fuse daemon ***
   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  listxattr call
   // filling in fuse structure
   TRY_GE0(llistxattr, info.md_path, list, size);

#if 0
   // TBD ...
   // *** remove any reserved xattrs from list ***
   // We could malloc our own storage here, listxattr into our storage,
   // remove any reserved xattrs, then copy to user's storage.  Or, we
   // could just use the caller's space to receive results, and then remove
   // any reserved xattrs from that list.  That potentially allows a user
   // to discover the *names* of reserved xattrs (seeing them before we've
   // deleted them).  Because the user can't actually get values for the
   // reserved xattrs, and their names are to be documented for public
   // consumption, the former approach seems secure enough.

   char* name = list;
   char* end  = list + rc_ssize;
   while (name < end) {
      if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize)) {
         size_t len = strlen(name) +1;
         assert(name + len < end); /* else llistxattr() should return neg */

         // shuffle subsequent keys forward to cover this one
         memmove(name, name + len, end - name + len);

         // wipe the tail clean
         memset(end - len, 0, len);

         size -= len;
         end -= len;
      }
   }
#endif

   POP_USER();
   return 0;
}


int marfs_mkdir (const char* path,
                 mode_t      mode) {
   PUSH_USER();

   PathInfo  info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // Check/act on quota num files

   // No need for access check, just try the op
   // Appropriate mkdirlike call filling in fuse structure 
   TRY0(mkdir, info.md_path, mode);

   POP_USER();
   return 0;
}


// [See discussion at marfs_create().]
//
// This only gets called when fuse determines file doesn't exist and needs
// to be created.  If it needs to be truncated, fuse calls
// truncate/ftruncate.
// 
// It might make sense to do object-creation, and initialization of
// PathInfo.pre, here.  However, open() is where we do tests that should be
// done before creating the object.  Maybe we should move all those here,
// as well?  Meanwhile, we currently construct obj-id inside stat_xattrs(),
// in the case where MD exists but xattrs don't.
// 
int marfs_mknod (const char* path,
                 mode_t      mode,
                 dev_t       rdev) {

   //   // mknod() is now superceded by create().
   //   // Making sure this never gets called ...
   //   assert(0);

   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op
   // requires RMWM NOTE: We assure that open(), if called after us, can't
   // discover that user lacks sufficient access.  However, because we
   // don't know what the open call might be, we may be imposing
   // more-restrictive constraints than necessary.
   //
   //   CHECK_PERMS(info.ns->iperms, (R_META | W_META));
   //   CHECK_PERMS(info.ns->iperms, (R_META | W_META | W_DATA | T_DATA));
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA | T_DATA));

   // Check/act on quotas of total-space and total-num-names
   CHECK_QUOTAS(&info);

   // No need for access check, just try the op
   // Appropriate mknod-like/open-create-like call filling in fuse structure
   TRY0(mknod, info.md_path, mode, rdev);

   POP_USER();
   return 0;
}




// OPEN
//
// We maintain a MarFS_FileHandle, which has info needed by
// read/write/close, etc.
//
// Fuse will never call open() with O_CREAT or O_TRUNC.  In the O_CREAT
// case, it will just call maknod() first.  In the O_TRUNC case, it calls
// truncate.  Either way, these flags are stripped off.  We had a
// conversation about the fact that mknod() doesn't have access to the
// open-flags, whereas create() does, but decided mknod() should check
// RM/WM/RD/WD/TD
//
// No need to check quotas here, that's also done in mknod.
//
// Fuse guarantees that it will always call close on any open files, when a
// user-process exits.  However, if we throw an error (return non-zero)
// *during* the open, I'm assuming the user is not considered to have that
// file open.  Therefore, we re-#define RETURN() to add some custom
// clean-up to the common macros.  (See discussion below.)



// A FileHandle is dynamically allocated in marfs_open().  We want to make
// sure any error-returns will deallocate the file-handle.  In C++, it's
// easy to get an object to do cleanup when it goes out of scope.  In C, we
// need to add some code before every return, to do any cleanup that might
// be needed before returning.
//
// RETURN() is used inside TRY(), which is the basis of all the test-macros
// defined in common.h.  So, we redefine that to add our cleanup checks.
//
#undef RETURN
#define RETURN(VALUE)                             \
   do {                                           \
      free((MarFS_FileHandle*)ffi->fh);           \
      ffi->fh = 0;                                \
      return (VALUE);                             \
   } while(0)


int marfs_open (const char*            path,
                struct fuse_file_info* ffi) {

   assert(ffi->fh == 0);
   PUSH_USER();

   // Poke the xattr stuff into some memory for the file (poke the address
   //    of that memory into the fuse open structure so you have access to
   //    it when the file is open)
   //
   //    also poke how to access the objrepo for where/how to write and how to read
   //    also put space for read to attach a structure for object read mgmt
   //
   if (! (ffi->fh = (uint64_t) calloc(1, sizeof(MarFS_FileHandle))))
      return ENOMEM;

   MarFS_FileHandle* fh   = (MarFS_FileHandle*)ffi->fh; /* shorthand */
   PathInfo*         info = &fh->info;                  /* shorthand */
   EXPAND_PATH_INFO(info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM/RD 
   //   If wronly/rdwr/trunk  RM/WM/RD/WD/TD
   //   If append we don’t support that
   //
   // NOTE: FUSE doesn't actually call open with O_TRUNC, but instead calls
   //       truncate(), in that case.  Similarly, we won't get called with
   //       O_CREAT, cause mknod() handles that.
   //
   assert(! (ffi->flags & O_CREAT));
   assert(! (ffi->flags & O_TRUNC));

   if (ffi->flags & (O_RDONLY)) {
      fh->flags |= FH_READING;
      ACCESS(info->md_path, R_OK);
      CHECK_PERMS(info->ns->iperms, (R_META | R_DATA));
   }
   else if (ffi->flags & (O_WRONLY)) {
      fh->flags |= FH_WRITING;
      ACCESS(info->md_path, W_OK);
      CHECK_PERMS(info->ns->iperms, (R_META | W_META | R_DATA | W_DATA));
   }

   //   if (info->flags & (O_TRUNC)) {
   //      CHECK_PERMS(info->ns->iperms, (T_DATA));
   //   }

   // unsupported operations
   if (ffi->flags & (O_RDWR)) { /* for now */
      fh->flags |= (FH_READING | FH_WRITING);
      RETURN(EPERM);
   }
   if (ffi->flags & (O_APPEND))
      RETURN(EPERM);


   STAT_XATTR(info);

   // open md file in asked for mode
   if (! has_any_xattrs(info, MD_MARFS_XATTRS)) {
      fh->md_fd = open(info->md_path,
                       (ffi->flags & (O_RDONLY | O_WRONLY | O_RDWR)));
      if (fh->md_fd < 0)
         RETURN(-errno);
   }

  POP_USER();
  return 0;
}
#undef RETURN
#define RETURN(VALUE) return(VALUE)



int marfs_opendir (const char*            path,
                   struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  opendir call filling in fuse structure
   ///   mode_t mode = ~(fuse_get_context()->umask); /* ??? */
   ///   TRY_GE0(opendir, info.md_path, ffi->flags, mode);
   TRY_GE0(opendir, info.md_path);
   ffi->fh = rc_ssize;          /* open() successfully returned a dirp */

   POP_USER();
   return 0;
}


int marfs_read (const char*            path,
                char*                  buf,
                size_t                 size,
                off_t                  offset,
                struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM  RD
   CHECK_PERMS(info.ns->iperms, (R_META | R_DATA));

   // No need to call access as we called it in open for read
   // Case
   //   File has no xattr objtype
   //     Just read the bytes from the file and fill in fuse read buffer
   //   File is objtype packed or uni
   //      Make sure start and end are within the object according to (file size and objoffset)
   //      Make sure security is set up for accessing objrepo using table
   //      Read bytes from object server and fill in fuse read buffer
   //   File is objtype multipart
   //     Make sure start and end are within the object according to (file size and objoffset)
   //     Make sure security is set up for accessing objrepo using table
   //     If this is the first read, 
   //           Malloc space for read obj mgmt. and put address in fuse open table area
   //           read data from metadata file (which is already open and is the handle 
   //           passed in and put in buffer pointed to in fuse open table)
   //   File is striped
   //       We will implement this later perhaps
   //      look up in read obj mgmt. area for which object(s)
   //      for loop objects needed to honor read, read obj data and fill in fuse read buffer
   POP_USER();
   return 0;
}


int marfs_readdir (const char*            path,
                   void*                  buf,
                   fuse_fill_dir_t        filler,
                   off_t                  offset,
                   struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  readdir call filling in fuse structure  (fuse does this in chunks)
   DIR*           dirp = (DIR*)ffi->fh;
   struct dirent* dent;
   
   while (1) {
      // #if _POSIX_C_SOURCE >= 1 || _XOPEN_SOURCE || _BSD_SOURCE || _SVID_SOURCE || _POSIX_SOURCE
      //      struct dirent* dent_r;       /* for readdir_r() */
      //      TRY0(readdir_r, dirp, dent, &dent_r);
      //      if (! dent_r)
      //         break;                 /* EOF */
      //      if (filler(buf, dent_r->d_name, NULL, 0))
      //         break;                 /* no more room in <buf>*/

      // #else
      errno = 0;
      TRY_GE0(readdir, dirp);
      if (! rc_ssize) {
         if (errno)
            return -errno;      /* error */
         break;                 /* EOF */
      }
      dent = (struct dirent*)rc_ssize;
      if (filler(buf, dent->d_name, NULL, 0))
         break;                 /* no more room in <buf>*/
      // #endif
      
   }

   POP_USER();
   return 0;
}


int marfs_readlink (const char* path,
                    char*       buf,
                    size_t      size) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate readlinklike call filling in fuse structure 
   TRY0(readlink, info.md_path, buf, size);

   POP_USER();
   return 0;
}


//   "This is the only FUSE function that doesn't have a directly
//    corresponding system call, although close(2) is related. Release is
//    called when FUSE is completely done with a file; at that point, you
//    can free up any temporarily allocated data structures. The IBM
//    document claims that there is exactly one release per open, but I
//    don't know if that is true."

int marfs_release (const char*            path,
                   struct fuse_file_info* ffi) {
   // if writing there will be an objid stuffed into a address  in fuse open table
   //       seal that object if needed
   //       free the area holding that objid
   // if  reading, there might be a malloced space for read obj mgmt. in fuse open table
   //       close any objects if needed
   //       free the area holding that stuff
   // close the metadata file handle
   return 0;
}


//  [Like release(), this doesn't have a directly corresponding system
//  call.]  This is also the only function I've seen (sa far) that gets
//  called with fuse_context->uid of 0, even when running as non-root.
//  This seteuid() will fail.
//
// NOTE: Testing as myself, I notice releasedir() gets called with
// fuse_context.uid ==0.  Other functions are all called with
// fuse_context.uid == my_uid.  I’m ignoring push/pop UID in this case, in
// order to be able to continue debugging.

int marfs_releasedir (const char*            path,
                      struct fuse_file_info* ffi) {
   LOG(LOG_INFO, "releasedir %s\n", path);
   LOG(LOG_INFO, "entry -- skipping push_user(%ld)\n", fuse_get_context()->uid);
   //   PUSH_USER();
   size_t rc = 0;

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  closedir call filling in fuse structure
   DIR* dirp = (DIR*)ffi->fh;
   TRY0(closedir, dirp);

   LOG(LOG_INFO, "exit -- skipping pop_user()\n");
   //   POP_USER();
   return 0;
}


int marfs_removexattr (const char* path,
                       const char* name) {
   // *** this may not be needed until we implement user xattrs in the fuse daemon ***
   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // *** make sure they aren’t removing a reserved xattr***
   if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize))
      return EPERM;

   // No need for access check, just try the op
   // Appropriate  removexattr call filling in fuse structure 
   TRY0(lremovexattr, info.md_path, name);

   POP_USER();
   return 0;
}


int marfs_rename (const char* path,
                  const char* to) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  rename call filling in fuse structure 
   TRY0(rename, info.md_path, to);

   POP_USER();
   return 0;
}

// using looked up mdpath, do statxattr and get object name
int marfs_rmdir (const char* path) {
   // (we wont trash directories, we will preserve full paths of all files trashed instead)
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate rmdirlike call filling in fuse structure 
   TRY0(rmdir, info.md_path);

   POP_USER();
   return 0;
}


int marfs_setxattr (const char* path,
                    const char* name,
                    const char* value,
                    size_t      size,
                    int         flags) {
   // *** this may not be needed until we implement user xattrs in the fuse daemon ***
   return -ENOSYS;

   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // *** make sure they aren’t setting a reserved xattr***
   if (! strncmp(MarFS_XattrPrefix, name, MarFS_XattrPrefixSize))
      return EPERM;

   // No need for access check, just try the op
   // Appropriate  setxattr call filling in fuse structure 
   TRY0(lsetxattr, info.md_path, name, value, size, flags);

   POP_USER();
   return 0;
}

int marfs_statfs (const char*      path,
                  struct statvfs*  statbuf) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // Open and read from lazy-fsinfo data file updated by batch process  fsinfopath 
   //    Size of file sytem is quota etc.
   POP_USER();
   return 0;
}

int marfs_symlink (const char* path,
                   const char* to) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  symlink call filling in fuse structure 
   TRY0(symlink, info.md_path, to);

   POP_USER();
   return 0;
}

// *** this may not be needed until we implement write in the fuse daemon ***
int marfs_truncate (const char* path,
                    off_t       size) {

   // Check/act on truncate-to-zero only.
   if (size)
      return -EPERM;

   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // If this is not just a normal md, it's the file data
   STAT_XATTR(&info); // to get xattrs

   // Call access syscall to check/act if allowed to truncate for this user 
   ACCESS(info.md_path, (W_OK));

   // copy metadata to trash, resets original file zero len and no xattr
   TRASH_DUP_FILE(&info, path);

   POP_USER();
   return 0;
}


int marfs_unlink (const char* path) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // Call access() syscall to check/act if allowed to unlink for this user 
   ACCESS(info.md_path, (W_OK));

   // rename file with all xattrs into trashdir, preserving objects and paths 
   TRASH_FILE(&info, path);

   POP_USER();
   return 0;
}


// deprecated in 2.6
// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int marfs_utime(const char*     path,
                struct utimbuf* buf) {   
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0(utime, info.md_path, buf);

   POP_USER();
   return 0;
}

// System is giving us timestamps that should be applied to the path.
// http://fuse.sourceforge.net/doxygen/structfuse__operations.html
int marfs_utimens(const char*           path,
                  const struct timespec tv[2]) {   
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure
   // NOTE: we're assuming expanded path is absolute, so dirfd is ignored
   TRY_GE0(utimensat, 0, info.md_path, tv, AT_SYMLINK_NOFOLLOW);

   POP_USER();
   return 0;
}


int marfs_write(const char*            path,
                const char*            buf,
                size_t                 size,
                off_t                  offset,
                struct fuse_file_info* ffi) {
   // *** this may not be needed until we implement write in the fuse daemon ***
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // No need to call access as we called it in open for write
   // Make sure security is set up for accessing objrepo using iwrite_datarepo

   // If file has no xattrs its just a normal use the md file for data,
   //   just do the write and return – don’t bother with all this stuff below

   // If first write, check/act on quota bytes
   // If first write, it has to start at offset 0, if not fail
   // If write does not start at previous end of file, fail
   // If first write allocate space for current obj being written put addr in fuse open table
   // If first write or if new file length will make object bigger than chunksize 
   //    seal old ojb get new obj
   // If first write, add objrepo, objid, objtype(unitype), confversion, datasecurity, chnksz xattrs
   // If “new” obj
   //   If first “new” obj
   //       Write old and new objid to md file set end marker
   //       Change objid xattr to file
   //        Add change objtype to multipart
   //   Else
   //       Write new objid to mdfile;
   //      }
   //   Put objid into current obj being written into fuse open table place
   //   }
   // Write bytes to object
   // Trunc file to current last byte  set end marker
   LOG(LOG_ERR, "write not implemented yet\n");
   assert(0); // TBD

   POP_USER();
   return 0;
}




// ---------------------------------------------------------------------------
// unimplemented routines, for now
// ---------------------------------------------------------------------------

#if 0

int marfs_bmap(const char* path,
               size_t      blocksize,
               uint64_t*   idx) {
   // don’t support  its is for block mapping
   return 0;
}


// UNDER CONSTRUCTION
//
// We don’t need this as if its not present, fuse will call mknod()+open().
// Don’t implement.
//
// NOTE: If create() is not defined, fuse calls mknod() then open().
//       However, mknod() doesn't know whether the file is being opened
//       with TRUNC or not.  Therefore, it can't formulate the correct call
//       to CHECK_PERMS().  Therefore, it has to check all perms that
//       *might* be checked in open() [so as to avoid the case where mknod
//       succeeds but open() fails].
//
//       However, if create() is defined, fuse calls that to do the create
//       and open in one step.  This allows us to check the appropriate set
//       of meta-perms.

int marfs_create(const char*            path,
                 mode_t                 mode,
                 struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM  RD 
   //   If wronly/rdwr/trunk  RMWMRDWD
   //   If append we don’t support that
   if (info.flags & (O_RDONLY)) {
      ACCESS(info.md_path, W_OK);
      CHECK_PERMS(info.ns->iperms, (R_META | R_DATA));
   }
   else if (info.flags & (O_WRONLY)) {
      ACCESS(info.md_path, W_OK);
      CHECK_PERMS(info.ns->iperms, (R_META | W_META | | R_DATA | W_DATA));
   }

   if (info.flags & (O_APPEND | O_RDWR)) {
      return EPERM;
   }
   if (info.flags & (O_APPEND | O_TRUNC)) { /* can this happen, with create()? */
      CHECK_PERMS(info.ns->iperms, (T_DATA));
   }


   // Check/act on iperms from expanded_path_info_structure, this op
   // requires RMWM
   //
   // NOTE: We assure that open(), if called after us, can't discover that
   //       user lacks sufficient access.  However, because we don't know
   //       what the open call might be, we may be imposing
   //       more-restrictive constraints than necessary.
   //
   //   CHECK_PERMS(info.ns->iperms, (R_META | W_META));
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA | T_DATA));

   // Check/act on quota num names
   // No need for access check, just try the op
   // Appropriate mknod-like/open-create-like call filling in fuse structure
   TRY0(mknod, info.md_path, mode, rdev);

   POP_USER();
   return 0;


   return 0;
}


// obsolete, in fuse 2.6
int marfs_fallocate(const char*            path,
                    int                    mode,
                    off_t                  offset,
                    off_t                  length,
                    struct fuse_file_info* ffi) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWMRDWD
   CHECK_PERMS(info.ns->iperms, (R_META | W_META | R_DATA | W_DATA));

   // Check space quota
   //    If  we get here just return ok  this is just a check to see if you can write to the fs
   POP_USER();
   return 0;
}


int marfs_fgetattr(const char*            path,
                   struct stat*           st,
                   struct fuse_file_info* ffi) {
   PUSH_USER();

   // don’t need path info    (this is for a file that is open, so everything is resolved)
   // don’t need to check on IPERMS
   // No need for access check, just try the op
   // appropriate fgetattr/fstat call filling in fuse structure (dont mess with xattrs)

   POP_USER();
   return 0;
}

// new in 2.6, yet deprecated
// combines opendir(), readdir(), closedir() into one call.
int  marfs_getdir(const char *path, fuse_dirh_t , fuse_dirfil_t) {
   assert(0);
}


int marfs_flock(const char*            path,
                struct fuse_file_info* ffi,
                int                    op) {
   // don’t implement or throw error
   return 0;
}

int marfs_link (const char* path,
                const char* to) {
   // for now, I think we should not allow link – its pretty complicated to do
   return 0;
}


int marfs_lock(const char*            path,
               struct fuse_file_info* ffi,
               int                    cmd,
               struct flock*          locks) {
   // don’t support it, either don’t implement or throw error
   return 0;
}

int marfs_poll(const char*             path,
               struct fuse_file_info*  ffi,
               struct fuse_pollhandle* ph,
               unsigned*               reventsp) {
   // either don’t implement or just return 0;
   return 0;
}


#endif











// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------


int main(int argc, char* argv[])
{
   INIT_LOG();
   LOG(LOG_INFO, "starting\n");

   load_config("~/marfs.config");
   init_xattr_specs();

   struct fuse_operations marfs_oper = {
      .init        = marfs_init,
      .destroy     = marfs_destroy,

      .access      = marfs_access,
      .chmod       = marfs_chmod,
      .chown       = marfs_chown,
      // .fallocate   = marfs_fallocate  /* not in 2.6 */
      .flush       = marfs_flush,
      .fsync       = marfs_fsync,
      .fsyncdir    = marfs_fsyncdir,
      .ftruncate   = marfs_ftruncate,
      .getattr     = marfs_getattr,
      .getxattr    = marfs_getxattr,
      .ioctl       = marfs_ioctl,
      .listxattr   = marfs_listxattr,
      .mkdir       = marfs_mkdir,
      .mknod       = marfs_mknod,
      .open        = marfs_open,
      .opendir     = marfs_opendir,
      .read        = marfs_read,
      .readdir     = marfs_readdir,
      .readlink    = marfs_readlink,
      .release     = marfs_release,
      .releasedir  = marfs_releasedir,
      .removexattr = marfs_removexattr,
      .rename      = marfs_rename,
      .rmdir       = marfs_rmdir,
      .setxattr    = marfs_setxattr,
      .statfs      = marfs_statfs,
      .symlink     = marfs_symlink,
      .truncate    = marfs_truncate,
      .unlink      = marfs_unlink,
      .utime       = marfs_utime, /* deprecated in 2.6 */
      .utimens     = marfs_utimens,
      .write       = marfs_write,

#if 0
      .bmap        = marfs_bmap,
      .create      = marfs_create,
      .fgetattr    = marfs_fgetattr,
      .flock       = marfs_flock,
      .getdir      = marfs_getdir, /* deprecated in 2.6 */
      .link        = marfs_link,
      .lock        = marfs_lock,
      .poll        = marfs_poll,
#endif
   };

   return fuse_main(argc, argv, &marfs_oper, NULL);
}
