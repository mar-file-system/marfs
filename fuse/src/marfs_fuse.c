

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

#include "marfs_fuse.h"
#include "common.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <stdio.h>


void* marfs_init(struct fuse_conn_info* conn) {

}

// called when fuse file system exits
void marfs_destroy (void* private_data) {
   // nothing for us to do here, we wont have dirty data when the fuse
   // daemon exits. I suppose they wait for all threads to finish before
   // leaving, so this should be ok.
}




int marfs_getattr (const char*  path,
                   struct stat* stbuf) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, R_META);

   // No need for access check, just try the op
   // appropriate statlike call filling in fuse structure (dont mess with xattrs here etc.)
   TRY0(lstat, path, stbuf);

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
   TRY0(readlink, path, buf, size);

   POP_USER();
   return 0;
}

int marfs_mknod (const char* path,
                 mode_t      mode,
                 dev_t       rdev) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // Check/act on quota num names
   // No need for access check, just try the op
   // Appropriate mknodlike/opencreatelike call filling in fuse structure
   TRY0(mknod, path, mode, rdev);

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
   TRY0(mkdir, path, mode);

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
   TRASH_FILE(info, path);

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
   TRY0(rmdir, pathname);

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
   TRY0(symlink, path, to);

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
   TRY0(rename, path, to);

   POP_USER();
   return 0;
}

int marfs_link (const char* path,
                const char* to) {
   // for now, I think we should not allow link – its pretty complicated to do
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
   TRY0(lchmod, path, mode);

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
   TRY0(lchown, path, uid, gid);

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
   STAT_XATTR(info, path); // to get xattrs

   // Call access syscall to check/act if allowed to truncate for this user 
   ACCESS(path, (W_OK));

   // copy metadata to trash, resets original file zero len and no xattr
   TRASH_DUP_FILE();

   POP_USER();
   return 0;
}




int marfs_open (const char*            path,
                struct fuse_file_info* info) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure
   //   If readonly RM  RD 
   //   If wronly/rdwr/trunk  RMWMRDWD
   //   If append we don’t support that

   // *** TBD: didn't I read somewhere that FUSE doesn't call open with
   // e.g. O_TRUNC, but instead calls truncate(), in that case?
   // And similarly with O_CREAT?
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // Call access to see if we can do this operation on a file
   int access_flags = 0;
   if (info.flags & (O_RDONLY | O_RDWR))
      access_flags |= R_OK;
   if (info.flags & (O_WRONLY | O_RDWR))
      access_flags |= W_OK;
   ACCESS(path, (access_flags));

   // If create check/act on quota num names
   STAT_XATTR(info, path); // to get xattrs

   // Poke the xattr stuff into some memory for the file (poke the address of that 
   //    memory into the fuse open structure 
   //    (so you have access to it when the file is open)
   //     also poke how to access the objrepo for where/how to write and how to read
   //     also put space for read to attach a structure for object read mgmt

   //If trunc
   //  Call access syscall to check/act if allowed to truncate for this user
   //
   // NOTE: I read somewhere that fuse will just call our ftruncate(), instead of
   //       passing us a truncate flag.  True?
   if (info.flags & O_TRUNC) {
      assert(0);                /* I want to see whether this actually happens */

      // copy metadata to trash, resets original file zero len and no xattr
      TRASH_DUP_FILE();
   }

  // open md file in asked for mode
  POP_USER();
  return 0;
}

int marfs_read (const char*            path,
                char*                  buf,
                size_t                 size,
                off_t                  offset,
                struct fuse_file_info* info) {
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

int marfs_write(const char*            path,
                const char*            buf,
                size_t                 size,
                off_t                  offset,
                struct fuse_file_info* info) {
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

int marfs_flush (const char*            path,
                 struct fuse_file_info* info) {
   // I don’t think we will have dirty data that we can control
   // I guess we could call flush on the filehandle  that is being written
   // But the only data we will write is multi-part objects, 
   // All other data would be to some object interface
   return 0;
}

int marfs_release (const char*            path,
                   struct fuse_file_info* info) {
   // if writing there will be an objid stuffed into a address  in fuse open table
   //       seal that object if needed
   //       free the area holding that objid
   // if  reading, there might be a malloced space for read obj mgmt. in fuse open table
   //       close any objects if needed
   //       free the area holding that stuff
   // close the metadata file handle
   return 0;
}

int marfs_fsync (const char*            path,
                 int                    isdatasync,
                 struct fuse_file_info* info) {
   // I don’t know if we do anything here, I don’t think so, we will be in
   // sync at the end of each thread end
   return 0; // Just return
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
   // No need for access check, just try the op
   // Appropriate  setxattr call filling in fuse structure 
   TRY0(lsetxattr, path, name, value, size, flags);

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
   // No need for access check, just try the op
   // Appropriate  getxattr call filling in fuse structure
   TRY0(lgetxattr, path, name, value, size);

   POP_USER();
   return 0;
}

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
   // *** remove any reserved xattrs from list ***
   // filling in fuse structure


   POP_USER();
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
   // No need for access check, just try the op
   // Appropriate  removexattr call filling in fuse structure 

   POP_USER();
   return 0;
}

int marfs_opendir (const char*            path,
                   struct fuse_file_info* info) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  opendir call filling in fuse structure
   mode_t mode = ~(fuse_get_context()->umask); /* ??? */
   TRY0(open, path, info->flags, mode);
   info->fh = rc;               /* open() successfully returned a dirp */

   POP_USER();
   return 0;
}

int marfs_readdir (const char*            path,
                   void*                  buf,
                   fuse_fill_dir_t        filler,
                   off_t                  offset,
                   struct fuse_file_info* info) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  readdir call filling in fuse structure  (fuse does this in chunks)
   int dirp = info->fh;
   struct dirent* dent;
   /// struct dirent* dent = readdir(dirp);
   TRY0(readdir_r, dirp,  (struct dirent*)buf, &dent);


   POP_USER();
   return 0;
}

int marfs_releasedir (const char*            path,
                      struct fuse_file_info* info) {
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RM
   CHECK_PERMS(info.ns->iperms, (R_META));

   // No need for access check, just try the op
   // Appropriate  closedir call filling in fuse structure  
   POP_USER();
   return 0;
}

int marfs_fsyncdir (const char*            path,
                    int                    isdatasync,
                    struct fuse_file_info* info) {
   // don’t think there is anything to do here, we wont have dirty data
   // unless its trash
   return 0; // just return
}


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
 
   POP_USER();
   return 0;
}

int marfs_create(const char*            path,
                 mode_t                 mode,
                 struct fuse_file_info* info) {
   // we don’t need this as if its not present, fuse will call mdnod and open
   // don’t implement
   return 0;
}

int marfs_ftruncate(const char*            path,
                    off_t                  length,
                    struct fuse_file_info* info) {

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
   STAT_XATTR(info);
   if (! has_resv_xattrs(info))
      // TBD.  (data stored directly in file)
      ;

   //***** this may or may not work – may need a trash_dup_file() that uses
   //***** ftruncate since the file is already open (may need to modify the
   //***** trash_dup_file to use trunc or ftrunc depending on if file is
   //***** open or not

   // [jti: I think I read that FUSE will never call open() with O_TRUNC,
   // but will instead call truncate first, then open.  However, a user
   // might still call or truncate() or ftruncate() explicitly.  For these
   // cases, I guess we assume the file is already open, and the filehandle
   // is good.]

   // copy metadata to trash, resets original file zero len and no xattr
   TRASH_DUP_FILE();

   POP_USER();
   return 0;
}

int marfs_fgetattr(const char*            path,
                   struct stat*           st,
                   struct fuse_file_info* info) {
   PUSH_USER();

   // don’t need path info    (this is for a file that is open, so everything is resolved)
   // don’t need to check on IPERMS
   // No need for access check, just try the op
   // appropriate fgetattr/fstat call filling in fuse structure (dont mess with xattrs)
   

   POP_USER();
   return 0;
}

int marfs_lock(const char*            path,
               struct fuse_file_info* info,
               int                    cmd,
               struct flock*          locks) {
   // don’t support it, either don’t implement or throw error
   return 0;
}

int marfs_utimens(const char*           path,
                  const struct timespec tv[2]) {   
   PUSH_USER();

   PathInfo info;
   EXPAND_PATH_INFO(&info, path);

   // Check/act on iperms from expanded_path_info_structure, this op requires RMWM
   CHECK_PERMS(info.ns->iperms, (R_META | W_META));

   // No need for access check, just try the op
   // Appropriate  utimens call filling in fuse structure 
   POP_USER();
   return 0;
}

int marfs_bmap(const char* path,
               size_t      blocksize,
               uint64_t*   idx) {
   // don’t support  its is for block mapping
   return 0;
}

int marfs_ioctl(const char*            path,
                int                    cmd,
                void*                  arg,
                struct fuse_file_info* info,
                unsigned int           flags,
                void*                  data) {
   // if we need an ioctl for something or other
   // ***Maybe a way to get fuse deamon to read up new config file
   // *** we need a way for daemon to read up new config file without stopping
   return 0;
}

int marfs_poll(const char*             path,
               struct fuse_file_info*  info,
               struct fuse_pollhandle* ph,
               unsigned*               reventsp) {
   // either don’t implement or just return 0;
   return 0;
}

int marfs_flock(const char*            path,
                struct fuse_file_info* info,
                int                    op) {
   // don’t implement or throw error
   return 0;
}

int marfs_fallocate(const char*            path,
                    int                    mode,
                    off_t                  offset,
                    off_t                  length,
                    struct fuse_file_info* info) {
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






int main(int argc, char* argv[])
{

   struct fuse_operations marfs_oper = {
      .getattr     = marfs_getattr,
      .readlink    = marfs_readlink,
      .mknod       = marfs_mknod,
      .mkdir       = marfs_mkdir,
      .unlink      = marfs_unlink,
      .rmdir       = marfs_rmdir,
      .symlink     = marfs_symlink,
      .rename      = marfs_rename,
      .link        = marfs_link,
      .chmod       = marfs_chmod,
      .chown       = marfs_chown,
      .truncate    = marfs_truncate,
      .open        = marfs_open,
      .read        = marfs_read,
      .write       = marfs_write,
      .statfs      = marfs_statfs,
      .flush       = marfs_flush,
      .release     = marfs_release,
      .fsync       = marfs_fsync,
      .setxattr    = marfs_setxattr,
      .getxattr    = marfs_getxattr,
      .listxattr   = marfs_listxattr,
      .removexattr = marfs_removexattr,
      .opendir     = marfs_opendir,
      .readdir     = marfs_readdir,
      .releasedir  = marfs_releasedir,
      .fsyncdir    = marfs_fsyncdir,
      .init        = marfs_init,
      .destroy     = marfs_destroy,
      .access      = marfs_access,
      .create      = marfs_create,
      .ftruncate   = marfs_ftruncate,
      .fgetattr    = marfs_fgetattr,
      .lock        = marfs_lock,
      .utimens     = marfs_utimens,
      .bmap        = marfs_bmap,
      .ioctl       = marfs_ioctl,
      .poll        = marfs_poll,
      .flock       = marfs_flock,
      .fallocate   = marfs_fallocate
   };

   return fuse_main(argc, argv, &marfs_oper, NULL);
}
