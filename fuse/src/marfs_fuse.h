#ifndef _MARFS_FUSE_H
#define _MARFS_FUSE_H


#include "common.h"



#  ifdef __cplusplus
extern "C" {
#  endif

// "The return value will passed in the private_data field of
//  fuse_context to all file operations and as a parameter to the
//  destroy() method."
void* marfs_init(struct fuse_conn_info* conn);

void  marfs_destroy(void* private_data);



int  marfs_access(const char* path, int mask);

int  marfs_chmod(const char* path, mode_t mode);

int  marfs_chown(const char* path, uid_t uid, gid_t gid);

int  marfs_create(const char* path, mode_t mode, struct fuse_file_info* ffi);

int  marfs_flush(const char* path, struct fuse_file_info* fi);

int  marfs_fsync(const char* path, int isdatasync, struct fuse_file_info* fi);

int  marfs_fsyncdir(const char* path, int isdatasync, struct fuse_file_info* fi);

int  marfs_ftruncate(const char* path, off_t size, struct fuse_file_info* fi);
	
int  marfs_getattr(const char* path, struct stat* stbuf);

int  marfs_getxattr(const char* path, const char* name, char* value, size_t size);

int  marfs_ioctl(const char* path, int cmt, void* arg,
                 struct fuse_file_info* fi, unsigned int flags, void* data);

int  marfs_listxattr(const char* path, char* list, size_t size);

int  marfs_mkdir(const char* path, mode_t mode);

int  marfs_mknod(const char* path, mode_t mode, dev_t rdev);

int  marfs_open(const char* path, struct fuse_file_info* fi);

int  marfs_opendir(const char* path, struct fuse_file_info* fi);

int  marfs_read(const char* path, char* buf, size_t size, off_t offset,
                      struct fuse_file_info* fi);

int  marfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                         off_t offseet, struct fuse_file_info* fi);

int  marfs_readlink(const char* path, char* buf, size_t size);

int  marfs_release(const char* path, struct fuse_file_info* fi);

int  marfs_releasedir(const char* path, struct fuse_file_info* fi);

int  marfs_removexattr(const char* path, const char* name);

int  marfs_rename(const char* from, const char* to);

int  marfs_rmdir(const char* path);

int  marfs_setxattr(const char* path, const char* name,
                          const char* value, size_t size, int flags);

int  marfs_statfs(const char* path, struct statvfs* stbuf);

int  marfs_symlink(const char* to, const char* from);

int  marfs_truncate(const char* path, off_t size);

int  marfs_unlink(const char* path);

// deprecated in 2.6
int  marfs_utime(const char* path, struct utimbuf* tb);

int  marfs_utimens(const char* path, const struct timespec ts[2]);

int  marfs_write(const char* path, const char* buf, size_t size, off_t offset,
                 struct fuse_file_info* fi);



// currently unimplemented
#if 0

int  marfs_bmap(const char* path, size_t blocksize, uint64_t* blockno);

// not in 2.6
int  marfs_fallocate(const char* path, int mode, off_t offset, off_t length, struct fuse_file_info* fi);

int  marfs_fgetattr(const char* path, struct stat* stbuf);

int  marfs_flock(const char* path, struct fuse_file_info* fi, int op);

// deprecated in 2.6
int  marfs_getdir(const char *path, fuse_dirh_t , fuse_dirfil_t);

int  marfs_link(const char* from, const char* to);

int  marfs_lock(const char* path, struct fuse_file_info* fi, int cmd,
                      struct flock* locks);

int  marfs_poll(const char* path, struct fuse_file_info* fi, struct fuse_pollhandle* ph, unsigned* reventsp);

#endif



#  ifdef __cplusplus
}
#  endif



#endif // _MARFS_FUSE_H
