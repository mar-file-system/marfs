#include <fuse.h>

// This is the universal "state" object, where the FUSE impl stashes
// info that may be used across multiple callbacks.  For example,
// marfs_open() might save info needed by marfs_write().

typedef struct {
   
} FUSE_Conn_Info;


// "The return value will passed in the private_data field of
//  fuse_context to all file operations and as a parameter to the
//  destroy() method."
void* marfs_init(struct fuse_conn_info* conn);

void  marfs_destroy(void* private_data);



int  marfs_getattr(const char* path, struct stat* stbuf);

int  marfs_readlink(const char* path, char* buf, size_t size);

int  marfs_mknod(const char* path, mode_t mode, dev_t rdev);

int  marfs_mkdir(const char* path, mode_t mode);

int  marfs_unlink(const char* path);

int  marfs_rmdir(const char* path);

int  marfs_symlink(const char* to, const char* from);

int  marfs_rename(const char* from, const char* to);

int  marfs_link(const char* from, const char* to);

int  marfs_chmod(const char* path, mode_t mode);

int  marfs_chown(const char* path, uid_t uid, gid_t gid);

int  marfs_truncate(const char* path, off_t size);

int  marfs_open(const char* path, struct fuse_file_info* fi);

int  marfs_read(const char* path, char* buf, size_t size, off_t offset,
                      struct fuse_file_info* fi);

int  marfs_write(const char* path, char* buf, size_t size, off_t offset,
                       struct fuse_file_info* fi);

int  marfs_statfs(const char* path, struct statvfs* stbuf);

int  marfs_flush(const char* path, struct fuse_file_info* fi);

int  marfs_release(const char* path, struct fuse_file_info* fi);

int  marfs_fsync(const char* path, struct fuse_file_info* fi);

int  marfs_setxattr(const char* path, const char* name,
                          const char* value, size_t size, int flags);

int  marfs_getxattr(const char* path, const char* name, const char* value,
                          size_t size);

int  marfs_listxattr(const char* path, const char* list, size_t size);

int  marfs_removexattr(const char* path, const char* name);

int  marfs_opendir(const char* path, struct fuse_file_info* fi);

int  marfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                         off_t offseet, struct fuse_file_info* fi);

int  marfs_releasedir(const char* path, struct fuse_file_info* fi);

int  marfs_fsyncdir(const char* path, int isdatasync, struct fuse_file_info* fi);

int  marfs_access(const char* path, int mask);

int  marfs_ftruncate(const char* path, off_t size);
	
int  marfs_fgetattr(const char* path, struct stat* stbuf);

int  marfs_lock(const char* path, struct fuse_file_info* fi, int cmd,
                      struct flock* locks);

int  marfs_utimens(const char* path, const struct timespec ts[2]);

int  marfs_bmap(const char* path, size_t blocksize, uint64_t* blockno);

int  marfs_ioctl(const char* path, int cmt, void* arg,
                       struct fuse_file_info* fi, unsigned int flags, void* data);

int  marfs_poll(const char* path, struct fuse_file_info* fi, struct fuse_pollhandle* ph, unsigned* reventsp);

int  marfs_flock(const char* path, struct fuse_file_info* fi, int op);

int  marfs_fallocate(const char* path, int mode, off_t offset, off_t length, struct fuse_file_info* fi);
