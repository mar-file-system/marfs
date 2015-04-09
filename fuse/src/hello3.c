/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello
*/


// hello2: show UID / effective-UID, or details of fuse_file_info, etc
#include <unistd.h>
#include <sys/types.h>

// hello3: begin linking with the common.h tools, to test them
#include "common.h"
#include <syslog.h>
#include <stdarg.h>


#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#if 0
static const char *hello_str = "Hello World!\n";
#endif

static const char *hello_path = "/hello";


// void log(const char* fmt, ...) {
//    static const size_t BUF_SIZE=512;
//    static buf[BUF_SIZE];
// 
//    va_list  list;
//    va_start(list, fmt);
//    vsnprintf(buf, BUF_SIZE, fmt, list);
//    va_end(list);
// 
//    syslog(LOG_INFO, buf);
// }


static int hello_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	}
   else if (strncmp(path, hello_path, strlen(hello_path)) == 0) { /* changed to strNcmp */
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
#if 0
		stbuf->st_size = strlen(hello_str);
#else
		stbuf->st_size = 256;
#endif

      syslog(LOG_INFO, "getattr(%s)\n", path);
	}
   else
		res = -ENOENT;

	return res;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset,
                         struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	filler(buf, hello_path + 1, NULL, 0);

	return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi)
{
   if (strncmp(path, hello_path, strlen(hello_path)) != 0) /* changed to strNcmp */
		return -ENOENT;

	if ((fi->flags & 3) != O_RDONLY)
		return -EACCES;

	return 0;
}

// observed:
//   <size>   = 4096
//   <offset> = 0
static int hello_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
	(void) fi;
   if (strncmp(path, hello_path, strlen(hello_path)) != 0) /* strNcmp */
		return -ENOENT;

#if 0
	size_t len = strlen(hello_str);
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, hello_str + offset, size);
	} else
		size = 0;

	return size;
#elif 0
   // show some fuse_file_info details
   int count = snprintf(buf, size,
                        "Hello World, fi=0x%lx, fi->fh=%lx, ctx->private_data=0x%lx\n",
                        (ssize_t)fi,
                        (ssize_t)fi->fh,
                        (ssize_t)fuse_get_context()->private_data);
   if (count < 0)
      return -EIO;
   return count;
#elif 0
   // show user-ID and effective-UID
   int count = snprintf(buf, size,
                        "Hello World, from UID=%lu, eUID=%lu\n",
                        (long unsigned)getuid(), (long unsigned)geteuid);
   if (count < 0)
      return -EIO;
   return count;
#else
   // show arguments to read()
   int count = snprintf(buf, size -1,
                        "path=%s, size=%lu, off:%lu\n", path, size, offset);
   if (count < 0)
      return -EIO;
   return count;
#endif
}



static struct fuse_operations hello_oper = {
	.getattr	= hello_getattr,
	.readdir	= hello_readdir,
	.open		= hello_open,
	.read		= hello_read,
};

int main(int argc, char *argv[])
{
	return fuse_main(argc, argv, &hello_oper, NULL);
}

