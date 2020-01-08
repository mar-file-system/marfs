#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <common.h>
#include <marfs_ops.h>


void show_usage(char* prog_name)
{
   fprintf(stderr, "Usage: %s <path>\n", prog_name);
}


void dump_statvfs(struct statvfs* st) {
   printf("--- results of marfs_statvfs():\n");
   printf("f_bsize    = %16lu  /* file system block size */\n",               st->f_bsize);
   printf("f_frsize   = %16lu  /* fragment size */\n",                        st->f_frsize);
   printf("f_blocks   = %16lu  /* size of fs in f_frsize units */\n",         st->f_blocks);
   printf("f_bfree    = %16lu  /* # free blocks */\n",                        st->f_bfree);
   printf("f_bavail   = %16lu  /* # free blocks for unprivileged users */\n", st->f_bavail);
   printf("f_files    = %16lu  /* # inodes */\n",                             st->f_files);
   printf("f_ffree    = %16lu  /* # free inodes */\n",                        st->f_ffree);
   printf("f_favail   = %16lu  /* # free inodes for unprivileged users */\n", st->f_favail);
   printf("f_fsid     = %16lu  /* file system ID */\n",                       st->f_fsid);
   printf("f_flag     = %16lu  /* mount flags */\n",                          st->f_flag);
   printf("f_namemax  = %16lu  /* maximum filename length */\n",              st->f_namemax);
}

int main(int argc, char* argv[])
{
   int c;

   if (argc != 2) {
      show_usage(argv[0]);
      exit(1);
   }
   char* path = argv[1];

   char path_template[MC_MAX_PATH_LEN];
   struct statvfs st;

   //first need to read config
   if (read_configuration()) {
      printf("ERROR: Reading Marfs configuration failed\n");
      return -1;
   }
   if (init_xattr_specs()) {
      printf("ERROR: init_xattr_specs failed\n");
      return -1;
   }

   if (marfs_statvfs(marfs_sub_path(path), &st)) {
      printf("statvfs(%s) failed (%d): %s\n", path, errno, strerror(errno));
      exit (1);
   }

   // show the contents of the statvfs struct
   dump_statvfs(&st);

   return 0;
}
