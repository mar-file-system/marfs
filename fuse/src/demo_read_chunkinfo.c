// ---------------------------------------------------------------------------
// Show how to read through chunkinfo in the metadata of a MarFS N:1 file.
//
// example of how you could use this:
//
//   $ make test_read_chunkinfo
//
//   $ [run pftool to create an Nto1 file]
//
//   $ ./test_read_chunkinfo filename
//
//
// The chunkinfo for an N:1 file is written into the MD file by fuse or
// pftool.  If the file is incomplete (i.e. has a "marfs_restart" xattr),
// and is being written from pftool, there will likely be some chunkinfo
// spots that do not have data.  That's because pftool tasks may be
// generating the objects in a non-sequential way, and the MD file may be
// truncated to be larger than the MD data that has been written there, so
// far.
//
// Such "missing" chunks would not represent objects that GC needs to
// clean-up.  GC can identify such chunks because they will be all zeros.
// A simple way to test this is to check that chunk_info->config_vers_maj
// is zero.  If so, then the chunk can be skipped by GC.  Otherwise, GC
// would construct an object-ID from the "objid" xattr, and replace the
// chunk-no with chunk_info->chunk_no, and delete that object.
//
// 
// ---------------------------------------------------------------------------

#include "marfs_base.h"
#include "common.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#include <sys/types.h>
#include <attr/xattr.h>
#include <assert.h>

void show_chunkinfo(MultiChunkInfo* chunk_info) {
   printf("Chunk:\n");
   printf("  config_vers:      %d.%d\n", chunk_info->config_vers_maj, chunk_info->config_vers_min);
   printf("  chunk_no:         %ld\n", chunk_info->chunk_no);
   printf("  logical_offset:   %ld\n", chunk_info->logical_offset);
   printf("  chunk_data_bytes: %ld\n", chunk_info->chunk_data_bytes);
   printf("  correction:       %ld\n", chunk_info->correct_info);
   printf("  encryption:       %ld\n", chunk_info->encrypt_info);
}

int
main(int argc, char* argv[]) {

   if (argc != 2) {
      fprintf(stderr, "usage: %s <marfs_md_file>\n", argv[0]);
      exit(1);
   }
   const char* md_path = argv[1];

   // initialize a list of known MarFS xattrs, used by stat_xattrs(), etc.
   init_xattr_specs();

   // stat_xattrs() -> str_2_pre() will need find_repo_by_name()
   read_configuration();

   // read_chunkinfo() expects a MarFS filehandle.  It only uses the
   // MD-path member, but it does handle MDAL, if that option is
   // compiled-in.  So, this approach should continue to work, after we
   // start using MDAL.
   MarFS_FileHandle fh;
   memset(&fh, 0, sizeof(MarFS_FileHandle));

   PathInfo* info = &fh.info;
   strncpy(info->post.md_path, md_path, MARFS_MAX_MD_PATH); // use argv[1]
   info->post.md_path[MARFS_MAX_MD_PATH -1] = 0;
   if (stat_xattrs(info)) {    // parse all xattrs for the MD file
      fprintf(stderr, "stat_xattrs() failed for MD file: '%s'\n", md_path);
      return -1;
   }

   // assure the MD is open for reading
   fh.flags |= FH_READING;
   if (open_md(&fh)) {
      fprintf(stderr, "open_md() failed for MD file: '%s'\n", md_path);
      return -1;
   }


   // read chunkinfo
   MultiChunkInfo chunk_info;
   while (! read_chunkinfo(&fh, &chunk_info)) {
      show_chunkinfo(&chunk_info);
   }


   printf("done.\n");
   return 0;
}

