// ---------------------------------------------------------------------------
// Show how to read and write MarFS system xattr value on a file.
//
// example of how you could use this:
//
//   $ make demo_read_write_xattrs2
//
//   $ rm -f foo.x
//   $ ./demo_read_write_xattrs2 foo.x  [1st time: create file, install xattrs]
//   $ ./demo_read_write_xattrs2 foo.x  [2nd time: read xattrs, modify xattrs, save]
//
//   $ attr -l foo.x                    [show xattrs]
//   $ attr -g marfs_objid foo.x        [show contents of specific xattr]
//   $ attr -g marfs_post foo.x         [show contents of specific xattr]
//
// ---------------------------------------------------------------------------

#include <marfs_base.h>
#include <common.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/xattr.h>
#include <assert.h>

void show_pre(MarFS_XattrPre* pre) {
   printf("Pre:\n");
   printf("  config_vers:  %f\n",  pre->config_vers);
   printf("  md_ctime:     %ld\n", pre->md_ctime);
   printf("  obj_ctime:    %ld\n", pre->obj_ctime);

   printf("  type:         %c\n",  encode_obj_type(pre->obj_type));
   printf("  compression:  %c\n",  encode_compression(pre->compression));
   printf("  correction:   %c\n",  encode_correction(pre->correction));
   printf("  encryption:   %c\n",  encode_encryption(pre->encryption));

   printf("  MarFS_Repo:   %s\n",  (pre->repo ? pre->repo->name : "NULL"));
   printf("  chunk_size:   %ld\n", pre->chunk_size);
   printf("  chunk_no:     %ld\n", pre->chunk_no);

   printf("  md_inode:     %ld\n", pre->md_inode);
   //   uint16_t           shard;    // TBD: for hashing directories across shard nodes

   printf("  bucket:       %s\n",  pre->bucket);
   printf("  objid:        %s\n",  pre->objid);
}

int
main(int argc, char* argv[]) {

   if (argc != 2) {
      fprintf(stderr, "usage: %s <path>\n", argv[0]);
      exit(1);
   }
   const char* path = argv[1];


   // we will need a stat struct to initialize the Pre xattr
   struct stat st;
   while (lstat(path, &st)) {

      // NOTE: This is just a demo.  User specified a file on the
      //       command-line.  If file doesn't exist, create it now.
      if (errno == ENOENT) {

         fprintf(stderr, "%s doesn't exist, creating ... ", path);
         int fd = open(path, (O_CREAT | O_TRUNC));
         if (fd < 0) {
            fprintf(stderr, "ERROR %s\n", strerror(errno));
            exit(1);
         }
         fprintf(stderr, "done.\n");
         
         close(fd);
      }
      else {
         fprintf(stderr, "Error lstat(%s, ...) '%s'\n",
                 path, strerror(errno));
         exit(1);
      }
   }

   


#if 0
   // This approach uses the tools that support fuse.  It's more trouble
   // this way, because fuse always has certain support structures around
   // which we have to build-up just to use the fuse tools.  The nice thing
   // about this way is that we use expand_path_info(), which works from
   // the configuration-file to deal with namespaces, without us having to
   // know where the metadata-files are actually located.  Also we can use
   // stat_xattrs() and save_xattrs() to read/write the system xattrs, and
   // we can use has_any_xattrs() to test which ones were found.


   // initialize a list of known MarFS xattrs, used by stat_xattrs(), etc.
   init_xattr_specs();

   // The path is ignored, for now.  This just installs a hard-wired
   // config.  The main thing this does for is instantiate namespaces, etc.
   load_config("~/marfs.config");

   // This thing is used within fuse routines.  It contains parsed-out
   // versions of all the system xattrs, which will be filled by
   // stat_xattrs().
   PathInfo info;

   // stat_xattrs() will need there to be a Namespace in the PathInfo,
   // because if there is no Pre xattr on the path, stat_xattr() will
   // initialize the one inside PathInfo, and it will need Namespace for
   // some of the Pre members.
   info.ns = find_namespace("/test00");

   // If we were in fuse, expand_path_info() would translate the "path" seen
   // by fuse, and install the corresponding MDFS path into PathInfo.md_path.
   // This is where stat_xattrs() will look for system xattrs.
   //
   // expand_path_info(info, "/test00/foo"); // use a real path in the MDFS
   info.flags |= PI_EXPANDED; // pretend we called expand_path_info() for has_any_xattrs()

   // instead of expand_path_info(), we'll just stuff argv[1] into the PathInfo
   strncpy(info.md_path, path, MARFS_MAX_MD_PATH); // use argv[1]
   info.md_path[MARFS_MAX_MD_PATH] = 0;

   // stat_xattrs() gets MarFS-related xattr info from the file.  We parse
   // xattr values into the corresponding structs in the PathInfo object.
   // NOTE: In case the POST xattr value-string includes an md_path, we
   //       explicitly avoid overwriting the one we already installed.
   int rc = stat_xattrs(&info, 0);
   if (rc) {
      fprintf(stderr, "stat_xattrs failed (%d) %s\n", rc, strerror(rc));
      exit(1);
   }

   // flags in PathInfo.xattrs indicate which xattrs were filled.  You can
   // test them by looking at the individual bits
   if (info.xattrs & XVT_PRE) {
      printf("had xattr Pre\n");
      show_pre(&info.pre);
   }
   // or you can use has_any_xattrs()
   else if (has_any_xattrs(&info, MARFS_MD_XATTRS)) {
      printf("had xattr Pre and/or xattr Post (but not Pre, so probably Post\n");
      // I don't have a show_post(), yet ...
   }
   // or you can use has_all_xattrs()
   else if (has_all_xattrs(&info, MARFS_MD_XATTRS)) {
      printf("had both xattrs Pre AND Post\n");
      show_pre(&info.pre);
   }
   else {
      printf("No MD-related MarFS xattrs were found\n");
   }


   // Update some values in the Pre xattr
   time_t now = time(NULL);
   if (now == (time_t)-1) {
      fprintf(stderr, "time(NULL) failed (%d) %s\n", rc, strerror(rc));
      exit(1);
   }

   // PathInfo doesn't keep track of which xattrs structs you update
   info.pre.md_ctime = now;

   // pick which xattrs to install onto the file in <path>
   //
   // NOTE: stat_xattrs() will throw an error if a file haas only some of
   //       the MarFS MD-related system xattrs.  Therefore, we write both
   //       Pre and Post, for this demo, so that when you run the demo a
   //       second time (after creating the file from scratch the first
   //       time), you won't get an error in stat_xattrs().
   //
   // save_xattrs(&info, (XVT_PRE));
   save_xattrs(&info, (XVT_PRE | XVT_POST));

   // (generate string-version of time, just for printing)
   printf("\n\nupdated md_ctime to %ld\n", now);
   // // pre_2_str() already did this for us:
   // update_pre(&info.pre); // regenerate bucket/objid strings
   show_pre(&info.pre);


#else
   // This example just uses the simplest low-level approach, instead of
   // stat_xattrs() and save_xattrs().  There is no particular
   // understanding of "MarFS", except that we use the MarFS xattrs.
   //
   // We also don't use MarFS_XattrSpecs, which has a list of all the MarFS
   // system-xattrs.  See stat_xattrs() in common.c, for an idea on how to
   // use that.
   int rc = 0;

   //   // initialize a list of known MarFS xattrs, used by stat_xattrs(), etc.
   //   init_xattr_specs();

   // The path is ignored, for now.  This just installs a hard-wired
   // config.  The main reason we care is because we need a namespace
   // and maybe a repo.
   load_config("~/marfs.config");

   // parse xattr-value into a "Pre" struct.
   // If there is no such xattr, initialize with some example-values
   MarFS_XattrPre pre;
   const char*    key_name = MarFS_XattrPrefix "objid";
   char           objid_str[MARFS_MAX_OBJID_SIZE];

   if (lgetxattr(path, key_name, &objid_str, MARFS_MAX_OBJID_SIZE) == -1) {

      if (errno == ENODATA) {
         printf("path %s doesn't have xattr '%s'\n", path, key_name);

         // These are some pretend values to use for initialisation.
         // In fuse/pftool, we would know obj_type, namespace, etc.
         MarFS_ObjType    obj_type = OBJ_UNI;
         MarFS_Namespace* ns       = find_namespace("/test00");
         MarFS_Repo*      repo     = ns->iwrite_repo;

         // initialize the Pre struct
         init_pre(&pre, obj_type, ns, repo, &st);
      }
      else {
         fprintf(stderr, "Error lgetxattr(%s, %s, ...) '%s'\n",
                 path, key_name, strerror(errno));
         exit(1);
      }
   }
   else {
      printf("objid string is '%s'\n", objid_str);
      if ((rc = str_2_pre(&pre, objid_str, &st))) {
         fprintf(stderr, "Error str_2_pre(..., %s, %s, ...) '%s'\n",
                 path, objid_str, strerror(rc));
         exit(1);
      }
   }

   // show the contents of the parsed/created objid
   show_pre(&pre);

   // Update some values in the Pre xattr
   // NOTE: The bucket and objectid strings won't change until
   //       we call update_pre(), or pre_2_str(), etc.
   time_t now = time(NULL);
   if (now == (time_t)-1) {
      fprintf(stderr, "time(NULL) failed %s\n", strerror(errno));
      exit(1);
   }
   pre.md_ctime = now;

   // translate Pre to xattr-value-string
   if ((rc = pre_2_str(objid_str, sizeof(objid_str), &pre))) {
      fprintf(stderr, "Error pre_2_str(...) '%s'\n",
              strerror(rc));
      exit(1);
   }

   // save as xattr
   if (lsetxattr(path, key_name, objid_str, strlen(objid_str)+1, 0)) {
      fprintf(stderr, "Error lsetxattr(%s, %s, %s, ...) '%s'\n",
              path, key_name, objid_str, strerror(errno));
      exit(1);
   }
   
   // (generate string-version of time, just for printing)
   printf("\n\nupdated md_ctime to %ld\n", now);
   // // pre_2_str() already did this for us:
   // update_pre(&pre); // regenerate bucket/objid strings
   show_pre(&pre);


#endif

   printf("done.\n");
   return 0;
}

