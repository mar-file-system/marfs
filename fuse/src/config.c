#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


// When parsing a config file, translate strings representing
// correction-methods, into the corresponding enum.
CorrectionMethod lookup_correction(const char* token) {
   if      (! strcmp(token, "none"))     return CORRECT_NONE;
   else if (! strcmp(token, "crc"))      return CORRECT_CRC;
   else if (! strcmp(token, "checksum")) return CORRECT_CHECKSUM;
   else if (! strcmp(token, "hash"))     return CORRECT_HASH;
   else if (! strcmp(token, "raid"))     return CORRECT_RAID;
   else if (! strcmp(token, "erasure"))  return CORRECT_ERASURE;

   fprintf(stderr, "Unrecognized correction_method: %s\n", token);
   exit(1);
}



// When parsing a config file, translate strings representing
// compression-methods, into the corresponding enum.
CompressionMethod lookup_compression(const char* token) {
   if      (! strcmp(token, "none"))       return COMPRESS_NONE;
   else if (! strcmp(token, "erasure"))    return COMPRESS_ERASURE;
   else if (! strcmp(token, "run_length")) return COMPRESS_RUN_LENGTH;

   fprintf(stderr, "Unrecognized compression_method: %s\n", token);
   exit(1);
}




// Give us a pointer to your list-pointer.  Your list-pointer should start
// out having a value of NULL.  We maintain the list of repos ASCCENDING by
// min file-size handled.  Return false in case of conflicts.  Conflicts
// include overlapping ranges, or gaps in ranges.  Call with <max>==-1, to
// make range from <min> to infinity.
int insert_in_range(RangeList**  list,
                    size_t       min,
                    size_t       max,
                    MarFS_Repo*  repo) {

   RangeList** insert = list;   // ptr to place to store ptr to new element

   // leave <ptr> pointing to the inserted element
   RangeList*  this;
   for (this=*list; this; this=this->next) {

      if (min < this->min) {    // insert before <this>

         if (max == -1) {
            fprintf(stderr, "range [%ld, -1] includes range [%ld, %ld]\n",
                    min, this->min, this->max);
            return -1;
         }
         if (max < this->min) {
            fprintf(stderr, "gap between range [%ld, %ld] and [%ld, %ld]\n",
                    min, max, this->min, this->max);
            return -1;
         }
         if (max > this->min) {
            fprintf(stderr, "overlap in range [%ld, %ld] and [%ld, %ld]\n",
                    min, max, this->min, this->max);
            return -1;
         }

         // do the insert
         break;
      }
      insert = &this->next;
   }


   RangeList* elt = (RangeList*)malloc(sizeof(RangeList));
   elt->min  = min;
   elt->max  = max;
   elt->repo = repo;
   elt->next = *insert;
   *insert = elt;
   return 0;                    /* success */
}

// given a file-size, find the corresponding element in a RangeList, and
// return the corresponding repo.  insert_range() maintains repos in
// descending order of the block-sizes they handle, to make this as quick
// as possible.
MarFS_Repo* find_in_range(RangeList* list,
                          size_t     block_size) {
   while (list) {
      if (block_size >= list->min)
         return list->repo;
      list = list->next;
   }
   return NULL;
}




// ---------------------------------------------------------------------------
// load/organize/search through namespace and repo config info
//
// We constuct the "join" of Namespaces and Repos that are specified in the
// configuration files, as though repo-name were the PK/FK in two tables in
// a DBMS.  This is done at config-load time, and doesn't have to be
// particularly quick.
//
// FOR NOW: Just hard-code some values into a structure, matching our test
// set-up.
//
//
// We will eventually also generate code to allow FUSE and pftool to
// quickly move from a path to the appropriate namespace.  This *does* have
// to be fast.  I think a good approach would be to find the distinguishing
// points in all the paths that map to namespaces, and generate a function
// that automatically parses incoming paths at those points (no
// string-searching), and then uses a B-tree/suffix-tree to find the
// matching namespace.  [See open-source b-trees at "Attractive Chaos".  Go
// to their git-hub site.  WOW, they even have a suffix-tree in ksa.h]
//
// FOR NOW:  Just do a simple lookup in the hard-coded vector of namespaces.
// ---------------------------------------------------------------------------


// top level mount point for fuse/pftool
char* MarFS_mnt_top = NULL;

// quick-n-dirty
static MarFS_Namespace* _ns = NULL;

// quick-n-dirty
static MarFS_Repo*      _repo = NULL;



// Read specs for namespaces and repos.  Save them such that we can look up
// repos by name.  When reading namespace-specs, replace repo-names with
// pointers to the named repos (which must already have been parsed,
// obviously).
//
// NOTE: For now, we just hard-code some values into structures.
//
// TBD: Go through all the paths and identify the indices of substrings in
//       user paths that distinguish different namespaces.  Then, insert
//       all the paths into that suffix tree, mapping to namespaces.  That
//       will allow expand_path_info() to do very fast lookups.
int load_config(const char* config_fname) {

   // hard-coded repository
   _repo  = (MarFS_Repo*) malloc(sizeof(MarFS_Repo));
   *_repo = (MarFS_Repo) {
      .name         = "EMC_S3",
      .path         = "http://10.140.0.15:9020/jti",
      .flags        = MARFS_ONLINE,
      .proto        = PROTO_S3_EMC,
      .chunksize    = (1024 * 1024 * 512),
      .security     = SECURE_NONE,
      .latency_ms   = (10 * 1000),
   };


   // hard-coded range-list
   RangeList* ranges = (RangeList*) malloc(sizeof(RangeList));
   *ranges = (RangeList) {
      .min  =  0,
      .max  = -1,
      .repo = _repo
   };

   // hard-coded namespace
   _ns  = (MarFS_Namespace*) malloc(sizeof(MarFS_Namespace));
   *_ns = (MarFS_Namespace) {
      .mnt_suffix    = "/users/jti/projects/mar_fs/fuse/test/filesys/mnt",
      .md_path       = "/users/jti/projects/mar_fs/fuse/test/filesys/meta",
      .trash_path    = "/users/jti/projects/mar_fs/fuse/test/filesys/trash",
      .fsinfo_path   = "/users/jti/projects/mar_fs/fuse/test/filesys/stat",

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .compression = COMPRESS_ERASURE,
      .correction  = CORRECT_ERASURE,
      .correction_info = 0,

      .iwrite_repo = _repo,   /* interactive */
      .range_list  = ranges, /* batch */

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_units = (1024 * 1024), /* MB */
      .quota_space = 1024,          /* 1024 MB of data */
      .quota_names = 1,             /* 1M names */
   };

   // helper for find_namespace()
   _ns->mnt_suffix_len = strlen(_ns->mnt_suffix);


   // these are used by a parser (e.g. stat_xattr())
   // The string in MarFS_XattrPrefix is appended to all of them
   // TBD: free this in clean-up
   MarFS_xattr_specs = (XattrSpec*) calloc(16, sizeof(XattrSpec));

   MarFS_xattr_specs[0] = (XattrSpec) { XVT_OBJID,    "obj_id" };
   MarFS_xattr_specs[1] = (XattrSpec) { XVT_OBJTYPE,  "obj_type" };
   MarFS_xattr_specs[2] = (XattrSpec) { XVT_SIZE_T,   "obj_offset" };
   MarFS_xattr_specs[3] = (XattrSpec) { XVT_SIZE_T,   "chnksz" };
   MarFS_xattr_specs[4] = (XattrSpec) { XVT_FLOAT,    "conf_version" };
   MarFS_xattr_specs[5] = (XattrSpec) { XVT_COMPRESS, "compress" };
   MarFS_xattr_specs[6] = (XattrSpec) { XVT_CORRECT,  "correct" };
   MarFS_xattr_specs[7] = (XattrSpec) { XVT_UINT64,   "correct_info" };
   MarFS_xattr_specs[8] = (XattrSpec) { XVT_FLAGS,    "flags" };
   MarFS_xattr_specs[9] = (XattrSpec) { XVT_SECURITY, "sec_info" };

   MarFS_xattr_specs[9] = (XattrSpec) { XVT_NONE,     NULL };


   return 0;                    /* success */
}


// Find the namespace corresponding to this path.  We might pontentially
// have many namespaces (should be cheap to have as many as you want), and
// this lookup is done for every fuse call (and in parallel from pftool).
// Thus, it should eventually be made efficient.
//
// One way to make this fast would be to look through all the namespaces
// and identify the places where a path diverges for different namespaces.
// This becomes a series of hardcoded substring-ops, on the path.  Each one
// identifies the next suffix to us in a suffix tree.  (Attractive Chaos
// has an open source suffix-array impl).  The leaves would be pointers to
// Namespaces.
//
// NOTE: If the fuse mount-point is "/A/B", and you provide a path like
//       "/A/B/C", then the "path" seen by fuse callbacks is "/C".  In
//       otherwords, we should never see MarFS_mnt_top, as part of the
//       incoming path.
//
// For a quick first-cut, there's only one namespace.  Your path is either
// in it or fails.

MarFS_Namespace* find_namespace(const char* path) {
   //   size_t len = strlen(path);
   //   if (strncmp(MarFS_mnt_top, path, len))
   //      return NULL;
   //   return _ns;

   if (! strncmp(_ns->mnt_suffix, path, _ns->mnt_suffix_len))
      return _ns;
}


MarFS_Repo*      find_repo(MarFS_Namespace* ns,
                           size_t           file_size,
                           int              interactive_write) { // bool
   if (interactive_write)
      return ns->iwrite_repo;
   else
      return find_in_range(ns->range_list, file_size);
}


// later, _repo will be a B-tree, or something, associating repo-names with
// repos.
MarFS_Repo*      find_repo_by_name(const char* repo_name) {
   if (strcmp(repo_name, _repo->name)
       return NULL;                /* err */
   return _repo;
}
