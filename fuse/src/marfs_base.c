#include "marfs_base.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>

// ---------------------------------------------------------------------------
// translations from strings used in config-files
// ---------------------------------------------------------------------------

// const char*   obj_type_name(MarFS_ObjType type) {
//    switch (type) {
//    case OBJ_UNI:     return "uni";
//    case OBJ_MULTI:   return "multi";
//    case OBJ_PACKED:  return "packed";
//    case OBJ_STRIPED: return "striped";
//    case OBJ_NONE:    return "none";
// 
//    default:
//       fprintf(stderr, "Unrecognized obj_type: %u\n", type);
//       exit(1);
//    }
// }

MarFS_ObjType lookup_obj_type(const char* token) {
   if      (! strcmp(token, "none"))     return OBJ_NONE;
   else if (! strcmp(token, "uni"))      return OBJ_UNI;
   else if (! strcmp(token, "multi"))    return OBJ_MULTI;
   else if (! strcmp(token, "packed"))   return OBJ_PACKED;
   else if (! strcmp(token, "striped"))  return OBJ_STRIPED;

   fprintf(stderr, "Unrecognized obj_type: %s\n", token);
   exit(1);
}

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

// ---------------------------------------------------------------------------
// translations to/from strings used in obj-ids
// ---------------------------------------------------------------------------

#define DEFINE_ENCODE(THING, TYPE, CHARS)                          \
   static const char*   THING##_index = CHARS;                     \
   static ssize_t       THING##_max   = -1;                        \
                                                                   \
   char encode_##THING(TYPE type) {                                \
      if (THING##_max < 0)                                         \
         THING##_max = strlen(THING##_index);                      \
      return ((type >= THING##_max) ? 0 : THING##_index[type]);    \
   }

// NOTE: This returns 0 for the not-found case
#define DEFINE_DECODE(THING, TYPE)                                      \
   TYPE decode_##THING (char code) {                                    \
      const char* pos = strchr(THING##_index, code);                    \
      return (pos ? (pos - THING##_index) : 0); /* not-found should be error */ \
   }


// encode_obj_type() / decode_obj_type()
DEFINE_ENCODE(obj_type, MarFS_ObjType, "_UMPS");
DEFINE_DECODE(obj_type, MarFS_ObjType);

// encode_compression() / decode_compression()
DEFINE_ENCODE(compression, CompressionMethod, "_R");
DEFINE_DECODE(compression, CompressionMethod);

// encode_correction() / decode_correction()
DEFINE_ENCODE(correction, CorrectionMethod, "_CKHRE");
DEFINE_DECODE(correction, CorrectionMethod);

// encode_encryption() / decode_encryption()
DEFINE_ENCODE(encryption, EncryptionMethod, "_");
DEFINE_DECODE(encryption, EncryptionMethod);




// ---------------------------------------------------------------------------
// xattrs
// ---------------------------------------------------------------------------

// Fill in fields, construct new obj-ID, etc.
int init_pre(MarFS_XattrPre*        pre,
             const char*            md_path,
             const MarFS_Namespace* ns,
             const MarFS_Repo*      repo,
             const struct stat*     st) {

   time_t now = time(NULL);     /* for obj_ctime */
   if (now == (time_t)-1)
      return errno;

   // --- initialize fields in info.pre
   pre->md_ctime     = st->st_ctime;
   pre->obj_ctime    = now;     /* TBD: update with info from HEAD request */
   pre->config_vers  = MarFS_config_vers;

   pre->compression  = ns->compression;
   pre->correction   = ns->correction;
   pre->encryption   = repo->encryption;

   pre->repo         = repo;
   pre->chunk_size   = repo->chunk_size;
   pre->chunk_no     = 0;
   pre->md_inode     = st->st_ino;

   // pre->slave = ...;    // TBD: for hashing directories across slave nodes

   // --- generate bucket-name
   int bkt_size = snprintf(pre->bucket, MARFS_MAX_BUCKET_SIZE,
                           "%s.%s",
                           ns->mnt_suffix, pre->repo->name);
   if (bkt_size < 0)
      return errno;
   if (bkt_size == MARFS_MAX_BUCKET_SIZE) /* overflow */
      return EINVAL;
   
   // --- generate obj-id

   // prepare date-string components
	struct tm md_ctime_tm;
   char md_ctime[MARFS_DATE_STRING_MAX];

   if (! gmtime_r(&pre->md_ctime, &md_ctime_tm))
      return EINVAL;            /* ?? */
   strftime(md_ctime, MARFS_DATE_STRING_MAX, MARFS_DATE_FORMAT, &md_ctime_tm);


	struct tm obj_ctime_tm;
   char obj_ctime[MARFS_DATE_STRING_MAX];

   if (! gmtime(&pre->obj_ctime))
      return EINVAL;            /* ?? */
   strftime(obj_ctime, MARFS_DATE_STRING_MAX, MARFS_DATE_FORMAT, &obj_ctime_tm);

   // config-version major and minor
   int major = (int)floorf(pre->config_vers);
   int minor = (int)floorf((pre->config_vers - major) * 1000.f);

   char compress = encode_compression(pre->compression);
   char correct  = encode_correction(pre->correction);
   char encrypt  = encode_encryption(pre->encryption);

   // put all components together
   int objid_size = snprintf(pre->obj_id, MARFS_MAX_OBJ_ID_SIZE,
                             "%03d_%03d/%c%c%c/%s.%s.%s",
                             major, minor,
                             compress, correct, encrypt,
                             md_ctime, obj_ctime, md_path);
   if (objid_size < 0)
      return errno;
   if (objid_size == MARFS_MAX_OBJ_ID_SIZE) /* overflow */
      return EINVAL;

   return 0;
}

// from MarFS_XattrPre to string
//
// We could just always use the object-ID as the xattr-value.  This
// approach was intended to allow future expansions that might add info to
// Pre without actually adding fields to the object-ID.  These would then
// we added into the xattr-value-string in some way that allowed easy
// extraction by str_2_pre().  (For example, we could add them after the
// obj-ID, separated by commas.
//
int pre_2_str(char* pre_str, size_t size, const MarFS_XattrPre* pre) {
   if (size < MARFS_MAX_OBJ_ID_SIZE)
      return -1;
   ssize_t bytes_printed = snprintf(pre_str, size,
                                    "%s/%s",
                                    pre->bucket,
                                    pre->obj_id);
   if (bytes_printed < 0)
      return errno;
   if (bytes_printed == size)   /* overflow */
      return EINVAL;

   return 0;
}



// parse an xattr-value string into a MarFS_XattrPre
//
// NOTE: For now, the string really is nothing more than the object ID.
//       However, that could possibly change at some point.  (e.g. we might
//       have some fields we want to add tot he XattrPre xattr-value,
//       without requiring they be added to the object-id.)
//
int str_2_pre(MarFS_XattrPre*    pre,
              char*              md_path,
              const char*        pre_str,
              const struct stat* st,
              int                has_obj_id) {

   if (! has_obj_id) {
      strncpy(pre->obj_id, pre_str, MARFS_MAX_OBJ_ID_SIZE);
      pre->obj_id[MARFS_MAX_OBJ_ID_SIZE] = 0;
   }

   int major;
   int minor;

   char compress;
   char correct;
   char encrypt;

   char md_ctime[MARFS_DATE_STRING_MAX];
   char obj_ctime[MARFS_DATE_STRING_MAX];

   // --- extract bucket, and some top-level fields
   int objid_size = sscanf(pre->obj_id, "%s/%03d_%03d/%c%c%c/%s/%s.%s",
                           pre->bucket,
                           &major, &minor,
                           &compress, &correct, &encrypt,
                           md_ctime, obj_ctime, md_path);

   if (objid_size == EOF)
      return errno;
   else if (objid_size != 6)
      return EINVAL;            /* ?? */

   // --- bucket-name includes MDFS root, and repo-name
   char* repo_name = strrchr(pre->bucket, '.');
   if (! repo_name)
      return EINVAL;            /* ?? */

   MarFS_Repo* repo = find_repo_by_name(repo_name);
   if (! repo)
      return EINVAL;            /* ?? */


   // parse encoded time-stamps
	struct tm  md_ctime_tm;
   char* time_str_ptr = strptime(md_ctime, MARFS_DATE_FORMAT, &md_ctime_tm);
   if (!time_str_ptr)
      return errno;
   else if (*time_str_ptr)
      return EINVAL;

	struct tm  obj_ctime_tm;
   time_str_ptr = strptime(obj_ctime, MARFS_DATE_FORMAT, &obj_ctime_tm);
   if (!time_str_ptr)
      return errno;
   else if (*time_str_ptr)
      return EINVAL;


   // --- fill in fields in Pre
   pre->md_ctime     = mktime(&md_ctime_tm);
   pre->obj_ctime    = mktime(&md_ctime_tm);
   pre->config_vers  = (float)major + ((float)minor / 1000.f);

   pre->compression  = decode_compression(compress);
   pre->correction   = decode_correction(correct);
   pre->encryption   = decode_encryption(encrypt);

   pre->repo         = repo;
   pre->chunk_size   = repo->chunk_size;
   pre->chunk_no     = 0;
   pre->md_inode     = st->st_ino;

   assert (pre->config_vers == MarFS_config_vers);
   return 0;
}





// initialize -- most fields aren't known, when stat_xattr() calls us
int init_post(MarFS_XattrPost* post, MarFS_Namespace* ns, MarFS_Repo* repo) {
   post->config_vers = MarFS_config_vers;
   post->obj_type = OBJ_NONE;   /* figured out later */
   return 0;
}

// from MarFS_XattrPost to string
int post_2_str(char* post_str, size_t size, const MarFS_XattrPost* post) {

   // config-version major and minor
   int major = (int)floorf(post->config_vers);
   int minor = (int)floorf((post->config_vers - major) * 1000.f);

   ssize_t bytes_printed = snprintf(post_str, size,
                                    "%03d_%03d,%c,%lx,%016lx,%016lx",
                                    major, minor,
                                    encode_obj_type(post->obj_type),
                                    post->obj_offset,
                                    post->correct_info,
                                    post->encrypt_info);
   if (bytes_printed < 0)
      return errno;
   if (bytes_printed == size)   /* overflow */
      return EINVAL;

   return 0;
}

// parse an xattr-value string into a MarFS_XattrPost
int str_2_post(MarFS_XattrPost* post, const char* post_str) {

   int major;
   int minor;

   char obj_type_code;

   // --- extract bucket, and some top-level fields
   int scanf_size = sscanf(post_str, "%03d_%03d,%c,%lx,%016lx,%016lx",
                           &major, &minor,
                           &obj_type_code,
                           &post->obj_offset,
                           &post->correct_info,
                           &post->encrypt_info);

   if (scanf_size == EOF)
      return errno;
   else if (scanf_size != 6)
      return EINVAL;            /* ?? */


   post->obj_type = decode_obj_type(obj_type_code);
   return 0;
}








// from MarFS_XattrSlave to string
int slave_2_str(char* slave_str,        const MarFS_XattrSlave* slave) {
   assert(0);                   // TBD
}

// from string to MarFS_XattrSlave
int str_3_slave(MarFS_XattrSlave* slave, const char* slave_str) {
   assert(0);                   // TBD
}














XattrSpec*  MarFS_xattr_specs = NULL;

int init_xattr_specs() {

   // these are used by a parser (e.g. stat_xattr())
   // The string in MarFS_XattrPrefix is appended to all of them
   // TBD: free this in clean-up
   MarFS_xattr_specs = (XattrSpec*) calloc(4, sizeof(XattrSpec));

   MarFS_xattr_specs[0] = (XattrSpec) { XVT_PRE,     "objid" };
   MarFS_xattr_specs[1] = (XattrSpec) { XVT_POST,    "post" };
   MarFS_xattr_specs[2] = (XattrSpec) { XVT_RESTART, "restart" };

   MarFS_xattr_specs[3] = (XattrSpec) { XVT_NONE,    NULL };

   return 0;
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

float MarFS_config_vers = 0.1;


// top level mount point for fuse/pftool
 char*                   MarFS_mnt_top = NULL;

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

   if (! config_fname)
      config_fname = CONFIG_DEFAULT;

   // hard-coded repository
   _repo  = (MarFS_Repo*) malloc(sizeof(MarFS_Repo));
   *_repo = (MarFS_Repo) {
      .name         = "emcs3_00",
      // .url          = "http://10.140.0.15:9020",
      .url          = "http://10.143.0.1:9020",
      .flags        = MARFS_ONLINE,
      .proto        = PROTO_S3_EMC,
      .chunk_size   = (1024 * 1024 * 512),
      .auth         = AUTH_S3_AWS_MASTER,
      .latency_ms   = (10 * 1000),
   };


   // hard-coded range-list
   RangeList* ranges = (RangeList*) malloc(sizeof(RangeList));
   *ranges = (RangeList) {
      .min  =  0,
      .max  = -1,
      .repo = _repo
   };


   MarFS_mnt_top = "/users/jti/projects/mar_fs/git/fuse/test/filesys/mnt";

   // hard-coded namespace
   //
   // TBD: Maybe trash_path should always be a subdir under the md_path, so
   //      that trash_file() variants can expect to just rename MDFS files,
   //      to move them into the trash.  We could enforce this by requiring
   //      it not to contain any slashes. (and to already exist?)
   //
   _ns  = (MarFS_Namespace*) malloc(sizeof(MarFS_Namespace));
   *_ns = (MarFS_Namespace) {
      .mnt_suffix     = "/test00",  // e.g. fuse exposes "<mnt_top>/test00"
      .md_path        = "/users/jti/projects/mar_fs/git/fuse/test/filesys/meta",
      .trash_path     = "/users/jti/projects/mar_fs/git/fuse/test/filesys/trash",
      .fsinfo_path    = "/users/jti/projects/mar_fs/git/fuse/test/filesys/stat",

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .compression = COMPRESS_NONE,
      .correction  = CORRECT_NONE,

      .iwrite_repo = _repo,   /* interactive writes */
      .range_list  = ranges,  /* batch */

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space_units = (1024 * 1024), /* MB */
      .quota_space = 1024,          /* 1024 MB of data */

      .quota_space_units = 1,
      .quota_names = 32,             /* 32 names */
   };

   // these make it quicker to parse parts of the paths
   _ns->mnt_suffix_len = strlen(_ns->mnt_suffix);
   _ns->md_path_len    = strlen(_ns->md_path);


   // helper for find_namespace()
   _ns->mnt_suffix_len = strlen(_ns->mnt_suffix);


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
   return NULL;
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
   if (strcmp(repo_name, _repo->name))
       return NULL;                /* err */
   return _repo;
}
