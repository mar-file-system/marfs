#include "marfs_base.h"

#include <stdlib.h>
#include <string.h>



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

float MarFS_config_vers = 0.001;


// top level mount point for fuse/pftool
char*                   MarFS_mnt_top = NULL;
size_t                  MarFS_mnt_top_len = 0;

// quick-n-dirty
// For now, this is a dynamically-allocated vector of pointers.
static MarFS_Namespace** _ns   = NULL;
static size_t            _ns_max = 0;   // max number of ptrs in _ns
static size_t            _ns_count = 0; // current number of ptrs in _nts

// quick-n-dirty
// For now, this is a dynamically-allocated vector of pointers.
static MarFS_Repo**      _repo = NULL;
static size_t            _repo_max = 0;   // max number of ptrs in _repo
static size_t            _repo_count = 0; // current number of ptrs in _repo



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
//
// TBD: Reject namespace-names that contain '-'.  See comments above
//      encode_namespace() decl, in marfs_base.h, for an explanation.
//
// TBD: Add 'log-level' to config-file.  Use it to either configure syslog,
//      or to set a flag that is tested by printf_log().  This would allow
//      diabling some of the output logging.  Should also control whether
//      we use aws_set_debug() to enable curl diagnostics.
//
// TBD: See object_stream.c We could configure a timeout for waiting on
//      GET/PUT.  Work need to be done on the stream_wait(), to honor this
//      timeout.
//
// TBD: Validation.  E.g. make sure repo.chunk_size is never less-than
//      MARFS_MAX_OBJID_SIZE (or else an MD file full of objIDs, for a
//      Multi-type object, could be truncated to a size smaller than its
//      contents.

MarFS_Repo* push_repo(MarFS_Repo* dummy) {
   if (! dummy) {
      LOG(LOG_ERR, "NULL repo\n");
      exit(1);
   }

   MarFS_Repo* repo = (MarFS_Repo*)malloc(sizeof(MarFS_Repo));
   if (! repo) {
      LOG(LOG_ERR, "alloc failed for '%s'\n", dummy->name);
      exit(1);
   }
   if (_repo_count >= _repo_max) {
      LOG(LOG_ERR, "No room for repo '%s'\n", dummy->name);
      exit(1);
   }

   LOG(LOG_INFO, "repo: %s\n", dummy->name);
   *repo = *dummy;
   _repo[_repo_count++] = repo;
   return repo;
}

MarFS_Namespace* push_namespace(MarFS_Namespace* dummy, MarFS_Repo* repo) {
   if (! dummy) {
      LOG(LOG_ERR, "NULL namespace\n");
      exit(1);
   }
   if (!dummy->is_root && !repo) {
      LOG(LOG_ERR, "NULL repo\n");
      exit(1);
   }

   MarFS_Namespace* ns = (MarFS_Namespace*)malloc(sizeof(MarFS_Namespace));
   if (! ns) {
      LOG(LOG_ERR, "alloc failed for '%s'\n", dummy->name);
      exit(1);
   }
   if (_ns_count >= _ns_max) {
      LOG(LOG_ERR, "No room for namespqace '%s'\n", dummy->name);
      exit(1);
   }

   LOG(LOG_INFO, "namespace: %-16s (repo: %s)\n", dummy->name, repo->name);
   *ns = *dummy;

   // use <repo> for everything.
   RangeList* ranges = (RangeList*) malloc(sizeof(RangeList));
   *ranges = (RangeList) {
      .min  =  0,
      .max  = -1,
      .repo = repo
   };
   ns->range_list  = ranges;
   ns->iwrite_repo = repo;

   // these make it quicker to parse parts of the paths
   ns->name_len       = strlen(ns->name);
   ns->mnt_suffix_len = strlen(ns->mnt_suffix);
   ns->md_path_len    = strlen(ns->md_path);

   _ns[_ns_count++] = ns;
   return ns;
}

int validate_config();          // fwd-decl

int load_config(const char* config_fname) {

   // config_fname is ignored, for now, but will eventually hold everythying
   if (! config_fname)
      config_fname = CONFIG_DEFAULT;

   MarFS_mnt_top       = "/marfs";
   MarFS_mnt_top_len   = strlen(MarFS_mnt_top);

   // ...........................................................................
   // hard-coded repositories
   //
   //     For sproxyd, repo.name must match an existing fast-cgi path
   //     For S3,      repo.name must match an existing bucket
   //
   // ...........................................................................

   _repo_max = 64;              /* the number we're about to allocate */
   _repo  = (MarFS_Repo**) malloc(_repo_max * sizeof(MarFS_Repo*));

   MarFS_Repo r_dummy;

   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_jti",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.21:81",
      .access_proto = PROTO_SPROXYD,
      // .chunk_size   = (1024 * 1024 * 256), /* max MarFS object (tune to match storage) */
      // .chunk_size   = (1024 * 1024 * 512), /* max MarFS object (tune to match storage) */
      .chunk_size   = (1024 * 1024 * 1028), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000) };
   push_repo(&r_dummy);

   // tiny, so detailed debugging (where we watch every char go over the line)
   //       won't be overwhelming at the scale needed for Multi.
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_2k",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.21:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (2048), /* i.e. max MarFS object (small for debugging) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000),
   };
   push_repo(&r_dummy);

   // For Alfred, testing GC and quota utilities
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_64M",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.22:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (1024 * 1024 * 64), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000) };
   push_repo(&r_dummy);

   // For Brett, unit-testing, small enough to make it easy to create MULTIs
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_1M",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.22:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (1024 * 1024 * 1), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000)
   };
   push_repo(&r_dummy);

   // @@@-HTTPS: For Brett, unit-testing, small enough to make it easy to create MULTIs
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_1M_https",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.22:444",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (1024 * 1024 * 1), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE | REPO_SSL),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000)
   };
   push_repo(&r_dummy);

   // free-for-all
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_pub",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.28:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (1024 * 1024 * 64), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000) };
   push_repo(&r_dummy);

   // S3 on EMC ECS
   r_dummy = (MarFS_Repo) {
      .name         = "emcS3_00",  // repo is s3: this must match existing bucket
      .host         = "10.140.0.15:9020", //"10.143.0.1:80",
      .access_proto = PROTO_S3_EMC,
      .chunk_size   = (1024 * 1024 * 256), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000),
   };
   push_repo(&r_dummy);

   // @@@-HTTPS: S3 on EMC ECS
   r_dummy = (MarFS_Repo) {
      .name         = "emcS3_00_https",  // repo is s3: this must match existing bucket
      .host         = "10.140.0.15:9021", //"10.143.0.1:443",
      .access_proto = PROTO_S3_EMC,
      .chunk_size   = (1024 * 1024 * 64), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE | REPO_SSL),
      .auth         = AUTH_S3_AWS_MASTER,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000),
   };
   push_repo(&r_dummy);

#if TBD
   // semi-direct experiment
   r_dummy = (MarFS_Repo) {
      .name         = "semi",
      .host         = "/gpfs/marfs-gpfs/fuse/semi", //"10.143.0.1:443",
      .access_proto = PROTO_SEMI_DIRECT,
      .chunk_size   = (1024 * 1024 * 1), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_NONE,
      .compression  = COMPRESS_NONE,
      .correction   = CORRECT_NONE,
      .encryption   = ENCRYPT_NONE,
      .latency_ms   = (10 * 1000),
   };
   push_repo(&r_dummy);
#endif



   // ...........................................................................
   // hard-coded namespaces
   //
   //     For sproxyd, namespace.name must match an existing sproxyd driver-alias
   //     For S3,      namespace.name is just part of the object-id
   //
   // NOTE: Two namespaces should not have the same mount-suffix, because
   //     Fuse will use this to look-up namespaces.  Two NSes also
   //     shouldn't have the same name, in case someone wants to lookup
   //     by-name.
   // ...........................................................................

   _ns_max = 64;              /* the number we're about to allocate */
   _ns  = (MarFS_Namespace**) malloc(_ns_max * sizeof(MarFS_Namespace*));

   MarFS_Namespace ns_dummy;

   // jti testing
   ns_dummy = (MarFS_Namespace) {
      .name           = "jti",
      .mnt_suffix     = "/jti",  // "<mnt_top>/jti" comes here

      .md_path        = "/gpfs/marfs-gpfs/fuse/test00/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/fuse/test00/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = (1024L * 1024 * 1024),          /* 1 GB of data */
      .quota_names = 32,             /* 32 names */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   //   push_namespace(&ns_dummy, find_repo_by_name("emcS3_00"));
   //   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_2k"));
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_jti"));


   // Alfred
   ns_dummy = (MarFS_Namespace) {
      .name           = "atorrez",
      .mnt_suffix     = "/atorrez",  // "<mnt_top>/atorrez" comes here

      .md_path        = "/gpfs/marfs-gpfs/project_a/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = -1,                  /* no limit */
      .quota_names = -1,        /* no limit */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_1M"));


   // Brett, unit 
   ns_dummy = (MarFS_Namespace) {
      .name           = "brettk",
      .mnt_suffix     = "/brettk",  // "<mnt_top>/brettk" comes here

      .md_path        = "/gpfs/marfs-gpfs/testing/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/testing/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/testing/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = -1,          /* no limit */
      .quota_names = -1,             /* no limit */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_1M"));


   // @@@-HTTPS: Brett, unit 
   ns_dummy = (MarFS_Namespace) {
      .name           = "brettk_https",
      .mnt_suffix     = "/brettk_https",  // "<mnt_top>/brettk_https" comes here

      .md_path        = "/gpfs/marfs-gpfs/testing_https/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/testing_https/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/testing_https/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = -1,          /* no limit */
      .quota_names = -1,             /* no limit */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_1M_https"));


   // free-for-all
   ns_dummy = (MarFS_Namespace) {
      .name           = "test",
      .mnt_suffix     = "/test",  // "<mnt_top>/test" comes here

      .md_path        = "/gpfs/fs1/fuse_community/mdfs",
      .trash_path     = "/gpfs/fs1/fuse_community/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/fs1/fuse_community/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = -1,          /* no limit */
      .quota_names = -1,             /* no limit */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_pub"));


   // EMC ECS install (with S3)
   ns_dummy = (MarFS_Namespace) {
      .name           = "s3",
      .mnt_suffix     = "/s3",  // "<mnt_top>/s3" comes here

      .md_path        = "/gpfs/fs2/fuse_s3/mdfs",
      .trash_path     = "/gpfs/fs2/fuse_s3/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/fs2/fuse_s3/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = (1024L * 1024 * 1024),          /* 1GB of data */
      .quota_names = 32,             /* 32 names */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("emcS3_00"));


   // @@@-HTTPS: EMC ECS install (with S3)
   ns_dummy = (MarFS_Namespace) {
      .name           = "s3_https",
      .mnt_suffix     = "/s3_https",  // "<mnt_top>/s3_https" comes here

      .md_path        = "/gpfs/fs2/fuse_s3_https/mdfs",
      .trash_path     = "/gpfs/fs2/fuse_s3_https/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/fs2/fuse_s3_https/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = (1024L * 1024 * 1024),          /* 1 GB of data */
      .quota_names = 32,             /* 32 names */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("emcS3_00_https"));


   //   // jti testing on machine without GPFS
   //   ns_dummy = (MarFS_Namespace) {
   //      .name           = "xfs",
   //      .mnt_suffix     = "/xfs",  // "<mnt_top>/xfs" comes here
   //
   //      .md_path        = "/mnt/xfs/jti/filesys/mdfs/test00",
   //      .trash_path     = "/mnt/xfs/jti/filesys/trash/test00",
   //      .fsinfo_path    = "/mnt/xfs/jti/filesys/fsinfo/test00",
   //
   //      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
   //      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
   //
   //      .dirty_pack_percent   =  0,
   //      .dirty_pack_threshold = 75,
   //
   //      .quota_space = (1024L * 1024 * 1024),          /* 1 GB of data */
   //      .quota_names = 32,             /* 32 names */
   //
   //      .shard_path  = NULL,
   //      .shard_count = 0,
   //
   //      .is_root = 0,
   //   };
   //   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_jti"));


   // jti testing on machine without GPFS
   ns_dummy = (MarFS_Namespace) {
      .name           = "ext4",
      .mnt_suffix     = "/ext4",  // "<mnt_top>/ext4" comes here

      .md_path        = "/root/marfs_non_gpfs/mdfs",
      .trash_path     = "/root/marfs_non_gpfs/trash",
      .fsinfo_path    = "/root/marfs_non_gpfs/fsinfo",

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = (1024L * 1024 * 1024),          /* 1 GB of data */
      .quota_names = 32,             /* 32 names */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   // push_namespace(&ns_dummy, find_repo_by_name("sproxyd_2k"));
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_jti"));


#ifdef TBD
   // jti testing semi-direct
   ns_dummy = (MarFS_Namespace) {
      .name           = "semi",
      .mnt_suffix     = "/semi",

      .md_path        = "/gpfs/marfs-gpfs/fuse/semi/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/fuse/semi/trash",
      .fsinfo_path    = "/gpfs/marfs-gpfs/fuse/semi/fsinfo",

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = (1024L * 1024 * 1024),          /* 1 GB of data */
      .quota_names = 32,             /* 32 names */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("semi"));
#endif





   // "root" is a special path
   //
   // NOTE: find_namespace_by_path() will only return this namespace if its
   //       <path> matches our <mnt_suffix> exactly.  That's because our
   //       mnt_suffix is (necessarily) a suffix of all paths.
   ns_dummy = (MarFS_Namespace) {
      .name           = "root",
      .mnt_suffix     = "/",
      .md_path        = "should_never_be_used",
      .trash_path     = "should_never_be_used",
      .fsinfo_path    = "should_never_be_used",

      .iperms = ( R_META ),     /* marfs_getattr() does manual stuff */
      .bperms = 0,

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space = -1,          /* no limit */
      .quota_names = -1,             /* no limit */

      .shard_path  = NULL,
      .shard_count = 0,

      .is_root = 1,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_1M"));


   if (validate_config())
      return -1;

   return 0;                    /* success */
}


// ad-hoc tests of various inconsitencies, or illegal states, that are
// possible after load_config() has loaded a config file.
int validate_config() {
   const size_t     recovery = sizeof(RecoveryInfo) +8;

   int retval = 0;
   int i;

   // repo checks
   for (i=0; i<_repo_count; ++i) {
      MarFS_Repo* repo = _repo[i];

      if (repo->chunk_size <= recovery) {
         LOG(LOG_ERR, "repo '%s' has chunk-size (%ld) "
             "less than the size of recovery-info (%ld)\n",
             repo->name, repo->chunk_size, recovery);
         retval = -1;
      }
   }

#if 0
   // TBD

   // namespace checks
   for (i=0; i<_ns_count; ++i) {
      MarFS_Namespace* ns = _ns[i];
      // ...
   }
#endif

   return retval;
}



// ...........................................................................
// NAMESPACES
// ...........................................................................

// Find the namespace corresponding to the mnt_suffx in a Namespace struct,
// which corresponds with a "namespace" managed by fuse.  We might
// pontentially have many namespaces (should be cheap to have as many as
// you want), and this lookup is done for every fuse call (and in parallel
// from pftool).  Also done every time we parse an object-ID xattr!  Thus,
// this should eventually be made efficient.
//
// One way to make this fast would be to look through all the namespaces
// and identify the places where a path diverges for different namespaces.
// This becomes a series of hardcoded substring-ops, on the path.  Each one
// identifies the next suffix in a suffix tree.  (Attractive Chaos has an
// open source suffix-array impl).  The leaves would be pointers to
// Namespaces.
//
// NOTE: If the fuse mount-point is "/A/B", and you provide a path like
//       "/A/B/C", then the "path" seen by fuse callbacks is "/C".  In
//       otherwords, we should never see MarFS_mnt_top, as part of the
//       incoming path.
//
// For a quick first-cut, there's only one namespace.  Your path is either
// in it or fails.

MarFS_Namespace* find_namespace_by_name(const char* name) {
   int i;
   size_t name_len = strlen( name );

   for (i=0; i<_ns_count; ++i) {
      MarFS_Namespace* ns = _ns[i];
      // We want to compare the whole name to ns->name, not just the first
      // name length characters.  That will match names that are substrings
      // of name to name incorrectly.
      //
      //  if (! strncmp(ns->name, name, ns->name_len))
      if (( ns->name_len == name_len ) && ( ! strcmp( ns->name, name ))) {
         return ns;
      }
   }
   return NULL;
}


/*
 * @@@-HTTPS:
 * The path that is passed into this function always starts with the
 * "/" character. That character and any others up to the next "/"
 * character are the namespace's mnt_suffix. A namespace's mnt_suffix
 * must begin with the "/" character and not contain any other "/"
 * characters after the initial one by definition. It is the FUSE
 * mount point and we'll always use a one-level mount point.
 */
MarFS_Namespace* find_namespace_by_path(const char* path) {
   int i;
   char *path_dup;
   char *path_dup_token;
   size_t path_dup_len;


   path_dup = strdup( path );
   path_dup_token = strtok( path_dup, "/" );
   path_dup_len = strlen( path_dup );

/*
 * At this point path_dup will include the leading "/" and any other
 * characters up to, but not including, the next "/" character in
 * path. This includes path_dup being able to be "/" (the root
 * namespace.
 */

   for (i=0; i<_ns_count; ++i) {
      MarFS_Namespace* ns = _ns[i];

      if (( ns->mnt_suffix_len == path_dup_len )
          && ( !strcmp( ns->mnt_suffix, path_dup ))) {
         free( path_dup );
         return ns;
      }
   }

   free( path_dup );
   return NULL;
}

// Let others traverse namespaces, without knowing how they are stored
NSIterator        namespace_iterator() {
   return (NSIterator){ .pos = 0 };
}

MarFS_Namespace*  namespace_next(NSIterator* it) {
   if (it->pos >= _ns_count)
      return NULL;
   else
      return _ns[it->pos++];
}




// ...........................................................................
// REPOS
// ...........................................................................

MarFS_Repo* find_repo(MarFS_Namespace* ns,
                      size_t           file_size,
                      int              interactive_write) { // bool
   if (interactive_write)
      return ns->iwrite_repo;
   else
      return find_in_range(ns->range_list, file_size);
}


// later, _repo will be a B-tree, or something, associating repo-names with
// repos.
MarFS_Repo* find_repo_by_name(const char* repo_name) {
   int i;
   for (i=0; i<_repo_count; ++i) {
      MarFS_Repo* repo = _repo[i];
      if (!strcmp(repo_name, repo->name))
         return repo;
   }
   return NULL;
}


// Let others traverse repos, without knowing how they are stored
RepoIterator repo_iterator() {
   return (RepoIterator){ .pos = 0 };
}

MarFS_Repo*  repo_next(RepoIterator* it) {
   if (it->pos >= _repo_count)
      return NULL;
   else
      return _repo[it->pos++];
}
