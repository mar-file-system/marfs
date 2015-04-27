#ifndef SERVER_CONFIG_H
#define SERVER_CONFIG_H

#include "marfs_xattrs.h"

#include <stdint.h>
#include <stddef.h>


#  ifdef __cplusplus
extern "C" {
#  endif


// <config_version> allows objects to be retrieved with the same
// configuration parameters that were in effect when they were stored.
//
// QUESTION: This could mean that the fields of all structs are fixed for
//           all time, and different versions simply use different values,
//           loaded from a file.  In that case, there's no need for the
//           structs themselves to change.
//
//           A much tougher alternative, is that the config structures
//           themselves could change (in addition to the field contents).
//           This would mean that run-time must be able to re-create
//           whatever info is needed to drive the FUSE daemon for different
//           historical configurations.  That could lead down some very
//           nasty ratholes (e.g. if the entire processing approach
//           changes).

extern float MarFS_config_version;


// extern would not be useable in PathInfo.md_path decl, below, and we
// don't want it dynamically-allocated.  TBD: Be sure the #define is
// associated with the config version.
#define              MARFS_MAX_MD_PATH       1024
#define              MARFS_MAX_BUCKET_SIZE     63
#define              MARFS_MAX_OBJ_ID_SIZE   1024
#define              MARFS_MAX_REPO_NAME       63


// ---------------------------------------------------------------------------
// Repository -- describes data (object) storage
// ---------------------------------------------------------------------------

typedef enum {
   MARFS_UPDATE_IN_PLACE = 0x01, // repo allows update-in-place ?
   MARFS_ONLINE          = 0x02, // repo is online?
   MARFS_ALLOWS_OFFLINE  = 0x04, // repo allows offline?
} RepoFlags;

typedef uint8_t  RepoFlagsType;


typedef enum {
   PROTO_DIRECT = 0,
   PROTO_CDMI,
   PROTO_S3,
   PROTO_S3_SCALITY,            // should include installed release version
   PROTO_S3_EMC,                // should include installed release version
} RepoAccessProto;


typedef enum {
   SECURE_NONE = 0,
   SECURE_PER_OBJ_AUTH,

} MarFSDataSecurity;


typedef struct MarFS_Repo {
   const char*       name;         // name for this repo 
   const char*       path;         // data path (URL or other) for this repo
   RepoFlagsType     flags;
   RepoAccessProto   proto;
   size_t            chunksize;    // chunksize for repo (Cf. Namespace.range_list)
   MarFSDataSecurity security;     // security method for this repo
   uint32_t          latency_ms;   // max time to wait for a response 
}  MarFS_Repo;



// ---------------------------------------------------------------------------
// Namespace  (Metadata filesystem, aka MDFS)
//
// Fuse/pftool will append MarFS_mnttop on the front of all the name space
// segments below to construct a namespace tree.  For, example:
// 
//    MarFS_mnttop = /redsea
//
//    MarFS_namespace.mnt.mntpath = /projecta
//    MarFS_namespace.mnt.mdpath  = /md/projecta
//
//    MarFS_namespace.mnt.mntpath = /projectb
//    MarFS_namespace.mnt.mdpath  = /md/project
//
// User refers to /redsea/projecta and that refers to metadata
// file-system/namespace in /md/projecta
//
//
// SECURITY:
//
// (a) Access to the object-side storage is (partly) mediated by access to
// the metadata filesystem.  The POSIX permissions on a metadata file
// control access to the underlying data.  For example:
//
//    ls /redsea/projecta/myfile
//
// Inside the FUSE handler, permission to access the corresponding
// object-storage can be mediated by accessing the metadata at
// /md/projecta/myfile.
//
// (b) However, in addition there are ExtraPerms which allow users to be
// given per-file read/write access to data/metadata.
//
//    RD = read  data
//    WD = write  "
//
//    RM = read  metadata
//    WM = write  "
//
// There is additional security on the object-side, so that users who gain
// access to object data for one file, can not access or delete that object
// (or other objects).  This is used, e.g., to extend/limit the 
//
// ---------------------------------------------------------------------------


extern char* MarFS_mnttop;        // top level mount point for fuse/pftool

// (RM read meta, WM write meta, RD read data, WD write data) 
typedef enum {
   R_META = 0x01,
   W_META = 0x02,

   R_DATA = 0x10,
   W_DATA = 0x20,
   T_DATA = 0x40,               // "truncate"
   U_DATA = 0x80,               // "unlink"  (i.e. delete)
   
} MarFSPermFlags;

typedef uint8_t  MarFS_Perms;


// TBD: Move this to a B-tree for maximum lookup speed
typedef struct RangeList {
	size_t             min;      // min block-size to go to this repo
	size_t             max;      // max (or -1)
	MarFS_Repo*        repo;
   struct RangeList*  next;
} RangeList;


// maintain list in order of increasing ranges.
// throw an error (or return non-zero), in case of conflicts.
extern int  insert_in_range(RangeList**  list,
                            size_t       min,
                            size_t       max,
                            MarFS_Repo*  repo);

// given a file-size, find the corresponding element in a RangeList,
// and return the corresponding repo.
extern MarFS_Repo* find_in_range(RangeList*  list,
                                 size_t      block_size);





// Somewhere there is a vector/B-tree/hash-table of these.  That thing
// should optimize lookups based on paths, so maybe a suffix-tree using
// distinct path-components of all namespaces seen by the config-file
// loader.
typedef struct MarFS_Namespace {

   const char*        mnt_suffix; // the part of path below MarFS_mnt_top
   const char*        md_path;    // path of corresponding metadata FS
   const char*        trash_path; // MDFS trash goes here
   const char*        fsinfo_path;// path is maintained (w/ trunc) to show global FS-info

   size_t             mnt_suffix_len; // computed at config-load time

   MarFS_Perms        iperms;   // RM/WM/RD/WD bits, for interactive (fuse)
   MarFS_Perms        bperms;   // RM/WM/RD/WD bits, for batch (pftool)

   CompressionMethod  compression;     // compression type
   CorrectionMethod   correction;      // correctness type  (like CRC/checksum/etc.)
   void*              correction_info; // correctness info  (e.g. the computed checksum)

   MarFS_Repo*        iwrite_repo;  // data repo for interactively written files (fuse)
   RangeList*         range_list;   // repos to use for different file-sizes (pftool)

   uint8_t            dirty_pack_percent;   // percent dirty (observed)
   uint8_t            dirty_pack_threshold; // pftool repack if percentage above this

   size_t             quota_units; // multiplier for quota_space and quota_names
   size_t             quota_space; // quota in space
   size_t             quota_names; // quota in names
}  MarFS_Namespace;



// ---------------------------------------------------------------------------
// configuration-file
// ---------------------------------------------------------------------------

// TBD: parse config-file, instantiate B-trees of MarFS_Namespace and
//      MarFS_Repo.
//
// QUES: do we really want to join these into one massive table, where
//       there is a row for every NS x Repo ?
//

extern int              load_config(const char* config_fname);

extern MarFS_Namespace* find_namespace(const char* path);

extern MarFS_Repo*      find_repo(MarFS_Namespace* ns,
                                  size_t           file_size,
                                  int              interactive_write); // bool






#  ifdef __cplusplus
}
#  endif

#endif  // SERVER_CONFIG_H
