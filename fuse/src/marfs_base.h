#ifndef MARFS_BASE_H
#define MARFS_BASE_H


// This #define must come before enything that might include <time.h>
// Otherwise, gcc 4.4.7 hasn't heard of gmtime_r(), or strptime()
//
// The manpage is wrong.  If you just define it without giving it a value,
// you might not get 'struct timespec'.  see:
// http://stackoverflow.com/questions/3875197/linux-gcc-with-std-c99-complains-about-not-knowing-struct-timespec

#if __STDC_VERSION__ >= 199901L
#  define _XOPEN_SOURCE 700       /* POSIX 2008 */
// #  define _XOPEN_SOURCE 600    /* POSIX 2004 */
#else
#  define _XOPEN_SOURCE 500       /* POSIX 1995 */
#endif

#include <time.h>
// #include "time.h.local_copy.experiments"

// #undef _XOPEN_SOURCE
// #undef __USE_XOPEN



#include <stdint.h>
#include <stddef.h>             // size_t
#include <sys/types.h>          // ino_t
#include <sys/stat.h>
#include <math.h>               // floorf



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

extern float MarFS_config_vers;


// extern would not be useable in PathInfo.md_path decl, below, and we
// don't want it dynamically-allocated.  TBD: Be sure the #define is
// associated with the config version.
#define   MARFS_MAX_MD_PATH       1024
#define   MARFS_MAX_BUCKET_SIZE     63
#define   MARFS_MAX_OBJ_ID_SIZE   1024
#define   MARFS_MAX_REPO_NAME       63

// max buffer for calls to write().  This should "match" (i.e. be a
// multiple or divide evenly into) the size of buffer we're using
// internally in the object-store.  For Scality, I'm assuming this will
// match a "stripe-size" of 128MB.  We could interrogate the store, to find
// out what this is, but it seems to me it maybe should be hardcoded per
// fuse version, instead.
#define   MARFS_WRITEBUF_MAX    (128 * 1024 * 1024)

// // All the encoded date-info etc goes into the object-name ?  This puts
// // everything in one bucket, but saves us some confusion.
// #define   MARFS_DEFAULT_BUCKET     "marfs"

// #define   MARFS_DATE_FORMAT        "%Y_%m_%d-%H_%M_%S"
#define   MARFS_DATE_FORMAT        "%Y_%m_%d--%H_%M_%S_%z"
#define   MARFS_DATE_STRING_MAX    64  /* after formatting */



// how objects are used to store "files"
// NOTE: co-maintain encode/decode_obj_type()
typedef enum {
   OBJ_NONE = 0,
   OBJ_UNI,            // one object per file
   OBJ_MULTI,          // file spans multiple objs (list of objs as chunks)
   OBJ_PACKED,         // multiple files per objects
   OBJ_STRIPED,        // (like Lustre does it)
   OBJ_FUSE,           // written by FUSE.  (not packed, maybe uni/multi. see Post xattr)
} MarFS_ObjType;

// extern const char*   obj_type_name(MarFS_ObjType type);
extern MarFS_ObjType lookup_obj_type(const char* obj_type_name);

extern char          encode_obj_type(MarFS_ObjType type);
extern MarFS_ObjType decode_obj_type(char);



// These types are used in xattrs, so they need corresponding "lookup"
// functions to convert strings to enumeration values.

// TBD ... compression is applied before erasure-coding?
// NOTE: co-maintain encode/decode_compression()
typedef enum {
   COMPRESS_NONE = 0,
   COMPRESS_RUN_LENGTH,
} CompressionMethod;

extern CompressionMethod lookup_compression(const char* compression_name);

extern char              encode_compression(CompressionMethod type);
extern CompressionMethod decode_compression(char);



// error-correction
// NOTE: co-maintain encode/decode_correction()
typedef enum {
   CORRECT_NONE = 0,
   CORRECT_CRC,
   CORRECT_CHECKSUM,
   CORRECT_HASH,
   CORRECT_RAID,
   CORRECT_ERASURE,
} CorrectionMethod;

typedef uint64_t CorrectInfo;   // e.g. checksum

extern CorrectionMethod lookup_correction(const char* correction_name);

extern char             encode_correction(CorrectionMethod meth);
extern CorrectionMethod decode_correction(char);




// TBD: object-encryption
// Cf. Repo.authentication, which is not the same thing
// NOTE: co-maintain encode/decode_encryption()
typedef enum {
   ENCRYPT_NONE = 0
} EncryptionMethod;

typedef uint64_t EncryptInfo;  // e.g. encryption key (doesn't go into object-ID)

extern EncryptionMethod  lookup_encryption(const char* encryption_name);

extern char              encode_encryption(EncryptionMethod type);
extern EncryptionMethod  decode_encryption(char);



// ---------------------------------------------------------------------------
// Repository -- describes data (object) storage
// ---------------------------------------------------------------------------

typedef enum {
   MARFS_ONLINE          = 0x01, // repo is online?
   MARFS_ALLOWS_OFFLINE  = 0x02, // repo allows offline?
   MARFS_UPDATE_IN_PLACE = 0x04, // repo allows update-in-place ?
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
   AUTH_NONE = 0,
   AUTH_S3_AWS_USER,            // AWS standard, with per-user keys
   AUTH_S3_AWS_MASTER,          // AWS standard, with a private "master" key
   AUTH_S3_PER_OBJ,             // (TBD) server enforces per-object-key
} MarFSAuthMethod;




typedef struct MarFS_Repo {
   const char*       name;         // (logical) name for this repo 
   const char*       url;          // URL prefix (e.g. "http://10.140.0.15:9020")
   RepoFlagsType     flags;
   RepoAccessProto   proto;
   size_t            chunk_size;   // chunksize for repo (Cf. Namespace.range_list)
   MarFSAuthMethod   auth;         // (current) authentication method for this repo
   EncryptionMethod  encryption;   // (current) encryption method for this repo
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
//    MarFS_namespace.mnt_suffix = /projecta
//    MarFS_namespace.md_path    = /md/projecta
//
//    MarFS_namespace.mnt_suffix = /projectb
//    MarFS_namespace.md_path    = /md/project
//
// User refers to /redsea/projecta and fuse/pftool translates to refer to
// metadata file-system in /md/projecta
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
//    RD = read  DATA
//    WD = write
//    TD = truncate
//    UD = unlink
//
//    RM = read  METADATA
//    WM = write
//
// There is additional security on the object-side, so that users who gain
// access to object data for one file can not access or delete that object
// (or other objects).
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
   const char*        md_path;    // path of (root of) corresponding MD FS
   const char*        trash_path; // MDFS trash goes here
   const char*        fsinfo_path;// path is trunc'ed to show global FS usage

   size_t             mnt_suffix_len; // computed at config-load time
   size_t             md_path_len;    // computed at config-load time

   MarFS_Perms        iperms;   // RM/WM/RD/WD bits, for interactive (fuse)
   MarFS_Perms        bperms;   // RM/WM/RD/WD bits, for batch (pftool)

   CompressionMethod  compression;     // compression type
   CorrectionMethod   correction;      // correctness type  (like CRC/checksum/etc.)

   MarFS_Repo*        iwrite_repo;     // ultimate size is unknown (fuse)
   RangeList*         range_list;      // repos for different file-sizes (pftool)

   uint8_t            dirty_pack_percent;   // percent dirty (observed)
   uint8_t            dirty_pack_threshold; // pftool repack if percentage above this

   size_t             quota_space_units; // multiplier
   size_t             quota_space;       // space-quota in space_units

   size_t             quota_name_units;  // multiplier
   size_t             quota_names;       // name-quota in name_units
}  MarFS_Namespace;




// ---------------------------------------------------------------------------
// Reserved Xattrs
//
// Fields in these structures are filled from corresponding xattr values
// attached to files in the MDFS.  The fields in each structure are encoded
// as substrings in a single xattr value string, for the given key.  For
// example, MarFS_XattrPost corresponds to the xattr value with key
// "marfs_post".  This value is a single string, which we parse to get the
// fields of the struct.  The reverse operation (converting a struct to a
// string) is also supported.  Thus, each struct has matching functions to
// parse it from a string, or convert it into a string, which are used when
// reading or writing the xattr value, respectively.
//
// Wouldn't it make sense to code this in C++?  Sure, but we're starting
// with C to avoid any issues with linking precompiled libraries.  After
// we're more clear on the SW architecture (e.g. what needs to link to
// what), conversion to C++ makes a lot of sense.
//
// NOTE: We are currently using lack of xattrs (on an MD file) as an
//       indicator that data is stored directly in the MD file (instead of
//       being stored in an object to which xattrs would refer).  This
//       really means a lack of the complete set of xattrs, needed for
//       MarFS_ReservedXattrs.  (See stat_xattrs(), in common.c)
//
//       You can test for this situation in fuse by doing the following:
//
//       EXPAND_PATH_INFO(&info, path);
//       STAT_XATTRS(&info, path);
//       if ( ! has_any_xattrs(&info, MD_MARFS_XATTRS) )
//           // MD file has no xattrs ...
//       }
//
//
// ---------------------------------------------------------------------------

// [Co-maintain with MarFS_XattrPrefixSize]
// this prefix is reserved for system-xattrs in the MDFS and on objects
#define XFS

#ifdef XFS
#  define   MarFS_XattrPrefix      "user.marfs_"
#else
#  define   MarFS_XattrPrefix      "marfs_"
#endif

// [Co-maintain with MarFS_XattrPrefix]
// This just saves us worrying whether the compiler will optimize away
// "strlen(MarFS_XattrPrefix)".
#define   MarFS_XattrPrefixSize  6


// ...........................................................................
// xattrs from the metadata FS are parsed and stored into a single structure.
// ...........................................................................

// "Pre" has info that can be written when object-storage behind a file is
// first opened.  These fields are formatted into object-names.
//
// NOTE: We record the compression/correction/etc, in addition to the
//       repo-name.  Information about compression/correction can be found
//       in the repo object, but it might presumably be re-configured over
//       time.  We need to know the compression (etc) that an object was
//       actually written with.  So those fields are stored as they were at
//       the time the object was written.  On the other hand, while the
//       authentication-method used by the repository may change, it
//       doesn't help to know what it used to be.
//
// NOTE: A time-stamp was formerly printed in reverse-order, at the
//       high-order (leftmost) end of the object-name, because AWS indexes
//       obj-ids in alphabetical order, and uses the index to respond to
//       GETs, so there would be a penalty for having all the obj-ids
//       stored in the same part of the index (all served by the same
//       servers).  However, we're not using AWS, and our obj-stores do not
//       map obj-ids to servers in the same way, so we're removing this
//       feature.

typedef struct MarFS_XattrPre {
   float              config_vers; // (major.minor) config that file was written with
   time_t             md_ctime;
   time_t             obj_ctime;    // might be versions in the trash

   CompressionMethod  compression;  // in addition to erasure-coding
   CorrectionMethod   correction;   // (e.g. CRC/checksum/etc.)
   EncryptionMethod   encryption;   // data-encryption (e.g. sha-256)

   const MarFS_Repo*  repo;         // data repository (name)
   size_t             chunk_size;   // from repo-config at write-time
   size_t             chunk_no;

   ino_t              md_inode;
   //   uint16_t           slave;    // TBD: for hashing directories across slave nodes

   char               bucket[MARFS_MAX_BUCKET_SIZE];
   char               obj_id[MARFS_MAX_OBJ_ID_SIZE];

} MarFS_XattrPre;

int print_objname(char* obj_name,      const MarFS_XattrPre* pre);

// from MarFS_XattrPre to string
int pre_2_str(char* pre_str, size_t size, const MarFS_XattrPre* pre);

// from string to MarFS_XattrPre
// <has_obj_id> indicates whether pre.obj_id is already filled-in
int str_2_pre(MarFS_XattrPre*    pre,
              char*              md_path,
              const char*        pre_str,
              const struct stat* st,
              int                has_obj_id); // from string

// initialize
int init_pre(MarFS_XattrPre*        pre,
             MarFS_ObjType          obj_type, // see NOTE above function def
             const char*            md_path,
             const MarFS_Namespace* ns,
             const MarFS_Repo*      repo,
             const struct stat*     st);




// "Post" has info that is not known until storage into the object(s)
// behind a file is completed.  For example, in the case of an object being
// written via fuse, we have no knowledge of the total size until the
// file-descriptor is closed.
typedef struct MarFS_XattrPost {
   float              config_vers;  // redundant w/ config_vers in Pre?
   MarFS_ObjType      obj_type;
   size_t             obj_offset;    // offset of file in the obj (for packed)
   CorrectInfo        correct_info;  // correctness info  (e.g. the computed checksum)
   EncryptInfo        encrypt_info;  // any info reqd to decrypt the data
} MarFS_XattrPost;


#define XATTR_POST_STRING_VALUE_SIZE  256 /* max */

// from MarFS_XattrPost to string
int post_2_str(char* post_str, size_t size, const MarFS_XattrPost* post);

// from string to MarFS_XattrPost
int str_2_post(MarFS_XattrPost* post, const char* post_str); // from string

int init_post(MarFS_XattrPost* post, MarFS_Namespace* ns, MarFS_Repo* repo);




// TBD: "Slave" will be used to redirect directory paths via hashing to a
// set of slaves for each directory.
typedef struct MarFS_XattrSlave {
   float              config_vers;
   // TBD ...
} MarFS_XattrSlave;

#define XATTR_SLAVE_STRING_VALUE_SIZE  256 /* max */


 // from MarFS_XattrSlave to string
int slave_2_str(char* slave_str,        const MarFS_XattrSlave* slave);

 // from string to MarFS_XattrSlave
int str_3_slave(MarFS_XattrSlave* slave, const char* slave_str); // from string





// These describe xattr keys, and the type of the corresponding values, for
// all the metadata fields in a MarFS_ReservedXattr.  These support a
// generic parser for extracting and parsing xattr data from a metadata
// file (or maybe also from object metadata).
//
// As they are found in stat_xattrs(), each flag is OR'ed into a counter,
// so that has_any_xattrs() can tell you whether specific xattrs were
// found.
//
// NOTE: co-maintain XattrMaskType, ALL_MARFS_XATTRS, MD_MARFS_XATTRS

typedef uint8_t XattrMaskType;  // OR'ed XattrValueTypes
typedef enum {
   XVT_NONE       = 0,          // marks the end of <xattr_specs>

   XVT_PRE        = 0x01,
   XVT_POST       = 0x02,
   XVT_RESTART    = 0x04,
   XVT_SLAVE      = 0x08,
   
} XattrValueType;

#define ALL_MARFS_XATTRS  0x07 /* mask of all implemented XattrValueTypes */
#define MD_MARFS_XATTRS  (XVT_PRE | XVT_POST)  /* XattrValueTypes concerned w/ MD */




// generic description of one of our reserved xattrs
typedef struct {
   XattrValueType  value_type;
   const char*     key_name;        // does not incl MarFS_XattrPrefix (?)
} XattrSpec;


/// typdef struct MarFS_XattrList {
///   char*                   name;
///   char*                   value;
///   struct MarFS_XattrList* next;
/// } MarFS_XattrList;


// An array of XattrSpecs.  Last one has value_type == XVT_NONE.
// initialized in load_config()
extern XattrSpec*  MarFS_xattr_specs;

int init_xattr_specs();


// ---------------------------------------------------------------------------
// configuration-file
// ---------------------------------------------------------------------------

// TBD: parse config-file, instantiate B-trees of MarFS_Namespace and
//      MarFS_Repo.
//
// QUES: do we really want to join these into one massive table, where
//       there is a row for every NS x Repo ?

#define CONFIG_DEFAULT  "~/marfs.config"

extern int              load_config(const char* config_fname);

extern MarFS_Namespace* find_namespace(const char* path);

extern MarFS_Repo*      find_repo(MarFS_Namespace* ns,
                                  size_t           file_size,
                                  int              interactive_write); // bool

extern MarFS_Repo*      find_repo_by_name(const char* name);





#  ifdef __cplusplus
}
#  endif

#endif  // SERVER_CONFIG_H
