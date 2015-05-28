#ifndef MARFS_BASE_H
#define MARFS_BASE_H

/*
This file is part of MarFS, which is released under the BSD license.


Copyright (c) 2015, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANS, LLC added functionality to the original work. The original work plus
LANS, LLC contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at <http://www.gnu.org/licenses/>.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2015, Los Alamos National Security, LLC All rights reserved.
Copyright 2015. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/


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

#include "logging.h"


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
#define   MARFS_MAX_OBJID_SIZE    1024

// Must fit in an S3 bucket (max 63 chars), with room left for
// namespace-name.  We also leave room for terminal '\0', because this is
// really used to allocate buffers when parsing objid xattr-values.
//
// // #define   MARFS_MAX_REPO_NAME       63
// #define MARFS_MAX_REPO_NAME        52 /* BUCKET_SIZE - "ver.%03d_%03d." */
#define MARFS_MAX_REPO_NAME         16

// Allows us to allocate buffers when parsing objid
// xattr-values.  If this is going to go into the
// "bucket" part of the object-ID, then it must fit there, with
// enough room left over to fit MAX_REPO_NAME
#define MARFS_MAX_NAMESPACE_NAME   (MARFS_MAX_BUCKET_SIZE - MARFS_MAX_REPO_NAME)

// "http://.../<bucket>/<objid>"
#define   MARFS_MAX_URL_SIZE      (128 + MARFS_MAX_BUCKET_SIZE + MARFS_MAX_OBJID_SIZE)




// max buffer for calls to write().  This should "match" (i.e. be a
// multiple or divide evenly into) the size of buffer we're using
// internally in the object-store.  For Scality, I'm assuming this will
// match a "stripe-size" of 128MB.  We could interrogate the store, to find
// out what this is, but it seems to me it maybe should be hardcoded per
// fuse version, instead.
#define   MARFS_WRITEBUF_MAX    (128 * 1024 * 1024)


// strptime() / strftime() are severely limited in our old version of glibc
// (2.12).  With a newer version, perhaps %Z would be understood by both of
// them.  Then, you could restore that version of the DATE_FORMAT, and do
// away with everything using the DST_FORMAT string, in update_pre() and
// str_to_pre().
//
// #define   MARFS_DATE_FORMAT        "%Y%m%d_%H%M%S%Z"

#define   MARFS_DATE_FORMAT        "%Y%m%d_%H%M%S%z"
#define   MARFS_DST_FORMAT         "_%d"


#define   MARFS_DATE_STRING_MAX    64  /* after formatting */



// OBJECT_ID FORMAT
//
// NOTE: The "objid" xattr-value includes the bucket.
//
// NOTE: The bucket given to S3 can't include slashes, so we can't reliably
//       encode the Marfs namespace there.  Therefore, I've rearranged the
//       order of elements in the xattr-string and in the obj-ID.
//
//   <bucket>                 [ ver.<version_major>_<version_minor>.<repo> ]
//
//   /
//
//   <object_type>            { _=NONE, Uni, Multi, Packed, Striped, Fuse }
//   <compress>               { _=NONE, Run_length }
//   <correct>                { _=NONE, cRc, checKsum, Hash, Rais, Erasure }
//   <encrypt>                { _=NONE, ? }
//   /inode.<inode>           [64-bits as 16 hex digits]
//   /obj_ctime.<obj_ctime>   [see MARFS_DATE_FORMAT]
//   /md_ctime.<md_ctime>     [see MARFS_DATE_FORMAT]
//   /ns.<namespace>
//   
//   /mdfs<MDFS_path>

#define NON_SLASH           "%[^/]"
#define NON_DOT             "%[^./]"

// <repo>.<encoded_namespace>
#define MARFS_BUCKET_RD_FORMAT  NON_DOT "." NON_SLASH
#define MARFS_BUCKET_WR_FORMAT  "%s.%s"


#define MARFS_OBJID_RD_FORMAT   "ver.%03d_%03d/%c%c%c%c/inode.%016lx/md_ctime.%[^/]/obj_ctime.%[^/]/chnksz.%lx/chnkno.%lx"
#define MARFS_OBJID_WR_FORMAT   "ver.%03d_%03d/%c%c%c%c/inode.%016lx/md_ctime.%s/obj_ctime.%s/chnksz.%lx/chnkno.%lx"


// #define MARFS_PRE_RD_FORMAT     MARFS_BUCKET_RD_FORMAT "/" MARFS_OBJID_RD_FORMAT  
#define MARFS_PRE_RD_FORMAT     NON_SLASH "/%s" 



#define MARFS_POST_FORMAT       "ver.%03d_%03d/%c/off.%ld/objs.%ld/bytes.%ld/corr.%016lx/crypt.%016lx/gc.%s"



// Do an in-place modification of a given namespace-name (i.e. the part of
// a path that sits below the fuse-mount, not including the contained
// files).  We are storing the namespace in the "bucket"-part of an
// object-id.  S3 buckets have to look like domain-names (i.e. they can
// only contain alphanums, dot, dash, underscore, with no repeated dots,
// and dot not first or last).  This means no slashes.  Namespaces contain
// slashes.  So, we encode them before adding them into he object-id.
//
// We could use an escaping-system (e.g. map slash to underscore, and use
// doubled underscores to represent a literal underscore), but that allows
// the encoded string to change size, which is awkward.  Instead we decree
// NAMESPACE-NAMES SHALL NOT CONTAIN DASHES.  Then we can always map slash
// to dash (in a non slap-dash way).  config-reader should reject
// namespaces containing dash.

int encode_namespace(char* dst, char* src);
int decode_namespace(char* dst, char* src);




// how objects are used to store "files"
// NOTE: co-maintain encode/decode_obj_type()
typedef enum {
   OBJ_NONE = 0,
   OBJ_UNI,            // one object per file
   OBJ_MULTI,          // file spans multiple objs (list of objs as chunks)
   OBJ_PACKED,         // multiple files per objects
   OBJ_STRIPED,        // (like Lustre does it)
   OBJ_FUSE,           // written by FUSE.  (i.e. not packed, maybe uni/multi.
                       // Only used in object-ID, not in Post xattr)
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
// Standardized date/time stringification
// ---------------------------------------------------------------------------

int epoch_to_str(char* str, size_t size, const time_t* time);
int str_to_epoch(time_t* time, const char* str, size_t size);


// ---------------------------------------------------------------------------
// Repository -- describes data (object) storage
// ---------------------------------------------------------------------------

typedef enum {
   REPO_ONLINE          = 0x01, // repo is online?
   REPO_ALLOWS_OFFLINE  = 0x02, // repo allows offline?
   REPO_UPDATE_IN_PLACE = 0x04, // repo allows update-in-place ?
   REPO_SSL             = 0x08  // e.g. use https://...
} RepoFlags;

typedef uint8_t  RepoFlagsType;


typedef enum {
   PROTO_DIRECT = 0,
   PROTO_CDMI,
   PROTO_S3,
   PROTO_S3_SCALITY,            // should include installed release version
   PROTO_S3_EMC,                // should include installed release version
} RepoAccessProto;

#define PROTO_IS_S3(PROTO)  ((PROTO) & (PROTO_S3 | PROTO_S3_SCALITY | PROTO_S3_EMC))


typedef enum {
   AUTH_NONE = 0,
   AUTH_S3_AWS_USER,            // AWS standard, with per-user keys
   AUTH_S3_AWS_MASTER,          // AWS standard, with a private "master" key
   AUTH_S3_PER_OBJ,             // (TBD) server enforces per-object-key
} MarFSAuthMethod;



typedef struct MarFS_Repo {
   const char*       name;         // (logical) name for this repo 
   const char*       host;         // e.g. "10.140.0.15:9020"
   RepoFlagsType     flags;
   RepoAccessProto   access_proto;
   size_t            chunk_size;   // max Uni-object (Cf. Namespace.range_list)
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
//       if ( ! has_any_xattrs(&info, MARFS_MD_XATTRS) )
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

// <linux/limits.h> has XATTR_SIZE_MAX,  XATTR_NAME_MAX, etc.
// <gpfs_fcntl.h> has GPFS_FCNTL_XATTR_MAX_NANMELEN/VALUELEN
#define MARFS_MAX_XATTR_SIZE (16 * 1024) /* TBD: define appropriately for GPFS vs etc */


// ...........................................................................
// Each system-xattr from the MDFS is parsed and stored into a single structure.
// ...........................................................................

// "Pre" has info that is known when object-storage behind a file is first
// opened.  (So, for example, it can't include length.)  Some or all of the
// members are formatted into object-names.
//
// NOTE: There is an object-type stored here, but it is only the part of an
//       object type that can be known at obj-open time, by fuse or pftool.
//       (See MarFS_XattrPost, for the true object-type, which can only be
//       known by fuse after the object has been written.)  The point of
//       this field is to indicate whether the object is PACKED, or not.
//       We also capture whether the object was written by Fuse, or not.
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

   const MarFS_Repo*      repo;
   const MarFS_Namespace* ns;

   float              config_vers;  // (major.minor) config that file was written with
   MarFS_ObjType      obj_type;     // This will only be { Packed, Fuse, or None }
                                    // see XattrPost for final correct type of object

   CompressionMethod  compression;  // in addition to erasure-coding
   CorrectionMethod   correction;   // (e.g. CRC/checksum/etc.)
   EncryptionMethod   encryption;   // data-encryption (e.g. sha-256)

   ino_t              md_inode;
   time_t             md_ctime;
   time_t             obj_ctime;    // might be versions in the trash

   size_t             chunk_size;   // from repo-config at write-time
   size_t             chunk_no;     // 0-based number of current chunk (object)

   //   uint16_t           slave;   // TBD: for hashing directories across slave nodes

   char               bucket[MARFS_MAX_BUCKET_SIZE];
   char               objid [MARFS_MAX_OBJID_SIZE]; // not including bucket

} MarFS_XattrPre;


int print_objname(char* obj_name,      const MarFS_XattrPre* pre);

// from MarFS_XattrPre to string
int pre_2_str(char* pre_str, size_t size, MarFS_XattrPre* pre);
int pre_2_url(char* url_str, size_t size, MarFS_XattrPre* pre);

// from string to MarFS_XattrPre
// <has_objid> indicates whether pre.objid is already filled-in
int str_2_pre(MarFS_XattrPre*    pre,
              const char*        pre_str,
              const struct stat* st); // from string

// initialize
int init_pre(MarFS_XattrPre*        pre,
             MarFS_ObjType          obj_type, // see NOTE above function def
             const MarFS_Namespace* ns,
             const MarFS_Repo*      repo,
             const struct stat*     st);

int update_pre(MarFS_XattrPre* pre);




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
   size_t             num_objects;   // number ChunkInfos written in MDFS file
   size_t             chunk_info_bytes; // total size of chunk-info in MDFS file
   char               gc_path[MARFS_MAX_MD_PATH]; // only if file is in trash
} MarFS_XattrPost;


#define XATTR_MAX_POST_STRING_VALUE_SIZE  256 /* max */

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



// ---------------------------------------------------------------------------
// RecoveryInfo
//
// This is a record that has information mostly from stat() of the metadata
// file in human-readable form.  This thing is written directly into the
// metadata file itself, in some cases.
//
// (This is recovery information captured at create time only; it is not
// updated upon metadata changes like chmod, chown, rename, etc.)
// ---------------------------------------------------------------------------

// TBD: fileinfo_2_str(), str_2_fileinfo().

typedef struct {
   float    config_vers;
   size_t   size;               // Size of record
   ino_t    inode;
   mode_t   mode;
   uid_t    uid;
   gid_t    gid;
   time_t   mtime;
   time_t   ctime;
   char     mdfs_path[MARFS_MAX_MD_PATH]; // full path in the MDFS
} RecoveryInfo;


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
