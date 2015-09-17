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
// #define MARFS_MAX_REPO_NAME        52 /* BUCKET_SIZE - "ver.%03hu_%03hu." */
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

// // <repo>.<encoded_namespace>
// #define MARFS_BUCKET_RD_FORMAT  NON_DOT "." NON_SLASH
// #define MARFS_BUCKET_WR_FORMAT  "%s.%s"
#define MARFS_BUCKET_RD_FORMAT  NON_SLASH
#define MARFS_BUCKET_WR_FORMAT  "%s"


#define MARFS_OBJID_RD_FORMAT   "%[^/]/ver.%03hu_%03hu/%c%c%c%c/inode.%010ld/md_ctime.%[^/]/obj_ctime.%[^/]/unq.%hhd/chnksz.%lx/chnkno.%lx"
#define MARFS_OBJID_WR_FORMAT   "%s/ver.%03hu_%03hu/%c%c%c%c/inode.%010ld/md_ctime.%s/obj_ctime.%s/unq.%hhd/chnksz.%lx/chnkno.%lx"


// #define MARFS_PRE_RD_FORMAT     MARFS_BUCKET_RD_FORMAT "/" MARFS_OBJID_RD_FORMAT  
#define MARFS_PRE_RD_FORMAT     NON_SLASH "/%s" 



#define MARFS_POST_FORMAT       "ver.%03hu_%03hu/%c/off.%ld/objs.%ld/bytes.%ld/corr.%016lx/crypt.%016lx/flags.%02hhX/mdfs.%s"

#define MARFS_MAX_POST_STRING_WITHOUT_PATH  256 /* max */
#define MARFS_MAX_POST_STRING_SIZE          (MARFS_MAX_POST_STRING_WITHOUT_PATH + MARFS_MAX_MD_PATH)


#define MARFS_REC_INFO_FORMAT   "ver.%03hu_%03hu/inode.%010ld/mode.%08x/uid.%d/gid.%d/mtime.%s/ctime.%s/mdfs.%s"

// Two files in the trash.  Original MDFS is renamed to the name computed
// in expand_trash_info().  Then another file with the same name, extended
// with this format, has contents that hold the original MDFS path (for
// undelete).  The expand_trash_path() result (in PathInfo.trash_path) is
// printed with this format, to create the companion path, whose contents
// hold the original MDFS path.
#define MARFS_TRASH_COMPANION_SUFFIX ".path"


// // (see comments at MultiChunkInfo, below)
// #define MARFS_MULTI_MD_FORMAT   "ver.%03hu.%03hu,off.%ld,len.%ld,obj.%s\n"




#ifdef STATIC_CONFIG
# include "marfs_base_static_config.h"

#else
# include "marfs_configuration.h"
typedef MarFS_CompType      CompressionMethod;
typedef uint64_t            CorrectInfo;
typedef MarFS_CorrectType   CorrectionMethod;
typedef uint64_t            EncryptInfo;
typedef MarFS_SecType       MarFSAuthMethod; // new config confused re encrypt vs auth
typedef MarFS_SecType       EncryptionMethod;
#define CORRECT_NONE        CORRECTTYPE_NONE;
#endif


// some things can't be done in common/configuration/src
extern int validate_config();


// Namespace.flags was eliminated
#define IS_ROOT_NS(NS)  ((NS->mnt_path[0] == '/') && (NS->mnt_path_len == 1))


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
// NOTE: You must co-maintain string-constants in encode/decode_obj_type()
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



// ---------------------------------------------------------------------------
// Standardized date/time stringification
// ---------------------------------------------------------------------------

int epoch_to_str(char* str, size_t size, const time_t* time);
int str_to_epoch(time_t* time, const char* str, size_t size);




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
#  define   MarFS_XattrPrefixSize  11  /* would compiler optimize "strlen(MarFS_XattrPrefix)" ? */

#else
#  define   MarFS_XattrPrefix      "marfs_"
#  define   MarFS_XattrPrefixSize  6   /* would compiler optimize "strlen(MarFS_XattrPrefix)" ? */

#endif

// [Co-maintain with MarFS_XattrPrefix]


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
//
// NOTE: We thought obj_ctime would make trashed versions of a file have a
//       unique object-ID, because the trashed file uses an object-ID that
//       was created then, and this one is created now.  But, obj_ctime is
//       only a time_t (1-second resolution).  If you are overwriting a
//       marfs object that was *just* created, you might get the same
//       object-ID for the new file.  So, what do we do, have fuse wait for
//       a second?  Return an error?  Neither of those seemed acceptable,
//       so I'm adding the "unique" field.  This will always be zero,
//       except for files that were created as a result of truncating
//       another file of the same name, within the same second.

typedef struct MarFS_XattrPre {

   const MarFS_Repo*      repo;
   const MarFS_Namespace* ns;

   uint16_t           config_vers_maj;  // version of config that file was written with
   uint16_t           config_vers_min;

   MarFS_ObjType      obj_type;     // This will only be { Packed, Fuse, or None }
                                    // see XattrPost for final correct type of object

   CompressionMethod  compression;  // in addition to erasure-coding
   CorrectionMethod   correction;   // (e.g. CRC/checksum/etc.)
   EncryptionMethod   encryption;   // data-encryption (e.g. sha-256)

   ino_t              md_inode;
   time_t             md_ctime;
   time_t             obj_ctime;    // might be mult versions in trash
   uint8_t            unique;       // might be mult versions in trash w/same obj_ctime

   size_t             chunk_size;   // from repo-config at write-time
   size_t             chunk_no;     // 0-based number of current chunk (object)

   // uint16_t           shard;        // TBD: for hashing directories across shard-nodes

   char               bucket[MARFS_MAX_BUCKET_SIZE];
   char               objid [MARFS_MAX_OBJID_SIZE]; // not including bucket

} MarFS_XattrPre;


int print_objname(char* obj_name,      const MarFS_XattrPre* pre);

// from MarFS_XattrPre to string
int pre_2_str(char* pre_str, size_t size, MarFS_XattrPre* pre);

#if 0
// COMMENTED OUT.  This is now replaced by update_pre()  [?]
int pre_2_url(char* url_str, size_t size, MarFS_XattrPre* pre);
#endif

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

// UPDATE: POST is now also where we keep the full-path to the meta-data
// file.  This was formerly kept in PathInfo, but we're moving it into an
// xattr in order to allow a GPFS inode-scan to have direct access to the
// corresponding path, using only info in the xattrs.  expand_path_info()
// used to build the path in PathInfo.md_path.  We could keep it there, and
// copy to PathInfo.post, but that seems wasteful.  Instead, we'll build it
// directly in post.
//
// NOTE: The <chunks> field means different things for different object-types.
//       Multi:  <chunks> is the number of ChunkInfos written in MDFS file
//       Packed: <chunks> is number of files stored in the object

typedef enum {
   POST_TRASH           = 0x01, // file is in trash?
} PostFlags;

typedef uint8_t  PostFlagsType;




typedef struct MarFS_XattrPost {
   uint16_t           config_vers_maj; // redundant w/ config_vers in Pre?
   uint16_t           config_vers_min; // redundant w/ config_vers in Pre?
   MarFS_ObjType      obj_type;      // type of storage
   size_t             obj_offset;    // offset of file in the obj (Packed)
   CorrectInfo        correct_info;  // correctness info  (e.g. the computed checksum)
   EncryptInfo        encrypt_info;  // any info reqd to decrypt the data
   size_t             chunks;        // (context-dependent.  See NOTE)
   size_t             chunk_info_bytes; // total size of chunk-info in MDFS file (Multi)
   char               md_path[MARFS_MAX_MD_PATH]; // full path to MDFS file
   PostFlagsType      flags;
} MarFS_XattrPost;



// from MarFS_XattrPost to string
int post_2_str(char* post_str, size_t size, const MarFS_XattrPost* post, MarFS_Repo* repo);

// from string to MarFS_XattrPost
int str_2_post(MarFS_XattrPost* post, const char* post_str); // from string

int init_post(MarFS_XattrPost* post, MarFS_Namespace* ns, MarFS_Repo* repo);




// TBD: "Shard" will be used to redirect directory paths via hashing to a
// set of shards for each directory.
typedef struct MarFS_XattrShard {
   uint16_t              config_vers_maj;
   uint16_t              config_vers_min;
   // TBD ...
} MarFS_XattrShard;

#define XATTR_SHARD_STRING_VALUE_SIZE  256 /* max */


// from MarFS_XattrShard to string
int shard_2_str(char* shard_str,        const MarFS_XattrShard* shard);

// from string to MarFS_XattrShard
int str_2_shard(MarFS_XattrShard* shard, const char* shard_str); // from string



// ---------------------------------------------------------------------------
// RecoveryInfo
//
// This is a record that has information mostly from stat() of the metadata
// file in human-readable form.  This thing is written directly into the
// object itself.
//
// This is recovery information captured at create time only; it is not
// updated upon metadata changes like chmod, chown, rename, etc.
// Therefore, yes, it is probably out-of-date.  It's purpose is to allow
// regenerating some semblance of the MDFS, using only the contents of
// objects, in the event of a catastrophic meltdown of the MDFS.
//
// NOTE: Now that we are using Scality sproxyd, we have the further problem
//     that you can't easily get a list of "all existing objects".
//     Instead, you'd have to do some grovelling through low-level scality
//     metadata.  But you could do that.
// ---------------------------------------------------------------------------

// OBSOLETE?  The recovery info is just the contents of the Post xattr?
// [Plus the MDFS filename.]  Objects already have (most of) the Pre xattr
// in their obj-id.  What remains unknown is MDFS filename, etc, which in
// in the Post xattr.

typedef struct {
   uint16_t config_vers_maj;
   uint16_t config_vers_min;
   ino_t    inode;
   mode_t   mode;
   uid_t    uid;
   gid_t    gid;
   time_t   mtime;
   time_t   ctime;
   char     mdfs_path[MARFS_MAX_MD_PATH]; // full path in the MDFS
   char     post[MARFS_MAX_POST_STRING_WITHOUT_PATH]; // POST only has path for trash
} RecoveryInfo;

// from RecoveryInfo to string
int rec_2_str(char* rec_info_str, const size_t max_size, const RecoveryInfo* rec_info);

// from string to RecoveryInfo
int str_2_rec(RecoveryInfo* rec_info, const char* rec_info_str); // from string



// ---------------------------------------------------------------------------
// MultiChunkInfo
//
// Multi-type marfs MD files contain a "blob" for each chunk.  Each blob
// specifies the offset of the corresponding object (i.e. position
// represented by the beginning of this object in the stream of data making
// up the total object), the size of data in the object (because there is a
// "recovery" blob at the end of each object which is not part of the
// user-data), the checksum, encryption, and compression keys, etc.
//
// All object-IDs are the same as what's in the Pre xattr, differing only
// in chunk-number, so we don't need to store those.
//
// When writing from fuse, we fill every object to the brim before moving
// on to the next one.  Therefore, for any offset into the user-data, we
// can compute the name of the object containing that data.  If something
// fails during a write, and the write is restarted, we start over from
// scratch, and all previous progress is trashed.
//
// However, pftool wants the ability to restart a large copy, picking up
// where it left off.  It also may do an N:1 style of write, so chunks may
// be written out-of-order, such that the existing set after a failure is
// not contiguous.  Therefore, pftool restart needs a way to figure out
// which chunks were written.  (The fact that not all of them were written
// will be indicated by the presence of a RESTART xattr.)
//
// Because we want to be able to compute an offset for chunk-info, and
// simply seek into the MD file to find it, we want all the fields, and the
// chunk-info-records to have known sizes and offsets.  Therfore, we leave
// out the usual human-readability support, and just write raw binary,
// (in network-byte-order).
// ---------------------------------------------------------------------------

typedef struct {
   uint16_t      config_vers_maj;
   uint16_t      config_vers_min;
   size_t        chunk_no;         // from MarFS_XattrPost.chunk_no
   size_t        logical_offset;   // offset of this chunk in user-data
   size_t        chunk_data_bytes; // not counting recovery-info (at the end)
   CorrectInfo   correct_info;     // from MarFS_XattrPost.correct_info
   EncryptInfo   encrypt_info;     // from MarFS_XattrPost.encrypt_info
} MultiChunkInfo;

ssize_t chunkinfo_2_str(char* str, const size_t max_size, const MultiChunkInfo* chnk);
ssize_t str_2_chunkinfo(MultiChunkInfo* chnk, const char* str, const size_t str_len);






#  ifdef __cplusplus
}
#  endif

#endif  // SERVER_CONFIG_H
