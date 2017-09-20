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

#ifndef _XOPEN_SOURCE
#  define _XOPEN_SOURCE_WAS_UNDEF 1
#  if __STDC_VERSION__ >= 199901L
#    define _XOPEN_SOURCE 700       /* POSIX 2008 */
// #    define _XOPEN_SOURCE 600    /* POSIX 2004 */
#  else
#    define _XOPEN_SOURCE 500       /* POSIX 1995 */
#  endif
#endif

#include <time.h>

#if  _XOPEN_SOURCE_WAS_UNDEF
#  undef _XOPEN_SOURCE
// #  undef __USE_XOPEN
#endif



#include <stdint.h>
#include <stddef.h>             // size_t
#include <sys/types.h>          // ino_t
#include <sys/stat.h>
#include <math.h>               // floorf

#include "logging.h"


#  ifdef __cplusplus
extern "C" {
#  endif


// ---------------------------------------------------------------------------
//                                WARNING
//
// If you change any of these constants, you may also need to increment
// CONFIG_VERS_MINOR/MAJOR.  For example, some of these constants affect
// the way recovery-info is encoded at the end of objects.  For recovery to
// be reliable, changes in the structure of per-object-recovery-info need
// to be reflected in special cases within the parsers, selected via the
// version-number recorded in the recovery-info.  Similarly, there are
// defns here that affect the layout of data in xattr strings, which
// control how MD for a given file is understood, and must be reliably
// parsed based on version-number.
//
// It's possible to make changes that don't require changing the
// version-number, but be careful with assumptions.
// ---------------------------------------------------------------------------


// Not part of the constrained S3 object-names, so any reasonable number will do
#define   MARFS_MAX_HOST_SIZE      128

// extern would not be useable in PathInfo.md_path decl, below, and we
// don't want it dynamically-allocated.  TBD: Be sure the #define is
// associated with the config version.
#define   MARFS_MAX_MD_PATH       1024 /* path in MDFS */
#define   MARFS_MAX_NS_PATH       1024 /* path in namespace */
#define   MARFS_MAX_BUCKET_SIZE     63 /* S3 spec */
#define   MARFS_MAX_OBJID_SIZE     256

// Must fit in an S3 bucket (max 63 chars), with room left for
// namespace-name.  We also leave room for terminal '\0', because this is
// really used to allocate buffers when parsing objid xattr-values.
#define   MARFS_MAX_NS_ALIAS_NAME         16

// Namespace name is no longer constrained as to its length, because it is
// now the NS "alias", rather than the name, which goes into the part of
// the URL that corresponds to an S3 bucket.  However, some users of the
// library would like to know how big a buffer should be, to be big enough
// to hold any namespace name.  One thing we can say about NS name is that
// it must not be bigger than an obj-ID (because it is a part of the
// obj-ID).  So, we'll provide that, and enforce it in
// read_configuration().
#define   MARFS_MAX_NAMESPACE_NAME         MARFS_MAX_OBJID_SIZE


// Allows us to allocate buffers when parsing objid
// xattr-values.  If this is going to go into the
// "bucket" part of the object-ID, then it must fit there, with
// enough room left over to fit MAX_NS_ALIAS_NAME
#define   MARFS_MAX_REPO_NAME   (MARFS_MAX_BUCKET_SIZE - MARFS_MAX_NS_ALIAS_NAME)

// "http://.../<bucket>/<objid>"
#define   MARFS_MAX_URL_SIZE         (10 + MARFS_MAX_HOST_SIZE + 2 + MARFS_MAX_BUCKET_SIZE + MARFS_MAX_OBJID_SIZE)



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
//   <object_type>            { NONE, Uni, Multi, Packed, Striped, Fuse }
//   <compress>               { NONE }
//   <correct>                { NONE }  (TBD?:  cRc, checKsum, Hash, RAID, Erasure)
//   <encrypt>                { NONE }
//   /inode.<inode>           [64-bits as 16 hex digits]
//   /obj_ctime.<obj_ctime>   [see MARFS_DATE_FORMAT]
//   /md_ctime.<md_ctime>     [see MARFS_DATE_FORMAT]


#define NON_SLASH           "%[^/]"
#define NON_DOT             "%[^./]"

// // <repo>.<encoded_namespace>
// #define MARFS_BUCKET_RD_FORMAT  NON_DOT "." NON_SLASH
// #define MARFS_BUCKET_WR_FORMAT  "%s.%s"
#define MARFS_BUCKET_RD_FORMAT  NON_SLASH
#define MARFS_BUCKET_WR_FORMAT  "%s"

// COMAINTAIN: update_pre & posix_dal_open
#define MARFS_OBJID_RD_FORMAT   "%[^/]/ver.%03hu_%03hu/ns.%[^/]/%c%c%c%c/inode.%010ld/md_ctime.%[^/]/obj_ctime.%[^/]/unq.%hhd/chnksz.%lx/chnkno.%lu"
#define MARFS_OBJID_WR_FORMAT   "%s/ver.%03hu_%03hu/ns.%s/%c%c%c%c/inode.%010ld/md_ctime.%s/obj_ctime.%s/unq.%hhd/chnksz.%lx/chnkno.%lu"

// #define MARFS_PRE_RD_FORMAT     MARFS_BUCKET_RD_FORMAT "/" MARFS_OBJID_RD_FORMAT  
#define MARFS_PRE_RD_FORMAT     NON_SLASH "/%s" 

#define MARFS_MAX_PRE_SIZE      (MARFS_MAX_BUCKET_SIZE + 1 + MARFS_MAX_OBJID_SIZE) /* max */



#define MARFS_POST_RD_FORMAT       "ver.%03hu_%03hu/%c/off.%ld/objs.%ld/bytes.%ld/corr.%016lx/crypt.%016lx/flags.%02hhX/mdfs.%[^\t\n]"
#define MARFS_POST_WR_FORMAT       "ver.%03hu_%03hu/%c/off.%ld/objs.%ld/bytes.%ld/corr.%016lx/crypt.%016lx/flags.%02hhX/mdfs.%s"

#define MARFS_MAX_POST_SIZE_WITHOUT_PATH  256 /* max */
#define MARFS_MAX_POST_SIZE               (MARFS_MAX_POST_SIZE_WITHOUT_PATH + MARFS_MAX_MD_PATH)

// This xattr only exists while a file is opened for writing, before
// closing We capture the desired final mode, so that a file without
// write-accessibility can be exist as write-accessible while it is being
// written.  This is necessary in order to allow us to install xattrs on
// the MDFS file.
#define MARFS_RESTART_FORMAT    "ver.%03hu_%03hu/flags.0x%02hhX/mode.oct%06o"
#define MARFS_MAX_RESTART_SIZE  48 /* max */


// first part of the recovery-info
// (if you change this, you should also change MARFS_CONFIG_MAJOR/MINOR)
#define MARFS_REC_HEAD_FORMAT   "HEAD:/ver.%03hu_%03hu/rsize.%08d/dsize.%lu/mode.oct%08o/uid.%d/gid.%d/md_mtime.0x%016lx"
#define MARFS_REC_HEAD_SIZE     256 /* max */

// last part of the recovery-info
// (if you change this, you should also change MARFS_CONFIG_MAJOR/MINOR)
#define MARFS_REC_TAIL_FORMAT   "TAIL:/nfiles.0x%016lx/reclen.0x%016lx"
#define MARFS_REC_TAIL_SIZE     58 /* incl terminal-null, which will be printed */


// This is the part of recovery-info that is written for Uni or Multi, and
// one of these is written for each Packed file in an object.  This doesn't
// include the final RECOVERY_INFO_TAIL, written once per-object, at the
// end of all recovery-info, for Uni, Multi, or Packed.
#define MARFS_REC_BODY_SIZE                     \
   (                                            \
    MARFS_REC_HEAD_SIZE + 1 +                   \
    MARFS_MAX_PRE_SIZE  + 1 +                   \
    MARFS_MAX_POST_SIZE + 1 +                   \
    MARFS_MAX_MD_PATH   + 1                     \
   )


// This is the (max) size of a single recovery-info (e.g. Uni/Multi)
// including the final tail.  In the case of packed, we'd have many of the
// "body" units, with a single "tail" unit.
// 
#define MARFS_REC_UNI_SIZE                      \
   (                                            \
    MARFS_REC_BODY_SIZE +                       \
    MARFS_REC_TAIL_SIZE + 1                     \
   )



// Two files in the trash.  Original MDFS is renamed to the name computed
// in expand_trash_info().  Then another file with the same name, extended
// with this format, has contents that hold the original MDFS path (for
// undelete).  The expand_trash_path() result (in PathInfo.trash_path) is
// printed with this format, to create the companion path, whose contents
// hold the original MDFS path.
#define MARFS_TRASH_COMPANION_SUFFIX ".path"


// // (see comments at MultiChunkInfo, below)
// #define MARFS_MULTI_MD_FORMAT   "ver.%03hu.%03hu,off.%ld,len.%ld,obj.%s\n"




# include "marfs_configuration.h"


// types for correction/encryption info, encoded into obj-iDs
// These would be e.g. the encryption-key, itself (?)
typedef uint64_t            CorrectInfo;
typedef uint64_t            EncryptInfo;


// some things can't be done in common/configuration/src
extern int validate_configuration();


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
                       // Only used in object-ID, not in Post xattr:
   OBJ_FUSE,           //   written by FUSE.   (implies not-packed, maybe uni/multi)
   OBJ_Nto1,           //   written by pftool. (implies not-packed, maybe uni/multi)
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
// Only used for writing recovery-info
// ---------------------------------------------------------------------------

int stat_to_str(char* str, size_t size, const struct stat* st);
int str_to_stat(struct stat* st, const char* str, size_t size);


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
//       object-ID for the new file.  So, do we have fuse wait for a
//       second?  Return an error?  Neither of those seemed acceptable, so
//       I'm adding the "unique" field.  This will always be zero, except
//       for files that were created as a result of truncating another file
//       of the same name, within the same second.

typedef uint8_t PreFlagsType;

typedef enum {
   PF_UPDATED = 0x01            // old stringifications are obsolete
} PreFlags;


typedef struct MarFS_XattrPre {

   const MarFS_Repo*      repo;     // as recorded in an object-ID
   const MarFS_Namespace* ns;       // as recorded in an object-ID

   ConfigVersType     config_vers_maj;  // version of config that file was written with
   ConfigVersType     config_vers_min;

   MarFS_ObjType      obj_type;     // This will only be { Packed, Fuse, Nto1, or None }
                                    // see XattrPost for final correct type of object

   MarFS_CompType     compression;  // in addition to erasure-coding
   MarFS_CorrectType  correction;   // (e.g. CRC/checksum/etc.)
   MarFS_EncryptType  encryption;   // data-encryption (e.g. sha-256)

   ino_t              md_inode;
   time_t             md_ctime;
   time_t             obj_ctime;    // might be mult versions in trash
   uint8_t            unique;       // might be mult versions in trash w/same obj_ctime

   size_t             chunk_size;   // from repo-config at write-time
   size_t             chunk_no;     // 0-based number of current chunk (object)

   // uint16_t           shard;        // TBD: for hashing directories across shard-nodes
   PreFlagsType       flags;

   // for randomized IP-addresses in configurations with host_count > 1
   unsigned int       seed;         // for randomization of hosts
   unsigned int       hostname_hash;// subverts randomization, if non-zero
   char               host  [MARFS_MAX_HOST_SIZE];

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
              const struct stat* st);

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
   ConfigVersType     config_vers_maj; // redundant w/ config_vers in Pre?
   ConfigVersType     config_vers_min; // redundant w/ config_vers in Pre?
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
int post_2_str(char* post_str, size_t size, const MarFS_XattrPost* post, const MarFS_Repo* repo, int add_md_path);

// from string to MarFS_XattrPost
int str_2_post(MarFS_XattrPost* post, const char* post_str, uint8_t reset, int parse_md_path);

int init_post(MarFS_XattrPost* post, MarFS_Namespace* ns, MarFS_Repo* repo);






// Restart.
//
// The mere presence of a restart xattr was formerly sufficient to indicate
// that the corresponding file was not yet complete.  The xattr was
// installed at open(), and removed at close(), so its presence signalled
// that the file hadn't (yet) been properly closed.  This allows us to
// forbid access to incompletely-written files (or possibly to know when
// you are trying to open a file that is still being written).
//
// We just gave open() the same mode-bits that the user wanted the file to
// have.  But, if those mode-bits were for read-only, then it breaks MarFS
// ability to put xattrs on the file.  So, new scheme, in such cases, we
// store the final mode-bits in the restart xattr, and do open for writing
// with writable-access, so we can continue to manipulate xattrs while the
// file is open.  The xattr keeps the final mode, which is installed at
// close().

typedef enum {
   RESTART_OLD        = 0x01,
   RESTART_MODE_VALID = 0x02,
} RestartFlags;

typedef uint8_t  RestartFlagsType;


typedef struct MarFS_XattrRestart {
   ConfigVersType     config_vers_maj; // redundant w/ config_vers in Pre?
   ConfigVersType     config_vers_min; // redundant w/ config_vers in Pre?
   mode_t             mode;
   RestartFlagsType   flags;
} MarFS_XattrRestart;



int init_restart(MarFS_XattrRestart* restart);

int restart_2_str(char*                     restart_str,
                  size_t                    max_size,
                  const MarFS_XattrRestart* restart);

int str_2_restart(MarFS_XattrRestart* restart, const char* restart_str);






// TBD: "Shard" will be used to redirect directory paths via hashing to a
// set of shards for each directory.
typedef struct MarFS_XattrShard {
   ConfigVersType     config_vers_maj;
   ConfigVersType     config_vers_min;
   // TBD ...
} MarFS_XattrShard;

#define XATTR_SHARD_STRING_VALUE_SIZE  256 /* max */


// from MarFS_XattrShard to string
int shard_2_str(char* shard_str,        const MarFS_XattrShard* shard);

// from string to MarFS_XattrShard
int str_2_shard(MarFS_XattrShard* shard, const char* shard_str);



#if 0
// COMMENTED OUT.  The RecoveryInfo structure is not used during writing of
// recovery-info.  For that, see write_recoveryinfo(), in common.c.
//
// We don't yet support actually reading recovery-info.  During recovery,
// we would want to quickly load the stored recovery-info into a struct.
// We might only be looking to recover files with specific metadata
// properties.  This structure would be used for that purpose.



// ---------------------------------------------------------------------------
// RecoveryInfo
//
// We capture some metadata info at file-creation time, and store it in the
// object-store, along with the user data.  The purpose is to allow
// regeneration of the key metadata associated with a user's file, should
// the MDFS be damaged or destroyed.  This is only information as it
// existed at creation-time; it isn't maintained during
// chmod/chown/rename/etc.
//
// Recovery-info is currently meant to be human-readable, so it's all just
// ASCII text.  Someday, we can consider optimizing storage of this data
// (and potentially recovery), by saving it in a binary format (e.g. a
// formatted set of network-byte-order values.  In order to avoid any
// dependence on compiler-versions, to determine the specific layout or
// size of the recovery-info, we write it all by hand, in
// write_recoveryinfo().
//
// PACKED objects keep their per-object recovery-info at the tail of their
// local data, within in the larger object, when they are packed together.
// (i.e. [data1][recovery1][packed2][recovery2]...)  This allows packing to
// be done without any reprocessing of the recovery info for the packed
// objects, which remains correct.
//
// Recovery-information is captured at create-time; it is not updated upon
// metadata changes like chmod, chown, rename, etc.  Therefore, yes, it is
// probably out-of-date.  Its purpose is to allow regenerating some
// semblance of the MDFS, using only the contents of objects, in the event
// of a catastrophic meltdown of the MDFS.  It would be disastrous
// performance implications to try to maintain it, after creation.
//
//
// TBD:
//
// We don't actually have any support in place for reading recovery-info
// from objects.  When we do, this structure would be populated with data
// retrieved from the recovery-info in an object.
//
// We don't necessarily want to go to the expense of parsing the PRE and
// POST xattr strings into Pre and Post structs (e.g. if we only want to
// restore certain pathnames, then we only need that parsing after we've
// found a matching pathname.
//
// So, there would probably be two levels of operation for this struct:
//
// (a) capture the strings from the recovery info, without any
//     dynamic-allocation (e.g. just using the buffer holding recovery-info
//     you got form the object.
//
// (b) parse out and initalize Pre, Post, and stat structs, from this info.
//
//
// FORMAT:
//
// // Our goal is to write a fixed size of data, in a format we can read-back,
// // regardless of compiler or compiler-version.  The data is the members of
// // the RecoveryInfo struct, each converted to network-byte-order.  The
// // actual struct generated by the compiler may involve some alignment
// // padding, but padding and (therefore) the size of the struct could change
// // between compilers, or compiler versions.  So, we don't want to just write
// // the serialized structure contents (because they would be byte-order
// // specific), nor even all the fields as individual network-byte-order
// // values (because the padding might be compiler-specific).
// //
// // Therefore, we define a static size which is big enough to store all the
// // members of the struct, and we save them out as a series of
// // correctly-sized, unaligned, network-byte-order bytes, filling that size.
// // We put our own padding at a known spot in the middle.  (See
// // recoveryinfo_2_str(), and str_2_recoveryinfo().)
// //
// // We also need a fixed size that this storage will require, so that
// // e.g. marfs_write() can reserve exactly the proper number of bytes at the
// // tail-end of an object-stream, such that the addition of the RecoveryInfo
// // will fill up the object to exactly Repo.chunk_size, specified in the
// // configuration.
// //
// // We need known values to appear at the very beginning and the very end of
// // the stored data (i.e. version-numbers at the beginning, and data-sizes
// // at the very end), as explained in the RECOVERY section.
// //
// // The simplest thing is to use sizeof(RecoveryInfo) as the criteria for
// // how much storage is needed, though the objects may actually take up
// // less, and insert our own custom padding in a way that doesn't depend on
// // the compiler's layout of structs.
//
//
// RECOVERY:
//
// In the event that metadata is lost, we can recover it in a limited way
// (MD as it existed at the time objects were written), as follows:
//
//   (a) Read the last 8 bytes of data in the object. This holds the size
//       of the RecoveryInfo.
//
//   (b) Read the next-to-last 8 bytes of data.  This holds the size of the
//       data itself (not counting RecoveryInfo).
//
//   (c) Skip to the beginning of the recovery-info.  This points to stored
//       software version numbers.  Call str_2_recoveryinfo().  This will
//       use the version-numbers to understand how to recover the rest of
//       the stored recovery-info into a RecoveryInfo struct.
//
//   (d) Skip to the beginning of the data.  (Using info from step (b).)
//       You can now generate metadata for this object, using the
//       RecoveryInfo.
//
//   (e) If this is a packed object, step (d) might not have moved all the
//       way to the beginning of the object-data.  In that case, go to step
//       (a), again.
//    
//
// NOTE: Now that we are using Scality sproxyd, we have the further problem
//     that you can't easily get a list of "all existing objects".
//     Instead, you'd have to do some grovelling through low-level scality
//     metadata.  But you could do that.
//
// NOTE:  We actually save the object-ID.  Why?  Presumably, if you can read
//     the recovery-info out of the object, then you already have the object-ID, right?
//     In some object-systems (e.g. sproxyd), the user-level object-ID is hashed (etc)
//     to produce the internal key, which is the ID used internally.  Our object-IDs
//     encode some information
//
//

// ---------------------------------------------------------------------------

// We're "hardcoding" the types, to guarantee we know how big each member
// is.  Shouldn't be a problem if this size is larger than the size your
// compiler uses for the corresponding type (e.g. gid_t), as long as you
// aren't trying to recover (e.g.) an inode on a machine that says inode_t
// is 32-bits, where the recovery-info actually has more than 32
// significant digits.
//
// *** WARNING: If you change any of these, you MUST also change
// MARFS_CONFIG_MAJOR/MINOR, in marfs_configuration.h, and the
// recoveryinf_2_str() / str_2_recoveryinfo() functions, to handle the new
// version.
//
typedef struct {
   ConfigVersType config_vers_maj;
   ConfigVersType config_vers_min;
   mode_t         mode;               //    assumed uint32_t
   uid_t          uid;                //    assumed uint32_t
   gid_t          gid;                //    assumed uint32_t
   time_t         mtime;              //    assumed uint64_t

   // NOTE: It's possible someone could want to change the pre-defined size
   //       of e.g. info.md_path.  Then, we might be hosed, if we need to
   //       read previously-written recovery-info into this struct.  We
   //       could protect against this problem, by putting warnings around
   //       the #defines of these sizes: "if you change these sizes, you
   //       must also change MARFS_CONFIG_MARJOR/MINOR, and make changes in
   //       str_2_secoveryinfo() to accomodate the different versions."
   //
   //       But that's ugly, and error-prone.  Instead, we'll store them as
   //       string-pointer.  If you are ever doing recovery, you'll
   //       probably be glad if recoveryinfo_2_str() is just easy, and it
   //       just works.

   //   char     mdfs_path[MARFS_MAX_MD_PATH]; // full path in the MDFS
   //   char     pre[MARFS_MAX_BUCKET_SIZE + MARFS_MAX_OBJID_SIZE]; // obj-ID
   //   char     post[MARFS_MAX_POST_STRING_WITHOUT_PATH]; // POST only has path for trash

   //   char*           mdfs_path; // full path in the MDFS   (*** don't free() this)
   //   MarFS_XattrPre  pre; // obj-ID
   //   MarFS_XattrPost post; // POST only has path for trash

   // call str_2_pre() and str_2_post(), if you want to examine the
   // contents of the pre/post strings.

   char*         mdfs_path; // full path in the MDFS        (*** don't free() this)
   char*         pre_str;   // obj-ID                       (*** don't free() this)
   char*         post_str;  // POST only has path for trash (*** don't free() this)

   // the pointers above just point into this.
   char*          str_data;
   
   // NOTE: We are depending on the following being the last data in the
   //       structure generated by the compiler.  This is important because
   //       these will be written into the tail of all objects, and
   //       recovery of a lost MDFS will depend on reading these two values
   //       correctly, so the rest of the recovery-info can be discovered.
   //       They are aligned, and end on an aligned boundary, so the
   //       compiler should have no need to add padding after them.  Right?
   //
   //       We currently only have gcc 4.4.7, so we can't use '-std=c11'
   //       and do a static, compile-time check (e.g. with "static_assert
   //       (sizeof(RecoveryInfo) == ...").  Therefore, we really should
   //       validate this dynamically ... maybe in validate_config()?
   //     
   uint64_t       user_data_size;      // user-data, in this object only
   uint64_t       rec_data_off;        // recovery-info data, offset in object

} RecoveryInfo;



// // from RecoveryInfo to string
// int recoveryinfo_2_str(char* rec_info_str, const size_t max_size, const RecoveryInfo* rec_info);

// from string to RecoveryInfo
int str_2_recoveryinfo(RecoveryInfo* rec_info, const char* rec_str, size_t str_size);

#endif



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
   ConfigVersType config_vers_maj;
   ConfigVersType config_vers_min;
   size_t         chunk_no;         // from MarFS_XattrPost.chunk_no
   size_t         logical_offset;   // offset of this chunk in user-data
   size_t         chunk_data_bytes; // not counting recovery-info (at the end)
   CorrectInfo    correct_info;     // from MarFS_XattrPost.correct_info
   EncryptInfo    encrypt_info;     // from MarFS_XattrPost.encrypt_info
} MultiChunkInfo;

ssize_t chunkinfo_2_str(char* str, const size_t max_size, const MultiChunkInfo* chnk);
ssize_t str_2_chunkinfo(MultiChunkInfo* chnk, const char* str, const size_t str_len);






#  ifdef __cplusplus
}
#  endif

#endif  // SERVER_CONFIG_H
