#ifndef MARFS_BASE_STATIC_CONFIG_H
#define MARFS_BASE_STATIC_CONFIG_H

// THIS IS TEMPORARY.  Brett's marfs_configuration.h replaces a lot of
// these decls/defns, and introduces a real config-reader (instead of our
// static one).  If you use marfs_configuration.h,



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



#  ifdef __cplusplus
extern "C" {
#  endif

// These types are used in xattrs, so they need corresponding "lookup"
// functions to convert strings to enumeration values.

// TBD ... compression is applied before erasure-coding?
// NOTE: You must co-maintain string-constants in encode/decode_compression()
typedef enum {
   COMPRESS_NONE = 0,
} CompressionMethod;

extern CompressionMethod lookup_compression(const char* compression_name);

extern char              encode_compression(CompressionMethod type);
extern CompressionMethod decode_compression(char);



// error-correction
// NOTE: You must co-maintain string-constants in encode/decode_correction()
typedef enum {
   CORRECT_NONE = 0,
} CorrectionMethod;

typedef uint64_t CorrectInfo;   // e.g. checksum

extern CorrectionMethod lookup_correction(const char* correction_name);

extern char             encode_correction(CorrectionMethod meth);
extern CorrectionMethod decode_correction(char);




// TBD: object-encryption
// Cf. Repo.authentication, which is not the same thing
// NOTE: You must co-maintain string-constants in encode/decode_encryption()
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

// typedef enum {
//    REPO_ONLINE          = 0x01, // repo is online?
//    REPO_ALLOWS_OFFLINE  = 0x02, // repo allows offline?
//    REPO_UPDATE_IN_PLACE = 0x04, // repo allows update-in-place ?
//    REPO_SSL             = 0x08  // e.g. use https://...
// } RepoFlags;
// 
// typedef uint8_t  RepoFlagsType;

typedef uint8_t MarFS_Bool;


typedef enum {
   ACCESSMETHOD_DIRECT = 0,            // data stored directly into MD files
   ACCESSMETHOD_SEMI_DIRECT,           // data stored in separate FS, w/ reference from MD xattr
   ACCESSMETHOD_CDMI,
   ACCESSMETHOD_SPROXYD,               // should include installed release version
   ACCESSMETHOD_S3,
   ACCESSMETHOD_S3_SCALITY,            // should include installed release version
   ACCESSMETHOD_S3_EMC,                // should include installed release version
} MarFS_AccessMethod;

#define ACCESSMETHOD_IS_S3(ACCESSMETHOD)  ((ACCESSMETHOD) & (ACCESSMETHOD_S3 | ACCESSMETHOD_S3_SCALITY | ACCESSMETHOD_S3_EMC | ACCESSMETHOD_SPROXYD))



typedef enum {
   AUTH_NONE = 0,
   AUTH_S3_AWS_USER,            // AWS standard, with per-user keys
   AUTH_S3_AWS_MASTER,          // AWS standard, with a private "master" key
   AUTH_S3_PER_OBJ,             // (TBD) server enforces per-object-key
} MarFSAuthMethod;


// We allow <bkt_name> to be different from the top-level name, so that
// names can be descriptive, whereas bucket-name may be constrained in some
// way (e.g. for Scality sproxyd, the fastcgi module is configured to use
// the first component of the URL to match a local path, which redirects to
// the sproxyd daemon.  We might not have thought to give this a name that
// is meaningful, and/or several repos could have the same name -- defaults
// to "/proxy".)
//
// NOTE: In the case where access_method == SEMI_DIRECT, <host> holds a
//     directory where a scatter-tree will hold the semi-direct storage.

typedef struct MarFS_Repo {
   const char*         name;         // (logical) name for this repo 
   const char*         host;         // e.g. "10.140.0.15:9020"

   //   RepoFlagsType       flags;
   MarFS_Bool          ssl;
   MarFS_Bool          update_in_place; // repo allows overwriting parts of data?
   MarFS_Bool          is_online;

   MarFS_AccessMethod  access_method;
   size_t              chunk_size;   // max Uni-object (Cf. Namespace.range_list)
   size_t              pack_size;    // max (?) size for packed objects
   MarFSAuthMethod     auth;         // (current) authentication method for this repo
   CompressionMethod   compression;  // compression type
   CorrectionMethod    correction;   // correctness type  (like CRC/checksum/etc.)
   EncryptionMethod    encryption;   // (current) encryption method for this repo
   uint32_t            latency_ms;   // max time to wait for a response 
   char*               online_cmds;  // command(s) to bring repo online
   size_t              online_cmds_len;
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


// NOTE: Do NOT include a final slash.
extern char*  MarFS_mnt_top;        // top level mount point for fuse/pftool
extern size_t MarFS_mnt_top_len;    // strlen(MarFS_mnt_top)

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
//
// NOTE: For Scality sproxyd, we assume that namespace.name is identical to
//     the "driver-alias" used in sproxyd requests.  This is configured in
//     /etc/sproxyd.conf on the repo server, as the alias of a given
//     "driver" for "by-path" access.  This means that the mount-suffix
//     used here actually selects which sproxyd driver is to be used.b

typedef struct MarFS_Namespace {

   const char*        name;
   const char*        mnt_path;   // the part of path below MarFS_mnt_top
   const char*        md_path;    // path of (root of) corresponding MD FS
   const char*        trash_md_path; // MDFS trash goes under here
   const char*        fsinfo_path;// path is trunc'ed to show global FS usage

   size_t             name_len;       // computed at config-load time
   size_t             mnt_path_len;   // computed at config-load time
   size_t             md_path_len;    // computed at config-load time

   MarFS_Perms        iperms;   // RM/WM/RD/WD bits, for interactive (fuse)
   MarFS_Perms        bperms;   // RM/WM/RD/WD bits, for batch (pftool)

   MarFS_Repo*        iwrite_repo;     // final size unknown ("interactive" = fuse)
   RangeList*         range_list;      // repos for different file-sizes (pftool)

   uint8_t            dirty_pack_percent;   // percent dirty (observed)
   uint8_t            dirty_pack_threshold; // pftool repack if percentage above this

   size_t             quota_space;       // space-quota in space_units
   size_t             quota_names;       // name-quota in name_units

   const char*        shard_path;  // where the shards are
   uint32_t           shard_count; // number of shards, at shard_path
}  MarFS_Namespace;



// ---------------------------------------------------------------------------
// configuration-file
// ---------------------------------------------------------------------------

// TBD: parse config-file, instantiate B-trees of MarFS_Namespace and
//      MarFS_Repo.
//
// QUES: do we really want to join these into one massive table, where
//       there is a row for every NS x Repo ?

#define CONFIG_DEFAULT  "~/marfs.config"

extern int              read_config(const char* config_fname);



// ...........................................................................
// NAMESPACES
// ...........................................................................

extern MarFS_Namespace* find_namespace_by_name    (const char* name);
extern MarFS_Namespace* find_namespace_by_mnt_path(const char* path);


// --- support for traversing namespaces (without knowing how they are stored)
//
// For example: here's some code to walk all Namespaces, doing something
//
//   NSIterator it = namespace_iterator();
//   MarFS_Namespace*  ns;
//   while (ns = namespace_next(&it)) {
//      ... // do something
//   }

typedef struct {
  size_t pos;
} NSIterator;

NSIterator        namespace_iterator();
MarFS_Namespace*  namespace_next(NSIterator* it);

// typedef int (*NSFunction)(MarFS_Namespace* ns); // ptr to fn that takes NS, returns int
// extern int apply_to_namespaces(NSFunction fn);


// ...........................................................................
// REPOS
// ...........................................................................

extern MarFS_Repo*      find_repo(MarFS_Namespace* ns,
                                  size_t           file_size,
                                  int              interactive_write); // bool
extern MarFS_Repo*      find_repo_by_name(const char* name);


// --- support for traversing repos (without knowing how they are stored)
//
// For example: here's some code to walk all Repos, doing something
//
//   RepoIterator it = repo_iterator();
//   MarFS_Repo*  repo;
//   while (repo = repo_next(&it)) {
//      ... // do something
//   }

typedef struct {
  size_t pos;
} RepoIterator;

extern RepoIterator     repo_iterator();
extern MarFS_Repo*      repo_next(RepoIterator* it);

// typedef int (*RepoFunction)(MarFS_Repo* repo); // ptr to fn that takes Repo, returns int
// extern int apply_to_repos(RepoFunction fn);



#  ifdef __cplusplus
}
#  endif

#endif // MARFS_BASE_STATIC_CONFIG_H
