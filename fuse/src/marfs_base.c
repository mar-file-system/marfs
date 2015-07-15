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

#include "marfs_base.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <arpa/inet.h>          // htonl(), etc


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
//       LOG(LOG_ERR, "Unrecognized obj_type: %u\n", type);
//       exit(1);
//    }
// }

MarFS_ObjType lookup_obj_type(const char* token) {
   if      (! strcmp(token, "none"))     return OBJ_NONE;
   else if (! strcmp(token, "uni"))      return OBJ_UNI;
   else if (! strcmp(token, "multi"))    return OBJ_MULTI;
   else if (! strcmp(token, "packed"))   return OBJ_PACKED;
   else if (! strcmp(token, "striped"))  return OBJ_STRIPED;

   LOG(LOG_ERR, "Unrecognized obj_type: %s\n", token);
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

   LOG(LOG_ERR, "Unrecognized correction_method: %s\n", token);
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
DEFINE_ENCODE(obj_type, MarFS_ObjType, "_UMPSF");
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


// NOTE: <src> and <dest> can be the same
// NOTE: we assume load_config() guarantees that no namespace contains '-'
int encode_namespace(char* dst, char* src) {
   int i;
   for (i=0; src[i]; ++i) {
      dst[i] = ((src[i] == '/') ? '-' : src[i]); // change '/' to '-'
   }
   return 0;
}
int decode_namespace(char* dst, char* src) {
   int i;
   for (i=0; src[i]; ++i) {
      dst[i] = ((src[i] == '-') ? '/' : src[i]); // change '-' to '/'
   }
   return 0;
}



// ---------------------------------------------------------------------------
// Standardized date/time stringification
// ---------------------------------------------------------------------------


// See comments above MARFS_DATA_FORMAT (in marfs_base.h)
int epoch_to_str(char* str, size_t size, const time_t* time) {
   struct tm tm;

   // DEBUGGING
   LOG(LOG_INFO, "* epoch_to_str epoch:            %016lx\n", *time);

   // time_t -> struct tm
   if (! localtime_r(time, &tm)) {
      LOG(LOG_ERR, "localtime_r failed: %s\n", strerror(errno));
      return -1;
   }

   // DEBUGGING
   __attribute__ ((unused)) struct tm* dbg = &tm;
   LOG(LOG_INFO, "* epoch_2_str localtime:         %4d-%02d-%02d %02d:%02d:%02d (%d)\n",
           1900+(dbg->tm_year),
           dbg->tm_mon,
           dbg->tm_mday,
           dbg->tm_hour,
           dbg->tm_min,
           dbg->tm_sec,
           dbg->tm_isdst);

   // struct tm -> string
   size_t strf_size = strftime(str, size, MARFS_DATE_FORMAT, &tm);
   if (! strf_size) {
      LOG(LOG_ERR, "strftime failed even more than usual: %s\n", strerror(errno));
      return -1;
   }

   // DEBUGGING
   LOG(LOG_INFO, "* epoch_2_str to-string (1)      %s\n", str);

   // add DST indicator
   snprintf(str+strf_size, size-strf_size, MARFS_DST_FORMAT, tm.tm_isdst);

   // DEBUGGING
   LOG(LOG_INFO, "* epoch_2_str to-string (2)      %s\n", str);

   return 0;
}


int str_to_epoch(time_t* time, const char* str, size_t size) {
   struct tm tm;
   //   memset(&tm, 0, sizeof(tm)); // DEBUGGING

   LOG(LOG_INFO, "* str_to_epoch str:              %s\n", str);

   char* time_str_ptr = strptime(str, MARFS_DATE_FORMAT, &tm);
   if (!time_str_ptr) {
      LOG(LOG_ERR, "strptime failed (1): %s\n", strerror(errno));
      return -1;
   }
   else if (*time_str_ptr) {

      // parse DST indicator
      if (sscanf(time_str_ptr, MARFS_DST_FORMAT, &tm.tm_isdst) != 1) {
         LOG(LOG_ERR, "sscanf failed, at '...%s': %s\n", time_str_ptr, strerror(errno));
         return -1;
      }
   }
   else {
      LOG(LOG_ERR, "expected DST, after time-string, at '...%s'\n", time_str_ptr);
      return -1;
   }

   // DEBUGGING
   __attribute__ ((unused)) struct tm* dbg = &tm;
   LOG(LOG_INFO, "* str_to_epoch from string: (1)  %4d-%02d-%02d %02d:%02d:%02d (%d)\n",
           1900+(dbg->tm_year),
           dbg->tm_mon,
           dbg->tm_mday,
           dbg->tm_hour,
           dbg->tm_min,
           dbg->tm_sec,
           dbg->tm_isdst);

   // struct tm -> epoch
   *time = mktime(&tm);

   // DEBUGGING
   LOG(LOG_INFO, "* str_to_epoch epoch:            %016lx\n", *time);

   return 0;
}

// ---------------------------------------------------------------------------
// xattrs
// ---------------------------------------------------------------------------


// Fill in fields, construct new obj-ID, etc.  This is called to initialize
// a brand new MarFS_XattrPre, in the case where a file didn't already have
// one.
//
// ino_t is ultimately __ULONGWORD, which should never be more than 64-bits
// (in the current world).
//
// NOTE: We wanted to indicate whether obj-type is PACKED or not, in the
//       obj-ID.  This should just be the encoded object-type, same as what
//       is found in the Post xattr.  However, at the time we are
//       initializing the Pre struct, and constructing the object-ID, we
//       might not know the final object-type.  (pftool will know, but fuse
//       won't know whether the object will ultimately be UNI or MULTI.
//       However, it will know that the object-type is not packed.)  If the
//       caller is pftool, it can pass in the final type, to become part of
//       the object-name.  If fuse is calling, it can assign type FUSE.
//       This allows reconstruction (from object-IDs alone) to know whether
//       the object is packed, without requiring fuse to know all the
//       details of the object-type.  If you're looking at xattrs, the
//       result of object-ID in the Pre xattr may indicate FUSE-type, but
//       Post is where you should be looking for the object-type.
//
//       This also offers a way for restart to identify objects that were
//       being written via fuse.
//
int init_pre(MarFS_XattrPre*        pre,
             MarFS_ObjType          obj_type, /* see NOTE */
             const MarFS_Namespace* ns,
             const MarFS_Repo*      repo,
             const struct stat*     st) {

   time_t now = time(NULL);     /* for obj_ctime */
   if (now == (time_t)-1)
      return errno;

   // --- initialize fields in info.pre
   pre->repo         = repo;
   pre->ns           = ns;

   pre->config_vers  = MarFS_config_vers;

   pre->obj_type     = obj_type;
   pre->compression  = ns->compression;
   pre->correction   = ns->correction;
   pre->encryption   = repo->encryption;

   pre->md_inode     = st->st_ino;
   pre->md_ctime     = st->st_ctime;
   pre->obj_ctime    = now;     /* TBD: update with info from HEAD request */

   pre->chunk_size   = repo->chunk_size;
   pre->chunk_no     = 0;

   // pre->shard = ...;    // TBD: for hashing directories across shard nodes

   // generate bucket and obj-id
   return update_pre(pre);
}



// before writing an xattr value-string, and maybe before opening an object
// connection, we may want to update the bucket and objid strings, in case
// any fields have changed.
//
// For example, maybe you truncated a MarFS object, which actually copied
// it to the trash.  Now, you're writing a new object, so you have a new
// Pre.obj_ctime.  The obj-id for that object should reflect this new
// ctime, not the one it had when you parsed the orginal obj-id.
//
// Or maybe marfs_write was about to exceed Pre.chunk_size, so you closed
// off one object, updated the chunk_no, and now you want to regenerate the
// obj-id for the new chunk, so you can generate a new URL, for the
// ObjectStream.  [*** WAIT!  In this case, we are supposed to keep the
// same object-id, all-except for the chunk-number.  *** OKAY: What we're
// doing here is updaing the objid from the struct-member-values.  If the
// ctimes haven't been changed, then that part of the objid wont change.
// In this case, you'd just be changing the chunk-number, to get the new
// objid.]
//
// NOTE: S3 requires the bucket and object-name to be separate strings.
//       But Scality sproxyd treats them as a single string.  Because they
//       must have the capability of being separated, we store them
//       separately.
//
// NOTE: Packed objects are created externally, but read by fuse.  They
//       can't be presumed to have an indoe that matches the actual inode
//       of the MDFS file.  Therefore, in the case of packed, we should
//       avoid updating that field.  (However, because we're using an
//       sprintf() we'll just assume that the "inode" for packed files is
//       always zero.)

int update_pre(MarFS_XattrPre* pre) {

   // --- generate bucket-name

   int write_count;
   write_count = snprintf(pre->bucket, MARFS_MAX_BUCKET_SIZE,
                          MARFS_BUCKET_WR_FORMAT,
                          pre->repo->name);
   if (write_count < 0)
      return errno;
   if (write_count == MARFS_MAX_BUCKET_SIZE) /* overflow */
      return EINVAL;


   // --- generate obj-id

   //   // convert '/' to '-' in namespace-name
   //   char ns_name[MARFS_MAX_NAMESPACE_NAME];
   //   if (encode_namespace(ns_name, (char*)pre->ns->mnt_suffix))
   //      return EINVAL;

   // config-version major and minor
   int major = (int)floorf(pre->config_vers);
   int minor = (int)floorf((pre->config_vers - major) * 1000.f);

   char type     = encode_obj_type(pre->obj_type);
   char compress = encode_compression(pre->compression);
   char correct  = encode_correction(pre->correction);
   char encrypt  = encode_encryption(pre->encryption);

   // PACKED objects have a real inode-value in their object-ID (in order
   // to assure unique-ness).  But it might not match this particular file.
   // Therefore, for packed objects, we should leave it as is.

   // prepare date-string components
   char md_ctime_str[MARFS_DATE_STRING_MAX];
   char obj_ctime_str[MARFS_DATE_STRING_MAX];

   if (epoch_to_str(md_ctime_str, MARFS_DATE_STRING_MAX, &pre->md_ctime)) {
      LOG(LOG_ERR, "error converting Pre.md_time to string\n");
      return -1;
   }
   if (epoch_to_str(obj_ctime_str, MARFS_DATE_STRING_MAX, &pre->obj_ctime)) {
      LOG(LOG_ERR, "error converting Pre.md_time to string\n");
      return -1;
   }

   // put all components together
   write_count = snprintf(pre->objid, MARFS_MAX_OBJID_SIZE,
                          MARFS_OBJID_WR_FORMAT,
                          pre->ns->name /* ns_name */ ,
                          major, minor,
                          type, compress, correct, encrypt,
                          (uint64_t)pre->md_inode,
                          md_ctime_str, obj_ctime_str,
                          pre->chunk_size, pre->chunk_no);
   if (write_count < 0)
      return errno;
   if (write_count >= MARFS_MAX_OBJID_SIZE) /* overflow */
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
int pre_2_str(char* pre_str, size_t max_size, MarFS_XattrPre* pre) {

   // contents may have changed
   update_pre(pre);

   int write_count = snprintf(pre_str, max_size,
                              "%s/%s",
                              pre->bucket,
                              pre->objid);
   if (write_count < 0)
      return errno;
   if (write_count == max_size)   /* overflow */
      return EINVAL;

   return 0;
}



int pre_2_url(char* pre_str, size_t max_size, MarFS_XattrPre* pre) {

   // contents may have changed
   update_pre(pre);

   int write_count = snprintf(pre_str, max_size,
                              "%s://%s/%s/%s",
                              ((pre->repo->flags & REPO_SSL) ? "https" : "http"),
                              pre->repo->host,
                              pre->bucket,
                              pre->objid);

   if (write_count < 0)         // (errno set?)
      return -1;                
   if (write_count == max_size) { // overflow
      errno = EINVAL;
      return -1;
   }
   return 0;
}



// parse an xattr-value string into a MarFS_XattrPre
//
// If <has_objid> is non-zero, caller has already populated Pre.bucket and
// Pre.objid.
//
// NOTE: For now, the string really is nothing more than the object ID.
//       However, that could possibly change at some point.  (e.g. we might
//       have some fields we want to add tot he XattrPre xattr-value,
//       without requiring they be added to the object-id.)
//
// NOTE: strptime() and strftime() are crapulous, regarding DST and
//       time-zones.  If strptime() decides you are in DST, and you use
//       "%z", then strptime() will simply adjust the time-zone east by an
//       hour.  This is a really stoopid thing to do, because strftime()
//       

int str_2_pre(MarFS_XattrPre*    pre,
              const char*        pre_str, // i.e. an xattr-value
              const struct stat* st) {

   // parse bucket and objid separately
   int read_count;

   read_count = sscanf(pre_str, MARFS_PRE_RD_FORMAT,
                       pre->bucket,
                       pre->objid);
   if (read_count == EOF)       // errno is set (?)
      return -1;
   if (read_count != 2) {
      errno = EINVAL;
      return -1;
   }


   int   major;
   int   minor;

   char  obj_type;              /* ignored, see NOTE above init_pre() */
   char  compress;
   char  correct;
   char  encrypt;

   ino_t md_inode;

   size_t chunk_size;
   size_t chunk_no;

   char md_ctime_str[MARFS_DATE_STRING_MAX];
   char obj_ctime_str[MARFS_DATE_STRING_MAX];

   // --- parse bucket components

   // NOTE: We put repo first, because it seems less-likely we'll want a
   //       dot in a repo-name, than in a namespace, and we're using dot as
   //       a delimiter.  It will still be easy to construct
   //       delimiter-based S3 commands, to search for all entries with a
   //       given namespace, in a known repo.
   char repo_name[MARFS_MAX_REPO_NAME];

   read_count = sscanf(pre->bucket, MARFS_BUCKET_RD_FORMAT,
                       repo_name);

   if (read_count == EOF)       // errno is set (?)
      return -1;
   else if (read_count != 1) {
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // --- parse "obj-id" components (i.e. the part below bucket)

   // Holds namespace-name from obj-id, so we can decode_namespace(), then
   // find the corresponding namespace, for Pre.ns.  Do we ever care about
   // this?  The only reason we need it is because update_pre() uses it to
   // re-encode the bucket string.
   char  ns_name[MARFS_MAX_NAMESPACE_NAME];
   
   read_count = sscanf(pre->objid, MARFS_OBJID_RD_FORMAT,
                       ns_name,
                       &major, &minor,
                       &obj_type, &compress, &correct, &encrypt,
                       &md_inode,
                       md_ctime_str, obj_ctime_str,
                       &chunk_size, &chunk_no);

   if (read_count == EOF)       // errno is set (?)
      return -1;
   else if (read_count != 12) {
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // --- conversions and validation

   // find repo from repo-name
   MarFS_Repo* repo = find_repo_by_name(repo_name);
   if (! repo) {
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // find namespace from namespace-name
   //
   //   if (decode_namespace(ns_name, ns_name)) {
   //      errno = EINVAL;
   //      return -1;
   //   }
   MarFS_Namespace* ns = find_namespace_by_name(ns_name);
   if (! ns) {
      errno = EINVAL;
      return -1;
   }

   // should we believe the inode in the obj-id, or the one in caller's stat struct?
   //
   // NOTE: Packed objects (if they contain more than one logical object)
   //     can't possibly have the correct inode in their object-ID, in all
   //     cases.  But we don't want them to have all-zeros, either, because
   //     then they wouldn't be reliably-unique.  Therefore, they are built
   //     with an indoe from one of their members, but it won't match the
   //     inode of the others.
   if ((md_inode != st->st_ino)
       && (decode_obj_type(obj_type) != OBJ_PACKED)) {
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // parse encoded time-stamps
   time_t  md_ctime;
   time_t  obj_ctime;

   if (str_to_epoch(&md_ctime, md_ctime_str, MARFS_DATE_STRING_MAX)) {
      LOG(LOG_ERR, "error converting string '%s' to Pre.md_time\n", md_ctime_str);
      return -1;
   }
   if (str_to_epoch(&obj_ctime, obj_ctime_str, MARFS_DATE_STRING_MAX)) {
      LOG(LOG_ERR, "error converting string '%s' to Pre.md_time\n", md_ctime_str);
      return -1;
   }


   // --- fill in fields in Pre
   pre->md_ctime     = md_ctime;
   pre->obj_ctime    = obj_ctime;
   pre->config_vers  = (float)major + ((float)minor / 1000.f);

   pre->obj_type     = decode_obj_type(obj_type);
   pre->compression  = decode_compression(compress);
   pre->correction   = decode_correction(correct);
   pre->encryption   = decode_encryption(encrypt);

   pre->ns           = ns;
   pre->repo         = repo;
   pre->chunk_size   = chunk_size;
   pre->chunk_no     = chunk_no;
   pre->md_inode     = md_inode; /* NOTE: from object-ID, not st->st_ino  */

   // validate version
   assert (pre->config_vers == MarFS_config_vers);
   return 0;
}





// initialize -- most fields aren't known, when stat_xattr() calls us
int init_post(MarFS_XattrPost* post, MarFS_Namespace* ns, MarFS_Repo* repo) {
   post->config_vers = MarFS_config_vers;
   post->obj_type    = OBJ_NONE;   /* figured out later */
   post->chunks      = 1;          // we don't create Packed objects
   post->chunk_info_bytes = 0;
   memset(post->gc_path, 0, MARFS_MAX_MD_PATH);
   return 0;
}


// from MarFS_XattrPost to string
int post_2_str(char* post_str, size_t max_size, const MarFS_XattrPost* post) {

   // config-version major and minor
   int major = (int)floorf(post->config_vers);
   int minor = (int)floorf((post->config_vers - major) * 1000.f);

   ssize_t bytes_printed = snprintf(post_str, max_size,
                                    MARFS_POST_FORMAT,
                                    major, minor,
                                    encode_obj_type(post->obj_type),
                                    post->obj_offset,
                                    post->chunks,
                                    post->chunk_info_bytes,
                                    post->correct_info,
                                    post->encrypt_info,
                                    post->gc_path);
   if (bytes_printed < 0)
      return -1;                  // errno is set
   if (bytes_printed == max_size) {   /* overflow */
      errno = EINVAL;
      return -1;
   }

   return 0;
}

// parse an xattr-value string into a MarFS_XattrPost
int str_2_post(MarFS_XattrPost* post, const char* post_str) {

   int   major;
   int   minor;
   float version;

   char  obj_type_code;

   // --- extract bucket, and some top-level fields
   int scanf_size = sscanf(post_str, MARFS_POST_FORMAT,
                           &major, &minor,
                           &obj_type_code,
                           &post->obj_offset,
                           &post->chunks,
                           &post->chunk_info_bytes,
                           &post->correct_info,
                           &post->encrypt_info,
                           (char*)&post->gc_path); // might be empty

   if (scanf_size == EOF)
      return -1;                // errno is set
   else if (scanf_size < 8) {
      errno = EINVAL;
      return -1;            /* ?? */
   }

   version = (float)major + ((float)minor / 1000.f);
   if (version != MarFS_config_vers) {
      errno = EINVAL;            /* ?? */
      return -1;
   }

   post->config_vers = version;
   post->obj_type    = decode_obj_type(obj_type_code);
   return 0;
}








// from MarFS_XattrShard to string
int shard_2_str(char* shard_str,        const MarFS_XattrShard* shard) {
   assert(0);                   // TBD
}
// from string to MarFS_XattrShard
int str_2_shard(MarFS_XattrShard* shard, const char* shard_str) {
   assert(0);                   // TBD
}





// from RecoveryInfo to string
int rec_2_str(char* rec_str, const size_t max_size, const RecoveryInfo* rec) {

   // UNDER CONSTRUCTION ...
   assert(0);

#if 0
   // config-version major and minor
   int major = (int)floorf(rec->config_vers);
   int minor = (int)floorf((rec->config_vers - major) * 1000.f);

   ssize_t bytes_printed = snprintf(rec_info_str, max_size,
                                    MARFS_REC_INFO_FORMAT,
                                    major, minor,
                                    rec->inode,
                                    rec->mode,
                                    rec->uid,
                                    rec->gid,
                                    mtime,
                                    ctime,
                                    post->gc_path);
   if (bytes_printed < 0)
      return -1;                  // errno is set
   if (bytes_printed == max_size) {   /* overflow */
      errno = EINVAL;
      return -1;
   }

#endif
   return 0;
}

// from string to RecoveryInfo.  Presumabl,y the string is what you got
// from the tail-end of an object.  Use this to convert the string to a
// RecoveryInfo struct.
int str_2_rec(RecoveryInfo* rec_info, const char* rec_info_str) {
   // TBD ...
   return -1;
}


// // this is just a sketch.
// int get_recovery_string() { }
// int get_next_recovery_string(RecoveryInfo* info) { }







// htonll() / ntohll() are not provided in our environment.  <endian.h> or
// <byteswap.h> make things easier, but these are non-standard.  Also, we're
// compiled with -Wall, so we avoid pointer-aliasing that makes gcc whine.
//
// TBD: Find the appropriate #ifdefs to make these definitions go away on
//     systems that already provide them.


// see http://esr.ibiblio.org/?p=5095
#define IS_LITTLE_ENDIAN (*(uint16_t *)"\0\xff" >= 0x100)

uint64_t htonll(uint64_t ll) {
   if (IS_LITTLE_ENDIAN) {
      uint64_t result;
      char* sptr = ((char*)&ll) +7; // gcc doesn't mind char* aliases
      char* dptr = (char*)&result; // gcc doesn't mind char* aliases
      int i;
      for (i=0; i<8; ++i)
         *dptr++ = *sptr--;
      return result;
   }
   else
      return ll;
}
uint64_t ntohll(uint64_t ll) {
   if (IS_LITTLE_ENDIAN) {
      uint64_t result;
      char* sptr = ((char*)&ll) +7; // gcc doesn't mind char* aliases
      char* dptr = (char*)&result; // gcc doesn't mind char* aliases
      int i;
      for (i=0; i<8; ++i)
         *dptr++ = *sptr--;
      return result;
   }
   else
      return ll;
}


typedef union {
   float    f;
   uint32_t i;
} UFloat32;

// We write MultiChunkInfo as binary data (in network-byte-order) in hopes
// that this will speed-up treating the MD file as a big index, during
// pftool restarts.  Return number of bytes moved, or -1 + errno.
//
// NOTE: If max_size == sizeof(MultiChunkInfo), this may still return a
//     size less than sizeof(MultiChunkInfo), because the struct may
//     include padding assoicated with alignment.

ssize_t chunkinfo_2_str(char* str, const size_t max_size, const MultiChunkInfo* chnk) {
   if (max_size < sizeof(MultiChunkInfo)) {
      errno = EINVAL;
      return -1;
   }
   char* dest = str;

#define COPY_OUT(SOURCE, TYPE, CONVERSION_FN)       \
   {  TYPE temp = CONVERSION_FN (SOURCE);           \
      memcpy(dest, (char*)&temp, sizeof(TYPE));    \
      dest += sizeof(TYPE);                        \
   }


   // version is copied byte-for-byte as a float (in network-byte-order)
   UFloat32 uf = (UFloat32){ .f = chnk->config_vers };
   uf.i = htonl(uf.i);
   memcpy(dest, (char*)&uf.i, 4);
   dest += sizeof(float);

   COPY_OUT(chnk->chunk_no,         size_t,      htonll);
   COPY_OUT(chnk->data_offset,      size_t,      htonll);
   COPY_OUT(chnk->chunk_data_bytes, size_t,      htonll);
   COPY_OUT(chnk->correct_info,     CorrectInfo, htonll);
   COPY_OUT(chnk->encrypt_info,     EncryptInfo, htonll);

#undef COPY_OUT

   return (dest - str);
}

// NOTE: We require str_len >= sizeof(MultiChunkInfo), even though it's
//     possible that chunkinfo_2_str() can encode a MultiChunkInfo into a
//     string that is smaller than sizeof(MultiChunkInfo), because of
//     padding for alingment, within the struct.  We're just playing it
//     safe.
ssize_t str_2_chunkinfo(MultiChunkInfo* chnk, const char* str, const size_t str_len) {
   if (str_len < sizeof(MultiChunkInfo)) {
      errno = EINVAL;
      return -1;
   }
   char* src = (char*)str;

#define COPY_IN(DEST, TYPE, CONVERSION_FN)       \
   {  TYPE temp;                                 \
      memcpy((char*)&temp, src, sizeof(TYPE));   \
      DEST = CONVERSION_FN( temp );              \
      src += sizeof(TYPE);                       \
   }

   // version is copied byte-for-byte as a float (in network-byte-order)
   UFloat32 uf;
   memcpy((char*)&uf.i, src, 4);
   uf.i = htonl(uf.i);
   chnk->config_vers = uf.f;
   src += sizeof(float);

   COPY_IN(chnk->chunk_no,         size_t,      htonll);
   COPY_IN(chnk->data_offset,      size_t,      htonll);
   COPY_IN(chnk->chunk_data_bytes, size_t,      htonll);
   COPY_IN(chnk->correct_info,     CorrectInfo, htonll);
   COPY_IN(chnk->encrypt_info,     EncryptInfo, htonll);

#undef COPY_IN

   return (src - str);
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
            LOG(LOG_ERR, "range [%ld, -1] includes range [%ld, %ld]\n",
                    min, this->min, this->max);
            return -1;
         }
         if (max < this->min) {
            LOG(LOG_ERR, "gap between range [%ld, %ld] and [%ld, %ld]\n",
                    min, max, this->min, this->max);
            return -1;
         }
         if (max > this->min) {
            LOG(LOG_ERR, "overlap in range [%ld, %ld] and [%ld, %ld]\n",
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

   // helper for find_namespace()
   ns->mnt_suffix_len = strlen(ns->mnt_suffix);

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
      .chunk_size   = (1024 * 1024 * 64), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .latency_ms   = (10 * 1000) };
   push_repo(&r_dummy);

   // tiny, for debugging
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_2k",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.21:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (2048), /* i.e. max MarFS object (small for debugging) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .latency_ms   = (10 * 1000),
   };
   push_repo(&r_dummy);

   // For Alfred, on his own server
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_64M",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.22:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (1024 * 1024 * 64), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .latency_ms   = (10 * 1000) };
   push_repo(&r_dummy);

   // For Brett, small enough to make it easy to create MULTIs
   r_dummy = (MarFS_Repo) {
      .name         = "sproxyd_1M",  // repo is sproxyd: this must match fastcgi-path
      .host         = "10.135.0.22:81",
      .access_proto = PROTO_SPROXYD,
      .chunk_size   = (1024 * 1024 * 1), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .latency_ms   = (10 * 1000)
   };
   push_repo(&r_dummy);

   // S3 on EMC ECS
   r_dummy = (MarFS_Repo) {
      .name         = "emcS3_00",  // repo is s3: this must match existing bucket
      .host         = "10.140.0.15:9020", //"10.143.0.1:80",
      .access_proto = PROTO_S3_EMC,
      .chunk_size   = (1024 * 1024 * 64), /* max MarFS object (tune to match storage) */
      .flags        = (REPO_ONLINE),
      .auth         = AUTH_S3_AWS_MASTER,
      .latency_ms   = (10 * 1000),
   };
   push_repo(&r_dummy);




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
      .name           = "test00",
      .mnt_suffix     = "/test00",  // "<mnt_top>/test00" comes here
      .md_path        = "/gpfs/marfs-gpfs/fuse/test00/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/fuse/test00/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .compression = COMPRESS_NONE,
      .correction  = CORRECT_NONE,

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space_units = (1024 * 1024), /* MB */
      .quota_space = 1024,          /* 1024 MB of data */

      .quota_space_units = 1,
      .quota_names = 32,             /* 32 names */

      .is_root = 0,
   };
   // push_namespace(&ns_dummy, find_repo_by_name("sproxyd_2k"));
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_jti"));


   // jti testing on machine without GPFS
   ns_dummy = (MarFS_Namespace) {
      .name           = "xfs",
      .mnt_suffix     = "/xfs",  // "<mnt_top>/test00" comes here

      .md_path        = "/mnt/xfs/jti/filesys/mdfs/test00",
      .trash_path     = "/mnt/xfs/jti/filesys/trash/test00",
      .fsinfo_path    = "/mnt/xfs/jti/filesys/fsinfo/test00",

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .compression = COMPRESS_NONE,
      .correction  = CORRECT_NONE,

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space_units = (1024 * 1024), /* MB */
      .quota_space = 1024,          /* 1024 MB of data */

      .quota_space_units = 1,
      .quota_names = 32,             /* 32 names */

      .is_root = 0,
   };
   // push_namespace(&ns_dummy, find_repo_by_name("sproxyd_2k"));
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_jti"));


   // Alfred
   ns_dummy = (MarFS_Namespace) {
      .name           = "atorrez",
      .mnt_suffix     = "/atorrez",  // "<mnt_top>/test00" comes here
      .md_path        = "/gpfs/marfs-gpfs/project_a/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/project_a/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .compression = COMPRESS_NONE,
      .correction  = CORRECT_NONE,

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space_units = (1024 * 1024), /* MB */
      .quota_space = -1,                  /* no limit */

      .quota_space_units = 1,
      .quota_names = -1,        /* no limit */

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_64M"));


   // Brett
   ns_dummy = (MarFS_Namespace) {
      .name           = "brettk",
      .mnt_suffix     = "/brettk",  // "<mnt_top>/test00" comes here
      .md_path        = "/gpfs/marfs-gpfs/testing/mdfs",
      .trash_path     = "/gpfs/marfs-gpfs/testing/trash", // NOT NEC IN THE SAME FILESET!
      .fsinfo_path    = "/gpfs/marfs-gpfs/testing/fsinfo", /* a file */

      .iperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),
      .bperms = ( R_META | W_META | R_DATA | W_DATA | T_DATA | U_DATA ),

      .compression = COMPRESS_NONE,
      .correction  = CORRECT_NONE,

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space_units = (1024 * 1024), /* MB */
      .quota_space = -1,          /* no limit */

      .quota_space_units = 1,
      .quota_names = -1,             /* no limit */

      .is_root = 0,
   };
   push_namespace(&ns_dummy, find_repo_by_name("sproxyd_1M"));


   // "root" is a special path
   ns_dummy = (MarFS_Namespace) {
      .name           = "root",
      .mnt_suffix     = "/",
      .md_path        = "should_never_be_used",
      .trash_path     = "should_never_be_used",
      .fsinfo_path    = "should_never_be_used",

      .iperms = 0,
      .bperms = 0,

      .compression = COMPRESS_NONE,
      .correction  = CORRECT_NONE,

      .dirty_pack_percent   =  0,
      .dirty_pack_threshold = 75,

      .quota_space_units = (1024 * 1024), /* MB */
      .quota_space = -1,          /* no limit */

      .quota_space_units = 1,
      .quota_names = -1,             /* no limit */

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
   for (i=0; i<_ns_count; ++i) {
      MarFS_Namespace* ns = _ns[i];
      if (! strncmp(ns->name, name, ns->name_len))
         return ns;
   }
   return NULL;
}
MarFS_Namespace* find_namespace_by_path(const char* path) {
   size_t path_len = strlen(path);

   int i;
   for (i=0; i<_ns_count; ++i) {
      MarFS_Namespace* ns = _ns[i];
      size_t           max_len = ((path_len > ns->mnt_suffix_len)
                                  ? path_len
                                  : ns->mnt_suffix_len);
      if (! strncmp(ns->mnt_suffix, path, max_len))
         return ns;
   }
   return NULL;
}




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
   for (i=0; i<_repo_max; ++i) {
      MarFS_Repo* repo = _repo[i];
      if (!strcmp(repo_name, repo->name))
         return repo;
   }
   return NULL;
}
