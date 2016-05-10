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
#include "mdal.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>          // HOST_NAME_MAX
#include <inttypes.h>        // uintmax_t, for printing ino_t with "%ju"
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>       // htonl(), etc


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


// --- encode_obj_type() / decode_obj_type()
DEFINE_ENCODE(obj_type, MarFS_ObjType, "_UMPSFN");
DEFINE_DECODE(obj_type, MarFS_ObjType);

// --- encode_compression() / decode_compression()
// DEFINE_ENCODE(compression, MarFS_CompType, "_R");
DEFINE_ENCODE(compression, MarFS_CompType, "_");
DEFINE_DECODE(compression, MarFS_CompType);

// --- encode_correction() / decode_correction()
// DEFINE_ENCODE(correction, MarFS_CorrectType, "_CKHRE");
DEFINE_ENCODE(correction, MarFS_CorrectType, "_");
DEFINE_DECODE(correction, MarFS_CorrectType);

// --- encode_encryption() / decode_encryption()
DEFINE_ENCODE(encryption, MarFS_EncryptType, "_");
DEFINE_DECODE(encryption, MarFS_EncryptType);


// NOTE: <src> and <dest> can be the same
// NOTE: we assume read_config() guarantees that no namespace contains '-'
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
//
// NOTE: This gets called twice, for every call to stat_xattrs(), and the
//     debugging output is fairly verbose.  It seems to be working
//     correctly, so I'm commenting out the loggging calls.

int epoch_to_str(char* str, size_t size, const time_t* time) {
   struct tm tm;

   LOG(LOG_INFO, " epoch_to_str epoch:            %016lx\n", *time);

   // time_t -> struct tm
   if (! localtime_r(time, &tm)) {
      LOG(LOG_ERR, "localtime_r failed: %s\n", strerror(errno));
      return -1;
   }

   //   // DEBUGGING
   //   __attribute__ ((unused)) struct tm* dbg = &tm;
   //   LOG(LOG_INFO, " epoch_2_str localtime:         %4d-%02d-%02d %02d:%02d:%02d (%d)\n",
   //           1900+(dbg->tm_year),
   //           dbg->tm_mon,
   //           dbg->tm_mday,
   //           dbg->tm_hour,
   //           dbg->tm_min,
   //           dbg->tm_sec,
   //           dbg->tm_isdst);

   // struct tm -> string
   size_t strf_size = strftime(str, size, MARFS_DATE_FORMAT, &tm);
   if (! strf_size) {
      LOG(LOG_ERR, "strftime failed even more than usual: %s\n", strerror(errno));
      return -1;
   }

   //   // DEBUGGING
   //   LOG(LOG_INFO, " epoch_2_str to-string (1)      %s\n", str);

   // add DST indicator
   snprintf(str+strf_size, size-strf_size, MARFS_DST_FORMAT, tm.tm_isdst);

   //   // DEBUGGING
   //   LOG(LOG_INFO, " epoch_2_str to-string (2)      %s\n", str);

   return 0;
}


// NOTE: This gets called twice, for every call to save_xattrs(), and the
//     debugging output is fairly verbose.  It seems to be working
//     correctly, so I'm commenting out the loggging calls.

int str_to_epoch(time_t* time, const char* str, size_t size) {
   struct tm tm;
   //   memset(&tm, 0, sizeof(tm)); // DEBUGGING

   LOG(LOG_INFO, " str_to_epoch str:              %s\n", str);

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

   //   // DEBUGGING
   //   __attribute__ ((unused)) struct tm* dbg = &tm;
   //   LOG(LOG_INFO, " str_to_epoch from string: (1)  %4d-%02d-%02d %02d:%02d:%02d (%d)\n",
   //           1900+(dbg->tm_year),
   //           dbg->tm_mon,
   //           dbg->tm_mday,
   //           dbg->tm_hour,
   //           dbg->tm_min,
   //           dbg->tm_sec,
   //           dbg->tm_isdst);

   // struct tm -> epoch
   *time = mktime(&tm);

   //   // DEBUGGING
   //   LOG(LOG_INFO, " str_to_epoch epoch:            %016lx\n", *time);

   return 0;
}

// ---------------------------------------------------------------------------
// xattrs
// ---------------------------------------------------------------------------

// If the repo uses host randomization, it will have a host name like
// "10.0.0.%d:81", and host_count greater than 1.  (See update_pre().)  We
// generate a randomw seed that is used to produce the value to be printed
// where the "%d" is.

int init_pre_seed(MarFS_XattrPre* pre, const MarFS_Repo* repo) {
   // prepare random seed, if configuration uses host-randomization
   if (repo->host_count > 1) {

      // seed the random-number generator from the clock
      // (i.e. in case we need to close/reopen)
      struct timespec ts;
      if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts)) {
         LOG(LOG_ERR, "clock_gettime() failed; %s\n", strerror(errno));
         return -1;
      }

      union {
         long         l;
         unsigned int ui;
      } down_cast;

      down_cast.l = ts.tv_nsec;
      pre->seed   = down_cast.ui;
   }

   return 0;
}

// this hash is used to assure that all tasks on the same host will
// target the same server (or set of servers).  See update_pre().
//
int init_pre_hostname_hash(MarFS_XattrPre* pre, const MarFS_Repo* repo) {
   // prepare hostname-hash, if configuration uses host-randomization
   if (repo->host_count > 1) {

      char hostname[HOST_NAME_MAX]; // see quibbles in gethostname(2) manpage
      if (gethostname(hostname, HOST_NAME_MAX)) {
         LOG(LOG_ERR, "gethostname() failed; %s\n", strerror(errno));
         return -1;
      }

      uint8_t hash = 0;
      char*   name_ptr;
      for (name_ptr=hostname; *name_ptr; ++name_ptr) {
         hash += *name_ptr;
      }
      pre->hostname_hash = hash;
   }

   return 0;
}

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

   // captures the version of the software, not what's in the config-file
   pre->config_vers_maj = MARFS_CONFIG_MAJOR; // marfs_config->version_major;
   pre->config_vers_min = MARFS_CONFIG_MINOR; // marfs_config->version_minor;

   pre->obj_type     = obj_type;

   pre->compression  = repo->comp_type;
   pre->correction   = repo->correct_type;
   pre->encryption   = repo->enc_type;

   pre->md_inode     = st->st_ino;
   pre->md_ctime     = st->st_ctime;
   pre->obj_ctime    = now;     /* TBD: update with info from HEAD request */
   pre->unique       = 0;

   pre->chunk_size   = repo->chunk_size;
   pre->chunk_no     = 0;

   // pre->shard = ...;    // TBD: for hashing directories across shard nodes

   // generate random-seed, if needed
   if (init_pre_seed(pre, repo)) {
      return -1;
   }
   // experimental alternative to random-seed
   if (init_pre_hostname_hash(pre, repo)) {
      return -1;
   }

   //   // generate bucket and obj-id
   //   return update_pre(pre);
   return 0;
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
// doing here is updating the objid from the struct-member-values.  If the
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
//       can't be presumed to have an inode that matches the actual inode
//       of the MDFS file.  Therefore, in the case of packed, we should
//       avoid updating that field.  (However, because we're using an
//       sprintf() we'll just assume that the "inode" for packed files is
//       always zero.)
//
// NOTE: If the conifguration specifies more than one host in the repo,
//       Then we expect the following features in the configuration:
//
//    marfs_config.host         = "10.135.0.%d:81"   (for example)
//    marfs_config.host_offset  = 15                 (for example)
//    marfs_config.host_count   = 4                  (for example)
//
//       This allows us to generate a valid random IP address w/in a
//       selected range IP-addrs (e.g. 10.135.0.15-18)
//
//       If you want to have DNS round-robin do this for you, you would
//       just set repo.host (in the conifguration) to a name that your DNS
//       service knows, and set host_count=1.

int update_pre(MarFS_XattrPre* pre) {

   pre->flags |= PF_UPDATED;    // re-stringification may be needed

   // --- generate bucket-name

   int write_count;
   write_count = snprintf(pre->bucket, MARFS_MAX_BUCKET_SIZE,
                          MARFS_BUCKET_WR_FORMAT,
                          pre->ns->alias);
   if (write_count < 0)
      return -1;
   if (write_count == MARFS_MAX_BUCKET_SIZE) { /* overflow */
      errno = EINVAL;
      return -1;
   }


   // --- generate obj-id

   //   // convert '/' to '-' in namespace-name
   //   char ns_name[MARFS_MAX_NS_NAME];
   //   if (encode_namespace(ns_name, (char*)pre->ns->mnt_suffix))
   //      return EINVAL;

   // config-version major and minor
   ConfigVersType major = pre->config_vers_maj;
   ConfigVersType minor = pre->config_vers_min;

   char           type     = encode_obj_type(pre->obj_type);
   char           compress = encode_compression(pre->compression);
   char           correct  = encode_correction(pre->correction);
   char           encrypt  = encode_encryption(pre->encryption);

   // PACKED objects have a real inode-value in their object-ID (in order
   // to assure unique-ness).  But it might not match this particular file.
   // Therefore, for packed objects, we should leave it as is.

   // prepare date-string components
   char           md_ctime_str[MARFS_DATE_STRING_MAX];
   char          obj_ctime_str[MARFS_DATE_STRING_MAX];

   if (epoch_to_str(md_ctime_str, MARFS_DATE_STRING_MAX, &pre->md_ctime)) {
      LOG(LOG_ERR, "error converting Pre.md_time to string\n");
      return -1;
   }
   if (epoch_to_str(obj_ctime_str, MARFS_DATE_STRING_MAX, &pre->obj_ctime)) {
      LOG(LOG_ERR, "error converting Pre.obj_time to string\n");
      return -1;
   }

   // put all components together
   write_count = snprintf(pre->objid, MARFS_MAX_OBJID_SIZE,
                          MARFS_OBJID_WR_FORMAT,
                          pre->repo->name,
                          major, minor,
                          pre->ns->name,
                          type, compress, correct, encrypt,
                          (uint64_t)pre->md_inode,
                          md_ctime_str, obj_ctime_str, pre->unique,
                          pre->chunk_size, pre->chunk_no);
   if (write_count < 0)
      return -1;
   if (write_count >= MARFS_MAX_OBJID_SIZE) { /* overflow */
      errno = EINVAL;
      return -1;
   }


   // generate host, if necessary
   if (pre->repo->host_count > 1) {

#if 1
      // This method allows tasks on a given host to talk to all the
      // servers in the configured range, at random.  Each request will
      // potentially have a distinct host IP-addr.
      uint8_t octet = (pre->repo->host_offset
                       + (rand_r(&pre->seed) % pre->repo->host_count));
#elif 0
      // The method above may be contributing to an 'incast' problem, for
      // reads.  The method here is an alternative.  Tasks on any given
      // host will all target a specific server in the configured range.
      uint8_t      octet  = (pre->repo->host_offset
                             + (pre->hostname_hash
                                % (int)pre->repo->host_count));
#else
      // QUES:  but, what if that server is down?
      // ANS:   okay, each host targets one of two specific servers,
      //        with randomization only between those two servers.
      //
      // No server will get requests from more than one host, as long as
      // the number of servers is at least 2x the number of client hosts.
      unsigned int binary = rand_r(&pre->seed) / (RAND_MAX / 2); // 0 or 1
      uint8_t      octet  = (pre->repo->host_offset
                             + ( ((2 * pre->hostname_hash) + binary)
                                 % (int)pre->repo->host_count));
#endif

      write_count = snprintf(pre->host, MARFS_MAX_HOST_SIZE,
                             pre->repo->host, octet);

      if (write_count < 0)
         return -1;
      if (write_count >= MARFS_MAX_OBJID_SIZE) { /* overflow */
         errno = EINVAL;
         return -1;
      }
   }
   else
      strncpy(pre->host, pre->repo->host, MARFS_MAX_HOST_SIZE);


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
      return -1;
   if (write_count == max_size) {  /* overflow */
      errno = EINVAL;
      return -1;
   }

   return 0;
}



#if 0
// COMMENTED OUT.  I think this is now replaced by update_pre()  [?]

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

#endif



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
// NOTE: Some callers wont have a stat struct, and we only use it for a
//       validation, so we'll allow it to be null, in which case we just
//       skip the test.  (For Alfred's GC tool, these things will be in the
//       trash, where the file inode is never going to be the same as the
//       indo in the xattr.

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


   uint16_t  major;
   uint16_t  minor;

   char      obj_type;          /* ignored, see NOTE above init_pre() */
   char      compress;
   char      correct;
   char      encrypt;

   ino_t     md_inode;

   size_t    chunk_size;
   size_t    chunk_no;

   char      md_ctime_str[MARFS_DATE_STRING_MAX];
   char      obj_ctime_str[MARFS_DATE_STRING_MAX];
   uint8_t   unique;

   // --- parse bucket components

   // having ns_alias first allows new namespaces to use existing sproxyd
   // fastcgi proxy names, eliminating server-side config, in such cases.
   char  ns_alias [MARFS_MAX_NS_ALIAS_NAME];

   read_count = sscanf(pre->bucket, MARFS_BUCKET_RD_FORMAT,
                       ns_alias);

   if (read_count == EOF) {     // errno is set (?)
      LOG(LOG_ERR, " parsing bucket '%s'\n", pre->bucket);
      return -1;
   }
   else if (read_count != 1) {
      LOG(LOG_ERR, "parsed %d items from '%s'\n", read_count, pre->bucket);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // --- parse "obj-id" components (i.e. the part below bucket)

   // have repo second allows Scality driver-alias to be used to create
   // alternative repos (e.g. with different erasure-coding schema),
   // without having to create another ring.
   char  repo_name[MARFS_MAX_REPO_NAME];

   // Holds namespace-name from obj-id, so we can decode_namespace(), then
   // find the corresponding namespace, for Pre.ns.  Do we ever care about
   // this?  The only reason we need it is because update_pre() uses it to
   // re-encode the bucket string.
   char  ns_name[MARFS_MAX_OBJID_SIZE]; // more than enough
   
   read_count = sscanf(pre->objid, MARFS_OBJID_RD_FORMAT,
                       repo_name,
                       &major, &minor,
                       ns_name,
                       &obj_type, &compress, &correct, &encrypt,
                       &md_inode,
                       md_ctime_str, obj_ctime_str, &unique,
                       &chunk_size, &chunk_no);

   if (read_count == EOF) {       // errno is set (?)
      LOG(LOG_ERR, "EOF parsing objid '%s'\n", pre->objid);
      return -1;
   }
   else if (read_count != 14) {
      LOG(LOG_ERR, "parsed %d items from '%s'\n", read_count, pre->objid);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // --- conversions and validation

   // find repo from repo-name
   MarFS_Repo* repo = find_repo_by_name(repo_name);
   if (! repo) {
      LOG(LOG_ERR, "couldn't find repo '%s'\n", repo_name);
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
      LOG(LOG_ERR, "couldn't find namespace '%s'\n", ns_name);
      errno = EINVAL;
      return -1;
   }

   // Q: should we believe the inode in the obj-id, or the one in caller's stat struct?
   //
   // NOTE: Packed objects (if they contain more than one logical object)
   //     can't possibly have the correct inode in their object-ID, in all
   //     cases.  But we don't want them to have all-zeros, either, because
   //     then they wouldn't be reliably-unique.  Therefore, they are built
   //     with an inode from one of their members, but it won't match the
   //     inode of the others.
   //
   // NOTE: If the file has been moved to the trash, the xattr-string is
   //    still an object-ID, and it won't match the st.st_ino in that case
   //    either.  (How do we test whether it's in the trash?  Maybe look at
   //    repo->trash_path, and see whether we're in there?)

#if 0
   // A: Caller wants us to parse an xattr string.  Just parse it!  It's
   //    just an object ID!  We don't need to validate inode values.

   if (st
       && (md_inode != st->st_ino)
       && (decode_obj_type(obj_type) != OBJ_PACKED)) {
      LOG(LOG_ERR, "non-packed obj, but MD-inode %ju != st->st_ino %ju \n",
          (uintmax_t)md_inode, (uintmax_t)st->st_ino);
      errno = EINVAL;            /* ?? */
      return -1;
   }
#endif

   // parse encoded time-stamps
   time_t  md_ctime;
   time_t  obj_ctime;

   if (str_to_epoch(&md_ctime, md_ctime_str, MARFS_DATE_STRING_MAX)) {
      LOG(LOG_ERR, "error converting string '%s' to Pre.md_ctime\n", md_ctime_str);
      errno = EINVAL;            /* ?? */
      return -1;
   }
   if (str_to_epoch(&obj_ctime, obj_ctime_str, MARFS_DATE_STRING_MAX)) {
      LOG(LOG_ERR, "error converting string '%s' to Pre.obj_ctime\n", obj_ctime_str);
      errno = EINVAL;            /* ?? */
      return -1;
   }

   // validate version
   // NOTE: This just assures us that we know how to parse this thing.
   //
   //   //   if ((   major != marfs_config->version_major)
   //   //       || (minor != marfs_config->version_minor)) {
   //   //      LOG(LOG_ERR, "xattr vers '%d.%d' != config %d.%d\n",
   //   //          major, minor,
   //   //          marfs_config->version_major, marfs_config->version_minor);
   //   //      errno = EINVAL;            /* ?? */
   //   //      return -1;
   //   //   }
   //
   //   if ( major != MARFS_CONFIG_VERSION) {
   //      LOG(LOG_ERR, "xattr vers '%d.%d' Parser expected 0.x\n",
   //          major, minor);
   //      errno = EINVAL;
   //      return -1;
   //   }
   //
   if ((   major != MARFS_CONFIG_MAJOR)
       || (minor >  MARFS_CONFIG_MINOR)) {
      LOG(LOG_ERR, "xattr vers '%d.%d' != config %d.%d\n",
          major, minor,
          MARFS_CONFIG_MAJOR, MARFS_CONFIG_MINOR);
      errno = EINVAL;
      return -1;
   }
   


   // --- fill in fields in Pre
   pre->config_vers_maj = major;
   pre->config_vers_min = minor;

   pre->md_ctime     = md_ctime;
   pre->obj_ctime    = obj_ctime;
   pre->unique       = unique;

   pre->obj_type     = decode_obj_type(obj_type);
   pre->compression  = decode_compression(compress);
   pre->correction   = decode_correction(correct);
   pre->encryption   = decode_encryption(encrypt);

   pre->ns           = ns;
   pre->repo         = repo;
   pre->chunk_size   = chunk_size;
   pre->chunk_no     = chunk_no;
   pre->md_inode     = md_inode; /* NOTE: from object-ID, not st->st_ino  */

   // generate random-seed, if needed
   if (init_pre_seed(pre, repo)) {
      LOG(LOG_ERR, "couldn't generate random-seed\n");
      errno = EINVAL;
      return -1;
   }
   // experimental alternative to random-seed
   if (init_pre_hostname_hash(pre, repo)) {
      LOG(LOG_ERR, "couldn't generate hostname-hash\n");
      errno = EINVAL;
      return -1;
   }


   return 0;
}





// initialize -- most fields aren't known, when stat_xattr() calls us
int init_post(MarFS_XattrPost* post, MarFS_Namespace* ns, MarFS_Repo* repo) {

   //   post->config_vers_maj = marfs_config->version_major;
   //   post->config_vers_min = marfs_config->version_minor;
   post->config_vers_maj = MARFS_CONFIG_MAJOR;
   post->config_vers_min = MARFS_CONFIG_MINOR;

   post->obj_type    = OBJ_UNI;    /* will be changed to Multi, if needed */
   post->chunks      = 1;          // will be updated for multi
   post->chunk_info_bytes = 0;

   //   post->flags       = 0;

   //   // this would have the effect of resetting md_path, in the middle
   //   // of stat_xattrs().  Instead, marfs_open() will wipe POST initially.
   //   memset(post->md_path, 0, MARFS_MAX_MD_PATH);

   return 0;
}


// from MarFS_XattrPost to string
//
// <repo> is the repo> that was written to.
//
// <add_md_path> allows future version of write_recoveryinfo() to stringify
// post without setting POST_TRASH in MarFS_XattrPost.flags.

int post_2_str(char*                  post_str,
               size_t                 max_size,
               const MarFS_XattrPost* post,
               const MarFS_Repo*      repo,
               int                    add_md_path) {

   // config-version major and minor
   const ConfigVersType major = post->config_vers_maj;
   const ConfigVersType minor = post->config_vers_min;

   // putting the md_path into the xattr is only useful if the marfs file
   // is in the trash, or is SEMI_DIRECT.  For other types of marfs files,
   // this md_path will be wrong as soon as the user renames it (or a
   // parent-directory) to some other path.  Therefore, one would never
   // want to trust it in those cases.  [Gary thought of an example where
   // several renames could get the path to point to the wrong file.]  So,
   // let's only write it when it is needed and reliable.
   //
   // We now also use this function to stringify POST for writing into the
   // recovery-info at the tail of an object.  In that case, we also want
   // the MD path, though the file will not be in the trash.  Yes, this may
   // be wrong, as described above, but it's purpose is just to capture the
   // path as it existed at object-creation-time.
   //
   // NOTE: Because we use the same xattr-field to point to the semi-direct
   //     file-system *OR* to the location of the file in the trash, we can
   //     not currently support moving semi-direct files to the trash.
   //     Deleting a semi-direct file must just delete it.
   const char* md_path = ( ((repo->access_method == ACCESSMETHOD_SEMI_DIRECT)
                            || (post->flags & POST_TRASH)
                            || add_md_path)
                           ? post->md_path
                           : "");

   ssize_t bytes_printed = snprintf(post_str, max_size,
                                    MARFS_POST_FORMAT,
                                    major, minor,
                                    encode_obj_type(post->obj_type),
                                    post->obj_offset,
                                    post->chunks,
                                    post->chunk_info_bytes,
                                    post->correct_info,
                                    post->encrypt_info,
                                    post->flags,
                                    md_path);
   if (bytes_printed < 0)
      return -1;                  // errno is set
   if (bytes_printed == max_size) {   /* overflow */
      errno = EINVAL;
      return -1;
   }

   return 0;
}

// parse an xattr-value string into a MarFS_XattrPost
//
// NOTE: We moved the "md_path" into the POST xattr.  That allows it to be
//     conveniently saved and restored as part of the POST xattr.  However,
//     we then realized that we only wanted md_path saved on xattrs for
//     trash and SEMI, and is null for everyone else.  Meanwhile,
//     expand_path_info() also initializes post.md_path, for fuse.  Thus,
//     if we call expand_path_info() then stat_xattrs() (which calls
//     str_2_post()), for a file that is neither trash nor SEMI, we want to
//     avoid affecting md_path.
//
// Meaning of <reset>
//
//     When fuse calls, the POST object already has md_path installed.  (In
//     fact, that is the path from which we read the xattr string-value.)
//     In that case, we also expect the parsed md_path from the xattr-value
//     to be empty, so the original is not overwritten.  However, some
//     other callers (e.g. MD inode scans) are calling with a raw POST
//     object, which they want to be initialized entirely from the string.
//     Those callers should call with <reset> being non-zero.


int str_2_post(MarFS_XattrPost* post, const char* post_str, uint8_t reset) {

   uint16_t major;
   uint16_t minor;
   char     obj_type_code;

   if (reset)
      post->md_path[0] = 0;

   // --- extract bucket, and some top-level fields
   int scanf_size = sscanf(post_str, MARFS_POST_FORMAT,
                           &major, &minor,
                           &obj_type_code,
                           &post->obj_offset,
                           &post->chunks,
                           &post->chunk_info_bytes,
                           &post->correct_info,
                           &post->encrypt_info,
                           &post->flags,
                           (char*)&post->md_path); // might be empty

   if (scanf_size == EOF)
      return -1;                // errno is set
   else if (scanf_size < 9) {
      errno = EINVAL;
      return -1;            /* ?? */
   }

   // validate version
   // NOTE: This just assures us that we know how to parse this thing.
   //
   //   //   if ((   major != marfs_config->version_major)
   //   //       || (minor != marfs_config->version_minor)) {
   //   //      LOG(LOG_ERR, "xattr vers '%d.%d' != config %d.%d\n",
   //   //          major, minor,
   //   //          marfs_config->version_major, marfs_config->version_minor);
   //   //      errno = EINVAL;            /* ?? */
   //   //      return -1;
   //   //   }
   //
   //   if ( major != 0) {
   //      LOG(LOG_ERR, "xattr vers '%d.%d' Parser expected 0.x\n",
   //          major, minor);
   //      errno = EINVAL;
   //      return -1;
   //   }
   //
   if ((   major != MARFS_CONFIG_MAJOR)
       || (minor >  MARFS_CONFIG_MINOR)) {
      LOG(LOG_ERR, "xattr vers '%d.%d' != config %d.%d\n",
          major, minor,
          MARFS_CONFIG_MAJOR, MARFS_CONFIG_MINOR);
      errno = EINVAL;
      return -1;
   }


   post->config_vers_maj = major;
   post->config_vers_min = minor;

   post->obj_type    = decode_obj_type(obj_type_code);
   return 0;
}








// from MarFS_XattrShard to string
int shard_2_str(char* shard_str,        const MarFS_XattrShard* shard) {
   LOG(LOG_ERR, "Not implemented\n");
   errno = ENOSYS;
   return -1;
}
// from string to MarFS_XattrShard
int str_2_shard(MarFS_XattrShard* shard, const char* shard_str) {
   LOG(LOG_ERR, "Not implemented\n");
   errno = ENOSYS;
   return -1;
}






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

// TO STRING
// perform a conversion (e.g. htons()), while moving data of type TYPE from
// SOURCE (variable) to DEST (string).  DEST is updated to a point just
// after the copied data.
#define COPY_OUT(DEST, SOURCE, TYPE, CONVERSION_FN)  \
   {  TYPE temp = CONVERSION_FN (SOURCE);            \
      memcpy(DEST, (char*)&temp, sizeof(TYPE));      \
      DEST += sizeof(TYPE);                          \
   }

// FROM STRING
// perform a conversion (e.g. ntohs()), while moving data of type TYPE from
// SOURCE (string) to DEST (variable).  SOURCE is updated to a point just
// after the used data.
#define COPY_IN(DEST, SOURCE, TYPE, CONVERSION_FN)  \
   {  TYPE temp;                                    \
      memcpy((char*)&temp, SOURCE, sizeof(TYPE));   \
      DEST = CONVERSION_FN( temp );                 \
      SOURCE += sizeof(TYPE);                       \
   }




#if 0
// UNDER CONSTRUCTION ...

// from string to RecoveryInfo.  Presumably the string is what you got
// from the tail-end of an object.  Use this to convert the string to a
// RecoveryInfo struct.
int str_2_recoveryinfo(RecoveryInfo* rec,
                       const char*   rec_str,
                       const size_t  str_size) {
   LOG(LOG_ERR, "Not implemented\n");
   errno = ENOSYS;
   return -1;


   char* rec_data_size_ptr = rec_str + str_size - sizeof(uint64_t);
   char* obj_data_size_ptr = rec_data_size_ptr  - sizeof(uint64_t);

   char* src = rec_str;

   COPY_IN(rec->config_vers_maj,  src, ConfigVersType, ntohs);
   COPY_IN(rec->config_vers_min,  src, ConfigVersType, ntohs);
   COPY_IN(rec->inode,            src, uint64_t,       ntohll);
   COPY_IN(rec->mode,             src, uint32_t,       ntohl);
   COPY_IN(rec->uid,              src, uint32_t,       ntohl);
   COPY_IN(rec->gid,              src, uint32_t,       ntohl);
   COPY_IN(rec->mtime,            src, uint64_t,       ntohll);
   COPY_IN(rec->ctime,            src, uint64_t,       ntohll);

   rec->mdfs_path = src;
   src += strlen(rec->mdfs_path);

   rec->pre = src;
   src += strlen(rec->pre);

   rec->post = src;
   src += strlen(rec->post);

   rec->str_data = rec_str;     // points to caller's data

   return 0;
}


// // this is just a sketch.
// int get_recovery_string() { }
// int get_next_recovery_string(RecoveryInfo* info) { }

#endif




// We write MultiChunkInfo as binary data (in network-byte-order) in hopes
// that this will speed-up treating the MD file as a big index, during
// pftool restarts.  Return number of bytes moved, or -1 and errno.
//
// NOTE: If max_size == sizeof(MultiChunkInfo), we may still return a
//     size less than sizeof(MultiChunkInfo), because the struct may
//     include padding associated with alignment.

ssize_t chunkinfo_2_str(char* str, const size_t max_size, const MultiChunkInfo* chnk) {
   if (max_size < sizeof(MultiChunkInfo)) {
      errno = EINVAL;
      return -1;
   }
   char* dest = str;

   COPY_OUT(dest, chnk->config_vers_maj,  ConfigVersType, htons);
   COPY_OUT(dest, chnk->config_vers_min,  ConfigVersType, htons);
   COPY_OUT(dest, chnk->chunk_no,         size_t,         htonll);
   COPY_OUT(dest, chnk->logical_offset,   size_t,         htonll);
   COPY_OUT(dest, chnk->chunk_data_bytes, size_t,         htonll);
   COPY_OUT(dest, chnk->correct_info,     CorrectInfo,    htonll);
   COPY_OUT(dest, chnk->encrypt_info,     EncryptInfo,    htonll);

   return (dest - str);
}

// <str> holds binary data (in network-byte-order) read from a Multi MD
// file, representing one chunk.  Parse it into a MultiChunkInfo.
//
// NOTE: We return the actual amount of data read.  In practice, the
//    MultiChunkInfo struct takes 48 bytes, but there are only 44 bytes of
//    data-values.  (4 bytes of padding, betw config_vers_min and
//    chunk_no.)  chunkinfo_2_str() will have written 44 bytes, but
//    write_chunkinfo() will write on 48-byte boundaries (because of the
//    sizeof MultiChunkInfo).  Therefore, when reading, we will also only
//    read 44 bytes.  Therefore, callers should be aware that our
//    return-value can legitimately be <= the <str_len> argument.
ssize_t str_2_chunkinfo(MultiChunkInfo* chnk, const char* str, const size_t str_len) {

   // We require str_len >= sizeof(MultiChunkInfo), even though it's
   // possible that chunkinfo_2_str() can encode a MultiChunkInfo into a
   // string that is smaller than sizeof(MultiChunkInfo), because of
   // padding for alignment, within the struct.  We're just playing it
   // safe.
   if (str_len < sizeof(MultiChunkInfo)) {
      errno = EINVAL;
      return -1;
   }
   char* src = (char*)str;

   COPY_IN(chnk->config_vers_maj,  src, ConfigVersType, ntohs);
   COPY_IN(chnk->config_vers_min,  src, ConfigVersType, ntohs);
   COPY_IN(chnk->chunk_no,         src, size_t,         ntohll);
   COPY_IN(chnk->logical_offset,   src, size_t,         ntohll);
   COPY_IN(chnk->chunk_data_bytes, src, size_t,         ntohll);
   COPY_IN(chnk->correct_info,     src, CorrectInfo,    ntohll);
   COPY_IN(chnk->encrypt_info,     src, EncryptInfo,    ntohll);

   return (src - str);
}



// ---------------------------------------------------------------------------
// validate the results of read_config()
// ---------------------------------------------------------------------------

// This is just a collection of ad-hoc tests for various inconsistencies
// and illegal states, that wouldn't be detected by the parser.
//
// Return 0 for success, non-zero for failure.
//


int validate_configuration() {

   int           retval = 0;
   const size_t  recovery = MARFS_REC_UNI_SIZE;

   // --- repo checks
   MarFS_Repo*   repo = NULL;
   RepoIterator  rit = repo_iterator();
   while ((repo = repo_next(&rit))) {

      // chunk_size must be greater than the size of the recovery-info that
      // is written into the tail of objects.
      //
      // NOTE: This shouldn't apply to repos that are DIRECT or SEMI_DIRECT
      //     because they won't store any recovery-data.
      //
      if (repo->chunk_size <= recovery) {
         LOG(LOG_ERR, "repo '%s' has chunk-size (%ld) "
             "less than the size of recovery-info (%ld)\n",
             repo->name, repo->chunk_size, recovery);
         retval = -1;
      }
   }


   // --- namespace checks
   MarFS_Namespace* ns = NULL;
   NSIterator       nit = namespace_iterator();
   while ((ns = namespace_next(&nit))) {

      // assure NS name (plus terminal NULL) doesn't exceed the length we defined
      if (ns->name_len >= MARFS_MAX_NAMESPACE_NAME) {
         LOG(LOG_ERR, "NS name '%s' has length %ld,"
             " exceeding the limit (%d)\n",
             ns->name, ns->name_len, MARFS_MAX_NAMESPACE_NAME);
         retval = -1;
      }

      // configuration just parsed the MDAL type.
      // Go find the corresponding MDAL and install
      MDAL* dir_MDAL = get_MDAL(ns->dir_MDAL_type);
      if ( ! dir_MDAL ) {
         LOG( LOG_ERR, "Couldn't find MDAL named \"%s\".\n",
              MDAL_type_name(ns->dir_MDAL_type) );
         retval = -1;
      }
      ns->dir_MDAL = dir_MDAL;

      MDAL* file_MDAL = get_MDAL(ns->file_MDAL_type);
      if ( ! file_MDAL ) {
         LOG( LOG_ERR, "Couldn't find MDAL named \"%s\".\n",
              MDAL_type_name(ns->file_MDAL_type) );
         retval = -1;
      }
      ns->file_MDAL = file_MDAL;

   }


   return retval;
}
