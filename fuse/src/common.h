#ifndef _MARFS_COMMON_H
#define _MARFS_COMMON_H

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



// Must come before anything else that might include <time.h>
#include "marfs_base.h"
#include "mdal.h"               // abstraction for MD file/dir ops
#include "object_stream.h"      // FileHandle needs ObjectStream
#include "marfs_configuration.h"

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>             // DIR*
#include <fcntl.h>
#include <utime.h>
#include <unistd.h>
#include <errno.h>

#  ifdef __cplusplus
extern "C" {
#  endif


typedef uint8_t  FuseContextFlagType;

typedef enum {
   PUSHED_USER  = 0x01,         // push_security user already ran
} FuseContextFlags;

// install this into fuse_get_context()->private_data?
typedef struct {
   FuseContextFlagType  flags;
   uid_t                user;   // if (flags & PUSHED_USER)
} MarFS_FuseContextInfo;



// // Human-readable argument to functions with an <is_interactive> parameter
// typedef enum {
//    MARFS_BATCH       = 0,
//    MARFS_INTERACTIVE = 1
// } MarFS_Interactivity;


// // Variation on the MarFS_Interactivity
// // Human-readable argument to functions with an <use_iperms> parameter
// typedef enum {
//    B_PERMS      = 0,
//    I_PERMS      = 1
// } MarFS_PermSelect;



// ---------------------------------------------------------------------------
// TRY, etc
//
// Macro-wrappers around common functions allow the fuse code to be a lot
// cleaner.  The return value from every function-call has to be tested,
// and perhaps return an error-code if things aren't right.  These hide the
// test-and-return.
// ---------------------------------------------------------------------------


#define TRY_DECLS()                                \
   __attribute__ ((unused)) size_t   rc = 0;       \
   __attribute__ ((unused)) ssize_t  rc_ssize = 0

// add log items when you enter/exit a function
#define ENTRY()                                                         \
   LOG(LOG_INFO, "\n");                                                 \
   LOG(LOG_INFO, "-> %s\n", __FUNCTION__);                              \
   TRY_DECLS()

#define EXIT()                                               \
   LOG(LOG_INFO, "<- %s\n", __FUNCTION__)



// For example, the errno set by stream_sync() might be inappropriate to
// return from marfs_read() versus marfs_release().  You could tailor the
// errno that TRY(...) will return, by changing PRE_RETURN().
#define PRE_RETURN()

// Override this, if you have some fuse-handler that wants to do
// something special before any error-return.  (See e.g. fuse_open)
#define RETURN(VALUE)  return(VALUE)




// TRY() macros test for return-values, and skip out early if they get a
//       non-zero.  The "do { ... } while()" just makes sure your macro
//       statements will still work correctly inside single-statment
//       conditions, like this:
//
//       if (...)
//         TRY(...);
//       else
//         TRY(...);
//
// NOTE: rc is lexically-scoped.  It's defined at the top of fuse functions
//       via ENTRY(), or PUSH_USER().  This allows us to use TRY_GE0() on
//       functions whose return value we care about.
//
// NOTE: TRY macros also invert the sign of the return value, as needed for
//       fuse.  This means they shouldn't be used within common functions,
//       which may in turn be wrapped inside TRY() by fuse routines.
//       [see __TRY0()]
//
// UPDATE: Now that utility-functions need to support both fuse and pftool,
//       we expect everything to just return 0 for success, or -1 with
//       errno for failure.  Most of the utility functions use TRY(), so
//       we'll just have that return -1 plus errno, for failure.  Fuse
//       proper can then use __TRY(), which should return -errno.


#define TRY0(FNCALL)                                                    \
   do {                                                                 \
      /* LOG(LOG_INFO, "TRY0(%s)\n", #FNCALL); */                       \
      rc = (size_t)FNCALL;                                              \
      if (rc) {                                                         \
         PRE_RETURN();                                                  \
         LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n",             \
             #FNCALL, rc, errno, strerror(errno));                      \
         RETURN(-1);                                                    \
      }                                                                 \
   } while (0)

// e.g. open() returns -1 or an fd.
#define TRY_GE0(FNCALL)                                                 \
   do {                                                                 \
      /* LOG(LOG_INFO, "TRY_GE0(%s)\n", #FNCALL); */                    \
      rc_ssize = (ssize_t)FNCALL;                                       \
      if (rc_ssize < 0) {                                               \
         PRE_RETURN();                                                  \
         LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n",             \
             #FNCALL, rc_ssize, errno, strerror(errno));                \
         RETURN(-1);                                                    \
      }                                                                 \
   } while (0)

// e.g. opendir() returns a pointer or NULL
#define TRY_GT0(FNCALL)                                                 \
   do {                                                                 \
      /* LOG(LOG_INFO, "TRY_GT0(%s)\n", #FNCALL); */                    \
      rc_ssize = (ssize_t)FNCALL;                                       \
      if (rc_ssize <= 0) {                                              \
         PRE_RETURN();                                                  \
         LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n",             \
             #FNCALL, rc_ssize, errno, strerror(errno));                \
         RETURN(-1);                                                    \
      }                                                                 \
   } while (0)






// FOR INTERNAL USE (by fuse/pftool) ONLY.
// [See "UPDATE", above]
//
#define __TRY0(FNCALL)                                                  \
   do {                                                                 \
      LOG(LOG_INFO, "TRY0: %s\n", #FNCALL);                             \
      rc = (size_t)FNCALL;                                              \
      if (rc) {                                                         \
         PRE_RETURN();                                                  \
         LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n",             \
             #FNCALL, rc, errno, strerror(errno));                      \
         RETURN(-errno);                                                \
      }                                                                 \
   } while (0)

#define __TRY_GE0(FNCALL)                                               \
   do {                                                                 \
      LOG(LOG_INFO, "TRY_GE0: %s\n", #FNCALL);                          \
      rc_ssize = (ssize_t)FNCALL;                                       \
      if (rc_ssize < 0) {                                               \
         PRE_RETURN();                                                  \
         LOG(LOG_INFO, "FAIL: %s (%ld), errno=%d '%s'\n\n",             \
             #FNCALL, rc_ssize, errno, strerror(errno));                \
         RETURN(-errno);                                                \
      }                                                                 \
   } while (0)





#define EXPAND_PATH_INFO(INFO, PATH)   TRY0( expand_path_info((INFO), (PATH)) )
#define TRASH_UNLINK(INFO, PATH)       TRY0( trash_unlink((INFO), (PATH)) )
#define TRASH_TRUNCATE(INFO, PATH)     TRY0( trash_truncate((INFO), (PATH)) )
#define TRASH_NAME(INFO, PATH)         TRY0( trash_name((INFO), (PATH)) )

#define STAT_XATTRS(INFO)              TRY0( stat_xattrs((INFO)) )
#define STAT(INFO)                     TRY0( stat_regular((INFO)) )

#define SAVE_XATTRS(INFO, MASK)        TRY0( save_xattrs((INFO), (MASK)) )



// return an error, if all the required permission-flags are not asserted
// in the iperms or bperms of the given NS.
#define CHECK_PERMS(NS, REQUIRED_PERMS)                                 \
   do {                                                                 \
      TRY_DECLS();                                                      \
      TRY_GE0( get_runtime_config(MARFS_INTERACTIVE) ); /* fuse calling? */ \
      MarFS_Perms actual_perms = (rc_ssize ? (NS)->iperms : (NS)->bperms); \
      LOG(LOG_INFO, "check_perms (%s) req:%08x actual:%08x\n",          \
          (rc_ssize ? "interactive" : "batch"),                         \
          (REQUIRED_PERMS), actual_perms);                              \
      if ((actual_perms & (REQUIRED_PERMS)) != (REQUIRED_PERMS)) {      \
         LOG(LOG_ERR, "check_perms denied\n");                          \
         errno = EACCES;   /* should be EPERM? (i.e. being root wouldn't help) */ \
         return -1;                                                     \
      }                                                                 \
   } while (0)


// this follows symlinks!
#if USE_MDAL
// NOTE: It might be wrong to expand path info here, as this macro is
//       often called on the info.post->md_path.
#define ACCESS(NAMESPACE, PATH, PERMS)                                  \
   TRY0( F_OP_NOCTX(access, (NAMESPACE), (PATH), (PERMS)) )
#else
#define ACCESS(_NS, PATH, PERMS) TRY0( access((PATH), (PERMS)) )
#endif

// NOTE: faccessat, with the AT_EACCESS flag is probably a good solution to
//       the problem of access testing against real user id, rather than
//       effective user id.
// With these args, this doesn't follow symlinks
#if USE_MDAL
#define FACCESSAT(NAMESPACE, PATH, PERMS)                               \
   TRY0( F_OP_NOCTX(faccessat, (NAMESPACE), 0, (PATH), (PERMS),         \
                    (AT_EACCESS | AT_SYMLINK_NOFOLLOW)) )
#else
#define FACCESSAT(PATH, PERMS)                                          \
   TRY0( faccessat(0, (PATH), (PERMS), (AT_EACCESS | AT_SYMLINK_NOFOLLOW) )
#endif


// ---------------------------------------------------------------------------
// xattrs (handling)    [see marfs_base.*, for xattr structs]
// ---------------------------------------------------------------------------


// These describe xattr keys, and the type of the corresponding values, for
// all the metadata fields in a MarFS_ReservedXattr.  These support a
// generic parser for extracting and parsing xattr data from a metadata
// file (or maybe also from object metadata).
//
// As they are found in stat_xattrs(), each flag is OR'ed into a counter,
// so that has_any_xattrs() can tell you whether specific xattrs were
// found.
//
// NOTE: co-maintain XattrMaskType, ALL_MARFS_XATTRS, MARFS_MD_XATTRS


typedef uint8_t XattrMaskType;  // OR'ed XattrValueTypes

typedef enum {
   XVT_NONE       = 0,          // marks the end of <xattr_specs>

   XVT_PRE        = 0x01,
   XVT_POST       = 0x02,
   XVT_RESTART    = 0x04,
   XVT_SHARD      = 0x08,
   
} XattrValueType;

// shorthand for useful XattrValueType combinations
#define MARFS_MD_XATTRS   (XVT_PRE | XVT_POST)     /* MD-related XattrValueTypes */
#define MARFS_ALL_XATTRS  (XVT_PRE | XVT_POST | XVT_RESTART | XVT_SHARD)  /* all XattrValueTypes */





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
// initialized in init_xattr_specs()
extern XattrSpec*  MarFS_xattr_specs;





// ---------------------------------------------------------------------------
// PathInfo
//
// used to accumulate FUSE-support information.
// see expand_path_info(), and stat_xattrs()
//
// NOTE: stat_xattrs() sets the flags in PathInfo.xattrs, for each
//       corresponding xattr that is found.  Use has_any_xattrs() or
//       has_all_xattrs(), with a mask, to check for presence of xattrs you
//       care about, after stat_xattr().  The <mask> is just
//       XattrValueTypes, OR'ed together.
//
//       Because this is C, and not C++, I'm not tracking which xattrs you
//       might be changing and updating.  However, when you call
//       save_xattrs(), you can provide another mask (i.e. just like
//       has_any_xattrs()), and we'll install all the xattrs that your mask
//       selects.  (You could use this to write different xattrs at
//       different times, or to update a specific one several times.)
//
// ---------------------------------------------------------------------------


typedef uint8_t  PathInfoFlagType;

typedef enum {
   PI_EXPANDED     = 0x01,      // expand_path_info() was called?
   PI_STAT_QUERY   = 0x02,      // i.e. maybe PathInfo.st empty for a reason
   PI_XATTR_QUERY  = 0x04,      // i.e. maybe PathInfo.xattr empty for a reason
   PI_TRASH_PATH   = 0x08,      // expand_trash_info() was called?
   //   PI_STATVFS      = 0x80,      // stvfs has been initialized from Namespace.fsinfo?
} PathInfoFlagValue;


typedef struct PathInfo {
   MarFS_Namespace*     ns;
   struct stat          st;
   // struct statvfs       stvfs;  // applies to Namespace.fsinfo

   MarFS_XattrPre       pre;
   MarFS_XattrPost      post;
   MarFS_XattrRestart   restart;
   MarFS_XattrShard     shard;

   XattrMaskType        xattrs;      // OR'ed XattrValueTypes, use has_any_xattrs()
   XattrMaskType        xattr_inits; // OR'ed XattrValueTypes, use has_any_xattrs()

   PathInfoFlagType     flags;
   char                 trash_md_path[MARFS_MAX_MD_PATH];
} PathInfo;


// ...........................................................................
// FileHandle
//
// Fuse open() dynamically-allocates one of these, and stores it in
// fuse_file_info.fh. The FUSE impl gives us state that may be accessed
// across multiple callbacks.  For example, marfs_open() might save info
// needed by marfs_write().
// ...........................................................................

typedef enum {
   FH_READING         = 0x0001,  // might someday allow O_RDWR
   FH_WRITING         = 0x0002,  // might someday allow O_RDWR
   FH_DIRECT          = 0x0004,  // i.e. PathInfo.xattrs has no MD_
   FH_Nto1_WRITES     = 0x0010,  // implies pftool calling. (Can write N:1)
   FH_MULTI_THR       = 0x0020,  // multi-threaded reads of FH (i.e. NFS)
   FH_RELEASING       = 0x0040,  // added to inform multi-thr during release
   FH_PACKED          = 0x0080,  // the object in the file handle is packed
} FHFlags;

typedef uint16_t FHFlagType;


// read() can maintain state here
// (see notes in WriteStatus, re marfs_data).
//
// If you seek() then read(), the only thing fuse sees is a change in the
// read-offset.  Unlike writes, it is legal to read discontiguous sections
// of a marfs file.  Therefore, unlike with write, read only needs to track
// the logical offset of the current "read head", to know whether the next
// read requires a close/re-open, or can continue to use the same stream.
//
// The "logical offset" is the position in the user's data for this file.
// For a multi-object, this ignores the recovery-info at the tail of all
// previous objects.  For a packed-object, this ignores all the packed
// "objects" in the object-data, before this object.
//
// When reading via fuse, the total amount to be read is not known at
// open-time, so we use open-ended byte-ranges with the requests.  Note
// that when reading with open-ended byte-ranges, the server may actually
// move more data than the user ends up wanting, but we don't know how much
// they will want until they explicitly close (or implicitly close by
// seeking to a different offset).  In these cases, stream_sync() has to
// tell curl that it doesn't want any more data, which results in the GET
// request returning an error code, which stream_sync() must identify and
// hide, in order to return success.
//
// In the case of pftool, we know the read-length at open-time, so we can
// allow reading of continuous spans using explicit byte-ranges, which
// avoids having the server move more data than necessary
// (e.g. recovery-info, or other objects in a PACKED), and may be more
// efficient.  In this case, the request will terminate with movement of
// the final char, and stream_sync() should not expect further callbacks
// made on the writefunc, (so there will be no need for trapping
// error-codes, etc, as described above).
//
// STUPID NFS TRICKS
//
// When a MarFS fuse-mount is exported over NFS, nfsd reads with multiple
// threads at different offsets on the same filehandle.  This means some
// fuse threads handling read will be asked to read at an offset that
// doesn't match the current stream position.  The default way to handle
// readds at a discontiguous offset is to close the stream and reopn.  With
// an object-stream back-end, this means terminating the GET, and starting
// a new GET.  Then, of course, a different fuse thread will eventually
// want to read the part we skipped over.  The result is horrendously-bad
// BW through NFS.
//
// We can get some relief from this by having threads block for a moment to
// allow other threads the chance to read the intervening part of the
// stream, so we avoid the close/reopen, in some cases.  But this means
// adding delays into the read, and nfsd also just gets more and more
// ambitious, trying wider and wider offsets that are decreasingly likely
// to be filled.
//
// So, the new approach to this problem is to first notice whether delaying
// to give other threads a chance to fill in the gaps ever results in some
// of that gap being (even partially) filled.  If so, then we know there
// are multiple-threads doing the reading on the same file-handle, and we
// can be fairly sure that only NFS would be arrogant enough to try
// something like that.  In turn, we make our own bold assumption that
// readers over NFS are really reading the whole file sequentially (so a
// request for an out-of-order offset will always eventually find that the
// stream before it has been consumed).  Therefore, any out-of-order reader
// can put his offset on a queue, along with a new lock.  Then he can wait
// on that new lock.  Everyone who finishes a read will check the queue to
// see if someone is waiting at exactly the offset as it stands after the
// current read, and, if so, they will post the corresponding lock upon
// which that other reader is waiting, at which point the other reader can
// maintain the queue and proceed with his read.  That's what the
// "read_queue" stuff is, in this file.  See also marfs_read().

typedef struct ReadQueueElt {
   off_t                offset;
   SEM_T                lock;      // thread waits to read at <offset>
   int                  rewinding; // other waiter doing close/reopen
   volatile struct ReadQueueElt* next;
} ReadQueueElt;


typedef struct {
   volatile size_t        log_offset;    // effective offset (shows contiguous reads)
   volatile size_t        data_remain;   // the unread part of marfs_open_at_offset()
   volatile ReadQueueElt* read_queue;    // out-of-order reads from NFS threads
} ReadStatus;


// write() can maintain state here
//
// <sys_writes> tracks data written into the object which is not user-data.
// For example, we write recovery-data into objects, using the same
// ObjectStream as is used to write user-data.  That will be included in
// the total write-count found in ObjectStream.written.  So, we count the
// purely-system-related writes in FileHandle.write_status.sys_writes.
// Thus, when it comes time to truncate the MDFS file to the size of the
// data written by the user, we can compute the appropriate size.
//
// In the case where someone calls marfs_open() with a specific
// content-length, we can pass this on to stream_open(), which converts it
// into a HTTP Content-Length header.  Scality sproxyd streams without
// content-length header (i.e. with chunked transfer-encoding) are
// apparently buffered until the stream closes, before being forward to
// storage (e.g. an entire MarFS chunk).  On the other hand, streams with
// content-lengths are forwarded as they are received.  Fuse doesn't know
// the size of the stream, so it can't take advantage of this, but pftool
// does, so it can.
//
// HOWEVER, we still need to break long pftool writes with given size
// into MarFS chunks (pftool could do it, but we have all the expertise
// here).  Meanwhile, fuse writes may also cross object-boundaries.
//
// stream_open() uses chunked-transfer-encoding when called with
// content_length=0.  Users then call marfs_write repeatedly, which closes
// and repoens new objects as needed, writting recovery-info at the end of
// each.  When called with content-length non-zero, stream_open() installs
// that as the content-length for the request.  In this case, pftool would
// be calling with the size of a logical chunk of user-data, expecting to
// write a complete object (or possibly a smaller size for the final part
// of a multi).  We add to the size of the request, so as to cover the
// recovery-info at the tail.  (Note that an individual pftool task may
// write multiple chunks to the same open "file".)
//
// In order to handle both cases, we track a content-length provided at
// open time in FileHandle.data_remain.  In the case of fuse this could be
// zero, as would be user_req, and sys_req.  However, for performance, fuse
// may also pick content-length = chunksize.  This implies a precomputed
// size of the recover-info that we write into the tail of the object.
// That can be accommdated by adding padding into the rec-info, at the
// indicated place.  (see marfs_base.h).

// For pftool, data_remain will be the total size of user-data to be
// written.  When closing/reopening at object boundaries, we want to
// decrement this by the amount of user-data in the previous request.
// However, we want the actual request to also include room for the
// recovery-info written at the end.  Thus, we need both user_req and
// sys_req, to track these two things.
//
// Both schemes (fuse and pftool) can now be handled by giving user_req +
// sys_req as the content-length argument to stream_open().  When closing a
// chunk, we can always write recovery-info at the tail.  We only decrement
// the data_remain by the previous request-size when we are reopening at an
// object-boundary (i.e. when making the next request).  This allows
// close-and-reopen to correctly track the remaining size, without getting
// screwed by truncate.
//
// Recovery-info must know the amount of user-data writen into the file.
// In the case of a multi, we could assume that recovery-info is always
// padded to a fixed size.  The user-data would then be chunksize -
// recinfosize.  At thge tail of a multi, we could divide the total written
// by this amount, and the remainder would be the amount in the final
// chunk.  However, that's likely to be complicated abnd obscure.  An
// easier approach is to record the amount of user-data written each time
// we write recovery-info.  Then teh amount of user-data in the current
// chunk is just the difference from the mark as it was when we last wrote
// recovery-info (accounting for sys_writes).

typedef struct {
   size_t        sys_writes;    // discount this much from FileHandle.os.written
   size_t        data_remain;   // remaining user-data size (incl current req)
   size_t        user_req;      // part of current request for user-data
   size_t        sys_req;       // part of current request for sys-data (recovery-info)
   size_t        rec_info_mark; // total user-data written as of last rec-info mark
} WriteStatus;



typedef struct {
   MDAL_Context  ctx;
   struct MDAL*  mdal;
} MDAL_Handle;



typedef struct {
   PathInfo        info;         // includes xattrs, MDFS path, etc
   char            ns_path[MARFS_MAX_NS_PATH];  // path in NS, not in MDFS

   // NOTE: These are temporarily both defined, so that file-handles will
   //       have a constant size, regardless of whether USE_MDAL is defined
   //       or not, to provide a short-term solution to the issue at
   //       https://github.com/pftool/pftool/issues/31
   //
   //#if USE_MDAL
   MDAL_Handle     f_handle;     // file-oriented MDAL ops
   //#else
   int             md_fd;        // opened for reading meta-data, or data
   //#endif

   curl_off_t      open_offset;  // [see comments at marfs_open_with_offset()]
   ReadStatus      read_status;  // buffer_management, current_offset, etc
   WriteStatus     write_status; // buffer-management, etc

   ObjectStream    os;           // handle for streaming access to objects
   uint8_t         os_init;      // tells weather or not the object streem is inizlized
   curl_off_t      objectSize;   // The size of the object for packed files
   int             fileCount;    // The number of files that have been packed
                                 // into the file

   FHFlagType      flags;
} MarFS_FileHandle;

// I'm not sure this is an improvement over all the #ifs
#if USE_MDAL
#  define MD_FILE_OP(...)   F_OP( __VA_ARGS__ )
#  define MD_DIR_OP(...)    D_OP( __VA_ARFS__ )
#  define MD_PATH_OP(...)   F_OP_NOCTX( __VA_ARGS__ )
// unfirtunately we need this in order to use the dir_MDAL for mkdir/rmdir
#  define MD_D_PATH_OP(...) D_OP_NOCTX( __VA_ARGS__ )
#else
// We don't always want the fh->md_fd (see write_trash_companion_file)
//  ... BUT ... maybe the only place we don't is the trash functions,
//  which can be easily (?) rewriten.
#  define MD_FILE_OP(OP, FH, ...)       (OP)( (FH)->md_fd, __VA_ARGS__ )
#  define MD_DIR_OP(OP, _NS, ...)      (OP)( __VA_ARGS__ )
#  define MD_PATH_OP(OP, _NS, ...)   (OP)( __VA_ARGS__ )
#  define MD_D_PATH_OP(OP, _NS, ...) (OP)( __VA_ARGS__ )
#endif

#if USE_MDAL
// shorthand
#  define F_CTX(FH)              &(FH)->f_handle.ctx
#  define F_MDAL(FH)             (FH)->f_handle.mdal

#  define F_OP(OP,FH, ...)       (*(FH)->f_handle.mdal->OP)(F_CTX(FH), ##__VA_ARGS__)
#  define F_OP_NOCTX(OP,NS, ...) (*(NS)->file_MDAL->OP)(__VA_ARGS__)
/*
 #  define F_OP(OP,FH, ...)                                            \
   do {                                                               \
   if (! (*(FH)->f_handle.mdal->OP)) {                                \
      LOG(LOG_ERR, "Function " #OP " not implemented for MDAL!\n");   \
      exit(EXIT_FAILURE);                                             \
   }                                                                  \
   else {                                                             \
      (*(FH)->f_handle.mdal->OP)(F_CTX(FH), ##__VA_ARGS__);           \
   } while (0)
*/
#endif


// some ops (e.g. rename()) have no file-descriptor/context.  For these,
// we'll just use the MDAL from the NS to select the implementation, and
// call the function without passing a context.  [Or, should we initialize
// a MDAL_Context, call with that, then destroy?]
#if USE_MDAL
#  define CTX_FREE_OP(OP,NS, ...)   (*(NS)->file_MDAL->OP)(__VA_ARGS__)
#endif



// fuse/pftool-agnostic updates of data_remain, etc. (see comments, above)
size_t  get_stream_wr_open_size(MarFS_FileHandle* fh, uint8_t decrement);




// directory-handle covers two cases:
// (a) Listing an MDFS directory -- just need a DIR*
// (b) List the marfs "root" directory -- need a Namespace-Iterator.
typedef struct {
   uint8_t   use_it;         // if non-zero, use <it>, else use internal.dirp/d_handle

   union {
   // NOTE: These are temporarily both defined, so that dir-handles will
   //       have a constant size, regardless of whether USE_MDAL is defined
   //       or not, to provide a short-term solution to the issue at
   //       https://github.com/pftool/pftool/issues/31
   //
      // #if USE_MDAL
      MDAL_Handle d_handle;  // dir-oriented MDAL ops
      // #else
      DIR*        dirp;
      // #endif
      NSIterator  it;
   } internal;

} MarFS_DirHandle;


#if USE_MDAL
// shorthand
#  define D_CTX(DH)          &(DH)->internal.d_handle.ctx
#  define D_MDAL(DH)         (DH)->internal.d_handle.mdal

#  define D_OP(OP,DH, ...)        (*(DH)->internal.d_handle.mdal->OP)(D_CTX(DH), ##__VA_ARGS__)
#  define D_OP_NOCTX(OP, NS, ...) (*(NS)->dir_MDAL->OP)(__VA_ARGS__)
/*
#  define D_OP(OP,DH, ...)                                            \
   do {                                                               \
   if (! (*(DH)->internal.d_handle.mdal->OP)) {                       \
      LOG(LOG_ERR, "Function " #OP " not implemented for MDAL!\n");   \
      exit(EXIT_FAILURE);                                             \
   }                                                                  \
   else {                                                             \
      (*(DH)->internal.d_handle.mdal->OP)(D_CTX(DH), ##__VA_ARGS__);  \
   } while (0)
*/
#endif



// // In C++ I'd just steal the templated Pool<T> classes from pftool, to
// // allow us to re-use dynamically-allocate objects.  Here, I'll make a
// // crude stack and functions to alloc/free.
// //
// // Hmmm.  That's going to lead to a locking bottleneck.  Maybe we don't
// // care, in fuse?
// 
// typedef struct Reusable {
//    void*            obj;
//    int              avail;
//    struct Reusable* next;
// } Reusable;
// 
// extern void* alloc_reusable(Reusable** ruse);
// extern void  free_reusable (Reusable** ruse, void* obj);




// strip the leading <mnt_top> from an arbitrary path.
// Return NULL if no match.
extern const char* marfs_sub_path(const char* path);
extern int         under_mdfs_top(const char* path);
extern int         follow_some_links(PathInfo* info, char* buf, size_t size);

extern int  init_mdfs();

// These initialize different parts of the PathInfo struct.
// Calling them redundantly is cheap and harmless.
extern int  expand_path_info (PathInfo* info, const char* path);
extern int  expand_trash_info(PathInfo* info, const char* path);

extern int  stat_regular     (PathInfo* info);
extern int  stat_xattrs      (PathInfo* info);
extern int  save_xattrs      (PathInfo* info, XattrMaskType mask);

extern int  md_exists        (PathInfo* info);

// initialize MarFS_xattr_specs
extern int  init_xattr_specs();

//extern int  has_marfs_xattrs (PathInfo* info);
extern int  has_all_xattrs (PathInfo* info, XattrMaskType mask);
extern int  has_any_xattrs (PathInfo* info, XattrMaskType mask);

extern int  trunc_xattrs  (PathInfo* info);

// need the path to initialize info->trash_md_path
extern int  trash_unlink  (PathInfo* info, const char* path);
extern int  trash_truncate(PathInfo* info, const char* path);

extern int  check_quotas  (PathInfo* info);

extern int  update_url     (ObjectStream* os, PathInfo* info);
extern int  update_timeouts(ObjectStream* os, PathInfo* info);

// currently just opens for writing.
extern int  open_md   (MarFS_FileHandle* fh, int writing_p);
extern int  is_open_md(MarFS_FileHandle* fh);
extern int  close_md  (MarFS_FileHandle* fh);

extern int  opendir_md(MarFS_DirHandle* fh, PathInfo* info);
extern int  closedir_md(MarFS_DirHandle *dh);

// write MultiChunkInfo (as binary data in network-byte-order), into file
// From fuse, <user_data_written> is total from zero. From pftool, it's the
// amount in this block.
extern int     write_chunkinfo(MarFS_FileHandle*     fh,
                               // size_t                open_offset,
                               size_t                user_data_written,
                               int                   size_is_per_chunk);

extern int     read_chunkinfo (MarFS_FileHandle* fh, MultiChunkInfo* chnk);

extern int     seek_chunkinfo (MarFS_FileHandle* fh, size_t chunk_no);

extern ssize_t count_chunkinfo(MarFS_FileHandle* fh);


extern ssize_t write_recoveryinfo(ObjectStream* os, PathInfo* info, MarFS_FileHandle* fh);


// support for pftool, doing N:1 writes
extern ssize_t get_chunksize(const char* path,
                             size_t      file_size,
                             size_t      desired_chunk_size,
                             uint8_t     adjust_for_recovery_info);

extern ssize_t get_chunksize_with_info(PathInfo*   info,
                                       size_t      file_size,
                                       size_t      desired_chunk_size,
                                       uint8_t     adjust_for_recovery_info);


// support for pftool, before opening, and after closing.
// This is called once, single-threaded, before/after any parallel activity.
extern int     batch_pre_process (const char* path, size_t file_size);
extern int     batch_post_process(const char* path, size_t file_size);

// support for marfs_read() when fuse is expoerted over NFS
extern volatile ReadQueueElt* enqueue_reader(off_t offset, MarFS_FileHandle* fh);
extern void          dequeue_reader(off_t offset, MarFS_FileHandle* fh);
extern void          check_read_queue     (MarFS_FileHandle* fh);
extern void          terminate_all_readers(MarFS_FileHandle* fh);



#  ifdef __cplusplus
}
#  endif


#endif  // _MARFS_COMMON_H
