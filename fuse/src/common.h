#ifndef _MARFS_CONFIG_H
#define _MARFS_CONFIG_H

#include "config.h"

#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

typedef uint8_t  FuseContextFlagType;

typedef enum {
   PUSHED_USER  = 0x01,         // push_security user already ran
} FuseContextFlags;

// install this into fuse_get_context()->private_data?
typedef struct {
   FuseContextFlagType  flags;
   uid_t                user;   // if (flags & PUSHED_USER)
} MarFS_FuseContextInfo;



// Human-readable argument to functions with an <is_interactive> parameter
typedef enum {
   MARFS_BATCH       = 0,
   MARFS_INTERACTIVE = 1
} MarFS_Interactivity;


// // Variation on the MarFS_Interactivity
// // Human-readable argument to functions with an <use_iperms> parameter
// typedef enum {
//    B_PERMS      = 0,
//    I_PERMS      = 1
// } MarFS_PermSelect;



// TRY() macros test for return-values, and skip out early if they get a
//       non-zero.  The "do { ... } while()" just makes sure your macro
//       statements will still work correctly inside conditions, like this:
//
//       if (...)
//         TRY(...);
//       else
//         TRY(...);
//
// NOTE: rc is lexically-scoped.  It's defined at the top of fuse functions
//       via PUSH_USER().  This allows us to use TRY_GE0() on functions
//       whose return value we care about.
//
// NOTE: TRY macros also invert the sign of the return value, as needed for
//       fuse.  This means they shouldn't be used within common functions,
//       which may in turn be wrapped inside TRY() by fuse routines.
//       [see __TRY0()]


#define TRY0(FUNCTION, ...)                                             \
   do {                                                                 \
      rc = FUNCTION(__VA_ARGS__);                                       \
      if (rc) {                                                         \
         return -rc; /* negated for FUSE */                             \
      }                                                                 \
   } while (0)


// e.g. open() returns -1 or an fd.
#define TRY_GE0(FUNCTION, ...)                                          \
   do {                                                                 \
      rc = FUNCTION(__VA_ARGS__);                                       \
      if (rc < 0) {                                                     \
         return -rc; /* negated for FUSE */                             \
      }                                                                 \
   } while (0)



// FOR INTERNAL USE ONLY.  (Not for calling directly from fuse routines)
// This version doesn't invert the value of the return-code
#define __TRY0(FUNCTION, ...)                                           \
   do {                                                                 \
      rc = FUNCTION(__VA_ARGS__);                                       \
      if (rc) {                                                         \
         return rc; /* NOT negated! */                                  \
      }                                                                 \
   } while (0)





#define PUSH_USER()                                                     \
   int rc;                                                              \
   uid_t saved_euid = -1;                                               \
   TRY0(push_user, &saved_euid)


#define POP_USER()                                                      \
   TRY0(pop_user, &saved_euid)


// // return 0 if all argument permission-flags are asserted in the
// // iperms or bperms of the given NS.
// extern int  check_marfs_perms(MarFS_Perms      perms,
//                               int              is_interactive, // iperms/bperms
//                               MarFS_Namespace* ns);

#define CHECK_PERMS(ACTUAL_PERMS, REQUIRED_PERMS)                       \
   do {                                                                 \
      if (((ACTUAL_PERMS) & (REQUIRED_PERMS)) != (REQUIRED_PERMS))      \
         return -EACCES;   /* should be EPERM? (i.e. being root wouldn't help) */ \
   } while (0)


#define ACCESS(PATH, PERMS)            TRY0(access, (PATH), (PERMS))
#define EXPAND_PATH_INFO(INFO, PATH)   TRY0(expand_path_info, (INFO), (PATH))
#define TRASH_FILE(PATH, INFO)         TRY0(trash_file, (PATH), (INFO))
#define TRASH_DUP_FILE(PATH, INFO)     TRY0(trash_dup_file, (PATH), (INFO))
#define TRASH_NAME(PATH, INFO)         TRY0(trash_name, (PATH), (INFO))

#define STAT_XATTR(PATH)               TRY0(stat_xattr, (INFO))
#define STAT(PATH)                     TRY0(trash_name, (INFO))





int  push_user();
int  pop_user();


// --- PathInfo
//
//     used to accumulate FUSE-support information.
//     see expand_path_info(), and stat_xattr()


typedef uint8_t  PathInfoFlagType;
typedef enum {
   PI_EXPANDED     = 0x01,      // expand_path_info() was called?
   PI_STAT_QUERY   = 0x02,      // i.e. maybe PathInfo.st empty for a reason
   PI_XATTR_QUERY  = 0x04,      // i.e. maybe PathInfo.xattr empty for a reason
   PI_TRASH_PATH   = 0x08,      // expand_trash_info() was called?
} PathInfoFlagValue;


typedef struct {
   MarFS_Namespace*     ns;
   struct stat          st;
   MarFS_ReservedXattr  xattr;
   PathInfoFlagType     flags;
   char                 md_path[MARFS_MAX_MD_PATH];
   char                 md_trash_path[MARFS_MAX_MD_PATH];
} PathInfo;


// These initialize different parts of the PathInfo struct.
// Calling them redundantly is cheap and harmless.
extern int  expand_path_info (PathInfo* info, const char* path);
extern int  expand_trash_info(PathInfo* info, const char* path);

extern int  stat_xattr       (PathInfo* info);
extern int  stat_regular     (PathInfo* info);

extern int  exists           (PathInfo* info);
extern int  has_resv_xattrs  (PathInfo* info);

// need the path to initialize info->md_trash_path
extern int trash_file   (PathInfo* info, const char* path);
extern int trashdup_file(PathInfo* info, const char* path);
extern int trash_name   (PathInfo* info, const char* path);

extern int trunc_xattr  (PathInfo* info, const char* path);






#endif
