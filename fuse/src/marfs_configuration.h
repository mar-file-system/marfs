#ifndef __MARFS_CONFIGURATION_H__
#define __MARFS_CONFIGURATION_H__

/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.
 
Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/


#include <stdint.h>
#include "xdal_common.h"        /* xDALConfigOpt */

#include "erasure.h"         /* see erasureUtils */


#  ifdef __cplusplus
extern "C" {
#  endif


// ...........................................................................
// This is the version of the SOFTWARE.  This shows up in 3 places:
//
// (1) config-file parsing.  In this case the config-file will have a
//     version, and the parser compares it with the defines here, to
//     assure that it is competent to perform the parse.
//
// (2) xattr parsing.  Metadata files written by this software will have
//     xattrs identifying their config SW version.  When parsing xattrs
//     (e.g. in str_2_pre()), we can thus be sure we know which version of
//     the software wrote those xattrs.  This also identifies potential
//     changes in chunk-info written into MD files for Multis, or in the
//     recovery-info written into the tail of objects.
//
// (3) object-IDs.  The version number is also stored in obj-IDs, which
//     also include strings from xattrs.  As with (2), this potentially
//     allows future parsers to select the right parser for a given
//     version.  This also allows major changes in the code to be reflected
//     in a new "series" of object-IDs.  Thus, supposing there were some
//     problem, we could determine which objects were written with which
//     code version.
//     
// These are all somewhat independent.  config-version identifies
// config-reader parser that's needed.  This may relate to what information
// is kept in xattrs, but might not affect xattrs.  Meanwhile,
// recovery-info formats could change without any changes to the config
// reader.
//
// For now, we are just glossing the problem.  What goes into object-IDs is
// what is found in MARFS_CONFIG_MAJOR/MINOR, so changes to either (1) or
// (2) above, should be reflected in changes these #defines.  No reason you
// can't increment the version number whenever you want, as well, e.g. to
// associate objects with versions of code that were in use.  (See 1.9)
//
// HISTORY
//
// -- 0.1  First cut.
//
// -- 0.2 Added Repo.max_request_size.  Will be defaulted (to 0) by reader,
//        for older config-files.  No changes to xattrs, MD, or objects.
//
// -- 0.3 Added HTTP_DIGEST as an option for Repo.security_method
//
//        Changed Repo.max_request_size to Repo.max_get_size, because I
//        couldn't stand how ambiguous it was.
//
// -- 0.4 Now generating real recovery-info into the tails of objects
//
// -- 0.5 moved version-string as first thing after "HEAD:/", in rec-info
//
// -- 1.0 new objids, with http://host:port/ns_alias/repo/vers/ns/...
//        Makes adding new namespaces simpler for admins.
//        Allows repos to select sproxyd driver-alias.
//
// -- 1.1 recovery-info tail has *length* of recovery-info, rather than
//        offset.  This allows recovery in the more-efficient case where
//        the packer combines small objects without segregating the
//        recovery-info at the end of the packed object.  Format of the
//        object tail also uses "reclen" instead of "recoff".
//
// -- 1.2 config parsing changes. removed repo.pack_size.  Added
//        repo.min/max_pack_file_size, and repo.min/max_pack_file_count.
//
// -- 1.3 restart xattrs include an optional mode, which will be installed
//        at close-time.  This allows read-only files to be opened RW, so
//        that we can install xattrs.
//
// -- 1.4 Added mdfs_top to config, with checking in open() and opendir()
//        to forbid access, in case something gets past unshare.
//
// -- 1.5 Not strictly a change in config.  Moved parsing to libmarfs,
//        leaving libconfig with only the PA2X interface.
//
// -- 1.6 MarFS now includes the multi-component DAL
//
// -- 1.7 Added timing_flags spec to NS/Repo config, added parsing, and
//        abstracted out parsing of iperms/bperms to be similar.
//
// -- 1.8 fixed rename/unlink bug (issue 200) by having stat_xattrs()
//        ignore the md_path in the POST xattr, unless its new extra
//        argument is non-zero.
//
// -- 1.9 (no changes to xattr or obj-ID format, but we're introducing RDMA
//        (MDAL) to production, which will co-exist with NFS (MDAL).
//        Changing the object-version number in case we ever might wish we
//        had done so, in order to hunt for issues.)

#define MARFS_CONFIG_MAJOR  1
#define MARFS_CONFIG_MINOR  9

typedef uint16_t   ConfigVersType; // one value each for major and minor

// TBD for (2):
//
// #define MARFS_SW_MAJOR  0
// #define MARFS_SW_MINOR  2

// ...........................................................................



/*
 * Here are several enumeration types that we will need for various
 * members of the MarFS configuration structs.
 */

typedef enum {
   _FALSE = 0,                  // careful: FALSE/TRUE are predefined in C++
   _TRUE  = 1
} MarFS_Bool;

extern int lookup_boolean( const char* str, MarFS_Bool *enumeration );


// ...........................................................................

// co-maintain with accessmethod_str[], in marfs_configuration.c
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

extern int         lookup_accessmethod( const char* str, MarFS_AccessMethod *enumeration );
extern const char* accessmethod_string( MarFS_AccessMethod method );


// ...........................................................................

// co-maintain with securitymethod_str[], in marfs_configuration.c
typedef enum {
   SECURITYMETHOD_NONE = 0,
   SECURITYMETHOD_S3_AWS_USER,            // AWS standard, with per-user keys
   SECURITYMETHOD_S3_AWS_MASTER,          // AWS standard, with a private "master" key
   SECURITYMETHOD_S3_PER_OBJ,             // (TBD) server enforces per-object-key
   SECURITYMETHOD_HTTP_DIGEST,            // libcurl "digest" mode, w/ encypted user/pass
} MarFS_SecurityMethod;

extern int         lookup_securitymethod( const char* str, MarFS_SecurityMethod *enumeration );
extern const char* securitymethod_string( MarFS_SecurityMethod method );


// ...........................................................................
/*
 * These types are used in xattrs, so they need corresponding "lookup"
 * functions to convert strings to enumeration values.
 */

typedef enum {
   COMPTYPE_NONE = 0,
} MarFS_CompType;


typedef enum {
   CORRECTTYPE_NONE = 0,
} MarFS_CorrectType;

// // this type is for per-object correction-values, stored in obj-ID
// // It is now maintained in marfs_base.{c,h}
// typedef uint64_t CorrectTypeInfo;  // e.g. checksum


typedef enum {
   ENCRYPTTYPE_NONE = 0,
} MarFS_EncryptType;

// // this type is for per-object -values, stored in obj-ID
// // It is now maintained in marfs_base.{c,h}
// typedef uint64_t EncryptTypeInfo;  // e.g. encryption key (doesn't go into object-ID)




/*
 * Each MarFS_* enumeration will be identified by a single character, so when
 * new enumerations are added you must pick a unique character. That
 * character must appear in the string of valid characters in the position
 * that maps to the integer value its enumeration represents.
 *
 * NONE is always the first enumeration, identified by _, and has the
 * value 0 (zero).
 *
 * If a string is passed to lookup_* that is not one of the enumerations, -1
 * is returned. Otherwise, the enumeration argument is set to the enumeration
 * and 0 (zero) is returned.
 *
 * If a value not represented by a member of MarFS_* enumeration type is
 * passed to encode_*, -1 is returned. Otherwise, the character argument is
 * set to the code for this enumeration and 0 (zero) is returned.
 *
 * If a code that is not one of the MarFS_* enumeration type codes is passed
 * to decode_*, -1 is returned. Otherwise, the enumeration argument is set
 * to the enumeration for that code and 0 (zero) is returned.
 */

extern int lookup_comptype( const char* str, MarFS_CompType *enumeration );
extern int encode_comptype( MarFS_CompType enumeration, char *code );
extern int decode_comptype( char code, MarFS_CompType *enumeration );

extern int lookup_correcttype( const char* str, MarFS_CorrectType *enumeration );
extern int encode_correcttype( MarFS_CorrectType enumeration, char *code );
extern int decode_correcttype( char code, MarFS_CorrectType *enumeration );

extern int lookup_enctype( const char* str, MarFS_EncryptType *enumeration );
extern int encode_enctype( MarFS_EncryptType enumeration, char *code );
extern int decode_enctype( char code, MarFS_EncryptType *enumeration );



struct MDAL; // fwd-decl
struct DAL;  // fwd-decl



/*
 * We may not need this repository flags stuff if we strictly follow
 * the design documentation.

typedef enum {
   REPO_ONLINE          = 0x01, // repo is online?
   REPO_ALLOWS_OFFLINE  = 0x02, // repo allows offline?
   REPO_UPDATE_IN_PLACE = 0x04, // repo allows update-in-place ?
   REPO_SSL             = 0x08  // e.g. use https://...
} MarFS_RepoFlags;

typedef uint8_t  MarFS_RepoFlagsType;
 */

/*
 * (RM read meta, WM write meta, RD read data, WD write data) 
 *
 * SECURITY:
 *
 * (a) Access to the object-side storage is (partly) mediated by access to
 * the metadata filesystem.  The POSIX permissions on a metadata file
 * control access to the underlying data.  For example, given the following
 * permissions on a metadata file, the corresponding marfs data can only be
 * overwritten by user jti.
 *
 *   -rw-r--r--. 1 jti jti 5 Jan 28 09:52 /gpfs/marfs-gpfs/jti/mdfs/test_20160128_095227.30
 * 
 * (b) However, in addition there are iPerms and bPerms which allow broad
 * controls to be imposed on top of an entire namespace (for interactive or
 * batch accesses, respectively).  These controls are specified in the
 * configuration, and apply to all files in a given namespace.  So, for
 * example, all users could be shut out from writing metadata.  That would
 * mean no chmod/chown/rename/create/delete/etc.
 * 
 *   RD = read  DATA
 *   WD = write
 *   TD = truncate
 *   UD = unlink
 * 
 *   RM = read  METADATA
 *   WM = write
 * 
 * There is additional security on the object-side, so that users who gain
 * access to object data for one file can not access or delete that object
 * (or other objects).
 */

typedef enum {
  R_META = 0x01,
  W_META = 0x02,

  R_DATA = 0x10,
  W_DATA = 0x20,
  T_DATA = 0x40,               // "truncate"
  U_DATA = 0x80,               // "unlink"  (i.e. delete)
} MarFSPermFlags;

typedef uint8_t  MarFS_Perms;


/*
 * Collection of timing statistics can be enabled on repositories
 * and namespaces.  At run-time, the flags for a given namespace and
 * repo are OR'ed together.  Thus, admins can choose to add timing to
 * an entire repo, or to a specific namespace, or perhaps to a custom
 * namespace that is only used by admins.
 *
 * The options that are selectable in the configuration ultimately
 * boil down to the flags available in TimingStats in the erasureUtils
 * (libne) library
 */



/*
 * This is the MarFS repository type for use in the MarFS software
 * components. Users of this code are not expected to rely on or
 * even know the components of this struct. Utility functions will
 * be provided to get the various struct members. No user should
 * change the values once they are set by reading the configuration.
 */

typedef struct marfs_repo {
   char                 *name;
   size_t                name_len;

   char                 *host;
   size_t                host_len;
   uint8_t               host_offset; // for round-robin within a repo
   uint8_t               host_count;

   MarFS_Bool            update_in_place;
   MarFS_Bool            ssl;
   MarFS_Bool            is_online;
   MarFS_AccessMethod    access_method;
   size_t                chunk_size;
   size_t                max_get_size; // use 0 for unconstrained
   MarFS_SecurityMethod  security_method;
   MarFS_EncryptType     enc_type;
   MarFS_CompType        comp_type;
   MarFS_CorrectType     correct_type;

   ssize_t               min_pack_file_size; // use -1 for unconstrained
   ssize_t               max_pack_file_size;
   ssize_t               min_pack_file_count;
   ssize_t               max_pack_file_count;

   struct DAL           *dal;
   TimingFlagsValue      timing_flags; // see erasure.h

   char                 *online_cmds;
   size_t                online_cmds_len;
   unsigned long long    latency;
   uint32_t              write_timeout;
   uint32_t              read_timeout;
}
   MarFS_Repo,
   *MarFS_Repo_Ptr,
   **MarFS_Repo_List;

/*
 * This is the MarFS repository range type for use in the MarFS software
 * components. Users of this code are not expected to rely on or
 * even know the components of this struct. Utility functions will
 * be provided to get the various struct members. No user should
 * change the values once they are set by reading the configuration.
 */

typedef struct marfs_repo_range {
  int               min_size;
  int               max_size;
  MarFS_Repo_Ptr    repo_ptr;
} MarFS_Repo_Range, *MarFS_Repo_Range_Ptr, **MarFS_Repo_Range_List;

/*
 * This is the MarFS namespace type for use in the MarFS software
 * components. Users of this code are not expected to rely on or
 * even know the components of this struct. Utility functions will
 * be provided to get the various struct members. No user should
 * change the values once they are set by reading the configuration.
 */

// ---------------------------------------------------------------------------
// Namespace  (Metadata filesystem, aka MDFS)
//
// Fuse/pftool will append MarFS_mnttop on the front of all the name space
// segments below to construct a namespace tree.  For, example:
//
//    marfs_mnttop = /redsea
//
//    mntpath = /projecta
//    md_path    = /md/projecta
//
//    mntpath = /projectb
//    md_path    = /md/project
//
// User refers to /redsea/projecta and fuse/pftool translates to refer to
// metadata file-system in /md/projecta
//
// Somewhere there is a vector/B-tree/hash-table of these.  That thing
// should optimize lookups based on paths, so maybe a suffix-tree using
// distinct path-components of all namespaces seen by the config-file
// loader.
//
// NOTE: Formerly, we used the namespace.name as the second token in
//     generated URLs.  For Scality sproxyd, this corresponds to the
//     "driver-alias", in the sproxyd config.  However, (a) there are only
//     a limited number available, and (b) adding a new one requires
//     mucking with the config and restarting services.  So, we've added a
//     distinct "alias" to simplify marfs config.  The alias will now go
//     into the first token in generated URLs (matching sproxyd fastcgi
//     proxy, or S3 bucket).  The true namespace.name will also go into the
//     URL, but further down in the "path".
//
//     This new scheme will make it easier to proliferate namespaces
//     without a lot of server admin.
//
// ---------------------------------------------------------------------------


typedef struct marfs_namespace {
   char                 *name;
   size_t                name_len;
   char                 *alias;
   size_t                alias_len;
   char                 *mnt_path;
   size_t                mnt_path_len;

   MarFS_Perms           bperms;
   MarFS_Perms           iperms;

   char                 *md_path;
   size_t                md_path_len;

   MarFS_Repo_Ptr        iwrite_repo;
   MarFS_Repo_Range_List repo_range_list;
   int                   repo_range_list_count;

   char                 *trash_md_path;
   size_t                trash_md_path_len;

   char                 *fsinfo_path;
   size_t                fsinfo_path_len;

   long long             quota_space;
   long long             quota_names;
   TimingFlagsValue      timing_flags; // see erasure.h

   struct MDAL          *dir_MDAL;
   struct MDAL          *file_MDAL;
}
   MarFS_Namespace,
   *MarFS_Namespace_Ptr,
   **MarFS_Namespace_List;



/* This is currently used for the sole purpose of allowing us to avoid
 * threading a new flag through all the marfs_ops functions, stating
 * whether the caller is fuse (interactive), or not, and then having to
 * modify every caller, everywhere.
 * 
 * Instead, fuse can set a runtime flag in the configuration, and the
 * functions can all check that.  All existing apps (pftool, quota, GC,
 * tests, etc) can remain unchanged
 *
 */

typedef enum {
   MARFS_INTERACTIVE = 0x01,
} MarFS_RunTime_Flag;


typedef struct {
   int       flags;
} MarFS_RunTime_Config;


// e.g. set_runtime(MARFS_INTERACTIVE,1);
extern int set_runtime_config(MarFS_RunTime_Flag flag, int value);
extern int get_runtime_config(MarFS_RunTime_Flag flag);

/*
 * This is the MarFS configuration type for use in the MarFS software
 * components. Users of this code are not expected to rely on or
 * even know the components of this struct. Utility functions will
 * be provided to get the various struct members. No user should
 * change the values once they are set by reading the configuration.
 */

// marfs_config_version allows objects to be retrieved with the same
// configuration parameters that were in effect when they were stored.
//
// QUESTION: This could mean that the fields of all structs are fixed for
//           all time, and different versions simply use different values,
//           loaded from a file.  In that case, there's no need for the
//           structs themselves to change.
//
//           A much tougher alternative is that the config structures
//           themselves could change (in addition to the field contents).
//           This would mean that run-time must be able to re-create
//           whatever info is needed to drive the FUSE daemon for different
//           historical configurations.  That could lead down some very
//           nasty ratholes (e.g. if the entire processing approach
//           changes).


typedef struct marfs_config {
  char                 *name;
  size_t                name_len;
  uint16_t              version_major;
  uint16_t              version_minor;
  char                 *mnt_top;        // NOTE: Do NOT include a final slash.
  size_t                mnt_top_len;
  char                 *mdfs_top;        // NOTE: Do NOT include a final slash.
  size_t                mdfs_top_len;

  MarFS_RunTime_Config  runtime;

} MarFS_Config, *MarFS_Config_Ptr;

extern  MarFS_Config_Ptr  marfs_config;

/*
 * A couple functions to find a specific namespace record. We'll return
 * the pointer to that namespace record.
 */

extern MarFS_Namespace_Ptr find_namespace_by_name( const char *name );
extern MarFS_Namespace_Ptr find_namespace_by_mnt_path( const char *mnt_path );
extern MarFS_Namespace_Ptr find_namespace_by_mdfs_path( const char *mdfs_path );

/*
 * A couple functions to find a specific repo record. We'll return
 * the pointer to that repo record.
 *
 * Given a file-size, find the corresponding repo that is the namespace's
 * repository for files of this size.
 */

extern MarFS_Repo_Ptr find_repo_by_range (
                 MarFS_Namespace_Ptr namespacePtr,
                 size_t              file_size );

extern MarFS_Repo_Ptr find_repo_by_name( const char* name );


/*
 * Load the configuration into private static variables.
 * They can be 
 * return 0 for success, non-zero for failure.
 *
 * The configuration file is found by searching in this order: * 
 *
 * 1) Translating the MARFSCONFIGRC environment variable.
 * 2) Looking for it in $HOME/.marfsconfigrc.
 * 3) Looking for it in /etc/marfsconfigrc.
 *
 * If none of those are found, NULL is returned.
 */

extern int read_configuration();

/*
 * These functions return the configuration information that was given in
 * the configuration file.
 *
 * FOR NOW WE WILL LEAVE THEM OUT AND ALLOW THE USER TO LOOK AT THE
 * CONTENTS OF THE MarFS_Config STRUCTURE TO GET WHATEVER MEMBER
 * IS DESIRED.

extern char         *get_configuration_name( MarFS_Config_Ptr config );
extern double        get_configuration_version( MarFS_Config_Ptr config );
extern char         *get_configuration_mnt_top( MarFS_Config_Ptr config );
extern MarFS_Namespace_List get_configuration_namespace_list( MarFS_Config_Ptr config );
 */


/* DAL is given control of parsed config-options.  It will eventually need
   this to de-allocate them */
void free_xdal_config_options(xDALConfigOpt** opts);


/*
 * When the user is done with the configuration, for example if run-time
 * updates to a configuration are allowed, the memory must be returned
 * to the system to avoid memory leaks. This function properly frees all
 * the dynamically allocated memory to the system.
 *
 * -1 is returned if there is an error freeing the config, otherwise 0
 * (zero) is returned on success. The config will be set to NULL.
 */

extern int free_configuration();

/*
 * --- support for traversing namespaces (without knowing how they are stored)
 *
 * For example: here's some code to walk all Namespaces, doing something
 *
 *   NSIterator it = namespace_iterator();
 *   MarFS_Namespace*  ns;
 *   while (ns = namespace_next(&it)) {
 *      ... // do something
 *   }
 */

typedef struct {
  size_t pos;
} NSIterator;

extern NSIterator          namespace_iterator();
extern MarFS_Namespace_Ptr namespace_next( NSIterator *nsIterator );

extern int                 debug_namespace( MarFS_Namespace* ns );


/*
 * --- support for traversing repos (without knowing how they are stored)
 *
 * For example: here's some code to walk all Repos, doing something
 *
 *   RepoIterator it = repo_iterator();
 *   MarFS_Repo_Ptr repoPtr;
 *   while (repoPtr = repo_next(&it)) {
 *      ... // do something
 *   }
 */

typedef struct {
  size_t pos;
} RepoIterator;

extern RepoIterator   repo_iterator();
extern MarFS_Repo_Ptr repo_next( RepoIterator *repoIterator );

extern int            debug_repo( MarFS_Repo* repo );

/*
 * THIS IS HERE TO PRESERVE THE IDEA OF AN ITERATOR THAT CALLS A FUNCTION
 * WITH EACH ELEMENT OF THE LIST. THE DRAWBACK IS THAT IT COULD POTENTIALLY
 * REQUIRE THE USER TO USE GLOBAL VARIABLES IN THE FUNCTION WHOSE POINTER
 * IS PASSED HERE.
 *
 * This function can iterate on any MarFS_*_List type (e.g. MarFS_Repo_List).
 * The caller provides a function that returns an integer every time it is
 * handed a MarFS_*_Ptr type (e.g. MarFS_Repo_Ptr). So, here is a function
 * spec that a user might provide and how to call this function.
 *
 * int marfsRepoPtrCallback( MarFS_Repo_Ptr repoPtr ) {
 *   ... body of function here.
 * }
 *
 * int retVal;
 * MarFS_Repo_List myRepoList;
 *
 * retVal = iterate_marfs_list( myRepoList, marfsRepoPtrCallback );

extern int iterate_marfs_list( void **marfs_list, int ( *marfsPtrCallback )( void *marfsPtr ));
 */



// --- access top-level configuration parameters




#  ifdef __cplusplus
}
#  endif

#endif // __MARFS_CONFIGURATION_H__
