/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was
produced under U.S. Government contract DE-AC52-06NA25396 for Los
Alamos National Laboratory (LANL), which is operated by Los Alamos
National Security, LLC for the U.S. Department of Energy. The
U.S. Government has rights to use, reproduce, and distribute this
software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL SECURITY,
LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce
derivative works, such modified software should be clearly marked, so
as not to confuse it with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with
or without modification, are permitted provided that the following
conditions are met: 1. Redistributions of source code must retain the
above copyright notice, this list of conditions and the following
disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos
National Laboratory, LANL, the U.S. Government, nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL LOS
ALAMOS NATIONAL SECURITY, LLC OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code
identifier: LA-CC-15-039.

MarFS uses libaws4c for Amazon S3 object communication. The original
version is at https://aws.amazon.com/code/Amazon-S3/2601 and under the
LGPL license.  LANL added functionality to the original work. The
original work plus LANL contributions is found at
https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "marfs_auto_config.h"
#if defined(DEBUG_ALL)  ||  defined(DEBUG_API)
   #define DEBUG 1
#endif
#define LOG_PREFIX "api"

#include <logging.h>
#include "marfs.h"
#include "datastream/datastream.h"
#include "mdal/mdal.h"

//   -------------   INTERNAL DEFINITIONS    -------------

typedef struct marfs_ctxt_struct {
   marfs_config* config;
   marfs_interface itype;
   marfs_position pos;
}* marfs_ctxt;

typedef struct marfs_fhandle_struct {
   MDAL_FHANDLE metahandle; // for meta/direct access
   DATASTREAM   datastream; // for standard access
   marfs_ns*            ns; // reference to the containing NS
   marfs_interface   itype; // itype of creating ctxt ( for perm checks )
}* marfs_fhandle;

typedef struct marfs_dhandle_struct {
   MDAL_DHANDLE metahandle;
   marfs_ns*            ns;
   unsigned int      depth;
   marfs_config*    config; // reference to the containing config ( chdir validation only )
}* marfs_dhandle;


//   -------------   INTERNAL FUNCTIONS    -------------

int pathshift( marfs_ctxt ctxt, const char* tgtpath, char** subpath, marfs_position** oppos ) {
   // duplicate our pos structure and path
   char* modpath = strdup( tgtpath );
   if ( modpath == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate target path: \"%s\"\n", tgtpath );
      return -1;
   }
   *oppos = malloc( sizeof( marfs_position ) );
   if ( *oppos == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new marfs_position struct\n" );
      free( modpath );
      return -1;
   }
   MDAL curmdal = ctxt->pos.ns->prepo->metascheme.mdal;
   marfs_position* newpos = *oppos;
   newpos->ns = ctxt->pos.ns;
   newpos->depth = ctxt->pos.depth;
   newpos->ctxt = curmdal->dupctxt( ctxt->pos.ctxt );
   if ( newpos->ctxt == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate MDAL_CTXT of current position\n" );
      free( modpath );
      free( *oppos );
      *oppos = NULL;
      return -1;
   }
   // traverse the config
   int tgtdepth = config_traverse( ctxt->config, newpos, &(modpath), (ctxt->itype == MARFS_INTERACTIVE) ? 1 : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to traverse config for subpath: \"%s\"\n", modpath );
      MDAL curmdal = ctxt->pos.ns->prepo->metascheme.mdal;
      curmdal->destroyctxt( newpos->ctxt );
      free( modpath );
      free( *oppos );
      *oppos = NULL;
      return -1;
   }
   if ( tgtdepth == 0  &&  newpos->ctxt == NULL ) {
      if ( modpath ) { free( modpath ); } // should be able to ignore this path
      // need to target the NS itself, via full path
      char* nspath = NULL;
      if ( config_nsinfo( newpos->ns->idstr, NULL, &(nspath) ) ) {
         printf( "Failed to identify NS path of target: \"%s\"\n", newpos->ns->idstr );
         return -1;
      }
      modpath = nspath;
   }
   *subpath = modpath;
   return tgtdepth;
}

void pathcleanup( char* subpath, marfs_position* oppos ) {
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   if ( oppos->ctxt != NULL  &&  curmdal->destroyctxt( oppos->ctxt ) ) {
      LOG( LOG_WARNING, "Failed to destory MDAL ctxt post access() op\n" );
   }
   free( oppos );
   if ( subpath ) { free( subpath ); }
}


//   -------------   EXTERNAL FUNCTIONS    -------------

// MARFS CONTEXT MGMT OPS

/**
 * Initializes a MarFS Context structure based on the content of the referenced config file
 * Note -- This initialization process may act as a security barrier.  If the caller has
 *         EUID == root (or some marfs-admin-user), this function can access MDAL/DAL root
 *         dirs below a protected directory as a one time event.  After initialization,
 *         the caller can safely setuid() to a user, dropping all elevated perms yet
 *         maintaining access to the MDAL/DAL root dirs via the returned marfs_ctxt.
 * @param const char* configpath : Path of the config file to initialize based on
 * @param marfs_interface type : Interface type to use for MarFS ops ( interactive / batch )
 * @return marfs_ctxt : Newly initialized marfs_ctxt, or NULL if a failure occurred
 */
marfs_ctxt marfs_init( const char* configpath, marfs_interface type ) {
   // check for invalid args
   if ( configpath == NULL ) {
      LOG( LOG_ERR, "Received a NULL configpath value\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( type != MARFS_INTERACTIVE  &&  type != MARFS_BATCH ) {
      LOG( LOG_ERR, "Received a non-INTERACTIVE nor BATCH interface type value\n" );
      errno = EINVAL;
      return NULL;
   }
   // allocate our ctxt struct
   marfs_ctxt ctxt = malloc( sizeof( struct marfs_ctxt_struct ) );
   if ( ctxt != NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new marfs_ctxt\n" );
      return NULL;
   }
   // set our interface type
   ctxt->itype = type;
   // initialize our config
   if ( (ctxt->config = config_init( configpath )) == NULL ) {
      LOG( LOG_ERR, "Failed to initialize marfs_config\n" );
      free( ctxt );
      return NULL;
   }
   // initialize our positon to reference the root NS
   MDAL rootmdal = ctxt->config->rootns->prepo->metascheme.mdal;
   ctxt->pos.ns = ctxt->config->rootns;
   ctxt->pos.depth = 0;
   ctxt->pos.ctxt = rootmdal->newctxt( "/.", rootmdal->ctxt );
   if ( ctxt->pos.ctxt == NULL ) {
      LOG( LOG_ERR, "Failed to initialize MDAL_CTXT for rootNS\n" );
      config_term( ctxt->config );
      free( ctxt );
      return NULL;
   }
   // all done
   return ctxt;
}

/**
 * Sets a string 'tag' value for the given context struct, causing all output files to
 * include the string in metadata references and data object IDs
 * @param marfs_ctxt ctxt : marfs_ctxt to be updated
 * @param const char* ctag : New client tag string value
 * @return int : Zero on success, or -1 on failure
 */
int marfs_setctag( marfs_ctxt ctxt, const char* ctag ) {
   // check for invalid args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return -1;
   }
   if ( ctag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ctag reference\n" );
      errno = EINVAL;
      return -1;
   }
   // duplicate the client string
   char* newctag = strdup( ctag );
   if ( newctag == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate client tag string: \"%s\"\n", ctag );
      return -1;
   }
   // replace the original client tag
   free( ctxt->config->ctag );
   ctxt->config->ctag = newctag;
   return 0;
}

/**
 * Populate the given string with the config version of the provided marfs_ctxt
 * @param marfs_ctxt ctxt : marfs_ctxt to retrieve version info from
 * @param char* verstr : String to be populated
 * @param size_t len : Allocated length of the target string
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t marfs_configver( marfs_ctxt ctxt, char* verstr, size_t len ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // print out the config version string
   return snprintf( verstr, len, "%s", ctxt->config->version );
}

/**
 * Destroy the provided marfs_ctxt
 * @param marfs_ctxt ctxt : marfs_ctxt to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int marfs_term( marfs_ctxt ctxt ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // terminate the position MDAL_CTXT
   int retval = 0;
   MDAL curmdal = ctxt->pos.ns->prepo->metascheme.mdal;
   if ( curmdal->destroyctxt( ctxt->pos.ctxt ) ) {
      LOG( LOG_ERR, "Failed to destroy current position MDAL_CTXT\n" );
      retval = -1;
   }
   // terminate the config itself
   if ( config_term( ctxt->config ) ) {
      LOG( LOG_ERR, "Failed to destroy the config reference\n" );
      retval = -1;
   }
   return retval;
}


// METADATA PATH OPS

/**
 * Check access to the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param int mode : F_OK - check for file existance
 *                      or a bitwise OR of the following...
 *                   R_OK - check for read access
 *                   W_OK - check for write access
 *                   X_OK - check for execute access
 * @param int flags : A bitwise OR of the following...
 *                    AT_EACCESS - Perform access checks using effective uid/gid
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_access( marfs_ctxt ctxt, const char* path, int mode, int flags ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for access op\n" );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an access op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      // targetting a NS directly, without bothering to acquire a ctxt
      retval = curmdal->accessnamespace( curmdal->ctxt, subpath, mode, flags );
   }
   else {
      retval = curmdal->access( oppos->ctxt, subpath, mode, flags );
   }
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}

/**
 * Stat the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param struct stat* st : Stat structure to be populated
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_stat( marfs_ctxt ctxt, const char* path, struct stat *buf, int flags ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for stat op\n" );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a stat op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      // targetting a NS directly, without bothering to acquire a ctxt
      retval = curmdal->statnamespace( curmdal->ctxt, subpath, buf );
   }
   else {
      retval = curmdal->stat( oppos->ctxt, subpath, buf, flags );
   }
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}

/**
 * Edit the mode of the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param mode_t mode : New mode value for the file (see inode man page)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_chmod( marfs_ctxt ctxt, const char* path, mode_t mode, int flags ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for chmod op\n" );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a chmod op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      retval = curmdal->chmodnamespace( curmdal->ctxt, subpath, mode );
   }
   else {
      retval = curmdal->chmod( oppos->ctxt, subpath, mode, flags );
   }
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}

/**
 * Edit the ownership and group of the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param uid_t owner : New owner
 * @param gid_t group : New group
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_chown( marfs_ctxt ctxt, const char* path, uid_t uid, gid_t gid, int flags ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for chown op\n" );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a chown op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      retval = curmdal->chownnamespace( curmdal->ctxt, subpath, uid, gid );
   }
   else {
      retval = curmdal->chown( oppos->ctxt, subpath, uid, gid, flags );
   }
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}

/**
 * Rename the specified target to a new path
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* from : String path of the target
 * @param const char* to : Destination string path
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_rename( marfs_ctxt ctxt, const char* from, const char* to ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* frompos = NULL;
   char* frompath = NULL;
   int fromdepth = pathshift( ctxt, from, &(frompath), &(frompos) );
   if ( frompath == NULL ) {
      LOG( LOG_ERR, "Failed to identify 'from' target info for rename op\n" );
      return -1;
   }
   if ( fromdepth == 0 ) {
      LOG( LOG_ERR, "Cannot rename a MarFS namespace: from=\"%s\"\n", from );
      pathcleanup( frompath, frompos );
      errno = EPERM;
      return -1;
   }
   marfs_position* topos = NULL;
   char* topath = NULL;
   int todepth = pathshift( ctxt, to, &(topath), &(topos) );
   if ( todepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify 'to' target info for rename op\n" );
      pathcleanup( frompath, frompos );
      return -1;
   }
   if ( todepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS namespace with a rename op: to=\"%s\"\n", to );
      pathcleanup( frompath, frompos );
      pathcleanup( topath, topos );
      errno = EPERM;
      return -1;
   }
   // explicitly disallow cross-NS rename ( multi-MDAL op is an unsolved problem )
   if ( frompos->ns != topos->ns ) {
      LOG( LOG_ERR, "Cross NS rename() is explicitly forbidden\n" );
      errno = EPERM;
      pathcleanup( frompath, frompos );
      pathcleanup( topath, topos );
      return -1;
   }
   // check NS perms (only need to check one set, at we now know both positions share a NS)
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(topos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(topos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a rename op\n" );
      errno = EPERM;
      pathcleanup( frompath, frompos );
      pathcleanup( topath, topos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = topos->ns->prepo->metascheme.mdal;
   int retval = curmdal->rename( frompos->ctxt, frompath, topos->ctxt, topath );
   // cleanup references
   pathcleanup( frompath, frompos );
   pathcleanup( topath, topos );
   // return op result
   return retval;
}


/**
 * Create a symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* target : String path for the link to target
 * @param const char* linkname : String path of the new link
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_symlink( marfs_ctxt ctxt, const char* target, const char* linkname ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, linkname, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify linkname path info for symlink op\n" );
      return -1;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot replace MarFS NS with symlink: \"%s\"\n", linkname );
      pathcleanup( subpath, oppos );
      errno = EEXIST;
      return -1;
   }
   // NOTE -- we don't actually care about the target of a symlink
   //         MarFS path or not, cross-NS or not, it isn't relevant at this point.
   //         All of that will be identified and accounted for during path traversal.
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a symlink op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = curmdal->symlink( oppos->ctxt, target, subpath );
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}


/**
 * Read the target path of the specified symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target symlink
 * @param char* buf : Buffer to be populated with the link value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the link target string, or -1 if a failure occurred
 */
ssize_t marfs_readlink( marfs_ctxt ctxt, const char* path, char* buf, size_t size ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for readlink op\n" );
      return -1;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a readlink op: \"%s\"\n", path );
      pathcleanup( subpath, oppos );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a readlink op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = curmdal->readlink( oppos->ctxt, subpath, buf, size );
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}


/**
 * Unlink the specified file/symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_unlink( marfs_ctxt ctxt, const char* path ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for unlink op\n" );
      return -1;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot unlink a MarFS NS: \"%s\"\n", path );
      pathcleanup( subpath, oppos );
      errno = EPERM;
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an unlink op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = curmdal->unlink( oppos->ctxt, subpath );
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}


/**
 * Create a hardlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* oldpath : String path of the target file
 * @param const char* newpath : String path of the new hardlink
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_link( marfs_ctxt ctxt, const char* oldpath, const char* newpath, int flags ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oldpos = NULL;
   char* oldsubpath = NULL;
   int olddepth = pathshift( ctxt, oldpath, &(oldsubpath), &(oldpos) );
   if ( olddepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify old target info for link op\n" );
      return -1;
   }
   if ( olddepth == 0 ) {
      LOG( LOG_ERR, "Cannot link a MarFS NS to a new target: \"%s\"\n", oldpath );
      pathcleanup( oldsubpath, oldpos );
      errno = EPERM;
      return -1;
   }
   marfs_position* newpos = NULL;
   char* newsubpath = NULL;
   int newdepth = pathshift( ctxt, newpath, &(newsubpath), &(newpos) );
   if ( newdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify new target info for link op\n" );
      pathcleanup( oldsubpath, oldpos );
      return -1;
   }
   if ( newdepth == 0 ) {
      LOG( LOG_ERR, "Cannot replace a MarFS NS with a new link: \"%s\"\n", newpath );
      pathcleanup( oldsubpath, oldpos );
      pathcleanup( newsubpath, newpos );
      errno = EEXIST;
      return -1;
   }
   // explicitly disallow cross-NS link ( multi-MDAL op is an unsolved problem )
   if ( oldpos->ns != newpos->ns ) {
      LOG( LOG_ERR, "Cross NS rename() is explicitly forbidden\n" );
      errno = EPERM;
      pathcleanup( oldsubpath, oldpos );
      pathcleanup( newsubpath, newpos );
      return -1;
   }
   // check NS perms ( no need to check both positions, as they target the same NS )
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(newpos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(newpos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a link op\n" );
      pathcleanup( oldsubpath, oldpos );
      pathcleanup( newsubpath, newpos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oldpos->ns->prepo->metascheme.mdal;
   int retval = curmdal->link( oldpos->ctxt, oldsubpath, newpos->ctxt, newsubpath, flags );
   // cleanup references
   pathcleanup( oldsubpath, oldpos );
   pathcleanup( newsubpath, newpos );
   // return op result
   return retval;
}

/**
 * Create the specified directory
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the new directory
 * @param mode_t mode : Mode value of the new directory (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_mkdir( marfs_ctxt ctxt, const char* path, mode_t mode ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for mkdir op\n" );
      return -1;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a mkdir op: \"%s\"\n", path );
      pathcleanup( subpath, oppos );
      errno = EEXIST;
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a mkdir op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = curmdal->mkdir( oppos->ctxt, subpath, mode );
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}

/**
 * Delete the specified directory
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target directory
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_rmdir( marfs_ctxt ctxt, const char* path ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for rmdir op\n" );
      return -1;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot rmdir a MarFS NS: \"%s\"\n", path );
      pathcleanup( subpath, oppos );
      errno = EPERM;
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an rmdir op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   int retval = curmdal->rmdir( oppos->ctxt, subpath );
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}

/**
 * Return statvfs (filesystem) info for the current namespace
 * @param const marfs_ctxt ctxt : marfs_ctxt to retrieve info for
 * @param struct statvfs* buf : Reference to the statvfs structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_statvfs( marfs_ctxt ctxt, const char* path, struct statvfs *buf ) {
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      return 0;
   }
   // identify target info
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for statvfs op\n" );
      return -1;
   }
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      // this is the sole op for which we really do need an MDAL_CTXT for the NS
      oppos->ctxt = curmdal->newctxt( subpath, curmdal->ctxt );
      if ( oppos->ctxt == NULL ) {
         LOG( LOG_ERR, "Failed to establish new MDAL_CTXT for NS: \"%s\"\n", subpath );
         pathcleanup( subpath, oppos );
         return -1;
      }
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a statvfs op\n" );
      pathcleanup( subpath, oppos );
      return -1;
   }
   // perform the MDAL op
   int retval = curmdal->statvfs( oppos->ctxt, buf ); // subpath is irrelevant
   // modify buf values to reflect NS-specific info
   buf->f_bsize = oppos->ns->prepo->datascheme.protection.partsz;
   buf->f_frsize = buf->f_bsize;
   size_t datausage = curmdal->getdatausage( oppos->ctxt );
   // convert data usage to a could of blocks, rounding up
   if ( datausage % buf->f_bsize ) { datausage = (datausage / buf->f_bsize) + 1; }
   else if ( datausage ) { datausage = (datausage / buf->f_bsize); }
   size_t inodeusage = curmdal->getinodeusage( oppos->ctxt );
   buf->f_blocks = oppos->ns->dquota / buf->f_bsize;
   buf->f_bfree = ( datausage < buf->f_blocks ) ? buf->f_blocks - datausage : buf->f_blocks;
   buf->f_bavail = buf->f_bfree;
   buf->f_files = oppos->ns->fquota;
   buf->f_ffree = ( inodeusage < buf->f_files ) ? buf->f_files - inodeusage : buf->f_files;
   buf->f_favail = buf->f_ffree;
   // cleanup references
   pathcleanup( subpath, oppos );
   // return op result
   return retval;
}


// METADATA FILE HANDLE OPS

/**
 * Perform a stat operation on the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle to stat
 * @param struct stat* buf : Reference to a stat buffer to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fstat( marfs_fhandle fh, struct stat* buf ) {
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // perform the op
   return fh->ns->prepo->metascheme.mdal->fstat( fh->metahandle, buf );
}

/**
 * Update the timestamps of the target file
 * NOTE -- It is possible that the time values of the target file may not be updated
 *         until the referencing marfs_fhandle is closed or released.
 *         Thus, it is essential to check the return values of those functions as well.
 * @param marfs_fhandle fh : File handle on which to set timestamps
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int marfs_futimens(marfs_fhandle fh, const struct timespec times[2]) {
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( fh->itype != MARFS_INTERACTIVE  &&  !(fh->ns->bperms & NS_WRITEMETA) )  ||
        ( fh->itype != MARFS_BATCH        &&  !(fh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a futimens op\n" );
      errno = EPERM;
      return -1;
   }
   // perform the op
   if ( fh->datastream == NULL  &&  fh->ns->prepo->metascheme.directread ) {
      // only call the MDAL op directly if we both don't have a datastream AND
      // the config has enabled direct read
      return fh->ns->prepo->metascheme.mdal->futimens( fh->metahandle, times );
   }
   return datastream_utimens( &(fh->datastream), times );
}

/**
 * Set the specified xattr on the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to set the xattr
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fsetxattr(marfs_fhandle fh, const char* name, const void* value, size_t size, int flags) {
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( fh->itype != MARFS_INTERACTIVE  &&  !(fh->ns->bperms & NS_WRITEMETA) )  ||
        ( fh->itype != MARFS_BATCH        &&  !(fh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a fsetxattr op\n" );
      errno = EPERM;
      return -1;
   }
   // perform the op
   return fh->ns->prepo->metascheme.mdal->fsetxattr( fh->metahandle, 0, name, value, size, flags );
}

/**
 * Retrieve the specified xattr from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to retrieve the xattr
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t marfs_fgetxattr(marfs_fhandle fh, const char* name, void* value, size_t size) {
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // perform the op
   return fh->ns->prepo->metascheme.mdal->fgetxattr( fh->metahandle, 0, name, value, size );
}

/**
 * Remove the specified xattr from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to remove the xattr
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fremovexattr(marfs_fhandle fh, const char* name) {
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( fh->itype != MARFS_INTERACTIVE  &&  !(fh->ns->bperms & NS_WRITEMETA) )  ||
        ( fh->itype != MARFS_BATCH        &&  !(fh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a fremovexattr op\n" );
      errno = EPERM;
      return -1;
   }
   // perform the op
   return fh->ns->prepo->metascheme.mdal->fremovexattr( fh->metahandle, 0, name );
}

/**
 * List all xattr names from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to list xattrs
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t marfs_flistxattr(marfs_fhandle fh, char* buf, size_t size) {
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // perform the op
   return fh->ns->prepo->metascheme.mdal->flistxattr( fh->metahandle, 0, buf, size );
}


// DIRECTORY HANDLE OPS

/**
 * Open a directory, relative to the given marfs_ctxt
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target directory
 * @return marfs_dhandle : Open directory handle, or NULL if a failure occurred
 */
marfs_dhandle marfs_opendir(marfs_ctxt ctxt, const char *path) {
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      return NULL;
   }
   // identify the path target
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for opendir op\n" );
      return NULL;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an opendir op\n" );
      pathcleanup( subpath, oppos );
      errno = EPERM;
      return NULL;
   }
   // allocate a new dhandle struct
   marfs_dhandle rethandle = malloc( sizeof( struct marfs_dhandle_struct ) );
   if ( rethandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new dhandle struct\n" );
      pathcleanup( subpath, oppos );
      return NULL;
   }
   rethandle->ns = oppos->ns;
   rethandle->depth = oppos->depth;
   rethandle->config = ctxt->config;
   MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      // open the namespace without shifting our MDAL_CTXT
      rethandle->metahandle = curmdal->opendirnamespace( oppos->ctxt, subpath );
   }
   else {
      // open the dir via the standard call
      rethandle->metahandle = curmdal->opendir( oppos->ctxt, subpath );
   }
   // check for op success
   if ( rethandle->metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open handle for NS target: \"%s\"\n", subpath );
      pathcleanup( subpath, oppos );
      free( rethandle );
      return NULL;
   }
   pathcleanup( subpath, oppos );
   return rethandle;
}

/**
 * Iterate to the next entry of an open directory handle
 * @param marfs_dhandle dh : marfs_dhandle to read from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset
 *                          if all entries have been read, or NULL w/ errno set if a
 *                          failure occurred
 */
struct dirent *marfs_readdir(marfs_dhandle dh) {
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      return NULL;
   }
   // perform the op
   MDAL curmdal = dh->ns->prepo->metascheme.mdal;
   return curmdal->readdir( dh->metahandle );
}

/**
 * Close the given directory handle
 * @param marfs_dhandle dh : marfs_dhandle to close
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_closedir(marfs_dhandle dh) {
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      return -1;
   }
   // close the handle and free all memory
   MDAL curmdal = dh->ns->prepo->metascheme.mdal;
   int retval = curmdal->closedir( dh->metahandle );
   free( dh );
   return retval;
}

/**
 * Edit the given marfs_ctxt to reference the given marfs_dhandle for all path operations
 * NOTE -- This is an attempt to implement a current-working-directory style behavior for
 *         MarFS.  The idea is that any initialized context holds its own internal cwd
 *         reference.  All 'const char* path' relative path values will be interpreted
 *         relative to that cwd.
 *         By default, this internal cwd will be the root of the MarFS mountpoint.
 *         However, this function allows you to change that.
 * @param marfs_ctxt ctxt : marfs_ctxt to update
 * @param marfs_dhandle dh : Directory handle to be used by the marfs_ctxt
 *                           NOTE -- this operation will destroy the provided marfs_dhandle
 * @return int : Zero on success, -1 if a failure occurred
 */
int marfs_chdir(marfs_ctxt ctxt, marfs_dhandle dh) {
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // validate that the ctxt and dhandle reference the same config
   if ( ctxt->config != dh->config ) {
      LOG( LOG_ERR, "Received dhandle and marfs_ctxt do not reference the same config\n" );
      errno = EINVAL;
      return -1;
   }
   // chdir to the specified dhandle
   MDAL curmdal = dh->ns->prepo->metascheme.mdal;
   if ( curmdal->chdir( ctxt->pos.ctxt, dh->metahandle ) ) {
      LOG( LOG_ERR, "Failed to chdir MDAL_CTXT\n" );
      return -1;
   }
   // MDAL_CTXT has been updated; now update the position
   ctxt->pos.ns = dh->ns;
   ctxt->pos.depth = dh->depth;
   free( dh ); // the underlying MDAL_DHANDLE is no longer valid
   return 0;
}


// FILE HANDLE OPS

/**
 * Create a new MarFS file, overwriting any existing file, and opening a marfs_fhandle for it
 * NOTE -- this is the only mechanism for creating files in MarFS
 * @param marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param marfs_fhandle stream : Reference to an existing marfs_fhandle, or NULL
 *                               If non-NULL, the created file will be tied to the provided
 *                               stream, allowing it to be packed in with previous files.
 *                               The previous stream will be modified to reference the
 *                               new file and returned by this function.
 *                               If NULL, the created file will be tied to a completely
 *                               fresh stream.
 *                               NOTE -- Clients should essentially always tie new files to
 *                               an existing stream, when feasible to do so.
 * @param const char* path : Path of the file to be created
 * @param mode_t mode : Mode value of the file to be created
 * @return marfs_fhandle : marfs_fhandle referencing the created file,
 *                         or NULL if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
marfs_fhandle marfs_creat(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, mode_t mode) {
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      return NULL;
   }
   // identify the path target
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for create op\n" );
      return NULL;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&
            ( (oppos->ns->bperms & NS_RWMETA) != NS_RWMETA ) )
        ||
        ( ctxt->itype != MARFS_BATCH        &&
            ( (oppos->ns->iperms & NS_RWMETA) != NS_RWMETA ) ) 
      ) {
      LOG( LOG_ERR, "NS perms do not allow a create op\n" );
      pathcleanup( subpath, oppos );
      errno = EPERM;
      return NULL;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a create op\n" );
      pathcleanup( subpath, oppos );
      return NULL;
   }
   // check the state of our handle argument
   char newstream = 0;
   if ( stream == NULL ) {
      // allocate a fresh handle
      stream = malloc( sizeof( struct marfs_fhandle_struct ) );
      if ( stream == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a new marfs_fhandle struct\n" );
         pathcleanup( subpath, oppos );
         return NULL;
      }
      stream->ns = NULL;
      stream->datastream = NULL;
      stream->metahandle = NULL;
      newstream = 1;
   }
   else if ( stream->datastream == NULL  &&  stream->metahandle == NULL ) {
      // a double-NULL handle has been flushed or suffered a fatal error
      LOG( LOG_ERR, "Received a flushed marfs_fhandle\n" );
      pathcleanup( subpath, oppos );
      errno = EINVAL;
      return NULL;
   }
   else if ( stream->datastream == NULL  &&  stream->metahandle != NULL ) {
      // meta-only reference; attempt to close it
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      if ( curmdal->close( stream->metahandle ) ) {
         LOG( LOG_ERR, "Failed to close previous MDAL_FHANDLE\n" );
         stream->metahandle = NULL;
         pathcleanup( subpath, oppos );
         errno = EBADFD;
         return NULL;
      }
      stream->metahandle = NULL; // don't reattempt this op
   }
   // attempt the op
   if ( datastream_create( &(stream->datastream), subpath, oppos, mode, ctxt->config->ctag ) ) {
      LOG( LOG_ERR, "Failure of datastream_create()\n" );
      pathcleanup( subpath, oppos );
      if ( newstream ) { free( stream ); }
      else if ( stream->metahandle == NULL ) { errno = EBADFD; } // ref is now defunct
      return NULL;
   }
   // update our stream info to reflect the new target
   stream->ns = oppos->ns;
   stream->metahandle = stream->datastream->files[stream->datastream->curfile].metahandle;
   // cleanup and return
   pathcleanup( subpath, oppos ); // done with path info
   return stream;   
}

/**
 * Open an existing file
 * @param marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param marfs_fhandle stream : Reference to an existing marfs_fhandle, or NULL
 *                               If non-NULL, the previous marfs_fhandle will be modified
 *                               to reference the new file, preserving existing meta/data
 *                               values/buffers to whatever extent is possible.  The
 *                               modified handle will be returned by this function.
 *                               If NULL, the created file will be tied to a completely
 *                               fresh marfs_fhandle.
 *                               NOTE -- Clients should essentially always open new files
 *                               via an existing marfs_fhandle, when feasible to do so.
 * @param const char* path : Path of the file to be opened
 * @param marfs_flags flags : Flags specifying the mode in which to open the file
 * @return marfs_fhandle : marfs_fhandle referencing the opened file,
 *                         or NULL if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
marfs_fhandle marfs_open(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, marfs_flags flags) {
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      return NULL;
   }
   // check for invalid flags
   if ( flags != MARFS_READ  &&  flags != MARFS_WRITE ) {
      LOG( LOG_ERR, "Unrecognized flags value\n" );
      errno = EINVAL;
      return NULL;
   }
   // identify the path target
   marfs_position* oppos = NULL;
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos) );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for create op\n" );
      return NULL;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos->ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos->ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an open op\n" );
      pathcleanup( subpath, oppos );
      errno = EPERM;
      return NULL;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a create op\n" );
      pathcleanup( subpath, oppos );
      return NULL;
   }
   // check the state of our handle argument
   char newstream = 0;
   if ( stream == NULL ) {
      // allocate a fresh handle
      stream = malloc( sizeof( struct marfs_fhandle_struct ) );
      if ( stream == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a new marfs_fhandle struct\n" );
         pathcleanup( subpath, oppos );
         return NULL;
      }
      stream->ns = NULL;
      stream->datastream = NULL;
      stream->metahandle = NULL;
      newstream = 1;
   }
   else if ( stream->datastream == NULL  &&  stream->metahandle == NULL ) {
      // a double-NULL handle has been flushed or suffered a fatal error
      LOG( LOG_ERR, "Received a flushed marfs_fhandle\n" );
      pathcleanup( subpath, oppos );
      errno = EINVAL;
      return NULL;
   }
   else if ( stream->datastream == NULL  &&  stream->metahandle != NULL ) {
      // meta-only reference; attempt to close it
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      if ( curmdal->close( stream->metahandle ) ) {
         LOG( LOG_ERR, "Failed to close previous MDAL_FHANDLE\n" );
         stream->metahandle = NULL;
         pathcleanup( subpath, oppos );
         errno = EBADFD;
         return NULL;
      }
      stream->metahandle = NULL; // don't reattempt this op
   }
   // attempt the op, allowing a meta-only reference ONLY if directread is set for the 
   //    target NS and we are attempting to open for read
   MDAL_FHANDLE phandle = NULL;
   if ( datastream_open( &(stream->datastream), (flags == MARFS_READ) ? READ_STREAM : EDIT_STREAM, subpath, oppos, (oppos->ns->prepo->metascheme.directread  &&  flags == MARFS_READ) ? &(phandle) : NULL ) ) {
      // check for a meta-only reference
      if ( phandle != NULL ) {
         LOG( LOG_INFO, "Attempting to use meta-only reference for target file\n" );
         // need to release the current datastream
         if ( stream->datastream  &&  datastream_release( &(stream->datastream) ) ) {
            LOG( LOG_ERR, "Failed to release previous datastream reference\n" );
            stream->datastream = NULL;
            MDAL curmdal = oppos->ns->prepo->metascheme.mdal;
            if ( curmdal->close( phandle ) ) {
               // nothing to do besides complain
               LOG( LOG_WARNING, "Failed to close preserved MDAL_FHANDLE for new target\n" );
            }
            stream->metahandle = NULL;
            errno = EBADFD;
            return NULL;
         }
         // update stream info to reflect a meta-only reference
         stream->datastream = NULL;
         stream->metahandle = phandle;
         stream->ns = oppos->ns;
         stream->itype = ctxt->itype;
         // cleanup and return
         pathcleanup( subpath, oppos );
         return stream;
      }
      LOG( LOG_ERR, "Failure of datastream_open()\n" );
      pathcleanup( subpath, oppos );
      if ( newstream ) { free( stream ); }
      else if ( stream->metahandle == NULL ) { errno = EBADFD; } // ref is now defunct
      return NULL;
   }
   // update our stream info to reflect the new target
   stream->ns = oppos->ns;
   stream->metahandle = stream->datastream->files[stream->datastream->curfile].metahandle;
   stream->itype = ctxt->itype;
   // cleanup and return
   pathcleanup( subpath, oppos ); // done with path info
   return stream;
}

/**
 * Free the given file handle and 'complete' the underlying file
 * ( make readable and disallow further data modification )
 * NOTE -- For MARFS_READ handles, close and release ops are functionally identical
 * NOTE -- For marfs_fhandles produced by marfs_open(), this function will fail unless the
 *         original marfs_creat() handle has already been released ( the create handle must
 *         be released for the data size of the file to be determined )
 * @param marfs_fhandle stream : marfs_fhandle to be closed
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not
 *            be committed to the FS until 'finalization' of the data stream.  This means
 *            that it is *essential* to check the return code of this function.  A failure
 *            of this call may indicate incomplete operations throughout the *entire* data
 *            stream referenced by this handle.
 */
int marfs_close(marfs_fhandle stream) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // reject a flushed handle
   if ( stream->metahandle == NULL  &&  stream->datastream == NULL ) {
      LOG( LOG_ERR, "Received a flushed marfs_fhandle\n" );
      errno = EINVAL;
      return -1;
   }
   // check for datastream reference
   int retval = 0;
   if ( stream->datastream == NULL ) {
      // meta only reference
      LOG( LOG_INFO, "Closing meta-only marfs_fhandle\n" );
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      if ( (retval = curmdal->close( stream->metahandle )) ) {
         LOG( LOG_ERR, "Failed to close MDAL_FHANDLE\n" );
      }
   }
   else {
      // datastream reference
      LOG( LOG_INFO, "Closing datastream reference\n" );
      if ( (retval = datastream_close( &(stream->datastream) )) ) {
         LOG( LOG_ERR, "Failed to close datastream\n" );
      }
   }
   free( stream );
   return retval;
}

/**
 * Free the given file handle, but do not 'complete' the underlying file
 * NOTE -- For MARFS_READ handles, close and release ops are functionally identical
 * @param marfs_fhandle stream : marfs_fhandle to be closed
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not
 *            be committed to the FS until 'finalization' of the data stream.  This means
 *            that it is *essential* to check the return code of this function.  A failure
 *            of this call may indicate incomplete operations throughout the *entire* data
 *            stream referenced by this handle.
 */
int marfs_release(marfs_fhandle stream) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check for datastream reference
   int retval = 0;
   if ( stream->metahandle ) {
      // meta only reference
      LOG( LOG_INFO, "Closing meta-only marfs_fhandle\n" );
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      if ( (retval = curmdal->close( stream->metahandle )) ) {
         LOG( LOG_ERR, "Failed to close MDAL_FHANDLE\n" );
      }
   }
   else if ( stream->datastream ) {
      // datastream reference
      LOG( LOG_INFO, "Closing datastream reference\n" );
      if ( (retval = datastream_close( &(stream->datastream) )) ) {
         LOG( LOG_ERR, "Failed to close datastream\n" );
      }
   }
   free( stream );
   return retval;
}

/**
 * 'Complete' the file referenced by the given handle, but maintain the handle itself
 * NOTE -- This function exists to better facilitate FUSE integration and is unlikely to
 *         be useful outside of that context.  The handle structure will be maintained,
 *         but all subsequent marfs ops will fail against that handle, except for release.
 * @param marfs_fhandle stream : marfs_fhandle referencing the file to be completed
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- many ops, such as marfs_ftruncate, extend, utimens, and even write, may not
 *            be committed to the FS until 'finalization' of the data stream.  This means
 *            that it is *essential* to check the return code of this function.  A failure
 *            of this call may indicate incomplete operations throughout the *entire* data
 *            stream referenced by this handle.
 */
int marfs_flush(marfs_fhandle stream) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check for datastream reference
   int retval = 0;
   if ( stream->datastream ) {
      // datastream reference
      LOG( LOG_INFO, "Closing datastream reference\n" );
      if ( (retval = datastream_close( &(stream->datastream) )) ) {
         LOG( LOG_ERR, "Failed to close datastream\n" );
      }
   }
   else if ( stream->metahandle ) {
      // meta only reference
      LOG( LOG_INFO, "Closing meta-only marfs_fhandle\n" );
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      if ( (retval = curmdal->close( stream->metahandle )) ) {
         LOG( LOG_ERR, "Failed to close MDAL_FHANDLE\n" );
      }
   }
   stream->metahandle = NULL;
   stream->datastream = NULL;
   return retval;
}


// DATA FILE HANDLE OPS

/**
 * Read from the file currently referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle to be read from
 * @param void* buf : Reference to the buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
ssize_t marfs_read(marfs_fhandle stream, void* buf, size_t size) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_READDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_READDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a read op\n" );
      errno = EPERM;
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // read from datastream reference
      return datastream_read( &(stream->datastream), buf, size );
   }
   // meta only reference
   MDAL curmdal = stream->ns->prepo->metascheme.mdal;
   return curmdal->read( stream->metahandle, buf, size );
}

/**
 * Write to the file currently referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle to be written to
 * @param const void* buf : Reference to the buffer containing data to be written
 * @param size_t count : Number of bytes to be written
 * @return ssize_t : Number of bytes written, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
ssize_t marfs_write(marfs_fhandle stream, const void* buf, size_t size) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_WRITEDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_WRITEDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a write op\n" );
      errno = EPERM;
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // write to the datastream reference
      return datastream_write( &(stream->datastream), buf, size );
   }
   // meta only reference ( direct write is currently unsupported )
   LOG( LOG_ERR, "Cannot write to a meta-only reference\n" );
   errno = EPERM;
   return -1;
}

/**
 * Seek to the provided offset of the file referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle to seek
 * @param off_t offset : Offset for the seek
 *                       NOTE -- write handles can only seek to exact chunk boundaries
 * @param int whence : Flag defining seek start location ( see 'seek()' syscall manpage )
 * @return off_t : Resulting offset within the file, or -1 if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
off_t marfs_seek(marfs_fhandle stream, off_t offset, int whence) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // seek the datastream reference
      return datastream_seek( &(stream->datastream), offset, whence );
   }
   // meta only reference
   MDAL curmdal = stream->ns->prepo->metascheme.mdal;
   return curmdal->lseek( stream->metahandle, offset, whence );
}

/**
 * Identify the data object boundaries of the file referenced by the given marfs_fhandle
 * @param marfs_fhandle stream : marfs_fhandle for which to retrieve info
 * @param int chunknum : Index of the data chunk to retrieve info for ( beginning at zero )
 * @param off_t* offset : Reference to be populated with the data offset of the start of
 *                        the target data chunk
 *                        ( as in, marfs_seek( stream, 'offset', SEEK_SET ) will move
 *                        you to the start of this data chunk )
 * @param size_t* size : Reference to be populated with the size of the target data chunk
 * @return int : Zero on success, or -1 on failure
 */
int marfs_chunkbounds(marfs_fhandle stream, int chunknum, off_t* offset, size_t* size) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // identify datastream reference chunkbounds
      return datastream_chunkbounds( &(stream->datastream), chunknum, offset, size );
   }
   // meta only reference ( this function does not apply )
   LOG( LOG_ERR, "Cannot identify chunk boundaries for a meta-only reference\n" );
   errno = EINVAL;
   return -1;
}

/**
 * Truncate the file referenced by the given marfs_fhandle to the specified length
 * NOTE -- This operation can only be performed on 'completed' files
 * @param marfs_fhandle stream : marfs_fhandle to be truncated
 * @param off_t length : Target total file length to truncate to
 * @return int : Zero on success, or -1 on failure
 */
int marfs_ftruncate(marfs_fhandle stream, off_t length) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_WRITEDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_WRITEDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a truncate op\n" );
      errno = EPERM;
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // truncate the datastream reference
      return datastream_truncate( &(stream->datastream), length );
   }
   // meta only reference
   MDAL curmdal = stream->ns->prepo->metascheme.mdal;
   return curmdal->ftruncate( stream->metahandle, length );
}

/**
 * Extend the file referenced by the given marfs_fhandle to the specified total size
 * This makes the specified data size accessible for parallel write.
 * NOTE -- The final data object of the file will only be accessible after the creating
 *         marfs_fhandle has been released ( as that finalizes the file's data size ).
 *         This function can only be performed if no data has been written to the target
 *         file via this handle.
 * @param marfs_fhandle stream : marfs_fhandle to be extended
 * @param off_t length : Target total file length to extend to
 * @return int : Zero on success, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
int marfs_extend(marfs_fhandle stream, off_t length) {
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // extend the datastream reference
      return datastream_extend( &(stream->datastream), length );
   }
   // meta only reference ( this function does not apply )
   LOG( LOG_ERR, "Cannot extend a meta-only reference\n" );
   errno = EINVAL;
   return -1;
}

