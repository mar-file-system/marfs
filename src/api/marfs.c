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
#ifdef DEBUG_API
#define DEBUG DEBUG_API
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "api"
#include "logging/logging.h"

#include "marfs.h"
#include "datastream/datastream.h"
#include "mdal/mdal.h"
#include "general_include/restrictedchars.h"

#include <dirent.h>
#include <limits.h>

//   -------------   INTERNAL DEFINITIONS    -------------

#if  __WORDSIZE < 64
#define MARFS_DIR_NS_OFFSET_BIT 29
#else
#define MARFS_DIR_NS_OFFSET_BIT 61
#endif
#if ( 1UL << MARFS_DIR_NS_OFFSET_BIT ) > LONG_MAX
#error "MarFS directory NS offset bit position is invalid!"
#endif
#define MARFS_DIR_NS_OFFSET_MASK (long)( 1L << MARFS_DIR_NS_OFFSET_BIT )

typedef struct marfs_ctxt_struct {
   pthread_mutex_t        lock; // for serializing access to this structure (if necessary)
   marfs_config*        config;
   marfs_interface       itype;
   marfs_position          pos;
   pthread_mutex_t erasurelock; // for serializing libNE erasure functions (if necessary)
}* marfs_ctxt;

typedef struct marfs_fhandle_struct {
   pthread_mutex_t    lock; // for serializing access to this structure (if necessary)
   int               flags; // open flags for this file handle
   MDAL_FHANDLE metahandle; // for meta/direct access
   DATASTREAM   datastream; // for standard access
   marfs_ns*            ns; // reference to the containing NS
   marfs_interface   itype; // itype of creating ctxt ( for perm checks )
   size_t    dataremaining; // available data quota
}* marfs_fhandle;

typedef struct marfs_dhandle_struct {
   pthread_mutex_t    lock; // for serializing access to this structure (if necessary)
   MDAL_DHANDLE metahandle;
   marfs_ns*            ns;
   unsigned int      depth;
   marfs_interface   itype; // itype of creating ctxt ( for perm checks )
   long           location; // for tracking our position within this dir ( if applicable )
   size_t  subspcnamealloc; // for tracking the allocated d_name space in our dirent struct
   struct dirent subspcent; // for storing returned subspace direntries
   marfs_config*    config; // reference to the containing config ( for chdir validation )
}* marfs_dhandle;


//   -------------   INTERNAL FUNCTIONS    -------------

/**
 * Translates the given path to an actual marfs subpath, relative to some NS
 * @param marfs_ctxt ctxt : Current MarFS context
 * @param const char* tgtpath : Target path
 * @param char** subpath : Reference to be populated with the MarFS subpath
 * @param marfs_position* oppos : Reference to be populated with a new MarFS position
 * @param char linkchk : Flag indicating whether final path components should have symlink targets substituted
 *                       If zero, normal behavior ( substitute all path components for INTERACTIVE contexts )
 *                       If greater than zero, skip substitution of final component ( for targeting links themselves )
 * @return int : Depth of the target from the containing NS, or -1 if a failure occurred
 */
int pathshift( marfs_ctxt ctxt, const char* tgtpath, char** subpath, marfs_position* oppos, char linkchk ) {
   // duplicate our pos structure and path
   char* modpath = strdup( tgtpath );
   if ( modpath == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate target path: \"%s\"\n", tgtpath );
      return -1;
   }
   // duplicate position values, so that config_traverse() won't modify the active CTXT position
   if ( config_duplicateposition( &(ctxt->pos), oppos ) ) {
      LOG( LOG_ERR, "Failed to duplicate position of current marfs ctxt\n" );
      free( modpath );
      return -1;
   }
   // traverse the config
   int tgtdepth = config_traverse( ctxt->config, oppos, &(modpath), (ctxt->itype == MARFS_INTERACTIVE) ? 1 + linkchk : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to traverse config for subpath: \"%s\"\n", modpath );
      int origerrno = errno; // cache and restore errno, to better report the 'real' problem to users
      free( modpath );
      config_abandonposition( oppos );
      errno = origerrno; // restore cached errno
      return -1;
   }
   if ( tgtdepth == 0  &&  oppos->ctxt == NULL ) {
      if ( modpath ) { free( modpath ); } // should be able to ignore this path
      // need to target the NS itself, via full path
      char* nspath = NULL;
      if ( config_nsinfo( oppos->ns->idstr, NULL, &(nspath) ) ) {
         printf( "Failed to identify NS path of target: \"%s\"\n", oppos->ns->idstr );
         int origerrno = errno; // cache and restore errno, to better report the 'real' problem to users
         config_abandonposition( oppos );
         errno = origerrno; // restore cached errno
         return -1;
      }
      modpath = nspath;
   }
   *subpath = modpath;
   return tgtdepth;
}

void pathcleanup( char* subpath, marfs_position* oppos ) {
   if ( oppos ) { config_abandonposition( oppos ); }
   if ( subpath ) { free( subpath ); }
}

/**
 * Allocate and initialize a new struct marfs_fhandle_struct.
 */
marfs_fhandle new_marfs_fhandle( void ) {
   struct marfs_fhandle_struct* fh = calloc( 1, sizeof( struct marfs_fhandle_struct ) );
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new marfs_fhandle struct\n" );
      return NULL;
   }

   if ( pthread_mutex_init( &(fh->lock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize lock of new marfs_fhandle struct\n" );
      free( fh );
      return NULL;
   }

   return fh;
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
 * @param pthread_mutex_t* erasurelock : Reference to a pthread_mutex lock, to be used for synchronizing access
 *                                       to isa-l erasure generation functions in multi-threaded programs.
 *                                       If NULL, marfs will create such a lock internally.  In such a case,
 *                                       the internal lock will continue to protect multi-threaded programs
 *                                       ONLY if they exclusively use a single marfs_ctxt at a time.
 *                                       Multi-threaded programs using multiple marfs_ctxt references in parallel
 *                                       MUST create + initialize their own pthread_mutex and pass it into
 *                                       ALL marfs_init() calls.
 * @return marfs_ctxt : Newly initialized marfs_ctxt, or NULL if a failure occurred
 */
marfs_ctxt marfs_init( const char* configpath, marfs_interface type, pthread_mutex_t* erasurelock ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid args
   if ( configpath == NULL ) {
      LOG( LOG_ERR, "Received a NULL configpath value\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   if ( type != MARFS_INTERACTIVE  &&  type != MARFS_BATCH ) {
      LOG( LOG_ERR, "Received a non-INTERACTIVE nor BATCH interface type value\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // allocate our ctxt struct
   marfs_ctxt ctxt = calloc( 1, sizeof( struct marfs_ctxt_struct ) );
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new marfs_ctxt\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // set our interface type
   ctxt->itype = type;
   // initialize our local erasurelock
   if ( pthread_mutex_init( &(ctxt->erasurelock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize local erasurelock\n" );
      config_term( ctxt->config );
      free( ctxt );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // initialize our config
   if ( (ctxt->config = config_init( configpath, (erasurelock != NULL) ? erasurelock : &(ctxt->erasurelock) )) == NULL ) {
      LOG( LOG_ERR, "Failed to initialize marfs_config\n" );
      free( ctxt );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // verify our config
   int verifyflags = CFG_MDALCHECK;
   /**
   * TODO Need to handle automatic config verification in a more efficient way
   *   Full DAL scatter check is prohibitively intensive for startup of most programs
   *   Even full recursion of all NS paths *may* be too intensive for some cases
   */
   //int verifyflags = CFG_MDALCHECK | CFG_DALCHECK;
   //if ( getuid() == 0 ) { verifyflags |= CFG_RECURSE; } // only attempt to recurse if we are running as root ( guarantess sub-NS access )
   if ( config_verify( ctxt->config, ".", verifyflags ) ) {
      LOG( LOG_ERR, "Encountered uncorrected errors with the MarFS config\n" );
      config_term( ctxt->config );
      free( ctxt );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
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
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // initialize our structure lock
   if ( pthread_mutex_init( &(ctxt->lock), NULL ) ) {
      LOG( LOG_ERR,"Failed to initialize lock for marfs_ctxt\n" );
      rootmdal->destroyctxt( ctxt->pos.ctxt );
      config_term( ctxt->config );
      free( ctxt );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // all done
   LOG( LOG_INFO, "EXIT - Success\n" );
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   if ( ctag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ctag reference\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   if ( restrictedchars_check( ctag ) ) {
      LOG( LOG_ERR, "Received a ctag string containing restricted characters\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // duplicate the client string
   char* newctag = strdup( ctag );
   if ( newctag == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate client tag string: \"%s\"\n", ctag );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the ctxt lock
   if ( pthread_mutex_lock( &(ctxt->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_ctxt lock\n" );
      free( newctag );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // replace the original client tag
   free( ctxt->config->ctag );
   ctxt->config->ctag = newctag;
   pthread_mutex_unlock( &(ctxt->lock) );
   LOG( LOG_INFO, "EXIT - Success\n" );
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return 0;
   }
   // print out the config version string
   LOG( LOG_INFO, "EXIT - Success\n" );
   size_t retval = snprintf( verstr, len, "%s", ctxt->config->version );
   if ( retval ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Destroy the provided marfs_ctxt
 * @param marfs_ctxt ctxt : marfs_ctxt to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int marfs_term( marfs_ctxt ctxt ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the ctxt lock
   if ( pthread_mutex_lock( &(ctxt->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_ctxt lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // terminate the position MDAL_CTXT
   int retval = 0;
   MDAL curmdal = ctxt->pos.ns->prepo->metascheme.mdal;
   if ( curmdal->destroyctxt( ctxt->pos.ctxt ) ) {
      LOG( LOG_ERR, "Failed to destroy current position MDAL_CTXT\n" );
      retval = -1;
   }
   // terminate the config
   if ( config_term( ctxt->config ) ) {
      LOG( LOG_ERR, "Failed to destroy the config reference\n" );
      retval = -1;
   }
   // free the ctxt struct itself
   pthread_mutex_unlock( &(ctxt->lock) );
   pthread_mutex_destroy( &(ctxt->lock) );
   pthread_mutex_destroy( &(ctxt->erasurelock) );
   free( ctxt );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}


// MARFS INFO OPS

/**
 * Populates the given string with the path of the MarFS mountpoint
 * ( as defined by the MarFS config file )
 * @param marfs_ctxt ctxt : marfs_ctxt to retrieve mount path from
 * @param char* mountstr : String to be populated with the mount path
 * @param size_t len : Allocated length of the target string
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t marfs_mountpath( marfs_ctxt ctxt, char* mountstr, size_t len ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return 0;
   }
   // print out the config version string
   size_t retval = snprintf( mountstr, len, "%s", ctxt->config->mountpoint );
   if ( retval ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}


// METADATA PATH OPS
// 
// NOTE -- these skip acquiring the ctxt lock, for efficiency, as none modify the structure
//         However, this means that calling ctxt modification functions in parallel with these 
//         will result in undefined behavior.
//

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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for access op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an access op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos.ctxt == NULL ) {
      // targetting a NS directly, without bothering to acquire a ctxt
      retval = curmdal->accessnamespace( curmdal->ctxt, subpath, mode, flags );
   }
   else {
      retval = curmdal->access( oppos.ctxt, subpath, mode, flags );
   }
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for stat op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a stat op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos.ctxt == NULL ) {
      // targetting a NS directly, without bothering to acquire a ctxt
      retval = curmdal->statnamespace( curmdal->ctxt, subpath, buf );
   }
   else {
      retval = curmdal->stat( oppos.ctxt, subpath, buf, flags );
   }
   // adjust stat values, if necessary
   if ( tgtdepth == 0  &&  retval == 0 ) {
      // note subspaces in link count
      buf->st_nlink += oppos.ns->subnodecount;
   }
   else if ( tgtdepth != 0  &&  S_ISREG( buf->st_mode ) ) {
      // regular files may need link count adjusted to ignore ref path
      if ( buf->st_nlink > 1 ) { buf->st_nlink--; }
      if ( buf->st_size ) {
         // assume allocated blocks, based on logical file size ( saves us having to pull an FTAG xattr )
         blkcnt_t estblocks = ( buf->st_size / 512 ) + ( (buf->st_size % 512) ? 1 : 0 );
         if ( estblocks > buf->st_blocks ) { buf->st_blocks = estblocks; }
      }
   }
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Edit the mode of the specified file
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @param mode_t mode : New mode value for the file (see inode man page)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - (AVOID: POSIX LEAVES UNIMPLEMENTED) do not dereference a symlink target
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_chmod( marfs_ctxt ctxt, const char* path, mode_t mode, int flags ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for chmod op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a chmod op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos.ctxt == NULL ) {
      retval = curmdal->chmodnamespace( curmdal->ctxt, subpath, mode );
   }
   else {
      retval = curmdal->chmod( oppos.ctxt, subpath, mode, flags );
   }
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for chown op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a chown op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = 0;
   if ( tgtdepth == 0  &&  oppos.ctxt == NULL ) {
      retval = curmdal->chownnamespace( curmdal->ctxt, subpath, uid, gid );
   }
   else {
      retval = curmdal->chown( oppos.ctxt, subpath, uid, gid, flags );
   }
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position frompos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* frompath = NULL;
   int fromdepth = pathshift( ctxt, from, &(frompath), &(frompos), 1 );
   if ( frompath == NULL ) {
      LOG( LOG_ERR, "Failed to identify 'from' target info for rename op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT-From: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", fromdepth, frompos.ns->idstr, frompath );
   if ( fromdepth == 0 ) {
      LOG( LOG_ERR, "Cannot rename a MarFS namespace: from=\"%s\"\n", from );
      pathcleanup( frompath, &frompos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   marfs_position topos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* topath = NULL;
   int todepth = pathshift( ctxt, to, &(topath), &(topos), 1 );
   if ( todepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify 'to' target info for rename op\n" );
      pathcleanup( frompath, &frompos );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT-To: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", todepth, topos.ns->idstr, topath );
   if ( todepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS namespace with a rename op: to=\"%s\"\n", to );
      pathcleanup( frompath, &frompos );
      pathcleanup( topath, &topos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // explicitly disallow cross-NS rename ( multi-MDAL op is an unsolved problem )
   //    ...unless, it is between a ghost and the ghost's target
   if ( strcmp( frompos.ns->idstr, topos.ns->idstr )  &&
            ( !(frompos.ns->ghtarget)  ||  topos.ns->ghtarget  ||       // any of: from is not a ghost, to is a ghost,
               strcmp( frompos.ns->ghtarget->idstr, topos.ns->idstr ) ) //   or from has the wrong ghost tgt
            &&
            ( !(topos.ns->ghtarget)  ||  frompos.ns->ghtarget  ||       // any of: to is not a ghost, from is a ghost,
               strcmp( topos.ns->ghtarget->idstr, frompos.ns->idstr ) ) //   or to has the wrong ghost tgt
      ) {
      LOG( LOG_ERR, "Cross NS rename() is explicitly forbidden\n" );
      pathcleanup( frompath, &frompos );
      pathcleanup( topath, &topos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms (only need to check one set, at we now know both positions share a NS)
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(topos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(topos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a rename op\n" );
      errno = EPERM;
      pathcleanup( frompath, &frompos );
      pathcleanup( topath, &topos );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = topos.ns->prepo->metascheme.mdal;
   int retval = curmdal->rename( frompos.ctxt, frompath, topos.ctxt, topath );
   // cleanup references
   pathcleanup( frompath, &frompos );
   pathcleanup( topath, &topos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, linkname, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify linkname path info for symlink op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot replace MarFS NS with symlink: \"%s\"\n", linkname );
      pathcleanup( subpath, &oppos );
      errno = EEXIST;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // NOTE -- we don't actually care about the target of a symlink
   //         MarFS path or not, cross-NS or not, it isn't relevant at this point.
   //         All of that will be identified and accounted for during path traversal.
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a symlink op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = curmdal->symlink( oppos.ctxt, target, subpath );
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for readlink op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a readlink op: \"%s\"\n", path );
      pathcleanup( subpath, &oppos );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a readlink op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = curmdal->readlink( oppos.ctxt, subpath, buf, size );
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}


/**
 * Unlink the specified file/symlink
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_unlink( marfs_ctxt ctxt, const char* path ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for unlink op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot unlink a MarFS NS: \"%s\"\n", path );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an unlink op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = curmdal->unlink( oppos.ctxt, subpath );
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oldpos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* oldsubpath = NULL;
   int olddepth = pathshift( ctxt, oldpath, &(oldsubpath), &(oldpos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( olddepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify old target info for link op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT-Old: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", olddepth, oldpos.ns->idstr, oldsubpath );
   if ( olddepth == 0 ) {
      LOG( LOG_ERR, "Cannot link a MarFS NS to a new target: \"%s\"\n", oldpath );
      pathcleanup( oldsubpath, &oldpos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   marfs_position newpos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* newsubpath = NULL;
   int newdepth = pathshift( ctxt, newpath, &(newsubpath), &(newpos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( newdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify new target info for link op\n" );
      pathcleanup( oldsubpath, &oldpos );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT-New: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", newdepth, newpos.ns->idstr, newsubpath );
   if ( newdepth == 0 ) {
      LOG( LOG_ERR, "Cannot replace a MarFS NS with a new link: \"%s\"\n", newpath );
      pathcleanup( oldsubpath, &oldpos );
      pathcleanup( newsubpath, &newpos );
      errno = EEXIST;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // explicitly disallow cross-NS link ( multi-MDAL op is an unsolved problem )
   //    ...unless, it is between a ghost and the ghost's target
   if ( strcmp( oldpos.ns->idstr, newpos.ns->idstr )  &&
            ( !(oldpos.ns->ghtarget)  ||  newpos.ns->ghtarget  ||       // any of: old is not a ghost, new is a ghost,
               strcmp( oldpos.ns->ghtarget->idstr, newpos.ns->idstr ) ) //   or old has the wrong ghost tgt
            &&
            ( !(newpos.ns->ghtarget)  ||  oldpos.ns->ghtarget  ||        // any of: new is not a ghost, old is a ghost,
               strcmp( newpos.ns->ghtarget->idstr, oldpos.ns->idstr ) )  //   or new has the wrong ghost tgt
      ) {
         LOG( LOG_ERR, "Cross NS rename() is explicitly forbidden\n" );
         pathcleanup( oldsubpath, &oldpos );
         pathcleanup( newsubpath, &newpos );
         errno = EPERM;
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return -1;
   }
   // check NS perms ( no need to check both positions, as they target the same NS )
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(newpos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(newpos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a link op\n" );
      pathcleanup( oldsubpath, &oldpos );
      pathcleanup( newsubpath, &newpos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oldpos.ns->prepo->metascheme.mdal;
   int retval = curmdal->link( oldpos.ctxt, oldsubpath, newpos.ctxt, newsubpath, flags );
   // cleanup references
   pathcleanup( oldsubpath, &oldpos );
   pathcleanup( newsubpath, &newpos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}


/**
 * Update the timestamps of the target file
 * NOTE -- It is possible that the time values of a file will be updated at any time, while
 *         a referencing marfs_fhandle stream exists ( has not been closed or released ).
 *         marfs_futimens() protects against this, by caching time values and applying them
 *         only after the file has been finalized/completed.
 *         Thus, it is strongly recommend to use that function instead, when feasible, unless
 *         you are certain that the target file has been completed.
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the new directory
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference a symlink target
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int marfs_utimens( marfs_ctxt ctxt, const char* path, const struct timespec times[2], int flags ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for utimens op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a utimens op: \"%s\"\n", path );
      pathcleanup( subpath, &oppos );
      errno = EEXIST;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a utimens op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = curmdal->utimens( oppos.ctxt, subpath, times, flags );
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for mkdir op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a mkdir op: \"%s\"\n", path );
      pathcleanup( subpath, &oppos );
      errno = EEXIST;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a mkdir op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = curmdal->mkdir( oppos.ctxt, subpath, mode );
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Delete the specified directory
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target directory
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_rmdir( marfs_ctxt ctxt, const char* path ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for rmdir op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_WRITEMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an rmdir op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   int retval = -1;
   if ( tgtdepth == 0 ) {
      if ( oppos.ctxt == NULL ) {
         // destroy the namespace without creating an MDAL_CTXT
         retval = curmdal->destroynamespace( curmdal->ctxt, subpath );
      }
      else {
         // destroy the namespace relative to our current CTXT
         retval = curmdal->destroynamespace( oppos.ctxt, subpath );
      }
   }
   else {
      // just issue the base op
      retval = curmdal->rmdir( oppos.ctxt, subpath );
   }
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Return statvfs (filesystem) info for the current namespace
 * @param const marfs_ctxt ctxt : marfs_ctxt to retrieve info for
 * @param struct statvfs* buf : Reference to the statvfs structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_statvfs( marfs_ctxt ctxt, const char* path, struct statvfs *buf ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for invalid arg
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for statvfs op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   if ( tgtdepth == 0  &&  oppos.ctxt == NULL ) {
      // this is the sole op for which we really do need an MDAL_CTXT for the NS
      if ( config_fortifyposition( &oppos ) ) {
         LOG( LOG_ERR, "Failed to establish new MDAL_CTXT for NS: \"%s\"\n", subpath );
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return -1;
      }
   }
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a statvfs op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the MDAL op
   int retval = curmdal->statvfs( oppos.ctxt, buf ); // subpath is irrelevant
   // modify buf values to reflect NS-specific info
   buf->f_bsize = oppos.ns->prepo->datascheme.protection.partsz;
   buf->f_frsize = buf->f_bsize;
   off_t datausage = curmdal->getdatausage( oppos.ctxt );
   if ( datausage < 0 ) {
      LOG( LOG_WARNING, "Failed to retrieve data usage value for NS: \"%s\"\n", oppos.ns->idstr );
      datausage = 0;
   }
   // convert data usage to a could of blocks, rounding up
   if ( datausage % buf->f_bsize ) { datausage = (datausage / buf->f_bsize) + 1; }
   else if ( datausage ) { datausage = (datausage / buf->f_bsize); }
   off_t inodeusage = curmdal->getinodeusage( oppos.ctxt );
   if ( inodeusage < 0 ) {
      LOG( LOG_WARNING, "Failed to retrieve data usage value for NS: \"%s\"\n", oppos.ns->idstr );
      inodeusage = 0;
   }
   buf->f_blocks = oppos.ns->dquota / buf->f_frsize;
   buf->f_bfree = ( datausage < buf->f_blocks ) ? buf->f_blocks - datausage : 0;
   buf->f_bavail = buf->f_bfree;
   buf->f_files = oppos.ns->fquota;
   buf->f_ffree = ( inodeusage < buf->f_files ) ? buf->f_files - inodeusage : 0;
   buf->f_favail = buf->f_ffree;
   // cleanup references
   pathcleanup( subpath, &oppos );
   // return op result
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}


// METADATA FILE HANDLE OPS
// 
// NOTE -- these skip acquiring the handle lock, for efficiency, as none modify the structure
//         However, this means that calling handle modification functions in parallel with these 
//         will result in undefined behavior.
//

/**
 * Perform a stat operation on the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle to stat
 * @param struct stat* buf : Reference to a stat buffer to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fstat( marfs_fhandle fh, struct stat* buf ) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval = fh->ns->prepo->metascheme.mdal->fstat( fh->metahandle, buf );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(fh->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( fh->itype != MARFS_INTERACTIVE  &&  !(fh->ns->bperms & NS_WRITEMETA) )  ||
        ( fh->itype != MARFS_BATCH        &&  !(fh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a futimens op\n" );
      pthread_mutex_unlock( &(fh->lock) );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval;
   if ( fh->datastream == NULL  &&
        ( fh->ns->prepo->metascheme.directread  ||  (fh->flags & O_ASYNC) ) ) {
      // only call the MDAL op directly if we both don't have a datastream AND
      // either the config has enabled direct read OR the file handle is explicitly metadata only
      LOG( LOG_INFO, "Performing futimens call directly on metahandle\n" );
      retval = fh->ns->prepo->metascheme.mdal->futimens( fh->metahandle, times );
   }
   else {
      LOG( LOG_INFO, "Performing datastream_utimens call\n" );
      retval = datastream_utimens( &(fh->datastream), times );
   }
   pthread_mutex_unlock( &(fh->lock) );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( fh->itype != MARFS_INTERACTIVE  &&  !(fh->ns->bperms & NS_WRITEMETA) )  ||
        ( fh->itype != MARFS_BATCH        &&  !(fh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a fsetxattr op\n" );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval = fh->ns->prepo->metascheme.mdal->fsetxattr( fh->metahandle, 0, name, value, size, flags );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   ssize_t retval = fh->ns->prepo->metascheme.mdal->fgetxattr( fh->metahandle, 0, name, value, size );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Remove the specified xattr from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to remove the xattr
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_fremovexattr(marfs_fhandle fh, const char* name) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( fh->itype != MARFS_INTERACTIVE  &&  !(fh->ns->bperms & NS_WRITEMETA) )  ||
        ( fh->itype != MARFS_BATCH        &&  !(fh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a fremovexattr op\n" );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval = fh->ns->prepo->metascheme.mdal->fremovexattr( fh->metahandle, 0, name );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * List all xattr names from the file referenced by the given marfs_fhandle
 * @param marfs_fhandle fh : File handle for which to list xattrs
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t marfs_flistxattr(marfs_fhandle fh, char* buf, size_t size) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( fh == NULL ) {
      LOG( LOG_ERR, "Received a NULL file handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   ssize_t retval = fh->ns->prepo->metascheme.mdal->flistxattr( fh->metahandle, 0, buf, size );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}


// DIRECTORY HANDLE OPS

/**
 * Open a directory, relative to the given marfs_ctxt
 * @param const marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param const char* path : String path of the target directory
 * @return marfs_dhandle : Open directory handle, or NULL if a failure occurred
 */
marfs_dhandle marfs_opendir(marfs_ctxt ctxt, const char *path) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // identify the path target
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 0 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for opendir op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an opendir op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // allocate a new dhandle struct
   marfs_dhandle rethandle = calloc( 1, sizeof( struct marfs_dhandle_struct ) );
   if ( rethandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new dhandle struct\n" );
      pathcleanup( subpath, &oppos );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   if ( pthread_mutex_init( &(rethandle->lock), NULL ) ) {
      LOG( LOG_ERR, "Failed to initialize marfs_dhandle mutex lock\n" );
      free( rethandle );
      pathcleanup( subpath, &oppos );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   rethandle->ns = config_duplicatensref( oppos.ns );
   if ( rethandle->ns == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate op position NS\n" );
      free( rethandle );
      pathcleanup( subpath, &oppos );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   rethandle->depth = tgtdepth;
   rethandle->itype = ctxt->itype;
   rethandle->config = ctxt->config;
   rethandle->location = 0;
   rethandle->subspcent.d_name[0] = '\0';
   /**
    * NOTE -- yes, the readdir manpage *explicitly* states that 'use of sizeof(d_name) is 
    * incorrect'.  However, that is referring to a struct returned via readdir(), with 
    * populated values.  Obviously, strlen() will not function safely with an unpopulated 
    * struct.  Less obviously, the _D_ALLOC_NAMELEN macro is not guaranteed to function 
    * safely either.  sizeof() will always give us an underestimate of available d_name 
    * space, which at least won't break anything.
    * TODO -- if you know a better way to populate a dirent's d_name value, be my guest.
    *         (and delete this huge NOTE when you do!)
    */
   rethandle->subspcnamealloc = sizeof( rethandle->subspcent.d_name);
   bzero( &(rethandle->subspcent.d_name[0]), rethandle->subspcnamealloc );
   MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
   if ( tgtdepth == 0 ) {
      if ( oppos.ctxt == NULL ) {
         // open the namespace without shifting our MDAL_CTXT
         rethandle->metahandle = curmdal->opendirnamespace( curmdal->ctxt, subpath );
      }
      else {
         // open the namespace relative to our MDAL_CTXT
         rethandle->metahandle = curmdal->opendirnamespace( oppos.ctxt, subpath );
      }
   }
   else {
      // open the dir via the standard call
      rethandle->metahandle = curmdal->opendir( oppos.ctxt, subpath );
   }
   // check for op success
   if ( rethandle->metahandle == NULL ) {
      LOG( LOG_ERR, "Failed to open handle for NS target: \"%s\"\n", subpath );
      config_destroynsref( rethandle->ns );
      pathcleanup( subpath, &oppos );
      pthread_mutex_destroy( &(rethandle->lock) );
      free( rethandle );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   pathcleanup( subpath, &oppos );
   LOG( LOG_INFO, "EXIT - Success\n" );
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // acquire directory lock
   if ( pthread_mutex_lock( &(dh->lock) ) ) {
      LOG( LOG_ERR, "Failed to aqcuire marfs_dhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // cache our original errno value, and clear it
   int cachederrno = errno;
   errno = 0;
   // potentially insert a subspace entry
   if ( dh->depth == 0  &&  dh->ns->subnodecount  &&  (dh->location & MARFS_DIR_NS_OFFSET_MASK) == 0 ) {
      while ( dh->location < dh->ns->subnodecount ) {
         // stat the subspace, to identify inode info
         marfs_ns* tgtsubspace = (marfs_ns *)(dh->ns->subnodes[dh->location].content);
         char* subspacepath = NULL;
         if ( config_nsinfo( tgtsubspace->idstr, NULL, &(subspacepath) ) ) {
            LOG( LOG_ERR, "Failed to identify NS path of subspace: \"%s\"\n",
                 tgtsubspace->idstr );
            pthread_mutex_unlock( &(dh->lock) );
            LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
            return NULL;
         }
         // stat the subspace, to check for existence
         MDAL tgtmdal = tgtsubspace->prepo->metascheme.mdal;
         struct stat stval;
         int stnsres = tgtmdal->statnamespace( tgtmdal->ctxt, subspacepath, &(stval) );
         free( subspacepath );
         if ( stnsres  &&  errno != ENOENT ) {
            LOG( LOG_ERR, "Failed to stat subspace root: \"%s\"\n", subspacepath );
            pthread_mutex_unlock( &(dh->lock) );
            LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
            return NULL;
         }
         if ( stnsres == 0 ) {
            // populate and return the subspace dirent
            if ( snprintf( dh->subspcent.d_name, dh->subspcnamealloc, "%s", dh->ns->subnodes[dh->location].name ) >= dh->subspcnamealloc ) {
               LOG( LOG_ERR, "Dirent struct does not have sufficient space to store subspace name: \"%s\" (%zu bytes available)\n", dh->ns->subnodes[dh->location].name, dh->subspcnamealloc );
               pthread_mutex_unlock( &(dh->lock) );
               errno = ENAMETOOLONG;
               LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
               return NULL;
            }
            // increment our index
            dh->location++;
            if ( dh->location & MARFS_DIR_NS_OFFSET_MASK ) {
               if ( dh->location != dh->ns->subnodecount ) {
                  // indicate that our location has become invalid
                  dh->location = MARFS_DIR_NS_OFFSET_MASK | 1L;
                  LOG( LOG_ERR, "This readdir op has resulted in an excessive dir handle location value\n" );
               }
               else {
                  // overwrite our location, to indicate that we have finished subspace listing
                  dh->location = MARFS_DIR_NS_OFFSET_MASK;
               }
            }
            pthread_mutex_unlock( &(dh->lock) );
            LOG( LOG_INFO, "EXIT - Success\n" );
            errno = cachederrno;
            return &(dh->subspcent);
         }
         // increment our index
         dh->location++;
      }
      // overwrite our location, to indicate that we have finished subspace listing
      dh->location = MARFS_DIR_NS_OFFSET_MASK;
   }
   // check for an invalid location value
   if ( dh->location == (MARFS_DIR_NS_OFFSET_MASK | 1L) ) {
      pthread_mutex_unlock( &(dh->lock) );
      LOG( LOG_ERR, "Dir handle location value is invalid\n" );
      errno = EMSGSIZE;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // perform the op
   MDAL curmdal = dh->ns->prepo->metascheme.mdal;
   struct dirent* retval = NULL;
   char repeat = 1;
   while ( repeat ) {
      retval = curmdal->readdir( dh->metahandle );
      // filter out any restricted entries at the root of a NS
      if ( dh->depth == 0  &&  retval != NULL  &&  curmdal->pathfilter( retval->d_name ) ) {
         LOG( LOG_INFO, "Omitting hidden dirent: \"%s\"\n", retval->d_name );
      }
      else { repeat = 0; } // break on error, or if the entry wasn't filtered
   }
   pthread_mutex_unlock( &(dh->lock) );
   if ( retval != NULL ) { LOG( LOG_INFO, "EXIT - Success\n" ); errno = cachederrno; }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Identify the ( abstract ) location of an open directory handle
 * NOTE -- This 'location' can be used via marfs_seekdir() to allow for the repeating
 *         of marfs_readdir() results.  However, the 'location' value should be
 *         considered an opaque type, with no caller assumptions as to content,
 *         beyond error checking.
 * @param marfs_dhandle dh : marfs_dhandle to retrieve the location value of
 * @return long : Abstract location value, or -1 on error
 */
long marfs_telldir(marfs_dhandle dh) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire directory lock
   if ( pthread_mutex_lock( &(dh->lock) ) ) {
      LOG( LOG_ERR, "Failed to aqcuire marfs_dhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for an invalid location
   if ( dh->depth == 0  &&  dh->ns->subnodecount  &&  dh->location == (MARFS_DIR_NS_OFFSET_MASK | 1L) ) {
      pthread_mutex_unlock( &(dh->lock) );
      LOG( LOG_ERR, "Dir handle location value is invalid\n" );
      errno = EMSGSIZE;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   long retval = 0;
   // check for a post-NS offset
   if ( dh->depth != 0  ||  dh->ns->subnodecount == 0  ||  (dh->location & MARFS_DIR_NS_OFFSET_MASK) ) {
      // get the location value of the underlying MDAL_DHANDLE
      retval = dh->ns->prepo->metascheme.mdal->telldir( dh->metahandle );
      // if we have subspaces, this value requires additional checks/modification
      if ( dh->depth == 0  &&  dh->ns->subnodecount ) {
         // check for value collision
         if ( (retval & MARFS_DIR_NS_OFFSET_MASK) ) {
            pthread_mutex_unlock( &(dh->lock) );
            LOG( LOG_ERR, "MDAL handle location value collides with MarFS dir NS offset bit\n" );
            errno = EDOM;
            LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
            return -1;
         }
         // mark this as a post-NS value
         retval |= MARFS_DIR_NS_OFFSET_MASK;
      }
      pthread_mutex_unlock( &(dh->lock) );
      if ( retval != -1 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // otherwise, just return our current subspace offset
   pthread_mutex_unlock( &(dh->lock) );
   LOG( LOG_INFO, "EXIT - Success\n" );
   return dh->location;
}

/**
 * Set the position of an open directory handle, for future marfs_readdir() calls
 * @param marfs_dhandle dh : marfs_dhandle to seek
 * @param long loc : Location value to seek to
 *                   NOTE -- this value *must* come from a previous marfs_telldir()
 * @return int : Zero on success, or -1 on failure
 */
int marfs_seekdir(marfs_dhandle dh, long loc) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire directory lock
   if ( pthread_mutex_lock( &(dh->lock) ) ) {
      LOG( LOG_ERR, "Failed to aqcuire marfs_dhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   long origloc = dh->location;
   int retval = 0;
   // check for a post-NS location
   if ( dh->depth != 0  ||  dh->ns->subnodecount == 0  ||  (loc & MARFS_DIR_NS_OFFSET_MASK) ) {
      // possibly strip out our NS offset flag, while noting it in the dir handle struct
      if ( dh->depth == 0  &&  dh->ns->subnodecount ) {
         loc &= ~(MARFS_DIR_NS_OFFSET_MASK);
         dh->location = MARFS_DIR_NS_OFFSET_MASK;
      }
      retval = dh->ns->prepo->metascheme.mdal->seekdir( dh->metahandle, loc );
      pthread_mutex_unlock( &(dh->lock) );
      if ( retval != -1 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); dh->location = origloc; }
      return retval;
   }
   // otherwise, this value corresponds to an offset within our subspaces
   dh->location = loc;
   // potentially rewind our MDAL_DHANDLE
   if ( origloc & MARFS_DIR_NS_OFFSET_MASK ) {
      retval = dh->ns->prepo->metascheme.mdal->rewinddir( dh->metahandle );
   }
   pthread_mutex_unlock( &(dh->lock) );
   if ( retval != -1 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); dh->location = origloc; }
   return retval;
}

/**
 * Reset the position of an open directory handle, for future marfs_readdir() calls, to
 * the beginning of the dir
 * NOTE -- This will result in marfs_readdir() returning entries as though the directory
 *         handle had been freshly opened.
 *         This is also equivalent to issuing a marfs_seekdir() back to a location value
 *         provided by marfs_telldir() prior to any marfs_readdir() op being issued on
 *         the directory handle.
 * @param marfs_dhandle dh : marfs_dhandle to rewind
 * @return int : Zero on success, or -1 on failure
 */
int marfs_rewinddir(marfs_dhandle dh) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire directory lock
   if ( pthread_mutex_lock( &(dh->lock) ) ) {
      LOG( LOG_ERR, "Failed to aqcuire marfs_dhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   int retval = 0;
   // potentially rewind our MDAL_DHANDLE
   if ( dh->depth != 0  ||  dh->ns->subnodecount == 0  ||  (dh->location & MARFS_DIR_NS_OFFSET_MASK) ) {
      retval = dh->ns->prepo->metascheme.mdal->rewinddir( dh->metahandle );
   }
   pthread_mutex_unlock( &(dh->lock) );
   if ( retval != -1 ) { LOG( LOG_INFO, "EXIT - Success\n" ); dh->location = 0; }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Close the given directory handle
 * @param marfs_dhandle dh : marfs_dhandle to close
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_closedir(marfs_dhandle dh) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire directory lock
   if ( pthread_mutex_lock( &(dh->lock) ) ) {
      LOG( LOG_ERR, "Failed to aqcuire marfs_dhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // close the handle and free all memory
   MDAL curmdal = dh->ns->prepo->metascheme.mdal;
   int retval = curmdal->closedir( dh->metahandle );
   config_destroynsref( dh->ns );
   pthread_mutex_unlock( &(dh->lock) );
   pthread_mutex_destroy( &(dh->lock) );
   free( dh );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_dhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // validate that the ctxt and dhandle reference the same config
   if ( ctxt->config != dh->config ) {
      LOG( LOG_ERR, "Received dhandle and marfs_ctxt do not reference the same config\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the dir handle lock
   if ( pthread_mutex_lock( &(dh->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_dhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the ctxt lock
   if ( pthread_mutex_lock( &(ctxt->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_ctxt lock\n" );
      pthread_mutex_unlock( &(dh->lock) );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // chdir to the specified dhandle
   MDAL curmdal = dh->ns->prepo->metascheme.mdal;
   if ( curmdal->chdir( ctxt->pos.ctxt, dh->metahandle ) ) {
      LOG( LOG_ERR, "Failed to chdir MDAL_CTXT\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      pthread_mutex_unlock( &(ctxt->lock) );
      pthread_mutex_unlock( &(dh->lock) );
      return -1;
   }
   // MDAL_CTXT has been updated; now update the position
   ctxt->pos.ns = dh->ns;
   ctxt->pos.depth = dh->depth;
   pthread_mutex_unlock( &(ctxt->lock) );
   pthread_mutex_unlock( &(dh->lock) );
   pthread_mutex_destroy( &(dh->lock) );
   free( dh ); // the underlying MDAL_DHANDLE is no longer valid
   LOG( LOG_INFO, "EXIT - Success\n" );
   return 0;
}

// 
// NOTE -- these skip acquiring the dir lock, for efficiency, as none modify the structure
//         However, this means that calling dir modification functions in parallel with these 
//         will result in undefined behavior.
//

/**
 * Set the specified xattr on the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to set the xattr
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_dsetxattr(marfs_dhandle dh, const char* name, const void* value, size_t size, int flags) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL directory handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( dh->itype != MARFS_INTERACTIVE  &&  !(dh->ns->bperms & NS_WRITEMETA) )  ||
        ( dh->itype != MARFS_BATCH        &&  !(dh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a dsetxattr op\n" );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval = dh->ns->prepo->metascheme.mdal->dsetxattr( dh->metahandle, 0, name, value, size, flags );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Retrieve the specified xattr from the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to retrieve the xattr
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t marfs_dgetxattr(marfs_dhandle dh, const char* name, void* value, size_t size) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL directory handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   ssize_t retval = dh->ns->prepo->metascheme.mdal->dgetxattr( dh->metahandle, 0, name, value, size );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Remove the specified xattr from the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to remove the xattr
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int marfs_dremovexattr(marfs_dhandle dh, const char* name) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL directory handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( dh->itype != MARFS_INTERACTIVE  &&  !(dh->ns->bperms & NS_WRITEMETA) )  ||
        ( dh->itype != MARFS_BATCH        &&  !(dh->ns->iperms & NS_WRITEMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a dremovexattr op\n" );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval = dh->ns->prepo->metascheme.mdal->dremovexattr( dh->metahandle, 0, name );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * List all xattr names from the directory referenced by the given marfs_dhandle
 * @param marfs_dhandle dh : Directory handle for which to list xattrs
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t marfs_dlistxattr(marfs_dhandle dh, char* buf, size_t size) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( dh == NULL ) {
      LOG( LOG_ERR, "Received a NULL directory handle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   ssize_t retval = dh->ns->prepo->metascheme.mdal->dlistxattr( dh->metahandle, 0, buf, size );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // identify the path target
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for create op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms ( require RWMETA and WRITEDATA for file creation )
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&
            ( (oppos.ns->bperms & NS_RWMETA) != NS_RWMETA  ||
             !(oppos.ns->bperms & NS_WRITEDATA) ) )
        ||
        ( ctxt->itype != MARFS_BATCH        &&
            ( (oppos.ns->iperms & NS_RWMETA) != NS_RWMETA   ||
             !(oppos.ns->iperms & NS_WRITEDATA) ) ) 
      ) {
      LOG( LOG_ERR, "NS perms do not allow a create op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // check for NS target
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a create op\n" );
      pathcleanup( subpath, &oppos );
      errno = EISDIR;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // check NS quota
   MDAL tgtmdal = oppos.ns->prepo->metascheme.mdal;
   off_t inodeusage = 0;
   if ( oppos.ns->fquota ) {
      inodeusage = tgtmdal->getinodeusage( oppos.ctxt );
      if ( inodeusage < 0 ) {
         LOG( LOG_ERR, "Failed to retrieve NS inode usage info\n" );
      }
      else if ( inodeusage >= oppos.ns->fquota ) {
         LOG( LOG_ERR, "NS has excessive inode count (%zd)\n", inodeusage );
         inodeusage = -1;
      }
      if ( inodeusage < 0 ) {
         pathcleanup( subpath, &oppos );
         errno = EDQUOT;
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
   }
   off_t datausage = 0;
   if ( oppos.ns->dquota ) {
      datausage = tgtmdal->getdatausage( oppos.ctxt );
      if ( datausage < 0 ) {
         LOG( LOG_ERR, "Failed to retrieve NS data usage info\n" );
      }
      else if ( datausage >= oppos.ns->dquota ) {
         LOG( LOG_ERR, "NS has excessive data usage (%zd)\n", datausage );
         datausage = -1;
      }
      if ( datausage < 0 ) {
         pathcleanup( subpath, &oppos );
         errno = EDQUOT;
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
   }
   // check the state of our handle argument
   char newstream = 0;
   if ( stream == NULL ) {
      // allocate a fresh handle
      stream = new_marfs_fhandle();
      if ( stream == NULL ) {
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      newstream = 1;
   }
   else {
      // acquire the lock for an existing stream
      if ( pthread_mutex_lock( &(stream->lock) ) ) {
         LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      if ( stream->datastream == NULL  &&  stream->metahandle == NULL ) {
         // a double-NULL handle has been flushed or suffered a fatal error
         LOG( LOG_ERR, "Received a flushed marfs_fhandle\n" );
         pthread_mutex_unlock( &(stream->lock) );
         pathcleanup( subpath, &oppos );
         errno = EINVAL;
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      else if ( stream->datastream == NULL  &&  stream->metahandle != NULL ) {
         // meta-only reference; attempt to close it
         MDAL curmdal = stream->ns->prepo->metascheme.mdal;
         if ( curmdal->close( stream->metahandle ) ) {
            LOG( LOG_ERR, "Failed to close previous MDAL_FHANDLE\n" );
            stream->metahandle = NULL;
            pthread_mutex_unlock( &(stream->lock) );
            pathcleanup( subpath, &oppos );
            errno = EBADFD;
            LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
            return NULL;
         }
         stream->metahandle = NULL; // don't reattempt this op
      }
   }
   // duplicate the current NS ref
   marfs_ns* dupref = config_duplicatensref( oppos.ns );
   if ( dupref == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate op NS reference\n" );
      pathcleanup( subpath, &oppos );
      if ( newstream ) { free( stream ); }
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // attempt the op
   char hadstream = 0;
   if ( stream->datastream ) { hadstream = 1; }
   if ( datastream_create( &(stream->datastream), subpath, &oppos, mode, ctxt->config->ctag ) ) {
      LOG( LOG_ERR, "Failure of datastream_create()\n" );
      config_destroynsref( dupref );
      pathcleanup( subpath, &oppos );
      if ( newstream ) { free( stream ); }
      else {
         if ( stream->datastream == NULL  &&  hadstream ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
         if ( stream->metahandle == NULL ) { errno = EBADFD; } // ref is now defunct
         pthread_mutex_unlock( &(stream->lock) );
      }
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // update our stream info to reflect the new target
   if ( stream->ns ) { config_destroynsref( stream->ns ); }
   stream->flags = O_WRONLY | O_CREAT;
   stream->ns = dupref;
   stream->metahandle = stream->datastream->files[stream->datastream->curfile].metahandle;
   stream->itype = ctxt->itype;
   // cleanup and return
   if ( !(newstream) ) { pthread_mutex_unlock( &(stream->lock) ); }
   pathcleanup( subpath, &oppos ); // done with path info
   LOG( LOG_INFO, "EXIT - Success\n" );
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
 * @param int flags : A bitwise OR of the following...
 *                    O_RDONLY   - Read only access
 *                    O_WRONLY   - Write only access
 *                    O_RDWR     - Read + Write access
 *                                 ( note, only supported when combined with O_ASYNC; see below )
 *                    O_CREAT    - Create the target file, if it does not already exist
 *                                 ( note, only supported when combined with O_ASYNC; see below.
 *                                   Standard MarFS file creation should be done via the
 *                                   marfs_creat() function instead. )
 *                    O_EXCL     - Ensure this call creates the target file
 *                                 ( note, only supported when combined with O_ASYNC; see below )
 *                    O_NOFOLLOW - Do not follow a symlink target
 *                                 ( note, if targeting a symlink, always returns ELOOP,
 *                                   unless combined with O_PATH; see linux open() manpage.
 *                                   Only supported when combined with O_ASYNC; see below. )
 *                    O_PATH     - Obtain a handle which merely indicates an FS tree location
 *                                 ( note, see linux open() manpage.
 *                                   Only supported when combined with O_ASYNC; see below. )
 *                    O_ASYNC    - Obtain an MDAL ( meta-only ) reference
 *                                 ( note, here, the behavior of this flag differs entirely from
 *                                   standard POSIX / Linux.  This flag causes MarFS to bypass
 *                                   the DAL ( data object path ) and operate exclusively via
 *                                   the MDAL ( metadata path ). )
 *                    NOTE -- some of these flags may require the caller to define _GNU_SOURCE!
 * @return marfs_fhandle : marfs_fhandle referencing the opened file,
 *                         or NULL if a failure occurred
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
marfs_fhandle marfs_open(marfs_ctxt ctxt, marfs_fhandle stream, const char *path, int flags) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // check for invalid flags
   if ( (flags & O_ASYNC) == 0  &&
            (
              (flags & O_ACCMODE) == O_RDWR || 
              (flags & ~(O_ACCMODE)) != 0
            )
      ) {
      LOG( LOG_ERR, "Invalid flags value\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // identify the path target
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, path, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for create op\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   // check NS perms
   if ( ( ctxt->itype != MARFS_INTERACTIVE  &&  !(oppos.ns->bperms & NS_READMETA) )  ||
        ( ctxt->itype != MARFS_BATCH        &&  !(oppos.ns->iperms & NS_READMETA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow an open op\n" );
      pathcleanup( subpath, &oppos );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   if ( tgtdepth == 0 ) {
      LOG( LOG_ERR, "Cannot target a MarFS NS with a create op\n" );
      pathcleanup( subpath, &oppos );
      errno = EISDIR;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // check the state of our handle argument
   char newstream = 0;
   if ( stream == NULL ) {
      // allocate a fresh handle
      stream = new_marfs_fhandle();
      if ( stream == NULL ) {
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      newstream = 1;
      // acquire the lock, just in case
      if ( pthread_mutex_lock( &(stream->lock) ) ) {
         LOG( LOG_ERR, "Failed to acquire lock on new marfs_fhandle\n" );
         pthread_mutex_destroy( &(stream->lock) );
         free( stream );
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
   }
   else {
      // acquire the lock for an existing stream
      if ( pthread_mutex_lock( &(stream->lock) ) ) {
         LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      if ( stream->datastream == NULL  &&  stream->metahandle == NULL ) {
         // a double-NULL handle has been flushed or suffered a fatal error
         LOG( LOG_ERR, "Received a flushed marfs_fhandle\n" );
         pthread_mutex_unlock( &(stream->lock) );
         pathcleanup( subpath, &oppos );
         errno = EINVAL;
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      else if ( stream->datastream == NULL  &&  stream->metahandle != NULL ) {
         // meta-only reference; attempt to close it
         MDAL curmdal = stream->ns->prepo->metascheme.mdal;
         if ( curmdal->close( stream->metahandle ) ) {
            LOG( LOG_ERR, "Failed to close previous MDAL_FHANDLE\n" );
            stream->metahandle = NULL;
            pthread_mutex_unlock( &(stream->lock) );
            pathcleanup( subpath, &oppos );
            errno = EBADFD;
            LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
            return NULL;
         }
         stream->metahandle = NULL; // don't reattempt this op
      }
   }
   // duplicate the current NS ref
   marfs_ns* dupref = config_duplicatensref( oppos.ns );
   if ( dupref == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate op NS reference\n" );
      pathcleanup( subpath, &oppos );
      if ( !(newstream)  &&  stream->metahandle == NULL ) { errno = EBADFD; } // ref is now defunct
      pthread_mutex_unlock( &(stream->lock) );
      if ( newstream ) { free( stream ); }
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // check for MDAL-only O_ASYNC flag
   if ( flags & O_ASYNC ) {
      // poentially cleanup existing stream info
      if ( stream->datastream  &&  datastream_release( &(stream->datastream) ) ) {
         LOG( LOG_ERR, "Failed to release previous datastream reference\n" );
         stream->datastream = NULL;
         stream->metahandle = NULL;
         config_destroynsref( dupref );
         pathcleanup( subpath, &oppos );
         pthread_mutex_unlock( &(stream->lock) );
         errno = EBADFD;
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      if ( stream->ns ) { config_destroynsref( stream->ns ); }
      // open a meta-only reference for this file
      stream->flags = flags;
      stream->datastream = NULL;
      stream->ns = dupref;
      stream->itype = ctxt->itype;
      MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
      stream->metahandle = curmdal->open( oppos.ctxt, subpath, flags & ~(O_ASYNC) );
      if ( stream->metahandle == NULL ) {
         LOG( LOG_ERR, "Failed to open meta-only reference for the target file: \"%s\" ( %s )\n", path, strerror(errno) );
         config_destroynsref( dupref );
         pathcleanup( subpath, &oppos );
         pthread_mutex_unlock( &(stream->lock) );
         if ( !(newstream) ) { errno = EBADFD; }
         else { free( stream ); }
         LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
         return NULL;
      }
      // cleanup and return
      pthread_mutex_unlock( &(stream->lock) );
      pathcleanup( subpath, &oppos );
      LOG( LOG_INFO, "EXIT - Success\n" );
      return stream;
   }
   // attempt the op, allowing a meta-only reference ONLY if we are opening for read
   char hadstream = 0;
   if ( stream->datastream ) { hadstream = 1; }
   MDAL_FHANDLE phandle = NULL;
   if ( datastream_open( &(stream->datastream), ((flags & O_ACCMODE) == O_WRONLY) ? EDIT_STREAM : READ_STREAM, subpath, &oppos,
                         ( (flags & O_ACCMODE) == O_RDONLY ) ? &(phandle) : NULL ) ) {
      // check for a meta-only reference
      if ( phandle != NULL ) {
         LOG( LOG_INFO, "Attempting to use meta-only reference for target file\n" );
         // need to release the current datastream
         if ( stream->datastream  &&  datastream_release( &(stream->datastream) ) ) {
            LOG( LOG_ERR, "Failed to release previous datastream reference\n" );
            stream->datastream = NULL;
            MDAL curmdal = oppos.ns->prepo->metascheme.mdal;
            if ( curmdal->close( phandle ) ) {
               // nothing to do besides complain
               LOG( LOG_WARNING, "Failed to close preserved MDAL_FHANDLE for new target\n" );
            }
            config_destroynsref( dupref );
            stream->metahandle = NULL;
            pathcleanup( subpath, &oppos );
            pthread_mutex_unlock( &(stream->lock) );
            errno = EBADFD;
            LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
            return NULL;
         }
         // update stream info to reflect a meta-only reference
         stream->flags = flags | O_ASYNC;
         stream->datastream = NULL;
         stream->metahandle = phandle;
         if ( stream->ns ) { config_destroynsref( stream->ns ); }
         stream->ns = dupref;
         stream->itype = ctxt->itype;
         // cleanup and return
         pthread_mutex_unlock( &(stream->lock) );
         pathcleanup( subpath, &oppos );
         LOG( LOG_INFO, "EXIT - Success\n" );
         return stream;
      }
      LOG( LOG_ERR, "Failure of datastream_open()\n" );
      pathcleanup( subpath, &oppos );
      if ( stream->datastream == NULL  &&  hadstream ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
      if ( !(newstream)  &&  stream->metahandle == NULL ) { errno = EBADFD; } // ref is now defunct
      pthread_mutex_unlock( &(stream->lock) );
      if ( newstream ) { free( stream ); }
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return NULL;
   }
   // update our stream info to reflect the new target
   stream->flags = flags;
   if ( stream->ns ) { config_destroynsref( stream->ns ); }
   stream->ns = dupref;
   stream->metahandle = stream->datastream->files[stream->datastream->curfile].metahandle;
   stream->itype = ctxt->itype;
   // cleanup and return
   pthread_mutex_unlock( &(stream->lock) );
   pathcleanup( subpath, &oppos ); // done with path info
   LOG( LOG_INFO, "EXIT - Success\n" );
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // reject a flushed handle
   if ( stream->metahandle == NULL  &&  stream->datastream == NULL ) {
      LOG( LOG_ERR, "Received a flushed marfs_fhandle\n" );
      if ( stream->ns ) { config_destroynsref( stream->ns ); }
      pthread_mutex_unlock( &(stream->lock) );
      pthread_mutex_destroy( &(stream->lock) );
      free( stream );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
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
   stream->metahandle = NULL;
   stream->datastream = NULL;
   if ( stream->ns ) { config_destroynsref( stream->ns ); }
   pthread_mutex_unlock( &(stream->lock) );
   pthread_mutex_destroy( &(stream->lock) );
   free( stream );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   int retval = 0;
   if ( stream->datastream ) {
      // datastream reference
      LOG( LOG_INFO, "Releasing datastream reference\n" );
      if ( (retval = datastream_release( &(stream->datastream) )) ) {
         LOG( LOG_ERR, "Failed to release datastream\n" );
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
   if ( stream->ns ) { config_destroynsref( stream->ns ); }
   pthread_mutex_unlock( &(stream->lock) );
   pthread_mutex_destroy( &(stream->lock) );
   free( stream );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
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
   pthread_mutex_unlock( &(stream->lock) );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Set the file path recovery info to be encoded into data objects for the provided handle
 *    NOTE -- It is essential for all data objects to be encoded with matching recovery
 *            paths.  This happens automatically when an file is written out entirely from
 *            a single 'create' handle, but the recovery info can shift for files written
 *            in parallel ( if MARFS_WRITE handles are opened with varied paths ).
 * @param marfs_ctxt ctxt : marfs_ctxt to operate relative to
 * @param marfs_fhandle stream : MARFS_WRITE or create file handle for which to set the
 *                               recovery path
 * @param const char* recovpath : New recovery path to be set
 * @return int : Zero on success, or -1 on failure
 */
int marfs_setrecoverypath(marfs_ctxt ctxt, marfs_fhandle stream, const char* recovpath) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( ctxt == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_ctxt arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream == NULL ) {
      LOG( LOG_ERR, "Received marfs_fhandle has no underlying datastream\n" );
      pthread_mutex_unlock( &(stream->lock) );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // identify target info
   marfs_position oppos = { .ns = NULL, .depth = 0, .ctxt = NULL };
   char* subpath = NULL;
   int tgtdepth = pathshift( ctxt, recovpath, &(subpath), &(oppos), 1 );
   if ( tgtdepth < 0 ) {
      LOG( LOG_ERR, "Failed to identify target info for setrecoverypath op\n" );
      pthread_mutex_unlock( &(stream->lock) );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   LOG( LOG_INFO, "TGT: Depth=%d, NS=\"%s\", SubPath=\"%s\"\n", tgtdepth, oppos.ns->idstr, subpath );
   if ( oppos.ns != stream->ns ) {
      LOG( LOG_ERR, "Target NS (\"%s\") does not match stream NS (\"%s\")\n",
           oppos.ns->idstr, stream->ns->idstr );
      pthread_mutex_unlock( &(stream->lock) );
      pathcleanup( subpath, &oppos );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // perform the op
   int retval = datastream_setrecoverypath( &(stream->datastream), subpath );
   pthread_mutex_unlock( &(stream->lock) );
   pathcleanup( subpath, &oppos );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
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
ssize_t marfs_read(marfs_fhandle stream, void* buf, size_t count) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_READDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_READDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a read op\n" );
      pthread_mutex_unlock( &(stream->lock) );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // read from datastream reference
      ssize_t retval = datastream_read( &(stream->datastream), buf, count );
      pthread_mutex_unlock( &(stream->lock) );
      if ( retval >= 0 ) { LOG( LOG_INFO, "EXIT - Success (%zd bytes)\n", retval ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // meta only reference
   ssize_t retval = 0;
   if ( stream->ns->prepo->metascheme.directread == 0 ) {
      // direct read isn't enabled
      LOG( LOG_ERR, "Direct read is not enabled for this target\n" );
      errno = EPERM;
      retval = -1;
   }
   else if ( stream->flags & O_ASYNC ) {
      // perform the direct read
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      retval = curmdal->read( stream->metahandle, buf, count );
      // TODO : MUST prevent cached reads for migrated files
   }
   pthread_mutex_unlock( &(stream->lock) );
   if ( retval >= 0 ) { LOG( LOG_INFO, "EXIT - Success (%zd bytes)\n", retval ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_WRITEDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_WRITEDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a write op %d\n", (int)stream->ns->bperms );
      pthread_mutex_unlock( &(stream->lock) );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // write to the datastream reference
      ssize_t retval = datastream_write( &(stream->datastream), buf, size );
      if ( stream->datastream == NULL ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
      pthread_mutex_unlock( &(stream->lock) );
      if ( retval >= 0 ) { LOG( LOG_INFO, "EXIT - Success (%zd bytes)\n", retval ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // meta only reference ( direct write is currently unsupported )
   LOG( LOG_ERR, "Cannot write to a meta-only reference\n" );
   pthread_mutex_unlock( &(stream->lock) );
   errno = EPERM;
   LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      LOG( LOG_INFO, "Seeking datastream\n" );
      // seek the datastream reference
      off_t retval = datastream_seek( &(stream->datastream), offset, whence );
      if ( stream->datastream == NULL ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
      pthread_mutex_unlock( &(stream->lock) );
      if ( retval >= 0 ) { LOG( LOG_INFO, "EXIT - Success (offset=%zd)\n", retval ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // meta only reference
   off_t retval = 0;
   if ( stream->ns->prepo->metascheme.directread == 0 ) {
      // direct read isn't enabled
      LOG( LOG_ERR, "Direct read is not enabled for this target\n" );
      errno = EPERM;
      retval = -1;
   }
   else if ( stream->flags & O_ASYNC ) {
      LOG( LOG_INFO, "Seeking meta handle\n" );
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      retval = curmdal->lseek( stream->metahandle, offset, whence );
      // TODO : detect cached file migration
   }
   pthread_mutex_unlock( &(stream->lock) );
   if ( retval >= 0 ) { LOG( LOG_INFO, "EXIT - Success (offset=%zd)\n", retval ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
}

/**
 * Seek to the provided offset of the given marfs_fhandle AND read from that location
 * NOTE -- This function exists for the sole purpose of supporting the FUSE interface, 
 *         which performs reads, using this 'at-offset' format, in parallel
 * @param marfs_fhandle stream : marfs_fhandle to seek and read
 * @param off_t offset : Offset for the seek
 *                       NOTE -- this is assumed to be relative to the start of the file
 *                               ( as in, whence == SEEK_SET )
 * @param void* buf : Reference to the buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 on failure
 *    NOTE -- In most failure conditions, any previous marfs_fhandle reference will be
 *            preserved ( continue to reference whatever file it previously referenced ).
 *            However, it is possible for certain catastrophic error conditions to occur.
 *            In such a case, errno will be set to EBADFD and any subsequent operations
 *            against the provided marfs_fhandle will fail, besides marfs_release().
 */
ssize_t marfs_read_at_offset(marfs_fhandle stream, off_t offset, void* buf, size_t count) {
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_READDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_READDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a read op\n" );
      pthread_mutex_unlock( &(stream->lock) );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }

   // seek to the requested offset
   off_t offval;
   // check for datastream reference
   if ( stream->datastream ) {
      LOG( LOG_INFO, "Seeking datastream to %zd offset\n", offset );
      // seek the datastream reference
      offval = datastream_seek( &(stream->datastream), offset, SEEK_SET );
      if ( stream->datastream == NULL ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
   }
   else {
      // meta only reference
      if ( stream->ns->prepo->metascheme.directread == 0 ) {
         LOG( LOG_ERR, "Direct read is not enabled for this target\n" );
         errno = EPERM;
         offval = -1;
      }
      else if ( stream->flags & O_ASYNC ) {
         LOG( LOG_INFO, "Seeking meta handle to %zd offset\n", offset );
         MDAL curmdal = stream->ns->prepo->metascheme.mdal;
         offval = curmdal->lseek( stream->metahandle, offset, SEEK_SET );
         // TODO : detect cached file migration
      }
   }
   if ( offval != offset  ||  offval < 0 ) {
      pthread_mutex_unlock( &(stream->lock) );
      if ( offval < offset  &&  offval >=0 ) {
         LOG( LOG_INFO, "Reduced offset of %zd implies read beyond EOF ( returning zero bytes )\n", offval );
         return 0;
      }
      LOG( LOG_ERR, "Unexpected offset returned by seek: %zd\n", offval );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }

   // perform the read
   ssize_t retval;
   if ( stream->datastream ) {
      LOG( LOG_INFO, "Reading %zu bytes from datastream\n", count );
      // read from datastream reference
      retval = datastream_read( &(stream->datastream), buf, count );
      if ( stream->datastream == NULL ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
   }
   else {
      // meta only reference
      // perform the direct read
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      retval = curmdal->read( stream->metahandle, buf, count );
   }

   pthread_mutex_unlock( &(stream->lock) );
   if ( retval >= 0 ) { LOG( LOG_INFO, "EXIT - Success (%zd bytes)\n", retval ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // identify datastream reference chunkbounds
      int retval = datastream_chunkbounds( &(stream->datastream), chunknum, offset, size );
      pthread_mutex_unlock( &(stream->lock) );
      if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // meta only reference ( this function does not apply )
   LOG( LOG_ERR, "Cannot identify chunk boundaries for a meta-only reference\n" );
   pthread_mutex_unlock( &(stream->lock) );
   errno = EINVAL;
   LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check NS perms
   if ( ( stream->itype != MARFS_INTERACTIVE  &&  !(stream->ns->bperms & NS_WRITEDATA) )  ||
        ( stream->itype != MARFS_BATCH        &&  !(stream->ns->iperms & NS_WRITEDATA) ) ) {
      LOG( LOG_ERR, "NS perms do not allow a truncate op\n" );
      pthread_mutex_unlock( &(stream->lock) );
      errno = EPERM;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // truncate the datastream reference
      int retval = datastream_truncate( &(stream->datastream), length );
      pthread_mutex_unlock( &(stream->lock) );
      if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // meta only reference
   int retval = 0;
   if ( stream->ns->prepo->metascheme.directread == 0 ) {
      // direct read isn't enabled
      LOG( LOG_ERR, "Direct read is not enabled for this target\n" );
      errno = EPERM;
      retval = -1;
   }
   else if ( stream->flags & O_ASYNC ) {
      LOG( LOG_INFO, "Truncating meta-only reference\n" );
      MDAL curmdal = stream->ns->prepo->metascheme.mdal;
      retval = curmdal->ftruncate( stream->metahandle, length );
      // TODO : detect cached file migration
   }
   pthread_mutex_unlock( &(stream->lock) );
   if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
   else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
   return retval;
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
   LOG( LOG_INFO, "ENTRY\n" );
   // check for NULL args
   if ( stream == NULL ) {
      LOG( LOG_ERR, "Received a NULL marfs_fhandle arg\n" );
      errno = EINVAL;
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // acquire the lock for an existing stream
   if ( pthread_mutex_lock( &(stream->lock) ) ) {
      LOG( LOG_ERR, "Failed to acquire marfs_fhandle lock\n" );
      LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
      return -1;
   }
   // check for datastream reference
   if ( stream->datastream ) {
      // extend the datastream reference
      int retval = datastream_extend( &(stream->datastream), length );
      if ( stream->datastream == NULL ) { stream->metahandle = NULL; } // don't allow invalid meta handle to persist
      pthread_mutex_unlock( &(stream->lock) );
      if ( retval == 0 ) { LOG( LOG_INFO, "EXIT - Success\n" ); }
      else { LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) ); }
      return retval;
   }
   // meta only reference ( this function does not apply )
   LOG( LOG_ERR, "Cannot extend a meta-only reference\n" );
   pthread_mutex_unlock( &(stream->lock) );
   errno = EINVAL;
   LOG( LOG_INFO, "EXIT - Failure w/ \"%s\"\n", strerror(errno) );
   return -1;
}


