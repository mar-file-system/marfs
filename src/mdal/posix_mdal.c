
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
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "marfs_auto_config.h"
#ifdef DEBUG_MDAL
#define DEBUG DEBUG_MDAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "posix_mdal"
#include <logging.h>

#include "mdal.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>


//   -------------    POSIX DEFINITIONS    -------------

#define PMDAL_PREFX "MDAL_"
#define PMDAL_REF PMDAL_PREFX"reference"
#define PMDAL_SUBSP PMDAL_PREFX"subspaces"
#define PMDAL_SUBSTRLEN 14 // max length of all ref/path/subsp dir names
#define PMDAL_DUSE PMDAL_PREFX"datasize"
#define PMDAL_IUSE PMDAL_PREFX"inodecount"
#define PMDAL_XATTR "user."PMDAL_PREFX


//   -------------    POSIX STRUCTURES    -------------

typedef struct posixmdal_directory_handle_struct {
   DIR*     dirp; // Directory reference
}* POSIX_DHANDLE;

typedef struct posixmdal_scanner_struct {
   DIR*     dirp; // Directory reference
}* POSIX_SCANNER;

typedef struct posixmdal_file_handle_struct {
   int        fd; // File handle
}* POSIX_FHANDLE;

typedef struct posix_mdal_context_struct {
   int refd;   // Dir handle for NS ref tree ( or the secure root, if NS hasn't been set )
   int pathd;  // Dir handle of the user tree for the current NS ( or -1, if NS hasn't been set )
   dev_t dev;  // Device ID value associated with this context ( try to avoid accessing a non-marfs path )
}* POSIX_MDAL_CTXT;
   


//   -------------    POSIX INTERNAL FUNCTIONS    -------------

/**
 * Convert a given namespace path to a new path to the Posix MDAL subdir of that NS
 * @param const char* nspath : NS path
 * @param char* newpath : String to be populated
 * @param size_t newlen : Available size of the output string
 * @return size_t : Size of the resulting path ( assuming no output limits )
 */
size_t namespacepath( const char* nspath, char* newpath, size_t newlen ) {
   // parse over the path, figuring out the length of the new string we'll need to allocate
   char* orignewpath = newpath;
   size_t orignewlen = newlen;
   const char* parse = nspath;
   size_t totlen = 0;
   // assume we need a leading '../', to traverse to the NS root from the ref subdir
   totlen += 2;
   snprintf( newpath, newlen, ".." );
   if ( newlen <= 3 ) { newlen = 0; }
   else { newlen -= 2; newpath += 2; }
   while ( *parse != '\0' ) {
      // identify each sub-element
      const char* elemref = parse;
      int elemlen = 0;
      // move our parse pointer to the next path element
      while ( *parse != '\0' ) {
         if ( *parse == '/' ) {
            // traverse only to the next path component, skipping over duplicate '/' chars
            parse++;
            while ( *parse == '/' ) { parse++; }
            break;
         }
         elemlen++;
         parse++;
      }
      if ( elemlen == 0 ) {
         // catch the leading '/' and empty nspath cases
         if ( totlen != 2 ) {
            LOG( LOG_ERR, "Encountered unexpected '/' element while output at %zu\n", totlen );
            return 0;
         }
         // we must eliminate the '../' prefix
         // abs path doesn't need the prefix, empty str input should produce empty str res
         totlen = 0;
         newpath = orignewpath;
         newlen = orignewlen;
         LOG( LOG_INFO, "\"%s\" NS path is absolute, or empty\n", nspath );
      }
      else {
         int prout = 0;
         if ( strncmp( elemref, ".", elemlen ) == 0 ) {
            // local ref -- no need for subspace prefix
            prout = snprintf( newpath, newlen, "/." );
         }
         else if ( strncmp( elemref, "..", elemlen ) == 0 ) {
            // a parent namespace ref actually means double that for this MDAL
            // we have to traverse the intermediate PMDAL_SUBSP directory
            prout = snprintf( newpath, newlen, "/../.." );
         }
         else {
            // all other elements should be subspace references
            // we need to insert the intermediate subspace directory name
            prout = snprintf( newpath, newlen, "/%s/%.*s", PMDAL_SUBSP, elemlen, elemref );
         }
         totlen += prout;
         if ( prout + 1 >= newlen ) { newlen = 0; }
         else { newlen -= prout; newpath += prout; }
      }
   }
   LOG( LOG_INFO, "NS path of \"%s\" translates to \"%s\" (%zd len)\n", nspath, orignewpath, totlen );
   return totlen;
}


/**
 * Identify if the given xattr name is targeting a reserved value
 * @param const char* name : Xattr name string
 * @param char hidden : If zero, reserved names are not acceptable
 *                      If non-zero, only reserved names are acceptable
 * @return : Zero if the name is acceptable, -1 if not
 */
int xattrfilter( const char* name, char hidden ) {
   // parse over the string
   const char* parse = name;
   const char* reserved = PMDAL_XATTR;
   int compat = 0;
   int complen = strlen( PMDAL_XATTR );
   for( ; *parse != '\0'; parse++ ) {
      // compare against the reserved prefix
      if ( *parse == *(reserved + compat) ) {
         compat++; // progress to the next char
         if ( compat == complen ) {
            // prefix match
            if ( hidden ) { return 0; }
            else { return -1; }
         }
      }
      else {
         // prefix mismatch
         if ( hidden ) { return -1; }
         else { return 0; }
      }
   }
   // name is truncated version of reserved prefix, treat as mismatch
   if ( hidden ) { return -1; }
   return 0;
}


//   -------------    POSIX IMPLEMENTATION    -------------

// Path Filter

/**
 * Identify and reject any paths targeting reserved names
 * @param const char* path : Path to verify
 * @return : Zero if the path is acceptable, -1 if not
 */
int posixmdal_pathfilter( const char* path ) {
   // parse over the string
   const char* parse = path;
   const char* reserved = PMDAL_PREFX;
   int compat = 0;
   int complen = strlen( PMDAL_PREFX );
   for( ; *parse != '\0'; parse++ ) {
      // compare against the reserved prefix, so long as we are early enough in this element
      if ( compat < complen ) {
         if ( *parse == *(reserved + compat) ) {
            compat++; // progress to the next char
            if ( compat == complen ) {
               // prefix match!
               return -1;
            }
         }
         else {
            // any mismatch means we shouldn't continue checking this element
            compat = complen;
         }
      }
      // reset our comparison every time we hit a new path element
      if ( *parse == '/' ) { compat = 0; }
   }
   // we've traversed the entire path, with no prefix found
   return 0;
}


// Context Functions

/**
 * Destroy a given MDAL_CTXT ( such as following a dupctxt call )
 * @param MDAL_CTXT ctxt : MDAL_CTXT to be freed
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_destroyctxt ( MDAL_CTXT ctxt ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   // close the directory ref
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   char errorflag = 0; // just note errors, don't terminate until done
   if ( close( pctxt->refd )  ||  ( (pctxt->pathd >= 0)  &&  close( pctxt->pathd ) ) ) {
      LOG( LOG_ERR, "Failed to close some dir references\n" );
      errorflag = 1;
   }
   free( pctxt );
   if ( errorflag ) {
      return -1;
   }
   return 0;
}

/**
 * Duplicate the given MDAL_CTXT
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to duplicate
 * @return MDAL_CTXT : Reference to the duplicate MDAL_CTXT, or NULL if an error occurred
 */
MDAL_CTXT posixmdal_dupctxt ( const MDAL_CTXT ctxt ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   // create a new ctxt structure
   POSIX_MDAL_CTXT dupctxt = malloc( sizeof(struct posix_mdal_context_struct) );
   if ( !(dupctxt) ) {
      LOG( LOG_ERR, "Failed to allocate space for a new posix MDAL_CTXT\n" );
      return NULL;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // we shouldn't ever make use of dir offsets, so a real dup() call should be sufficient
   dupctxt->refd = dup( pctxt->refd );
   dupctxt->pathd = (pctxt->pathd >= 0) ? dup( pctxt->pathd ) : -1;
   if ( dupctxt->refd < 0  ||  ( dupctxt->pathd < 0  &&  pctxt->pathd >= 0 ) ) {
      LOG( LOG_ERR, "Failed to duplicate FD references for ctxt dirs\n" );
      if ( dupctxt->refd >= 0 ) { close( dupctxt->refd ); }
      if ( dupctxt->pathd >= 0 ) { close( dupctxt->pathd ); }
      free( dupctxt );
      return NULL;
   }
   dupctxt->dev = pctxt->dev;
   return (MDAL_CTXT) dupctxt;
}


// Management Functions

/**
 * Cleanup all structes and state associated with the given posix MDAL
 * @param MDAL mdal : MDAL to be freed
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_cleanup( MDAL mdal ) {
   // check for NULL mdal
   if ( !(mdal) ) {
      LOG( LOG_ERR, "Received a NULL MDAL reference\n" );
      errno = EINVAL;
      return -1;
   }
   // destroy the MDAL_CTXT struct
   int retval = 0;
   if ( posixmdal_destroyctxt( mdal->ctxt ) ) {
      LOG( LOG_ERR, "Failed to destroy the MDAL_CTXT reference\n" );
      retval = -1;
   }
   // free the entire MDAL
   free( mdal );
   return retval;
}

/**
 * Verify security of the given MDAL_CTXT
 * @param const MDAL_CTXT ctxt : MDAL_CTXT for which to verify security
 *                               NOTE -- this ctxt CANNOT be associated with a NS target
 *                                       ( it must be freshly initialized )
 * @param char fix : If non-zero, attempt to correct any problems encountered
 * @return int : A count of uncorrected security issues, or -1 if a failure occurred
 */
int posixmdal_checksec( const MDAL_CTXT ctxt, char fix ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // verify that this CTXT doesn't have a NS target
   if ( pctxt->pathd >= 0 ) {
      LOG( LOG_ERR, "Cannot verify the security of a CTXT after it has been associated with a NS target\n" );
      errno = EINVAL;
      return -1;
   }
   // stat the CTXT root dir
   struct stat pstat;
   if ( fstatat( pctxt->refd, ".", &pstat, 0 ) ) {
      LOG( LOG_ERR, "Failed to stat the root dir of the given MDAL_CTXT\n" );
      return -1;
   }
   // set up our parent string
   size_t stralloc = 1024;
   char* parentstr = calloc( 1, stralloc );
   if ( parentstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for parent dir string\n" );
      return -1;
   }
   size_t pstrlen = 0;
   // iterate up from the CTXT root dir, potentially to the FS root
   char foundsecdir = 0;
   dev_t prevdev = pstat.st_dev;
   ino_t previno = pstat.st_ino;
   uid_t uid = geteuid();
   gid_t gid = getegid();
   while ( !(foundsecdir) ) {
      // if necessary, expand our string allocation
      if ( pstrlen + 4 > stralloc ) {
         char* newparentstr = realloc( parentstr, stralloc + 1024 );
         if ( newparentstr == NULL ) {
            LOG( LOG_ERR, "Failed to extend parent string to an allocation of %zu bytes\n", stralloc + 1024 );
            free( parentstr );
            return -1;
         }
         parentstr = newparentstr;
         stralloc += 1024;
      }
      // extend our string to the next parent reference
      if ( snprintf( parentstr + pstrlen, stralloc - pstrlen, "../" ) != 3 ) {
         LOG( LOG_ERR, "Unexpected length of parent string: \"%s\"\n", parentstr );
         free( parentstr );
         errno = EDOM;
         return -1;
      }
      pstrlen += 3;
      // stat the next parent dir
      if ( fstatat( pctxt->refd, parentstr, &pstat, 0 ) ) {
         LOG( LOG_ERR, "Failed to stat parent dir: \"%s\"\n", parentstr );
         free( parentstr );
         return -1;
      }
      // check if we have hit the FS root
      if ( pstat.st_dev == prevdev  &&  pstat.st_ino == previno ) {
         // abort, if we've gotten this far
         break;
      }
      prevdev = pstat.st_dev;
      previno = pstat.st_ino;
      // check if this dir has appropriate perms
      if ( (pstat.st_mode & (S_IRWXG | S_IRWXO)) == 0  &&  pstat.st_uid == uid ) {
         LOG( LOG_INFO, "Detected that parent dir has appropriate perms: \"%s\"\n", parentstr );
         foundsecdir = 1;
      }
   }
   free( parentstr ); // regardless of result, done with this string
   if ( !(foundsecdir) ) {
      if ( fix ) {
         // attempt to chown/chmod the direct parent to appropriate perms
         LOG( LOG_INFO, "Chowning \"..\" to current UID/GID\n" );
         if ( fchownat( pctxt->refd, "..", uid, gid, 0 ) ) {
            LOG( LOG_ERR, "Failed to chown \"..\" to current UID/GID\n" );
            return 1;
         }
         LOG( LOG_INFO, "Chmoding \"..\" to 0700 mode\n" );
         if ( fchmodat( pctxt->refd, "..", 0700, 0 ) ) {
            LOG( LOG_ERR, "Failed to chmod \"..\" to 0700 mode\n" );
            return 1;
         }
      }
      else {
         // just complain
         return 1;
      }
   }
   return 0;
}


// Namespace Functions

/**
 * Set the namespace of the given MDAL_CTXT
 * @param MDAL_CTXT ctxt : Context to set the namespace of
 * @param const char* ns : Name of the namespace to set
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_setnamespace( MDAL_CTXT ctxt, const char* ns ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // open the new path dir, according to the target NS path
   int newpath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      // absoulute paths are opened via the root dir (skipping the leading '/')
      newpath = openat( pctxt->refd, (nspath + 1), O_RDONLY );
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      // relative paths are opened via the ref dir of the basectxt
      newpath = openat( pctxt->refd, nspath, O_RDONLY );
   }
   if ( newpath < 0 ) {
      LOG( LOG_ERR, "Failed to open the user path dir: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   free( nspath ); // done with this path
   // open the new ref dir, relative to the new path
   int newref = openat( newpath, PMDAL_REF, O_RDONLY );
   if ( newref < 0 ) {
      LOG( LOG_ERR, "Failed to open the ref dir of NS \"%s\"\n", ns );
      close( newpath );
      return -1;
   }
   // stat the reference dir to generate a new dev value
   struct stat stval;
   if ( fstat( newref, &(stval) ) ) {
      LOG( LOG_ERR, "Failed to stat reference dir of NS \"%s\"\n", ns );
      close( newref );
      close( newpath );
      return -1;
   }
   // close the previous path dir, if set
   if ( pctxt->pathd >= 0  &&  close( pctxt->pathd ) ) {
      LOG( LOG_WARNING, "Failed to close the previous path dir handle\n" );
   }
   pctxt->pathd = newpath; // update the context structure
   // close the previous ref dir, regardless
   if ( close( pctxt->refd ) ) {
      LOG( LOG_WARNING, "Failed to close the previous ref dir handle\n" );
   }
   pctxt->refd = newref; // update the context structure
   pctxt->dev = stval.st_dev;
   return 0;
}

/**
 * Create a new MDAL_CTXT reference, targeting the specified NS
 * @param const char* ns : Name of the namespace for the new MDAL_CTXT to target
 * @param const MDAL_CTXT basectxt : The new MDAL_CTXT will be created relative to this one
 * @return MDAL_CTXT : Reference to the new MDAL_CTXT, or NULL if an error occurred
 */
MDAL_CTXT posixmdal_newctxt ( const char* ns, const MDAL_CTXT basectxt ) {
   // check for NULL basectxt
   if ( !(basectxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   // check for NULL ns path
   if ( !(ns) ) {
      LOG( LOG_ERR, "Received a NULL NS path arg\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT pbasectxt = (POSIX_MDAL_CTXT) basectxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return NULL;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return NULL;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return NULL;
   }
   // create a new ctxt structure
   POSIX_MDAL_CTXT newctxt = malloc( sizeof(struct posix_mdal_context_struct) );
   if ( !(newctxt) ) {
      LOG( LOG_ERR, "Failed to allocate space for a new posix MDAL_CTXT\n" );
      free( nspath );
      return NULL;
   }
   // open the path dir, according to the target NS path
   if ( *nspath == '/' ) {
      // ensure the pbasectxt is set to the secureroot dir
      if ( pbasectxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( newctxt );
         free( nspath );
         return NULL;
      }
      // absoulute paths are opened via the root dir (skipping the leading '/')
      newctxt->pathd = openat( pbasectxt->refd, (nspath + 1), O_RDONLY );
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pbasectxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( newctxt );
         free( nspath );
         return NULL;
      }
      // relative paths are opened via the ref dir of the pbasectxt
      newctxt->pathd = openat( pbasectxt->refd, nspath, O_RDONLY );
   }
   if ( newctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Failed to open the user path dir: \"%s\"\n", nspath );
      free( newctxt );
      free( nspath );
      return NULL;
   }
   free( nspath ); // done with this path
   // open the reference dir relative to the new path
   newctxt->refd = openat( newctxt->pathd, PMDAL_REF, O_RDONLY );
   if ( newctxt->refd < 0 ) {
      LOG( LOG_ERR, "Failed to open the reference dir of NS \"%s\"\n", ns );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   struct stat stval;
   if ( fstat( newctxt->refd, &(stval) ) ) {
      LOG( LOG_ERR, "Failed to stat reference dir of NS \"%s\"\n", ns );
      close( newctxt->refd );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   newctxt->dev = stval.st_dev;
   return (MDAL_CTXT) newctxt;
}

/**
 * Create a new MDAL_CTXT reference, targeting one NS for user path operations and a different
 *  NS for reference path creation
 * @param const char* pathns : Namespace for the new MDAL_CTXT to target for user path ops
 * @param const MDAL_CTXT pathctxt : The pathns will be interpreted relative to this CTXT
 * @param const char* refns : Namespace for the new MDAL_CTXT to target for ref path ops
 * @param const MDAL_CTXT refctxt : The refns will be interpreted relative to this CTXT
 * @return MDAL_CTXT : Reference to the new MDAL_CTXT, or NULL if an error occurred
 */
MDAL_CTXT posixmdal_newsplitctxt ( const char* pathns, const MDAL_CTXT pathctxt, const char* refns, const MDAL_CTXT refctxt ) {
   // check for NULL args
   if ( !(pathctxt)  ||  !(refctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   if ( !(pathns)  ||  !(refns) ) {
      LOG( LOG_ERR, "Received a NULL namespace path\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT ppathctxt = (POSIX_MDAL_CTXT) pathctxt;
   POSIX_MDAL_CTXT prefctxt = (POSIX_MDAL_CTXT) refctxt;
   // create the corresponding posix path for the path NS
   size_t nspathlen = namespacepath( pathns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", pathns );
      return NULL;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", pathns );
      return NULL;
   }
   if ( namespacepath( pathns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", pathns );
      free( nspath );
      return NULL;
   }
   // create a new ctxt structure
   POSIX_MDAL_CTXT newctxt = malloc( sizeof(struct posix_mdal_context_struct) );
   if ( !(newctxt) ) {
      LOG( LOG_ERR, "Failed to allocate space for a new posix MDAL_CTXT\n" );
      free( nspath );
      return NULL;
   }
   // open the path dir, according to the target NS path
   if ( *nspath == '/' ) {
      // ensure the ppathctxt is set to the secureroot dir
      if ( ppathctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( newctxt );
         free( nspath );
         return NULL;
      }
      // absoulute paths are opened via the root dir (skipping the leading '/')
      newctxt->pathd = openat( ppathctxt->refd, (nspath + 1), O_RDONLY );
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( ppathctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( newctxt );
         free( nspath );
         return NULL;
      }
      // relative paths are opened via the ref dir of the pbasectxt
      newctxt->pathd = openat( ppathctxt->refd, nspath, O_RDONLY );
   }
   if ( newctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Failed to open the user path dir: \"%s\"\n", nspath );
      free( newctxt );
      free( nspath );
      return NULL;
   }

   // stat the opened path dir, for later comparison
   struct stat pathstat;
   if ( fstat( newctxt->pathd, &(pathstat) ) ) {
      LOG( LOG_ERR, "Failed to stat opened path dir: \"%s\"\n", nspath );
      close( newctxt->pathd );
      free( newctxt );
      free( nspath );
      return NULL;
   }
   free( nspath ); // done with this path

   // create the corresponding posix path for the ref NS
   nspathlen = namespacepath( refns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", refns );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   nspath = malloc( sizeof(char) * (nspathlen + 2 + strlen(PMDAL_REF)) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", refns );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   if ( namespacepath( refns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", refns );
      free( nspath );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   if ( sprintf( nspath + nspathlen, "/%s", PMDAL_REF ) != strlen(PMDAL_REF) + 1 ) {
      LOG( LOG_ERR, "Failed to append PMDAL_REF suffix to path of NS: \"%s\"\n", refns );
      free( nspath );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   // open the ref dir, according to the target NS path
   if ( *nspath == '/' ) {
      // ensure the prefctxt is set to the secureroot dir
      if ( prefctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         close( newctxt->pathd );
         free( newctxt );
         return NULL;
      }
      // absoulute paths are opened via the root dir (skipping the leading '/')
      newctxt->refd = openat( prefctxt->refd, (nspath + 1), O_RDONLY );
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( prefctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         close( newctxt->pathd );
         free( newctxt );
         return NULL;
      }
      // relative paths are opened via the ref dir of the pbasectxt
      newctxt->refd = openat( prefctxt->refd, nspath, O_RDONLY );
   }
   if ( newctxt->refd < 0 ) {
      LOG( LOG_ERR, "Failed to open the ref dir: \"%s\"\n", nspath );
      free( nspath );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   free( nspath ); // done with this path

   // stat the opened reference dir
   struct stat stval;
   if ( fstat( newctxt->refd, &(stval) ) ) {
      LOG( LOG_ERR, "Failed to stat reference dir of NS \"%s\"\n", refns );
      close( newctxt->refd );
      close( newctxt->pathd );
      free( newctxt );
      return NULL;
   }
   // verify that these dirs exist on the same device
   if ( stval.st_dev != pathstat.st_dev ) {
      LOG( LOG_ERR, "Cross-Device CTXT split detected\n" );
      close( newctxt->refd );
      close( newctxt->pathd );
      free( newctxt );
      errno = EXDEV;
      return NULL;
   }
   newctxt->dev = stval.st_dev;
   return (MDAL_CTXT) newctxt;
}

/**
 * Create the specified namespace root structures ( reference tree is not created by this func! )
 * @param MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace to be created
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_createnamespace( MDAL_CTXT ctxt, const char* ns ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 2 + PMDAL_SUBSTRLEN) ); // leave room for ref or path suffix
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // identify the target path and abort if the CTXT isn't in an appropriate state
   char* nstruepath = nspath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      nstruepath = nspath + 1; // need to skip the initial '/' char
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
   }
   // attempt to create the target directory
   int mkdirres = 0;
   char* nsparse = nstruepath;
   errno = 0;
   while ( mkdirres == 0  &&  nsparse != NULL ) {
      // iterate ahead in the stream, tokenizing into intermediate path components
      while ( 1 ) {
         if ( *nsparse == '/' ) { *nsparse = '\0'; break; } // cut string to next dir comp
         if ( *nsparse == '\0' ) { nsparse = NULL; break; } // end of str, prepare to exit
         nsparse++;
      }
      // isssue the mkdir op
      LOG( LOG_INFO, "Attempting to create dir: \"%s\"\n", nstruepath );
      if ( nsparse ) {
         // create all intermediate dirs with global access
         mkdirres = mkdirat( pctxt->refd, nstruepath, S_IRWXU | S_IXOTH );
      }
      else {
         // create the final dir with user-only access
         mkdirres = mkdirat( pctxt->refd, nstruepath, S_IRWXU );
      }
      // ignore any EEXIST errors, at this point
      if ( mkdirres  &&  errno == EEXIST ) { mkdirres = 0; errno = 0; }
      // if we cut the string short, we need to undo that and progress to the next str comp
      if ( nsparse ) { *nsparse = '/'; nsparse++; }
   }
   if ( mkdirres ) { // check for error conditions ( except EEXIST )
      LOG( LOG_ERR, "Failed to create path to NS root: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   // construct the path of the reference subdir
   if ( snprintf( nspath + nspathlen, 2 + strlen(PMDAL_REF), "/%s", PMDAL_REF ) >= 
         2 + strlen(PMDAL_REF) ) {
      LOG( LOG_ERR, "Failed to properly generate the path of the ref subdir of NS:\"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // attempt to create the ref subdir
   LOG( LOG_INFO, "Attempting to create ref dir: \"%s\"\n", nstruepath );
   if ( mkdirat( pctxt->refd, nstruepath, S_IRWXU | S_IXOTH | S_IROTH ) ) {
      // here, we actually want to report EEXIST
      LOG( LOG_ERR, "Failed to create NS ref path: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   free( nspath );
   return 0;
}

/**
 * Destroy the specified namespace root structures
 * NOTE -- This operation will fail with errno=ENOTEMPTY if files/dirs persist in the 
 *         namespace or if inode / data usage values are non-zero for the namespace.
 *         This includes files/dirs within the reference tree.
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace to be deleted
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_destroynamespace ( const MDAL_CTXT ctxt, const char* ns ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 2 + PMDAL_SUBSTRLEN) ); // leave room for ref suffix
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // append the subpath dir name
   if ( snprintf( nspath + nspathlen, 2 + strlen(PMDAL_SUBSP), "/%s", PMDAL_SUBSP ) != 
         1 + strlen(PMDAL_SUBSP) ) {
      LOG( LOG_ERR, "Failed to properly generate the location of the subspace subdir of NS:\"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // attempt to unlink the subspace subdir
   int unlinkres;
   errno = 0;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      unlinkres = unlinkat( pctxt->refd, nspath + 1, AT_REMOVEDIR );
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      unlinkres = unlinkat( pctxt->refd, nspath, AT_REMOVEDIR );
   }
   if ( unlinkres  &&  errno != ENOENT ) { // ignore error from non-existent subspace dir
      LOG( LOG_ERR, "Failed to unlink NS subspace path: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   // construct the path of the reference subdir
   if ( snprintf( nspath + nspathlen, 2 + strlen(PMDAL_REF), "/%s", PMDAL_REF ) != 
         1 + strlen(PMDAL_REF) ) {
      LOG( LOG_ERR, "Failed to properly generate the path of the ref subdir of NS:\"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // attempt to unlink the ref subdir
   if ( *nspath == '/' ) {
      // no need to double check state of pathd
      unlinkres = unlinkat( pctxt->refd, nspath + 1, AT_REMOVEDIR );
   }
   else {
      unlinkres = unlinkat( pctxt->refd, nspath, AT_REMOVEDIR );
   }
   if ( unlinkres ) {
      LOG( LOG_ERR, "Failed to unlink NS ref subdir: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   // attempt to unlink the NS root dir
   *(nspath + nspathlen) = '\0'; // use NULL-term to truncate off the ref path
   if ( *nspath == '/' ) {
      // no need to double check state of pathd
      unlinkres = unlinkat( pctxt->refd, nspath + 1, AT_REMOVEDIR );
   }
   else {
      unlinkres = unlinkat( pctxt->refd, nspath, AT_REMOVEDIR );
   }
   if ( unlinkres ) {
      LOG( LOG_ERR, "Failed to unlink NS root path: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   free( nspath );
   return 0;
}

/**
 * Open a directory handle for the specified NS
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace target
 * @return MDAL_DHANDLE : Open directory handle, or NULL if a failure occurred
 */
MDAL_DHANDLE posixmdal_opendirnamespace( const MDAL_CTXT ctxt, const char* ns ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return NULL;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return NULL;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return NULL;
   }
   // identify the target path and abort it the CTXT isn't in an appropriate state
   char* nstruepath = nspath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return NULL;
      }
      nstruepath = nspath + 1; // need to skip the initial '/' char
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return NULL;
      }
   }
   // open the target
   int dfd = openat( pctxt->refd, nstruepath, O_RDONLY | O_DIRECTORY );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to open the target path: \"%s\"\n", nstruepath );
      free( nspath );
      return NULL;
   }
   free( nspath );
   // allocate a dir handle reference
   POSIX_DHANDLE dhandle = malloc( sizeof(struct posixmdal_directory_handle_struct) );
   if ( dhandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new directory handle\n" );
      close( dfd );
      return NULL;
   }
   // translate the FD to a DIR stream
   dhandle->dirp = fdopendir( dfd );
   if ( dhandle->dirp == NULL ) {
      LOG( LOG_ERR, "Failed to create directory stream from FD\n" );
      free( dhandle );
      close( dfd );
      return NULL;
   }
   return (MDAL_DHANDLE) dhandle;
}

/**
 * Check access to the specified NS
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace target
 * @param int mode : F_OK - check for file existence
 *                      or a bitwise OR of the following...
 *                   R_OK - check for read access
 *                   W_OK - check for write access
 *                   X_OK - check for execute access
 * @param int flags : A bitwise OR of the following...
 *                    AT_EACCESS - Perform access checks using effective uid/gid
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_accessnamespace (const MDAL_CTXT ctxt, const char* ns, int mode, int flags) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // identify the target path and abort it the CTXT isn't in an appropriate state
   char* nstruepath = nspath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      nstruepath = nspath + 1; // need to skip the initial '/' char
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
   }
   // perform the access() op against the namespace path ( ignoring SYMLINK_NOFOLLOW )
   int retval = faccessat( pctxt->refd, nstruepath, mode, flags & ~(AT_SYMLINK_NOFOLLOW) );
   free( nspath );
   return retval;
}

/**
 * Stat the specified NS
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace target
 * @param struct stat* st : Stat structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_statnamespace (const MDAL_CTXT ctxt, const char* ns, struct stat *buf) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // identify the target path and abort it the CTXT isn't in an appropriate state
   char* nstruepath = nspath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      nstruepath = nspath + 1; // need to skip the initial '/' char
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
   }
   // perform the stat() op against the namespace path ( always follow symlinks )
   int retval = fstatat( pctxt->refd, nstruepath, buf, 0 );
   // adjust stat link vals to ignore MDAL subdirs
   if ( retval == 0 ) {
      buf->st_nlink -= 2; // subspaces and references
   }
   else {
      LOG( LOG_ERR, "Failed to stat \"%s\" (%s)\n", nstruepath, strerror(errno) );
   }
   free( nspath );
   return retval;
}

/**
 * Edit the mode of the specified NS
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace target
 * @param mode_t mode : New mode value for the NS (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_chmodnamespace (const MDAL_CTXT ctxt, const char* ns, mode_t mode) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // identify the target path and abort it the CTXT isn't in an appropriate state
   char* nstruepath = nspath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      nstruepath = nspath + 1; // need to skip the initial '/' char
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
   }
   // perform the chmod() op against the namespace path ( always follow symlinks )
   int retval = fchmodat( pctxt->refd, nstruepath, mode, 0 );
   free( nspath );
   return retval;
}

/**
 * Edit the ownership and group of the specified NS
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace target
 * @param uid_t owner : New owner
 * @param gid_t group : New group
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_chownnamespace (const MDAL_CTXT ctxt, const char* ns, uid_t uid, gid_t gid) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // create the corresponding posix path for the target NS
   size_t nspathlen = namespacepath( ns, NULL, 0 );
   if ( nspathlen == 0 ) {
      LOG( LOG_ERR, "Failed to identify corresponding path for NS: \"%s\"\n", ns );
      return -1;
   }
   char* nspath = malloc( sizeof(char) * (nspathlen + 1) );
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen + 1 ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // identify the target path and abort it the CTXT isn't in an appropriate state
   char* nstruepath = nspath;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      nstruepath = nspath + 1; // need to skip the initial '/' char
   }
   else {
      // ensure the refd is set to an actual reference dir
      if ( pctxt->pathd < 0 ) {
         LOG( LOG_ERR, "Relative NS paths can only be used from a CTXT with a NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
   }
   // perform the chown() op against the namespace path ( always follow symlinks )
   int retval = fchownat( pctxt->refd, nstruepath, uid, gid, 0 );
   free( nspath );
   return retval;
}


// Usage Functions

/**
 * Set data usage value for the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param off_t bytes : Number of bytes used by the namespace
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_setdatausage( MDAL_CTXT ctxt, off_t bytes ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // allocate a path for the DUSE file
   char* dusepath = malloc( sizeof(char) * (strlen(PMDAL_DUSE) + 4) );
   if ( !(dusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the data usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( dusepath, (strlen(PMDAL_DUSE) + 4), "../%s", PMDAL_DUSE ) != strlen(PMDAL_DUSE) + 3 ) {
      LOG( LOG_ERR, "Failed to populate the usage file path\n" );
      free( dusepath );
      return -1;
   }
   // if we're setting to zero usage, just unlink the usage file
   if ( !(bytes) ) {
      // unlink, ignoring ENOENT errors
      errno = 0;
      if ( unlinkat( pctxt->refd, dusepath, 0 )  &&  errno != ENOENT ) {
         LOG( LOG_ERR, "Failed to unlink data useage file\n" );
         free( dusepath );
         return -1;
      }
      // cleanup and return
      free( dusepath );
      return 0;
   }
   // open a file handle for the DUSE path ( create with all perms open, if missing )
   int dusefd = openat( pctxt->refd, dusepath, O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO );
   free( dusepath ); // done with the path
   if ( dusefd < 0 ) {
      LOG( LOG_ERR, "Failed to open the data use file\n" );
      return -1;
   }
   // truncate the DUSE file to the provided length
   if ( ftruncate( dusefd, bytes ) ) {
      LOG( LOG_ERR, "Failed to truncate the data use file to the specified length of %zd\n", bytes );
      close( dusefd );
      return -1;
   }
   // close our file handle
   if ( close( dusefd ) ) {
      LOG( LOG_WARNING, "Failed to properly close the data use file handle\n" );
   }
   return 0;
}

/**
 * Retrieve the data usage value of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @return off_t : Number of bytes used by the namespace
 */
off_t posixmdal_getdatausage( MDAL_CTXT ctxt ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // allocate a path for the DUSE file
   char* dusepath = malloc( sizeof(char) * (strlen(PMDAL_DUSE) + 4) );
   if ( !(dusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the data usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( dusepath, (strlen(PMDAL_DUSE) + 4), "../%s", PMDAL_DUSE ) != strlen(PMDAL_DUSE) + 3 ) {
      LOG( LOG_ERR, "Failed to populate the usage file path\n" );
      free( dusepath );
      return -1;
   }
   // stat the DUSE file
   errno = 0;
   struct stat dstat;
   if ( fstatat( pctxt->refd, dusepath, &(dstat), 0 ) ) {
      free( dusepath ); // cleanup
      // if no file exists, assume zero usage
      if ( errno == ENOENT ) { errno = 0; return 0; }
      LOG( LOG_ERR, "Failed to stat the data use file\n" );
      return -1;
   }
   free( dusepath ); // done with the path
   return dstat.st_size;
}

/**
 * Set the inode usage value of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param off_t files : Number of inodes used by the namespace
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_setinodeusage( MDAL_CTXT ctxt, off_t files ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // allocate a path for the IUSE file
   char* iusepath = malloc( sizeof(char) * (strlen(PMDAL_IUSE) + 4) );
   if ( !(iusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the inode usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( iusepath, (strlen(PMDAL_IUSE) + 4), "../%s", PMDAL_IUSE ) != strlen(PMDAL_IUSE) + 3 ) {
      LOG( LOG_ERR, "Failed to populate the inode usage file path\n" );
      free( iusepath );
      return -1;
   }
   // if we're setting to zero usage, just unlink the usage file
   if ( !(files) ) {
      // unlink, ignoring ENOENT errors
      errno = 0;
      if ( unlinkat( pctxt->refd, iusepath, 0 )  &&  errno != ENOENT ) {
         LOG( LOG_ERR, "Failed to unlink inode useage file\n" );
         free( iusepath );
         return -1;
      }
      // cleanup and return
      free( iusepath );
      return 0;
   }
   // open a file handle for the IUSE path ( create with all perms open, if missing )
   int iusefd = openat( pctxt->refd, iusepath, O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO );
   free( iusepath ); // done with the path
   if ( iusefd < 0 ) {
      LOG( LOG_ERR, "Failed to open the inode use file\n" );
      return -1;
   }
   // truncate the IUSE file to the provided length
   if ( ftruncate( iusefd, files ) ) {
      LOG( LOG_ERR, "Failed to truncate the inode use file to the specified length of %zd\n", files );
      close( iusefd );
      return -1;
   }
   // close our file handle
   if ( close( iusefd ) ) {
      LOG( LOG_WARNING, "Failed to properly close the inode use file handle\n" );
   }
   return 0;
}

/**
 * Retrieve the inode usage value of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @return off_t : Number of inodes used by the current namespace
 */
off_t posixmdal_getinodeusage( MDAL_CTXT ctxt ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // allocate a path for the IUSE file
   char* iusepath = malloc( sizeof(char) * (strlen(PMDAL_IUSE) + 4) );
   if ( !(iusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the inode usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( iusepath, (strlen(PMDAL_IUSE) + 4), "../%s", PMDAL_IUSE ) != strlen(PMDAL_IUSE) + 3 ) {
      LOG( LOG_ERR, "Failed to populate the usage file path\n" );
      free( iusepath );
      return -1;
   }
   // stat the IUSE file
   struct stat istat;
   if ( fstatat( pctxt->refd, iusepath, &(istat), 0 ) ) {
      free( iusepath ); // cleanup
      // if no file exists, assume zero usage
      if ( errno == ENOENT ) { errno = 0; return 0; }
      LOG( LOG_ERR, "Failed to stat the inode use file\n" );
      return -1;
   }
   free( iusepath ); // done with the path
   return istat.st_size;
}


// Reference Path Functions

/**
 * Create the specified reference directory
 * @param const MDAL_CTXT ctxt : Current MDAL_CTXT, associated with a target namespace
 * @param const char* refdir : Path of the reference dir to be created
 * @param mode_t mode : Mode value of the new directory (see inode man page)
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_createrefdir ( const MDAL_CTXT ctxt, const char* refdir, mode_t mode ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue a mkdir
   if ( mkdirat( pctxt->refd, refdir, mode ) ) {
      LOG( LOG_ERR, "Failed to mkdir target path: \"%s\"\n", refdir );
      return -1;
   }
   return 0;
}

/**
 * Destroy the specified reference directory
 * NOTE -- This operation will fail with errno=ENOTEMPTY if files/dirs persist in the dir.
 * @param const MDAL_CTXT ctxt : Current MDAL_CTXT, associated with a target namespace
 * @param const char* refdir : Path of the reference dir to be destroyed
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_destroyrefdir ( const MDAL_CTXT ctxt, const char* refdir ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue a mkdir
   if ( unlinkat( pctxt->refd, refdir, AT_REMOVEDIR ) ) {
      LOG( LOG_ERR, "Failed to unlink target path: \"%s\"\n", refdir );
      return -1;
   }
   return 0;
}

/**
 * Hardlink the specified reference path to the specified user-visible path
 * @param const MDAL_CTXT ctxt : Current MDAL_CTXT, associated with a target namespace
 * @param char interref : If zero, 'newpath' is interpreted as a user-visible path
 *                        If non-zero, 'newpath' is interpreted as another reference path
 * @param const char* oldrpath : Reference path of the existing file target
 * @param const char* newpath : Path at which to create the hardlink
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_linkref ( const MDAL_CTXT ctxt, char interref, const char* oldrpath, const char* newpath ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // identify target type
   int tgtdir = pctxt->pathd;
   if ( interref ) {
      tgtdir = pctxt->refd;
   }
   // attempt hardlink creation
   if ( linkat( pctxt->refd, oldrpath, tgtdir, newpath, 0 ) ) {
      LOG( LOG_ERR, "Failed to link rpath \"%s\" to %s \"%s\"\n", oldrpath, (interref) ? "rpath" : "NS path", newpath );
      return -1;
   }
   return 0;
}

/**
 * Rename the specified reference path to a new reference path
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* from : String path of the reference
 * @param const char* to : Destination string reference path
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_renameref ( const MDAL_CTXT ctxt, const char* from, const char* to ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved an MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the rename op
   return renameat( pctxt->refd, from, pctxt->refd, to );
}

/**
 * Unlink the specified file reference path
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* rpath : String reference path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_unlinkref ( const MDAL_CTXT ctxt, const char* rpath ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue an unlink
   if ( unlinkat( pctxt->refd, rpath, 0 ) ) {
      LOG( LOG_ERR, "Failed to unlink target path: \"%s\"\n", rpath );
      return -1;
   }
   return 0;
}

/**
 * Stat the target reference path
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* rpath : String reference path of the target
 * @param struct stat* buf : Stat buffer to be populated
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_statref ( const MDAL_CTXT ctxt, const char* rpath, struct stat* buf ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the stat
   if ( fstatat( pctxt->refd, rpath, buf, 0 ) ) {
      LOG( LOG_ERR, "Failed to stat the target path: \"%s\"\n", rpath );
      return -1;
   }
   return 0;
}

/**
 * Open the specified reference path
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* rpath : String reference path of the target file
 * @param int flags : Flags specifying behavior (see the 'open()' syscall 'flags' value for full info)
 * @param mode_t mode : Mode value for file creation (see the 'open()' syscall 'mode' value for full info)
 * @return MDAL_FHANDLE : An MDAL_READ handle for the target file, or NULL if a failure occurred
 */
MDAL_FHANDLE posixmdal_openref ( const MDAL_CTXT ctxt, const char* rpath, int flags, mode_t mode ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return NULL;
   }
   // issue the open
   int fd = openat( pctxt->refd, rpath, flags, mode );
   if ( fd < 0 ) {
      LOG( LOG_ERR, "Failed to open reference path: \"%s\"\n", rpath );
      return NULL;
   }
   // allocate a new FHANDLE ref
   POSIX_FHANDLE fhandle = malloc( sizeof(struct posixmdal_file_handle_struct) );
   if ( fhandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new FHANDLE struct\n" );
      close( fd );
      return NULL;
   }
   fhandle->fd = fd;
   return (MDAL_FHANDLE) fhandle;
}


// Scanner Functions

/**
 * Open a reference scanner for the given location of the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param const char* rpath : Target reference dir location
 * @return MDAL_SCANNER : Newly opened reference scanner
 */
MDAL_SCANNER posixmdal_openscanner( MDAL_CTXT ctxt, const char* rpath ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return NULL;
   }
   // open the target
   int dfd = openat( pctxt->refd, rpath, O_RDONLY );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to open the target path: \"%s\"\n", rpath );
      return NULL;
   }
   // verify the target is a dir
   struct stat dirstat;
   if ( fstat( dfd, &(dirstat) )  ||  !(S_ISDIR(dirstat.st_mode)) ) {
      LOG( LOG_ERR, "Could not verify target is a directory: \"%s\"\n", rpath );
      close( dfd );
      return NULL;
   }
   // allocate a scanner reference
   POSIX_SCANNER scanner = malloc( sizeof(struct posixmdal_scanner_struct) );
   if ( scanner == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new scanner\n" );
      close( dfd );
      return NULL;
   }
   // translate the FD to a DIR stream
   scanner->dirp = fdopendir( dfd );
   if ( scanner->dirp == NULL ) {
      LOG( LOG_ERR, "Failed to create directory stream from FD\n" );
      close( dfd );
      return NULL;
   }
   return (MDAL_SCANNER) scanner;
}

/**
 * Close a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to be closed
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_closescanner ( MDAL_SCANNER scanner ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_SCANNER pscan = (POSIX_SCANNER) scanner;
   DIR* dirp = pscan->dirp;
   free( pscan );
   return closedir( dirp );
}

/**
 * Iterate to the next entry of a reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to retrieve an entry from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
 *                          entries have been read, or NULL w/ errno set if a failure occurred
 */
struct dirent* posixmdal_scan( MDAL_SCANNER scanner ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_SCANNER pscan = (POSIX_SCANNER) scanner;
   return readdir( pscan->dirp );
}

/**
 * Open a file, relative to a given reference scanner
 * NOTE -- this is implicitly an open for READ
 * @param MDAL_SCANNER scanner : Reference scanner to open relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @return MDAL_FHANDLE : An MDAL_READ handle for the target file, or NULL if a failure occurred
 */
MDAL_FHANDLE posixmdal_sopen( MDAL_SCANNER scanner, const char* path ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_SCANNER pscan = (POSIX_SCANNER) scanner;
   // retrieve the dir FD associated with the current dir stream
   int dfd = dirfd( pscan->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD for the current directory stream\n" );
      return NULL;
   }
   // issue the open, relative to the scanner FD
   int fd = openat( dfd, path, O_RDONLY );
   if ( fd < 0 ) {
      LOG( LOG_ERR, "Failed to open relative scanner path: \"%s\"\n", path );
      return NULL;
   }
   // allocate a new FHANDLE ref
   POSIX_FHANDLE fhandle = malloc( sizeof(struct posixmdal_file_handle_struct) );
   if ( fhandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new FHANDLE struct\n" );
      close( fd );
      return NULL;
   }
   fhandle->fd = fd;
   return (MDAL_FHANDLE) fhandle;
}

/**
 * Unlink a file, relative to a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to unlink relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_sunlink( MDAL_SCANNER scanner, const char* path ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_SCANNER pscan = (POSIX_SCANNER) scanner;
   // retrieve the dir FD associated with the current dir stream
   int dfd = dirfd( pscan->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD for the current directory stream\n" );
      return -1;
   }
   // issue the open, relative to the scanner FD
   if ( unlinkat( dfd, path, 0 ) < 0 ) {
      LOG( LOG_ERR, "Failed to unlink relative scanner path: \"%s\"\n", path );
      return -1;
   }
   return 0;
}

/**
 * Stat the target, relative to a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to operate relative to
 * @param const char* spath : String reference path of the target
 * @param struct stat* buf : Stat buffer to be populated
 * @return int : Zero on success, -1 if a failure occurred
 */
int posixmdal_sstat ( MDAL_SCANNER scanner, const char* spath, struct stat* buf ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_SCANNER pscan = (POSIX_SCANNER) scanner;
   // retrieve the dir FD associated with the current dir stream
   int dfd = dirfd( pscan->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD for the current directory stream\n" );
      return -1;
   }
   // issue the stat
   if ( fstatat( dfd, spath, buf, 0 ) ) {
      LOG( LOG_ERR, "Failed to stat the target path: \"%s\"\n", spath );
      return -1;
   }
   return 0;
}


// DHANDLE Functions

/**
 * Open a directory, relative to the given MDAL_CTXT
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : Relative path of the target directory from the ctxt
 * @return MDAL_DHANDLE : Open directory handle, or NULL if a failure occurred
 */
MDAL_DHANDLE posixmdal_opendir( MDAL_CTXT ctxt, const char* path ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return NULL;
   }
   // open the target
   int dfd = openat( pctxt->pathd, path, O_RDONLY | O_DIRECTORY );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to open the target path: \"%s\"\n", path );
      return NULL;
   }
   // allocate a dir handle reference
   POSIX_DHANDLE dhandle = malloc( sizeof(struct posixmdal_directory_handle_struct) );
   if ( dhandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new directory handle\n" );
      close( dfd );
      return NULL;
   }
   // translate the FD to a DIR stream
   dhandle->dirp = fdopendir( dfd );
   if ( dhandle->dirp == NULL ) {
      LOG( LOG_ERR, "Failed to create directory stream from FD\n" );
      free( dhandle );
      close( dfd );
      return NULL;
   }
   return (MDAL_DHANDLE) dhandle;
}


/**
 * Edit the given MDAL_CTXT to reference the given MDAL_DHANDLE for all path operations
 * NOTE -- this operation will destroy the provided MDAL_DHANDLE
 * @param MDAL_CTXT ctxt : MDAL_CTXT to update
 * @param MDAL_DHANDLE dh : Directory handle to be used by the MDAL_CTXT
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_chdir( MDAL_CTXT ctxt, MDAL_DHANDLE dh ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_DHANDLE reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   // retrieve the dir FD from the dir handle
   int dfd = dirfd( pdh->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD from the provided directory handle\n" );
      return -1;
   }
   int newdfd = dup( dfd );
   if ( newdfd < 0 ) {
      LOG( LOG_ERR, "Failed to duplicate the FD of the provided directory handle\n" );
      return -1;
   }
   // close the provided dir handle
   if ( closedir( pdh->dirp ) ) {
      LOG( LOG_WARNING, "Failed to close the provided directory handle\n" );
   }
   // close the previous path dir
   if ( close( pctxt->pathd ) ) {
      LOG( LOG_WARNING, "Failed to close the previous path directory FD\n" );
   }
   // free the provided dir handle
   free( dh );
   // update the context struct
   pctxt->pathd = newdfd;
   return 0;
}


/**
 * Set the specified xattr on the dir referenced by the given directory handle
 * @param MDAL_DHANDLE dh : Directory handle for which to set the xattr
 * @param char hidden : A non-zero value indicates to store this as a 'hidden' MDAL value
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_dsetxattr( MDAL_DHANDLE dh, char hidden, const char* name, const void* value, size_t size, int flags ) {
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL dir handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   // retrieve the dir FD from the dir handle
   int dfd = dirfd( pdh->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD from the provided directory handle\n" );
      return -1;
   }
   // the non-hidden op is more straightforward
   if ( !(hidden) ) {
      // filter out any reserved name
      if ( xattrfilter( name, 0 ) ) {
         LOG( LOG_ERR, "Xattr has a reserved name string: \"%s\"\n", name );
         errno = EPERM;
         return -1;
      }
      return fsetxattr( dfd, name, value, size, flags );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != (strlen(PMDAL_XATTR) + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   int retval = fsetxattr( dfd, newname, value, size, flags );
   if ( retval ) {
      LOG( LOG_ERR, "fsetxattr failure for \"%s\" value (%s)\n", newname, strerror(errno) );
   }
   free( newname ); // cleanup
   return retval;
}


/**
 * Retrieve the specified xattr from the dir referenced by the given directory handle
 * @param MDAL_DHANDLE dh : Directory handle for which to retrieve the xattr
 * @param char hidden : A non-zero value indicates to retrieve a 'hidden' MDAL value
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t posixmdal_dgetxattr( MDAL_DHANDLE dh, char hidden, const char* name, void* value, size_t size ) {
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL dir handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   // retrieve the dir FD from the dir handle
   int dfd = dirfd( pdh->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD from the provided directory handle\n" );
      return -1;
   }
   // the non-hidden op is more straightforward
   if ( !(hidden) ) {
      // filter out any reserved name
      if ( xattrfilter( name, 0 ) ) {
         LOG( LOG_ERR, "Xattr has a reserved name string: \"%s\"\n", name );
         errno = EPERM;
         return -1;
      }
      return fgetxattr( dfd, name, value, size );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != (strlen(PMDAL_XATTR) + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   ssize_t retval = fgetxattr( dfd, newname, value, size );
   free( newname ); // cleanup
   return retval;
}


/**
 * Remove the specified xattr from the dir referenced by the given directory handle
 * @param MDAL_DHANDLE dh : Directory handle for which to remove the xattr
 * @param char hidden : A non-zero value indicates to remove a 'hidden' MDAL value
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_dremovexattr( MDAL_DHANDLE dh, char hidden, const char* name ) {
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL dir handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   // retrieve the dir FD from the dir handle
   int dfd = dirfd( pdh->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD from the provided directory handle\n" );
      return -1;
   }
   // the non-hidden op is more straightforward
   if ( !(hidden) ) {
      // filter out any reserved name
      if ( xattrfilter( name, 0 ) ) {
         LOG( LOG_ERR, "Xattr has a reserved name string: \"%s\"\n", name );
         errno = EPERM;
         return -1;
      }
      return fremovexattr( dfd, name );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != (strlen(PMDAL_XATTR) + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   int retval = fremovexattr( dfd, newname );
   free( newname ); // cleanup
   return retval;
}


/**
 * List all xattr names from the dir referenced by the given directory handle
 * @param MDAL_DHANDLE dh : Directory handle for which to list xattrs
 * @param char hidden : A non-zero value indicates to list 'hidden' MDAL xattrs
 *                      ( normal xattrs excluded )
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t posixmdal_dlistxattr( MDAL_DHANDLE dh, char hidden, char* buf, size_t size ) {
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL dir handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   // retrieve the dir FD from the dir handle
   int dfd = dirfd( pdh->dirp );
   if ( dfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD from the provided directory handle\n" );
      return -1;
   }
   // perform the op, but capture the result
   ssize_t res = flistxattr( dfd, buf, size );
   if ( size == 0 ) {
      // special, no-output case, which can be immediately returned
      LOG( LOG_INFO, "Immediately returning result of size == 0 call\n" );
      return res;
   }
   else if ( res < 0 ) {
      // error case, which can be immediately returned
      bzero( buf, size ); // but zero out the user buf, just in case
      LOG( LOG_INFO, "Returning error res of call, with zeroed buffer\n" );
      return res;
   }
   // parse over the xattr list, limiting it to either 'hidden' or non-'hidden' values
   char* parse = buf;
   char* output = buf;
   while ( (parse - buf) < res ) {
      size_t elemlen = strlen(parse);
      if ( xattrfilter( parse, hidden ) == 0 ) {
         if ( hidden ) {
            // we need to omit the reserved xattr prefix
            size_t prefixlen = strlen( PMDAL_XATTR );
            elemlen -= prefixlen;
            parse += prefixlen;
         }
         // check if we need to shift this element to a previous position
         if ( output != parse ) {
            snprintf( output, elemlen + 1, "%s", parse );
         }
         LOG( LOG_INFO, "Preserving value: \"%s\"\n", output );
         output += elemlen + 1; // skip over the output string and NULL term
      }
      else {
         LOG( LOG_INFO, "Omitting xattr: \"%s\"\n", parse );
      }
      parse += elemlen + 1; // skip over the current element and NULL term
   }
   // explicitly zero out the remainder of the xattr list
   ssize_t listlen = (ssize_t)(output - buf);
   bzero( output, res - listlen );
   // return the length of the modified list
   LOG( LOG_INFO, "Result list is %zd characters long\n", listlen );
   return listlen;
}


/**
 * Iterate to the next entry of an open directory handle
 * @param MDAL_DHANDLE dh : MDAL_DHANDLE to read from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
 *                          entries have been read, or NULL w/ errno set if a failure occurred
 */
struct dirent* posixmdal_readdir( MDAL_DHANDLE dh ) {
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_DHANDLE reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   return readdir( pdh->dirp );
}


/**
 * Close the given directory handle
 * @param MDAL_DHANDLE dh : MDAL_DHANDLE to close
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_closedir( MDAL_DHANDLE dh ) {
   // check for a NULL dir handle
   if ( !(dh) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_DHANDLE reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_DHANDLE pdh = (POSIX_DHANDLE) dh;
   DIR* dirstream = pdh->dirp;
   free( dh );
   return closedir( dirstream );
}


// FHANDLE Functions

/**
 * Open a file, relative to the given MDAL_CTXT
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : Relative path to the target file
 * @param int flags : Flags specifying behavior (see the 'open()' syscall 'flags' value for full info)
 *                    Note -- This function CANNOT create new files ( O_CREAT is forbidden )
 * @return MDAL_FHANDLE : The newly opened MDAL_FHANDLE, or NULL if a failure occurred
 */
MDAL_FHANDLE posixmdal_open( MDAL_CTXT ctxt, const char* path, int flags ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return NULL;
   }
   // check for disallowed flag values
   if ( flags & O_CREAT ) {
      LOG( LOG_ERR, "Recieved disallowed O_CREAT flag\n" );
      errno = EINVAL;
      return NULL;
   }
   // issue the open
   int fd = openat( pctxt->pathd, path, flags );
   if ( fd < 0 ) {
      LOG( LOG_ERR, "Failed to open target path: \"%s\"\n", path );
      return NULL;
   }
   // ensure we're not opening a dir ( which requires opendir ) OR exiting the expected device ( potential security issue )
   struct stat fdstat;
   if ( fstat( fd, &(fdstat) ) ) {
      LOG( LOG_ERR, "Could not verify target is a file: \"%s\" (%s)\n", path, strerror(errno) );
      close( fd );
      return NULL;
   }
   else if ( fdstat.st_dev != pctxt->dev ) {
      LOG( LOG_ERR, "Target resides on a different device ( CTXT=%zd , TGT=%zd )\n", pctxt->dev, fdstat.st_dev );
      close( fd );
      errno = EXDEV;
      return NULL;
   }
   else if ( S_ISDIR(fdstat.st_mode) ) {
      LOG( LOG_ERR, "Target is a directory: \"%s\"\n", path );
      close( fd );
      errno = EISDIR;
      return NULL;
   }
   // allocate a new FHANDLE ref
   POSIX_FHANDLE fhandle = malloc( sizeof(struct posixmdal_file_handle_struct) );
   if ( fhandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new FHANDLE struct\n" );
      close( fd );
      return NULL;
   }
   fhandle->fd = fd;
   return (MDAL_FHANDLE) fhandle;
}

/**
 * Close the given MDAL_FHANDLE
 * @param fh : File handle to be closed
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_close( MDAL_FHANDLE fh ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   int fd = pfh->fd;
   free( pfh );
   return close( fd );
}

/**
 * Write data to the given MDAL_WRITE, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be written to
 * @param const void* buf : Buffer containing the data to be written
 * @param size_t count : Number of data bytes contained within the buffer
 * @return ssize_t : Number of bytes written, or -1 if a failure occurred
 */
ssize_t posixmdal_write( MDAL_FHANDLE fh, const void* buf, size_t count ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   return write( pfh->fd, buf, count );
}

/**
 * Read data from the given MDAL_READ, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be read from
 * @param void* buf : Buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 if a failure occurred
 */
ssize_t posixmdal_read( MDAL_FHANDLE fh, void* buf, size_t count ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   return read( pfh->fd, buf, count );
}

/**
 * Truncate the file referenced by the given MDAL_WRITE, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be truncated
 * @param off_t length : File length to truncate to
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_ftruncate( MDAL_FHANDLE fh, off_t length ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   return ftruncate( pfh->fd, length );
}

/**
 * Seek to the specified position in the file referenced by the given MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to seek
 * @param off_t offset : Number of bytes to seek over
 * @param int whence : SEEK_SET - seek from the beginning of the file
 *                     SEEK_CUR - seek from the current location
 *                     SEEK_END - seek from the end of the file
 * @return off_t : Resulting offset within the file, or -1 if a failure occurred
 */
off_t posixmdal_lseek( MDAL_FHANDLE fh, off_t offset, int whence ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   return lseek( pfh->fd, offset, whence );
}


/**
 * Set the specified xattr on the file referenced by the given MDAL_WRITE file handle
 * @param MDAL_FHANDLE fh : File handle for which to set the xattr
 * @param char hidden : A non-zero value indicates to store this as a 'hidden' MDAL value
 * @param const char* name : String name of the xattr to set
 * @param const void* value : Buffer containing the value of the xattr
 * @param size_t size : Size of the value buffer
 * @param int flags : Zero value    - create or replace the xattr
 *                    XATTR_CREATE  - create the xattr only (fail if xattr exists)
 *                    XATTR_REPLACE - replace the xattr only (fail if xattr missing)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_fsetxattr( MDAL_FHANDLE fh, char hidden, const char* name, const void* value, size_t size, int flags ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   // the non-hidden op is more straightforward
   if ( !(hidden) ) {
      // filter out any reserved name
      if ( xattrfilter( name, 0 ) ) {
         LOG( LOG_ERR, "Xattr has a reserved name string: \"%s\"\n", name );
         errno = EPERM;
         return -1;
      }
      return fsetxattr( pfh->fd, name, value, size, flags );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != (strlen(PMDAL_XATTR) + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   int retval = fsetxattr( pfh->fd, newname, value, size, flags );
   if ( retval ) {
      LOG( LOG_ERR, "fsetxattr failure for \"%s\" value (%s)\n", newname, strerror(errno) );
   }
   free( newname ); // cleanup
   return retval;
}


/**
 * Retrieve the specified xattr from the file referenced by the given MDAL_READ file handle
 * @param MDAL_FHANDLE fh : File handle for which to retrieve the xattr
 * @param char hidden : A non-zero value indicates to retrieve a 'hidden' MDAL value
 * @param const char* name : String name of the xattr to retrieve
 * @param void* value : Buffer to be populated with the xattr value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr value, or -1 if a failure occurred
 */
ssize_t posixmdal_fgetxattr( MDAL_FHANDLE fh, char hidden, const char* name, void* value, size_t size ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   // the non-hidden op is more straightforward
   if ( !(hidden) ) {
      // filter out any reserved name
      if ( xattrfilter( name, 0 ) ) {
         LOG( LOG_ERR, "Xattr has a reserved name string: \"%s\"\n", name );
         errno = EPERM;
         return -1;
      }
      return fgetxattr( pfh->fd, name, value, size );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != (strlen(PMDAL_XATTR) + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   ssize_t retval = fgetxattr( pfh->fd, newname, value, size );
   free( newname ); // cleanup
   return retval;
}


/**
 * Remove the specified xattr from the file referenced by the given MDAL_WRITE file handle
 * @param MDAL_FHANDLE fh : File handle for which to remove the xattr
 * @param char hidden : A non-zero value indicates to remove a 'hidden' MDAL value
 * @param const char* name : String name of the xattr to remove
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_fremovexattr( MDAL_FHANDLE fh, char hidden, const char* name ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   // the non-hidden op is more straightforward
   if ( !(hidden) ) {
      // filter out any reserved name
      if ( xattrfilter( name, 0 ) ) {
         LOG( LOG_ERR, "Xattr has a reserved name string: \"%s\"\n", name );
         errno = EPERM;
         return -1;
      }
      return fremovexattr( pfh->fd, name );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != (strlen(PMDAL_XATTR) + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   int retval = fremovexattr( pfh->fd, newname );
   free( newname ); // cleanup
   return retval;
}


/**
 * List all xattr names from the file referenced by the given MDAL_READ file handle
 * @param MDAL_FHANDLE fh : File handle for which to list xattrs
 * @param char hidden : A non-zero value indicates to list 'hidden' MDAL xattrs ( normal xattrs excluded )
 * @param char* buf : Buffer to be populated with xattr names
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the returned xattr name list, or -1 if a failure occurred
 */
ssize_t posixmdal_flistxattr( MDAL_FHANDLE fh, char hidden, char* buf, size_t size ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   // perform the op, but capture the result
   ssize_t res = flistxattr( pfh->fd, buf, size );
   if ( size == 0 ) {
      // special, no-output case, which can be immediately returned
      LOG( LOG_INFO, "Immediately returning result of size == 0 call\n" );
      return res;
   }
   else if ( res < 0 ) {
      // error case, which can be immediately returned
      bzero( buf, size ); // but zero out the user buf, just in case
      LOG( LOG_INFO, "Returning error res of call, with zeroed buffer\n" );
      return res;
   }
   // parse over the xattr list, limiting it to either 'hidden' or non-'hidden' values
   char* parse = buf;
   char* output = buf;
   while ( (parse - buf) < res ) {
      size_t elemlen = strlen(parse);
      if ( xattrfilter( parse, hidden ) == 0 ) {
         if ( hidden ) {
            // we need to omit the reserved xattr prefix
            size_t prefixlen = strlen( PMDAL_XATTR );
            elemlen -= prefixlen;
            parse += prefixlen;
         }
         // check if we need to shift this element to a previous position
         if ( output != parse ) {
            snprintf( output, elemlen + 1, "%s", parse );
         }
         LOG( LOG_INFO, "Preserving value: \"%s\"\n", output );
         output += elemlen + 1; // skip over the output string and NULL term
      }
      else {
         LOG( LOG_INFO, "Omitting xattr: \"%s\"\n", parse );
      }
      parse += elemlen + 1; // skip over the current element and NULL term
   }
   // explicitly zero out the remainder of the xattr list
   ssize_t listlen = (ssize_t)(output - buf);
   bzero( output, res - listlen );
   // return the length of the modified list
   LOG( LOG_INFO, "Result list is %zd characters long\n", listlen );
   return listlen;
}

/**
 * Perform a stat operation on the file referenced by the given MDAL_FHANDLE
 * @param MDAL_FHANDLE fh : File handle to stat
 * @param struct stat* buf : Reference to a stat buffer to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_fstat ( MDAL_FHANDLE fh, struct stat* buf ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   // issue the stat
   return fstat( pfh->fd, buf );
}

/**
 * Update the timestamps of the target file
 * @param MDAL_FHANDLE fh : File handle on which to set timestamps
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int posixmdal_futimens ( MDAL_FHANDLE fh, const struct timespec times[2] ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_FHANDLE pfh = (POSIX_FHANDLE) fh;
   // issue the utime call
   return futimens( pfh->fd, times );
}


// Path Functions

/**
 * Check access to the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param int mode : F_OK - check for file existance
 *                      or a bitwise OR of the following...
 *                   R_OK - check for read access
 *                   W_OK - check for write access
 *                   X_OK - check for execute access
 * @param int flags : A bitwise OR of the following...
 *                    AT_EACCESS - Perform access checks using effective uid/gid
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_access( MDAL_CTXT ctxt, const char* path, int mode, int flags ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // reject any unsupported flag values
   if ( flags & ~(AT_EACCESS | AT_SYMLINK_NOFOLLOW) ) {
      LOG( LOG_ERR, "Detected unsupported flag value\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the access op
   return faccessat( pctxt->pathd, path, mode, flags );
}

/**
 * Create a filesystem node
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target node
 * @param mode_t mode : Mode value for the created node (see inode man page)
 * @param dev_t dev : S_IFREG  - regular file
 *                    S_IFCHR  - character special file
 *                    S_IFBLK  - block special file
 *                    S_IFIFO  - FIFO
 *                    S_IFSOCK - socket
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_mknod( MDAL_CTXT ctxt, const char* path, mode_t mode, dev_t dev ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the mknod op
   return mknodat( pctxt->pathd, path, mode, dev );
}

/**
 * Edit the mode of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param mode_t mode : New mode value for the file (see inode man page)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_chmod( MDAL_CTXT ctxt, const char* path, mode_t mode, int flags ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // reject any unsupported flag values
   if ( flags & ~(AT_SYMLINK_NOFOLLOW) ) {
      LOG( LOG_ERR, "Detected unsupported flag value\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the chmod op
   return fchmodat( pctxt->pathd, path, mode, flags );
}

/**
 * Edit the ownership and group of the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param uid_t owner : New owner
 * @param gid_t group : New group
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_chown( MDAL_CTXT ctxt, const char* path, uid_t owner, gid_t group, int flags ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // reject any unsupported flag values
   if ( flags & ~(AT_SYMLINK_NOFOLLOW) ) {
      LOG( LOG_ERR, "Detected unsupported flag value\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the chown op
   return fchownat( pctxt->pathd, path, owner, group, flags );
}

/**
 * Stat the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param struct stat* st : Stat structure to be populated
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_stat( MDAL_CTXT ctxt, const char* path, struct stat* st, int flags ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // reject any unsupported flag values
   if ( flags & ~(AT_SYMLINK_NOFOLLOW) ) {
      LOG( LOG_ERR, "Detected unsupported flag value\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the stat op
   return fstatat( pctxt->pathd, path, st, flags );
}


/**
 * Create a hardlink
 * @param MDAL_CTXT oldctxt : MDAL_CTXT to operate relative to for 'oldpath'
 * @param const char* oldpath : String path of the target file
 * @param MDAL_CTXT newctxt : MDAL_CTXT to operate relative to for 'newpath'
 * @param const char* newpath : String path of the new hardlink
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_link( MDAL_CTXT oldctxt, const char* oldpath, MDAL_CTXT newctxt, const char* newpath, int flags ) {
   // check for NULL ctxt
   if ( !(oldctxt)  ||  !(newctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT poldctxt = (POSIX_MDAL_CTXT) oldctxt;
   POSIX_MDAL_CTXT pnewctxt = (POSIX_MDAL_CTXT) newctxt;
   // check for a valid NS path dir
   if ( poldctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved an 'old' MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pnewctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved an 'new' MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // reject any unsupported flag values
   if ( flags & ~(AT_SYMLINK_NOFOLLOW) ) {
      LOG( LOG_ERR, "Detected unsupported flag value\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the link op
   return linkat( poldctxt->pathd, oldpath, pnewctxt->pathd, newpath, flags );
}


/**
 * Create the specified directory
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the new directory
 * @param mode_t mode : Mode value of the new directory (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_mkdir( MDAL_CTXT ctxt, const char* path, mode_t mode ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the mkdir op
   return mkdirat( pctxt->pathd, path, mode );
}


/**
 * Delete the specified directory
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target directory
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_rmdir( MDAL_CTXT ctxt, const char* path ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the unlink op
   return unlinkat( pctxt->pathd, path, AT_REMOVEDIR );
}


/**
 * Read the target path of the specified symlink
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target symlink
 * @param char* buf : Buffer to be populated with the link value
 * @param size_t size : Size of the target buffer
 * @return ssize_t : Size of the link target string, or -1 if a failure occurred
 */
ssize_t posixmdal_readlink( MDAL_CTXT ctxt, const char* path, char* buf, size_t size ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the readlink op
   return readlinkat( pctxt->pathd, path, buf, size );
}


/**
 * Rename the specified target to a new path
 * @param MDAL_CTXT fromctxt : MDAL_CTXT to operate relative to for the 'from' path
 * @param const char* from : String path of the target
 * @param MDAL_CTXT toctxt : MDAL_CTXT to operate relative to for the 'to' path
 * @param const char* to : Destination string path
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_rename( MDAL_CTXT fromctxt, const char* from, MDAL_CTXT toctxt, const char* to ) {
   // check for NULL ctxt
   if ( !(fromctxt)  ||  !(toctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pfromctxt = (POSIX_MDAL_CTXT) fromctxt;
   POSIX_MDAL_CTXT ptoctxt = (POSIX_MDAL_CTXT) toctxt;
   // check for a valid NS path dir
   if ( pfromctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a 'from' MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   if ( ptoctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a 'to' MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the rename op
   return renameat( pfromctxt->pathd, from, ptoctxt->pathd, to );
}

/**
 * Return statvfs info for the current namespace
 * @param MDAL_CTXT ctxt : MDAL_CTXT to retrieve info for
 * @param struct statvfs* buf : Reference to the statvfs structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_statvfs( MDAL_CTXT ctxt, struct statvfs* buf ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the statvfs op against the reference dir of this NS ( result should be homogeneous across NS files )
   return fstatvfs( pctxt->refd, buf );
}

/**
 * Create a symlink
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* target : String path for the link to target
 * @param const char* linkname : String path of the new link
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_symlink( MDAL_CTXT ctxt, const char* target, const char* linkname ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the symlink op
   return symlinkat( target, pctxt->pathd, linkname );
}

/**
 * Unlink the specified file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posixmdal_unlink( MDAL_CTXT ctxt, const char* path ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the unlink op
   return unlinkat( pctxt->pathd, path, 0 );
}

/**
 * Update the timestamps of the target file
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the target file
 * @param const struct timespec times[2] : Struct references for new times
 *                                         times[0] - atime values
 *                                         times[1] - mtime values
 *                                         (see man utimensat for struct reference)
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero value on success, or -1 if a failure occurred
 */
int posixmdal_utimens ( MDAL_CTXT ctxt, const char* path, const struct timespec times[2], int flags ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the utime call
   return utimensat( pctxt->pathd, path, times, flags );
}


//   -------------    POSIX INITIALIZATION    -------------

MDAL posix_mdal_init( xmlNode* root ) {
   // check for NULL root
   if ( !(root) ) {
      LOG( LOG_ERR, "Received a NULL root XML node\n" );
      errno = EINVAL;
      return NULL;
   }
   // make sure we start on a 'ns_root' node
   if ( root->type == XML_ELEMENT_NODE  &&  strncmp( (char*)root->name, "ns_root", 8 ) == 0 ) {

      // make sure that node contains a text element within it
      if ( root->children != NULL  &&  root->children->type == XML_TEXT_NODE ) {

         // allocate space for our context struct
         POSIX_MDAL_CTXT pctxt = malloc( sizeof( struct posix_mdal_context_struct ) );
         if ( pctxt == NULL ) {
            LOG( LOG_ERR, "Failed to allocate context struct\n" );
            return NULL; // malloc will set errno
         }
         // initialize the pathd, indicating no path set
         pctxt->pathd = -1;
         // initialize the dev to an arbitrary value
         pctxt->dev = 0;

         // open the directory specified by the node content
         char* nsrootpath = strdup( (char*) root->children->content );
         int rootfd = open( nsrootpath, O_RDONLY );
         if ( rootfd < 0 ) {
            LOG( LOG_ERR, "Failed to open the target 'ns_root' directory: \"%s\"\n", nsrootpath );
            free( pctxt );
            free( nsrootpath );
            return NULL;
         }

         // verify the target is a dir
         struct stat dirstat;
         if ( fstat( rootfd, &(dirstat) )  ||  !(S_ISDIR(dirstat.st_mode)) ) {
            LOG( LOG_ERR, "Could not verify target is a directory: \"%s\"\n", nsrootpath );
            free( pctxt );
            free( nsrootpath );
            close( rootfd );
            return NULL;
         }
         free( nsrootpath ); // done with the nsroot string
         pctxt->refd = rootfd; // populate out root dir reference

         // allocate and populate a new MDAL structure
         MDAL pmdal = malloc( sizeof( struct MDAL_struct ) );
         if ( pmdal == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for an MDAL_struct\n" );
            posixmdal_destroyctxt( pctxt );
            return NULL; // malloc will set errno
         }
         pmdal->name = "posix";
         pmdal->ctxt = (MDAL_CTXT) pctxt;
         pmdal->pathfilter = posixmdal_pathfilter;
         pmdal->destroyctxt = posixmdal_destroyctxt;
         pmdal->dupctxt = posixmdal_dupctxt;
         pmdal->cleanup = posixmdal_cleanup;
         pmdal->checksec = posixmdal_checksec;
         pmdal->setnamespace = posixmdal_setnamespace;
         pmdal->newctxt = posixmdal_newctxt;
         pmdal->newsplitctxt = posixmdal_newsplitctxt;
         pmdal->createnamespace = posixmdal_createnamespace;
         pmdal->destroynamespace = posixmdal_destroynamespace;
         pmdal->opendirnamespace = posixmdal_opendirnamespace;
         pmdal->accessnamespace = posixmdal_accessnamespace;
         pmdal->statnamespace = posixmdal_statnamespace;
         pmdal->chmodnamespace = posixmdal_chmodnamespace;
         pmdal->chownnamespace = posixmdal_chownnamespace;
         pmdal->setdatausage = posixmdal_setdatausage;
         pmdal->getdatausage = posixmdal_getdatausage;
         pmdal->setinodeusage = posixmdal_setinodeusage;
         pmdal->getinodeusage = posixmdal_getinodeusage;
         pmdal->createrefdir = posixmdal_createrefdir;
         pmdal->destroyrefdir = posixmdal_destroyrefdir;
         pmdal->linkref = posixmdal_linkref;
         pmdal->renameref = posixmdal_renameref;
         pmdal->unlinkref = posixmdal_unlinkref;
         pmdal->statref = posixmdal_statref;
         pmdal->openref = posixmdal_openref;
         pmdal->openscanner = posixmdal_openscanner;
         pmdal->closescanner = posixmdal_closescanner;
         pmdal->scan = posixmdal_scan;
         pmdal->sopen = posixmdal_sopen;
         pmdal->sunlink = posixmdal_sunlink;
         pmdal->sstat = posixmdal_sstat;
         pmdal->opendir = posixmdal_opendir;
         pmdal->chdir = posixmdal_chdir;
         pmdal->dsetxattr = posixmdal_dsetxattr;
         pmdal->dgetxattr = posixmdal_dgetxattr;
         pmdal->dremovexattr = posixmdal_dremovexattr;
         pmdal->dlistxattr = posixmdal_dlistxattr;
         pmdal->readdir = posixmdal_readdir;
         pmdal->closedir = posixmdal_closedir;
         pmdal->open = posixmdal_open;
         pmdal->close = posixmdal_close;
         pmdal->write = posixmdal_write;
         pmdal->read = posixmdal_read;
         pmdal->ftruncate = posixmdal_ftruncate;
         pmdal->lseek = posixmdal_lseek;
         pmdal->fsetxattr = posixmdal_fsetxattr;
         pmdal->fgetxattr = posixmdal_fgetxattr;
         pmdal->fremovexattr = posixmdal_fremovexattr;
         pmdal->flistxattr = posixmdal_flistxattr;
         pmdal->fstat = posixmdal_fstat;
         pmdal->futimens = posixmdal_futimens;
         pmdal->access = posixmdal_access;
         pmdal->mknod = posixmdal_mknod;
         pmdal->chmod = posixmdal_chmod;
         pmdal->chown = posixmdal_chown;
         pmdal->stat = posixmdal_stat;
         pmdal->link = posixmdal_link;
         pmdal->mkdir = posixmdal_mkdir;
         pmdal->rmdir = posixmdal_rmdir;
         pmdal->readlink = posixmdal_readlink;
         pmdal->rename = posixmdal_rename;
         pmdal->statvfs = posixmdal_statvfs;
         pmdal->symlink = posixmdal_symlink;
         pmdal->unlink = posixmdal_unlink;
         pmdal->utimens = posixmdal_utimens;
         return pmdal;
      }
      else { LOG( LOG_ERR, "the \"ns_root\" node is expected to contain a path string\n" ); }
   }
   else { LOG( LOG_ERR, "root node of config is expected to be \"ns_root\"\n" ); }
   errno = EINVAL;
   return NULL; // failure of any condition check fails the function
}



