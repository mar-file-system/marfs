
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
#if defined(DEBUG_ALL)  ||  defined(DEBUG_MDAL)
   #define DEBUG 1
#endif
#define LOG_PREFIX "posix_mdal"
#include "logging/logging.h"

#include "mdal.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


//   -------------    POSIX DEFINITIONS    -------------

#define PMDAL_PREFX "MDAL_"
#define PMDAL_REF PMDAL_PREFX"reference"
#define PMDAL_PATH "path"
#define PMDAL_SUBSP PMDAL_PREFX"subspaces"
#define PMDAL_DUSE "datasize"
#define PMDAL_IUSE "inodecount"
#define PMDAL_XATTR "user."PMDAL_PREFX


//   -------------    POSIX STRUCTURES    -------------

typedef struct posix_directory_handle_struct {
   DIR*     dirp; // Directory reference
}* POSIX_DHANDLE;

typedef struct posix_scanner_struct {
   DIR*     dirp; // Directory reference
}* POSIX_SCANNER;

typedef struct posix_file_handle_struct {
   int        fd; // File handle
}* POSIX_FHANDLE;

typedef struct posix_mdal_context_struct {
   int refd;   // Dir handle for NS ref tree ( or the secure root, if NS hasn't been set )
   int pathd;  // Dir handle of the user tree for the current NS ( or -1, if NS hasn't been set )
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
   totlen += 3;
   snprintf( newpath, newlen, "../" );
   if ( 3 >= newlen ) { newlen = 0; }
   else { newlen -= 3; newpath += 3; }
   while ( *parse != '\0' ) {
      // identify each sub-element
      char* elemref = parse;
      // move our parse pointer to the next path element
      while ( *parse != '\0' ) {
         if ( *parse == '/' ) {
            // traverse only to the next path component, skipping over duplicate '/' chars
            parse++;
            while ( *parse == '/' ) { parse++; }
            break;
         }
         parse++;
      }
      if ( strncmp( elemref, "../", 3 ) == 0 ) {
         // a parent namespace ref actually means double that for this MDAL
         // we have to traverse the intermediate PMDAL_SUBSP directory
         int prout = snprintf( newpath, newlen, "../../" );
         totlen += prout;
         if ( prout >= newlen ) { newlen = 0; }
         else { newlen -= prout; newpath += prout }
      }
      else if ( *elemref == '/' ) {
         // this should only occur in the case of a leading '/' character
         if ( totlen != 3 ) {
            LOG( LOG_ERR, "Encountered unexpected '/' element at output position %zu\n", totlen );
            return 0;
         }
         // this means the path is absolute, so we must replace the '../' prefix with this '/' char
         totlen = 1;
         newpath = orignewpath;
         newlen = orignewlen;
         // insert that char, if we have room
         if ( newlen > 1 ) {
            *newpath = '/';
            newlen--;
            newpath++;
         }
         // ensure we append a NULL-term
         if ( newlen > 0 ) {
            *newpath = '\0';
            // check for end of ouput condition
            if ( newlen == 1 ) { newlen = 0; }
         }
      }
      else if ( strncmp( elemref, "./", 2 ) != 0 ) {
         // completely ignore './' elements, as they are not needed
         // all other elements should be subspace references
         // we need to insert the intermediate subspace directory name
         int prout = snprintf( newpath, newlen, "%s/", PMDAL_SUBSP );
         totlen += prout;
         if ( prout >= newlen ) { newlen = 0; }
         else { newlen -= prout; newpath += prout }
         // now we can copy the subspace name itself
         char* eparse = elemref;
         for( ; *eparse != '\0'  &&  *eparse != '/'; eparse++ ) {
            totlen++;
            if ( newlen > 1 ) {
               *newpath = *eparse;
               newlen--;
               newpath++;
               // if we've hit a '/', stop here
               if ( *eparse == '/' ) { break; }
            }
         }
         if ( newlen > 0 ) {
            // ensure we always leave a NULL-term
            *newpath = '\0';
            // check for end of ouput condition
            if ( newlen == 1 ) { newlen = 0; }
         }
      }
   }
   return totlen;
}

/**
 * Identify and reject any paths targeting reserved names
 * @param const char* path : Path to verify
 * @return : Zero if the path is acceptable, -1 if not
 */
int pathfilter( const char* path ) {
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

// Management Functions

/**
 * Cleanup all structes and state associated with the given posix MDAL
 * @param MDAL mdal : MDAL to be freed
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_cleanup( MDAL mdal ) {
   // check for NULL mdal
   if ( !(mdal) ) {
      LOG( LOG_ERR, "Received a NULL MDAL reference\n" );
      errno = EINVAL;
      return -1;
   }
   // destroy the MDAL_CTXT struct
   if ( posix_destroyctxt( mdal->ctxt ) ) {
      LOG( LOG_ERR, "Failed to destroy the MDAL_CTXT reference\n" );
      return -1;
   }
   // free the entire MDAL
   free( mdal );
   return 0;
}

/**
 * Duplicate the given MDAL_CTXT
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to duplicate
 * @return MDAL_CTXT : Reference to the duplicate MDAL_CTXT, or NULL if an error occurred
 */
MDAL_CTXT posix_dupctxt ( const MDAL_CTXT ctxt ) {
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
   return (MDAL_CTXT) dupctxt;
}

/**
 * Create a new MDAL_CTXT reference, targeting the specified NS
 * @param const char* ns : Name of the namespace for the new MDAL_CTXT to target
 * @param const MDAL_CTXT basectxt : The new MDAL_CTXT will be created relative to this one
 * @return MDAL_CTXT : Reference to the new MDAL_CTXT, or NULL if an error occurred
 */
MDAL_CTXT posix_newctxt ( const char* ns, const MDAL_CTXT basectxt ) {
   // check for NULL basectxt
   if ( !(basectxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return NULL;
   }
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
   if ( namespacepath( ns, nspath, nspathlen ) != nspathlen ) {
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
      // ensure the basectxt is set to the secureroot dir
      if ( basectxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( newctxt );
         free( nspath );
         return NULL;
      }
      // absoulute paths are opened via the root dir (skipping the leading '/')
      newctxt->pathd = openat( basectxt->refd, (nspath + 1), O_RDONLY );
   }
   else {
      // relative paths are opened via the ref dir of the basectxt
      newctxt->pathd = openat( basectxt->refd, nspath, O_RDONLY );
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
      free( nspath );
      return NULL;
   }
   return (MDAL_CTXT) newctxt;
}

/**
 * Destroy a given MDAL_CTXT ( such as following a dupctxt call )
 * @param MDAL_CTXT ctxt : MDAL_CTXT to be freed
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_destroyctxt ( MDAL_CTXT ctxt ) {
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


// Namespace Functions

/**
 * Set the namespace of the given MDAL_CTXT
 * @param MDAL_CTXT ctxt : Context to set the namespace of
 * @param const char* ns : Name of the namespace to set
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_setnamespace( MDAL_CTXT ctxt, const char* ns ) {
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
   if ( namespacepath( ns, nspath, nspathlen ) != nspathlen ) {
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
   return 0;
}

/**
 * Create the specified namespace root structures ( reference tree is not created by this func! )
 * @param MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace to be created
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_createnamespace( MDAL_CTXT ctxt, const char* ns ) {
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
   char* nspath = malloc( sizeof(char) * (nspathlen + 2 + strlen(PMDAL_REF)) ); // leave room for ref suffix
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
      free( nspath );
      return -1;
   }
   // attempt to create the target directory
   int mkdirres;
   if ( *nspath == '/' ) {
      // ensure the refd is set to the secureroot dir
      if ( pctxt->pathd >= 0 ) {
         LOG( LOG_ERR, "Absolute NS paths can only be used from a CTXT with no NS set\n" );
         errno = EINVAL;
         free( nspath );
         return -1;
      }
      mkdirres = mkdirat( pctxt->refd, nspath + 1, S_IRWXU );
   }
   else {
      mkdirres = mkdirat( pctxt->refd, nspath, S_IRWXU );
   }
   if ( mkdirres ) {
      LOG( LOG_ERR, "Failed to create NS root: \"%s\"\n", nspath );
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
   if ( *nspath == '/' ) {
      // no need to double check pathd state
      mkdirres = mkdirat( pctxt->refd, nspath + 1, S_IRWXU );
   }
   else {
      mkdirres = mkdirat( pctxt->pathd, nspath, S_IRWXU );
   }
   if ( mkdirres ) {
      LOG( LOG_ERR, "Failed to create NS ref path: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   free( nspath );
   return 0;
}

/**
 * Destroy the specified namespace root structures
 * NOTE -- This operation will fail with errno=ENOTEMPTY if files/dirs persist in the namespace.
 *         This includes files/dirs within the reference tree.
 * @param const MDAL_CTXT ctxt : Current MDAL context
 * @param const char* ns : Name of the namespace to be deleted
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_destroynamespace ( const MDAL_CTXT ctxt, const char* ns ) {
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
   char* nspath = malloc( sizeof(char) * (nspathlen + 2 + strlen(PMDAL_REF)) ); // leave room for ref suffix
   if ( !(nspath) ) {
      LOG( LOG_ERR, "Failed to allocate path string for NS: \"%s\"\n", ns );
      return -1;
   }
   if ( namespacepath( ns, nspath, nspathlen ) != nspathlen ) {
      LOG( LOG_ERR, "Inconsistent path generation for NS: \"%s\"\n", ns );
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
   // attempt to unlink the ref subdir
   int unlinkres;
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
      unlinkres = unlinkat( pctxt->refd, nspath, AT_REMOVEDIR );
   }
   if ( unlinkres ) {
      LOG( LOG_ERR, "Failed to unlink NS ref path: \"%s\"\n", nspath );
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
      unlinkres = unlinkat( pctxt->pathd, nspath, AT_REMOVEDIR );
   }
   if ( unlinkres ) {
      LOG( LOG_ERR, "Failed to unlink NS root path: \"%s\"\n", nspath );
      free( nspath );
      return -1;
   }
   free( nspath );
   return 0;
}


// Usage Functions

/**
 * Set data usage value for the current namespace
 * @param MDAL_CTXT ctxt : Current MDAL_CTXT, associated with the target namespace
 * @param off_t bytes : Number of bytes used by the namespace
 * @return int : Zero on success, -1 if a failure occurred
 */
int posix_setdatausage( MDAL_CTXT ctxt, off_t bytes ) {
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
   char* dusepath = malloc( sizeof(char) * (strlen(PMDAL_DUSE) + 1) );
   if ( !(dusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the data usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( dusepath, (strlen(PMDAL_DUSE) + 1), "%s", PMDAL_DUSE ) >= strlen(PMDAL_DUSE) ) {
      LOG( LOG_ERR, "Failed to populate the usage file path\n" );
      free( dusepath );
      return -1;
   }
   // open a file handle for the DUSE path ( create with all perms open, if missing )
   int dusefd = openat( pctxt->pathd, dusepath, O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO );
   free( dusepath ); // done with the path
   if ( dusefd < 0 ) {
      LOG( LOG_ERR, "Failed to open the data use file\n" );
      return -1;
   }
   // truncate the DUSE file to the provided length
   if ( ftruncate( dusefd, bytes ) ) {
      LOG( LOG_ERR, "Failed to truncate the data use file to the specified length of %zd\n", len );
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
off_t posix_getdatausage( MDAL_CTXT ctxt ) {
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
   char* dusepath = malloc( sizeof(char) * (strlen(PMDAL_DUSE) + 1) );
   if ( !(dusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the data usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( dusepath, (strlen(PMDAL_DUSE) + 1), "%s", PMDAL_DUSE ) >= strlen(PMDAL_DUSE) ) {
      LOG( LOG_ERR, "Failed to populate the usage file path\n" );
      free( dusepath );
      return -1;
   }
   // stat the DUSE file
   struct stat dstat;
   if ( fstatat( pctxt->pathd, dusepath, &(dstat) ) ) {
      LOG( LOG_ERR, "Failed to stat the data use file\n" );
      free( dusepath );
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
int posix_setinodeusage( MDAL_CTXT ctxt, off_t files ) {
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
   char* iusepath = malloc( sizeof(char) * (strlen(PMDAL_IUSE) + 1) );
   if ( !(iusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the inode usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( iusepath, (strlen(PMDAL_IUSE) + 1), "%s", PMDAL_IUSE ) >= strlen(PMDAL_IUSE) ) {
      LOG( LOG_ERR, "Failed to populate the inode usage file path\n" );
      free( iusepath );
      return -1;
   }
   // open a file handle for the IUSE path ( create with all perms open, if missing )
   int iusefd = openat( pctxt->pathd, iusepath, O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO );
   free( iusepath ); // done with the path
   if ( iusefd < 0 ) {
      LOG( LOG_ERR, "Failed to open the inode use file\n" );
      return -1;
   }
   // truncate the IUSE file to the provided length
   if ( ftruncate( iusefd, bytes ) ) {
      LOG( LOG_ERR, "Failed to truncate the inode use file to the specified length of %zd\n", len );
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
off_t posix_getinodeusage( MDAL_CTXT ctxt ) {
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
   char* iusepath = malloc( sizeof(char) * (strlen(PMDAL_IUSE) + 1) );
   if ( !(iusepath) ) {
      LOG( LOG_ERR, "Failed to allocate a string for the inode usage file\n" );
      return -1;
   }
   // populate the path
   if ( snprintf( iusepath, (strlen(PMDAL_IUSE) + 1), "%s", PMDAL_IUSE ) >= strlen(PMDAL_IUSE) ) {
      LOG( LOG_ERR, "Failed to populate the usage file path\n" );
      free( iusepath );
      return -1;
   }
   // stat the IUSE file
   struct stat istat;
   if ( fstatat( pctxt->pathd, iusepath, &(istat) ) ) {
      LOG( LOG_ERR, "Failed to stat the inode use file\n" );
      free( iusepath );
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
 * @return int : Zero on success, -1 if a failure occurred
 */
int (*createrefdir) ( const MDAL_CTXT ctxt, const char* refdir ) {
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
   if ( mkdirat( pctxt->refd, refdir, S_IRWXU | S_IRWXG | S_IRWXO ) ) {
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
int (*destroyrefdir) ( const MDAL_CTXT ctxt, const char* refdir ) {
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
 * @param const char* oldrpath : Reference path of the existing file target
 * @param const char* newpath : Path at which to create the hardlink
 * @return int : Zero on success, -1 if a failure occurred
 */
int (*linkref) ( const MDAL_CTXT, const char* oldrpath, const char* newpath ) {
   // check for NULL ctxt
   if ( !(ctxt) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_CTXT reference\n" );
      errno = EINVAL;
      return -1;
   }
   // reject any improper paths
   if ( pathfilter( newpath ) ) {
      LOG( LOG_ERR, "Rejecting reserved path target: \"%s\"\n", newpath );
      errno = EPERM;
      return -1;
   }
   POSIX_MDAL_CTXT pctxt = (POSIX_MDAL_CTXT) ctxt;
   // check for a valid NS path dir
   if ( pctxt->pathd < 0 ) {
      LOG( LOG_ERR, "Receieved a MDAL_CTXT with no namespace target\n" );
      errno = EINVAL;
      return -1;
   }
   // attempt hardlink creation
   if ( linkat( pctxt->refd, oldrpath, pctxt->pathd, newpath ) ) {
      LOG( LOG_ERR, "Failed to link rpath into the namespace\n" );
      return -1;
   }
   return 0;
}

/**
 * Unlink the specified file reference path
 * @param const MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* rpath : String reference path of the target file
 * @return int : Zero on success, or -1 if a failure occurred
 */
int (*unlinkref) ( const MDAL_CTXT ctxt, const char* rpath ) {
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
int posix_statref ( const MDAL_CTXT ctxt, const char* rpath, struct stat* buf ) {
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
MDAL_FHANDLE posix_openref ( const MDAL_CTXT ctxt, const char* rpath, int flags, mode_t mode ) {
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
   PMDAL_FHANDLE fhandle = malloc( sizeof(struct posix_file_handle_struct) );
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
MDAL_SCANNER posix_openscanner( MDAL_CTXT ctxt, const char* rpath ) {
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
   int dirfd = openat( pctxt->refd, rpath, O_RDONLY );
   if ( dirfd < 0 ) {
      LOG( LOG_ERR, "Failed to open the target path: \"%s\"\n", rpath );
      return NULL;
   }
   // verify the target is a dir
   struct stat dirstat;
   if ( fstat( dirfd, &(dirstat) )  ||  !(S_ISDIR(dirstat.st_mode)) ) {
      LOG( LOG_ERR, "Could not verify target is a directory: \"%s\"\n", rpath );
      close( dirfd );
      return NULL;
   }
   // allocate a scanner reference
   PMDAL_SCANNER scanner = malloc( sizeof(struct posix_scanner_struct) );
   if ( scanner == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new scanner\n" );
      close( dirfd );
      return NULL;
   }
   // translate the FD to a DIR stream
   scanner->dirp = fopendir( dirfd );
   if ( scanner->dirp == NULL ) {
      LOG( LOG_ERR, "Failed to create directory stream from FD\n" );
      close( dirfd );
      return NULL;
   }
   return (MDAL_SCANNER) scanner;
}

/**
 * Close a given reference scanner
 * @param MDAL_SCANNER scanner : Reference scanner to be closed
 * @return int : Zero on success, -1 if a failure occurred
 */
int (*closescanner) ( MDAL_SCANNER scanner ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return -1;
   }
   PMDAL_SCANNER pscan = (PDMAL_SCANNER) scanner;
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
struct dirent* posix_scan( MDAL_SCANNER scanner ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return NULL;
   }
   PMDAL_SCANNER pscan = (PDMAL_SCANNER) scanner;
   return readdir( pscan->dirp );
}

/**
 * Open a file, relative to a given reference scanner
 * NOTE -- this is implicitly an open for READ
 * @param MDAL_SCANNER scanner : Reference scanner to open relative to
 * @param const char* path : Relative path of the target file from the scanner
 * @return MDAL_FHANDLE : An MDAL_READ handle for the target file, or NULL if a failure occurred
 */
MDAL_FHANDLE posix_sopen( MDAL_SCANNER scanner, const char* path ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return NULL;
   }
   PMDAL_SCANNER pscan = (PDMAL_SCANNER) scanner;
   // retrieve the dir FD associated with the current dir stream
   int dirfd = dirfd( pscan->dirp );
   if ( dirfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD for the current directory stream\n" );
      return NULL;
   }
   // issue the open, relative to the scanner FD
   int fd = openat( dirfd, path, O_RDONLY );
   if ( fd < 0 ) {
      LOG( LOG_ERR, "Failed to open relative scanner path: \"%s\"\n", path );
      return NULL;
   }
   // allocate a new FHANDLE ref
   PMDAL_FHANDLE fhandle = malloc( sizeof(struct posix_file_handle_struct) );
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
int posix_sunlink( MDAL_SCANNER scanner, const char* path ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return -1;
   }
   PMDAL_SCANNER pscan = (PDMAL_SCANNER) scanner;
   // retrieve the dir FD associated with the current dir stream
   int dirfd = dirfd( pscan->dirp );
   if ( dirfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD for the current directory stream\n" );
      return -1;
   }
   // issue the open, relative to the scanner FD
   if ( unlinkat( dirfd, path, 0 ) < 0 ) {
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
int posix_sstat ( MDAL_SCANNER scanner, const char* spath, struct stat* buf ) {
   // check for a NULL scanner
   if ( !(scanner) ) {
      LOG( LOG_ERR, "Received a NULL scanner reference\n" );
      errno = EINVAL;
      return -1;
   }
   PMDAL_SCANNER pscan = (PDMAL_SCANNER) scanner;
   // retrieve the dir FD associated with the current dir stream
   int dirfd = dirfd( pscan->dirp );
   if ( dirfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD for the current directory stream\n" );
      return -1;
   }
   // issue the stat
   if ( fstatat( dirfd, spath, buf, 0 ) ) {
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
MDAL_DHANDLE posix_opendir( MDAL_CTXT ctxt, const char* path ) {
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
   int dirfd = openat( pctxt->pathd, path, O_RDONLY );
   if ( dirfd < 0 ) {
      LOG( LOG_ERR, "Failed to open the target path: \"%s\"\n", path );
      return NULL;
   }
   // verify the target is a dir
   struct stat dirstat;
   if ( fstat( dirfd, &(dirstat) )  ||  !(S_ISDIR(dirstat.st_mode)) ) {
      LOG( LOG_ERR, "Could not verify target is a directory: \"%s\"\n", rpath );
      close( dirfd );
      return NULL;
   }
   // allocate a dir handle reference
   PMDAL_DHANDLE dhandle = malloc( sizeof(struct posix_directory_handle_struct) );
   if ( dhandle == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a new directory handle\n" );
      close( dirfd );
      return NULL;
   }
   // translate the FD to a DIR stream
   dhandle->dirp = fopendir( dirfd );
   if ( dhandle->dirp == NULL ) {
      LOG( LOG_ERR, "Failed to create directory stream from FD\n" );
      free( dhandle );
      close( dirfd );
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
int posix_chdir( MDAL_CTXT ctxt, MDAL_DHANDLE dh ) {
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
   if ( !(dh)  ||  !(dh->dirp) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_DHANDLE reference\n" );
      errno = EINVAL;
      return -1;
   }
   // retrieve the dir FD from the dir handle
   int dirfd = dirfd( dh->dirp );
   if ( dirfd < 0 ) {
      LOG( LOG_ERR, "Failed to retrieve the FD from the provided directory stream\n" );
      return -1;
   }
   // close the previous path dir
   if ( close( pctxt->pathd ) ) {
      LOG( LOG_WARNING, "Failed to close the previous path directory FD\n" );
   }
   // free the provided dir handle
   free( dh );
   // update the context struct
   pctxt->pathd = dirfd;
   return 0;
}


/**
 * Iterate to the next entry of an open directory handle
 * @param MDAL_DHANDLE dh : MDAL_DHANDLE to read from
 * @return struct dirent* : Reference to the next dirent struct, or NULL w/ errno unset if all 
 *                          entries have been read, or NULL w/ errno set if a failure occurred
 */
struct dirent* posix_readdir( MDAL_DHANDLE dh ) {
   // check for a NULL dir handle
   if ( !(dh)  ||  !(dh->dirp) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_DHANDLE reference\n" );
      errno = EINVAL;
      return -1;
   }
   return readdir( dh->dirp );
}


/**
 * Close the given directory handle
 * @param MDAL_DHANDLE dh : MDAL_DHANDLE to close
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_closedir( MDAL_DHANDLE dh ) {
   // check for a NULL dir handle
   if ( !(dh)  ||  !(dh->dirp) ) {
      LOG( LOG_ERR, "Received a NULL MDAL_DHANDLE reference\n" );
      errno = EINVAL;
      return -1;
   }
   DIR* dirstream = dh->dirp;
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
MDAL_FHANDLE posix_open( MDAL_CTXT ctxt, const char* path, int flags ) {
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
   // reject any improper paths
   if ( pathfilter( path ) ) {
      LOG( LOG_ERR, "Rejecting reserved path target: \"%s\"\n", path );
      errno = EPERM;
      return -1;
   }
   // issue the open
   int fd = openat( pctxt->pathd, path, flags );
   if ( fd < 0 ) {
      LOG( LOG_ERR, "Failed to open target path: \"%s\"\n", path );
      return NULL;
   }
   // allocate a new FHANDLE ref
   PMDAL_FHANDLE fhandle = malloc( sizeof(struct posix_file_handle_struct) );
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
int posix_close( MDAL_FHANDLE fh ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   int fd = fh->fd;
   free( fh );
   return close( fd );
}

/**
 * Write data to the given MDAL_WRITE, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be written to
 * @param const void* buf : Buffer containing the data to be written
 * @param size_t count : Number of data bytes contained within the buffer
 * @return ssize_t : Number of bytes written, or -1 if a failure occurred
 */
ssize_t posix_write( MDAL_FHANDLE fh, const void* buf, size_t count ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   return write( fh->fd, buf, count );
}

/**
 * Read data from the given MDAL_READ, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be read from
 * @param void* buf : Buffer to be populated with read data
 * @param size_t count : Number of bytes to be read
 * @return ssize_t : Number of bytes read, or -1 if a failure occurred
 */
ssize_t posix_read( MDAL_FHANDLE fh, void* buf, size_t count ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   return read( fh->fd, buf, count );
}

/**
 * Truncate the file referenced by the given MDAL_WRITE, MDAL_DIRECT file handle
 * @param MDAL_FHANDLE fh : File handle to be truncated
 * @param off_t length : File length to truncate to
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_ftruncate( MDAL_FHANDLE fh, off_t length ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   return ftruncate( fh->fd, length );
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
off_t posix_lseek( MDAL_FHANDLE fh, off_t offset, int whence ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   return lseek( fh->fd, offset, whence );
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
int posix_fsetxattr( MDAL_FHANDLE fh, char hidden, const char* name, const void* value, size_t size, int flags ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
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
      return fsetxattr( fh->fd, name, value, size, flags );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != 
         (strlen(PMDAL_XATTR) + 1 + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   int retval = fsetxattr( fh->fd, newname, value, size, flags );
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
ssize_t posix_fgetxattr( MDAL_FHANDLE fh, char hidden, const char* name, void* value, size_t size ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
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
      return fgetxattr( fh->fd, name, value, size );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != 
         (strlen(PMDAL_XATTR) + 1 + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   ssize_t retval = fgetxattr( fh->fd, newname, value, size );
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
int posix_fremovexattr( MDAL_FHANDLE fh, char hidden, const char* name ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
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
      return fremovexattr( fh->fd, name );
   }
   // if this is a hidden value, we need to attach the appropriate prefix
   char* newname = malloc( sizeof(char) * (strlen(PMDAL_XATTR) + 1 + strlen(name)) );
   if ( newname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for a hidden xattr name string\n" );
      return -1;
   }
   if ( snprintf( newname, (strlen(PMDAL_XATTR) + 1 + strlen(name)), "%s%s", PMDAL_XATTR, name ) != 
         (strlen(PMDAL_XATTR) + 1 + strlen(name)) ) {
      LOG( LOG_ERR, "Failed to populate the hidden xattr name string\n" );
      free( newname );
      return -1;
   }
   // now we can actually perform the op
   int retval = fremovexattr( fh->fd, newname );
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
ssize_t posix_flistxattr( MDAL_FHANDLE fh, char hidden, char* buf, size_t size ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   // perform the op, but capture the result
   ssize_t res = flistxattr( fh->fd, buf, size );
   if ( size == 0 ) {
      // special, no-output case, which can be immediately returned
      return res;
   }
   else if ( res < 0 ) {
      // error case, which can be immediately returned
      bzero( buf, size ); // but zero out the user buf, just in case
      return res;
   }
   // parse over the xattr list, limiting it to either 'hidden' or non-'hidden' values
   char* parse = buf;
   char* output = buf;
   while ( (parse - buf) < (size - 1) ) {
      size_t elemlen = strlen(parse);
      if ( xattrfilter( parse, hidden ) == 0 ) {
         // check if we need to shift this element to a previous position
         if ( output != parse ) {
            snprintf( output, elemlen, "%s", parse );
         }
         output += elemlen + 1; // skip over the output string and NULL term
      }
      parse += elemlen + 1; // skip over the current element and NULL term
   }
   // return the length of the modified list
   return (size_t) output - buf;
}

/**
 * Perform a stat operation on the file referenced by the given MDAL_FHANDLE
 * @param MDAL_FHANDLE fh : File handle to stat
 * @param struct stat* buf : Reference to a stat buffer to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_fstat ( MDAL_FHANDLE fh, struct stat* buf ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the stat
   return fstat( fh->fd, buf );
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
int (*futimens) ( MDAL_FH fh, const struct timespec times[2] ) {
   // check for a NULL file handle
   if ( !(fh) ) {
      LOG( LOG_ERR, "Received a NULL file handle reference\n" );
      errno = EINVAL;
      return -1;
   }
   // issue the utime call
   return futimens( fh->fd, times );
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
int posix_access( MDAL_CTXT ctxt, const char* path, int mode, int flags ) {
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
int posix_mknod( MDAL_CTXT ctxt, const char* path, mode_t mode, dev_t dev ) {
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
int posix_chmod( MDAL_CTXT ctxt, const char* path, mode_t mode, int flags ) {
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
int posix_lchown( MDAL_CTXT ctxt, const char* path, uid_t owner, gid_t group, int flags ) {
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
int posix_stat( MDAL_CTXT ctxt, const char* path, struct stat* st, int flags ) {
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
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* oldpath : String path of the target file
 * @param const char* newpath : String path of the new hardlink
 * @param int flags : A bitwise OR of the following...
 *                    AT_SYMLINK_NOFOLLOW - do not dereference symlinks
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_link( MDAL_CTXT ctxt, const char* oldpath, const char* newpath, int flags ) {
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
   // issue the link op
   return linkat( pctxt->pathd, oldpath, pctxt->pathd, newpath, flags );
}


/**
 * Create the specified directory
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* path : String path of the new directory
 * @param mode_t mode : Mode value of the new directory (see inode man page)
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_mkdir( MDAL_CTXT ctxt, const char* path, mode_t mode ) {
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
int posix_rmdir( MDAL_CTXT ctxt, const char* path ) {
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
ssize_t posix_readlink( MDAL_CTXT ctxt, const char* path, char* buf, size_t size ) {
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
 * @param MDAL_CTXT ctxt : MDAL_CTXT to operate relative to
 * @param const char* from : String path of the target
 * @param const char* to : Destination string path
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_rename( MDAL_CTXT ctxt, const char* from, const char* to ) {
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
   // issue the rename op
   return renameat( pctxt->pathd, from, pctxt->pathd, to );
}

/**
 * Return statvfs info for the current namespace
 * @param MDAL_CTXT ctxt : MDAL_CTXT to retrieve info for
 * @param struct statvfs* buf : Reference to the statvfs structure to be populated
 * @return int : Zero on success, or -1 if a failure occurred
 */
int posix_statvfs( MDAL_CTXT ctxt, struct statvfs* buf ) {
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
int posix_symlink( MDAL_CTXT ctxt, const char* target, const char* linkname ) {
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
int posix_unlink( MDAL_CTXT ctxt, const char* path ) {
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


//   -------------    POSIX INITIALIZATION    -------------

DAL posix_mdal_init( xmlNode* root ) {
   // first, calculate the number of digits required for pod/cap/block/scatter
   int d_pod = num_digits( max_loc.pod );
   if ( d_pod < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum pod: %d\n", max_loc.pod );
      return NULL;
   }
   int d_cap = num_digits( max_loc.cap );
   if ( d_cap < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum cap: %d\n", max_loc.cap );
      return NULL;
   }
   int d_block = num_digits( max_loc.block );
   if ( d_block < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum block: %d\n", max_loc.block );
      return NULL;
   }
   int d_scatter = num_digits( max_loc.scatter );
   if ( d_scatter < 1 ) {
      errno = EDOM;
      LOG( LOG_ERR, "detected an inappropriate value for maximum scatter: %d\n", max_loc.scatter );
      return NULL;
   }

   // make sure we start on a 'dir_template' node
   if ( root->type == XML_ELEMENT_NODE  &&  strncmp( (char*)root->name, "dir_template", 13 ) == 0 ) {

      // make sure that node contains a text element within it
      if ( root->children != NULL  &&  root->children->type == XML_TEXT_NODE ) {

         // allocate space for our context struct
         POSIX_DAL_CTXT dctxt = malloc( sizeof( struct posix_dal_context_struct ) );
         if ( dctxt == NULL ) { return NULL; } // malloc will set errno

         // copy the dir template into the context struct
         dctxt->dirtmp = strdup( (char*)root->children->content );
         if ( dctxt->dirtmp == NULL ) { free(dctxt); return NULL; } // strdup will set errno

         // initialize all other context fields
         dctxt->tmplen = strlen( dctxt->dirtmp );
         dctxt->max_loc = max_loc;
         dctxt->dirpad = 0;

         // calculate a real value for dirpad based on number of p/c/b/s substitutions
         char* parse = dctxt->dirtmp;
         while ( *parse != '\0' ) {
            if ( *parse == '{' ) {
               // possible substituion, but of what type?
               int increase = 0;
               switch ( *(parse+1) ) {
                  case 'p':
                     increase = d_pod;
                     break;

                  case 'c':
                     increase = d_cap;
                     break;

                  case 'b':
                     increase = d_block;
                     break;

                  case 's':
                     increase = d_scatter;
                     break;
               }
               // if this looks like a valid substitution, check for a final '}'
               if ( increase > 0  &&  *(parse+2) == '}' ) { // NOTE -- we know *(parse+1) != '\0'
                  dctxt->dirpad += increase - 3; // add increase, adjusting for chars used in substitution
               }
            }
            parse++; // next char
         }

         // allocate and populate a new DAL structure
         DAL pdal = malloc( sizeof( struct DAL_struct ) );
         if ( pdal == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for a DAL_struct\n" );
            free(dctxt);
            return NULL;
         } // malloc will set errno
         pdal->name = "posix";
         pdal->ctxt = (DAL_CTXT) dctxt;
         pdal->io_size = 1048576;
         pdal->verify = posix_verify;
         pdal->migrate = posix_migrate;
         pdal->open = posix_open;
         pdal->set_meta = posix_set_meta;
         pdal->get_meta = posix_get_meta;
         pdal->put = posix_put;
         pdal->get = posix_get;
         pdal->abort = posix_abort;
         pdal->close = posix_close;
         pdal->del = posix_del;
         pdal->stat = posix_stat;
         pdal->cleanup = posix_cleanup;
         return pdal;
      }
      else { LOG( LOG_ERR, "the \"dir_template\" node is expected to contain a template string\n" ); }
   }
   else { LOG( LOG_ERR, "root node of config is expected to be \"dir_template\"\n" ); }
   errno = EINVAL;
   return NULL; // failure of any condition check fails the function
}



