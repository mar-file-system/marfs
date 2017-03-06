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


/*****************************************************************************
 *
 * INCLUDE FILES
 *
 ****************************************************************************/

#include <string.h>             /* memset needs this */
#include <stdlib.h>             /* malloc and strto* needs this */
#include <stdio.h>              /* *printf, etc. need this */
#include <strings.h>            /* strcasecmp(), etc */
#include <ctype.h>              /* toupper needs this */
#include <errno.h>              /* checking errno needs this */
#include <unistd.h>             /* access */

#include "logging.h"
#include "PA2X_interface.h"
#include "marfs_configuration.h"
#include "dal.h"
#include "mdal.h"



/*****************************************************************************
 *
 * GLOBAL VARIABLES
 *
 * We'll need these variables to persist for the life of the program. We
 * could probably delcare them as static in the read_configuration
 * function, or declare them globally here.
 *
 * These are static so that even though they are global to this file they
 * cannot be used in other C files. Folks must use the access functions
 * in this file to get the values of the lists and records to which these
 * point.
 *
 * DILEMMA: The previous point implies that read_configuration()
 *     initializes marfs_config, and config-related functions
 *     (e.g. get_repo_by_name()) operate on the default configuration.
 *     This also implies that read_configuration() should just return
 *     success or failure.  All access to contents of the config would go
 *     through the iterator functions (e.g. namespace_next()).
 *
 *     Alternatively, we could get rid of global marfs_config, and
 *     config-related functions (e.g. find_repo_by_name()) could all
 *     require a config argument.  In this scheme read_configuration()
 *     would return a pointer tot he configuration.
 *
 *     We could also do the latter, but say we'll use the default config if
 *     the config-argument is NULL.  This would provide maximum
 *     flexibility, but, for now, we don't need it because we can assume
 *     one configuration per application.
 *
 ****************************************************************************/

MarFS_Config_Ptr            marfs_config = NULL;

/* part of the reason for keeping these private, and forcing everyone to
 * use the iterator approach, is that that allows us to change this
 * implementation (making corresponding changes in the iterators) without
 * affecting any external code.
 */ 
static MarFS_Repo_List      marfs_repo_list = NULL;
static int                  repoCount      = 0;

static MarFS_Namespace_List marfs_namespace_list = NULL;
static int                  namespaceCount = 0;


// STRING() transforms a command-line -D argument-value into a string
// For example, we are given -DPARSE_DIR=/path/to/parse/src, and we want
// "/path/to/parse/src" It requires two steps, like this:
//
#define STRING2(X) #X
#define STRING(X) STRING2(X)


/*****************************************************************************
 *
 * FUNCTION IMPLEMENTATIONS
 *
 ****************************************************************************/


/*****************************************************************************
 *
 * --- support for traversing namespaces (without knowing how they are stored)
 *
 * For example: here's some code to walk all Namespaces, doing something
 *
 *   NSIterator it = namespace_iterator();
 *   MarFS_Namespace_Ptr ns;
 *   while (ns = namespace_next(&it)) {
 *      ... // do something
 *   }
 *
 ****************************************************************************/
NSIterator namespace_iterator() {
  return (NSIterator) { .pos = 0 };
}

MarFS_Namespace_Ptr namespace_next( NSIterator *nsIterator ) {

  if ( nsIterator->pos >= namespaceCount ) {
    return NULL;
  } else {
    // return marfs_config->namespace_list[nsIterator->pos++];
    return marfs_namespace_list[nsIterator->pos++];
  }
}

/*****************************************************************************
 *
 * --- support for traversing repos (without knowing how they are stored)
 *
 * For example: here's some code to walk all Repos, doing something
 *
 *   RepoIterator it = repo_iterator();
 *   MarFS_Repo_Ptr repoPtr;
 *   while (repoPtr = repo_next(&it)) {
 *      ... // do something
 *   }
 *
 ****************************************************************************/
RepoIterator repo_iterator() {
  return (RepoIterator) { .pos = 0 };
}

MarFS_Repo_Ptr repo_next( RepoIterator *repoIterator ) {

  if ( repoIterator->pos >= repoCount ) {
    return NULL;
  } else {
    return marfs_repo_list[repoIterator->pos++];
  }
}

/*****************************************************************************
 *
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
 *
int iterate_marfs_list( void **marfs_list, int ( *marfsPtrCallback )( void *marfsPtr )) {

  int k = 0, retVal;


  while( marfs_list[k] != NULL ) {
    retVal = marfsPtrCallback( marfs_list[k] );
// Check the return value and exit if not 0 (zero)
    k++;
  }
}
 *
 ****************************************************************************/



/*****************************************************************************
 *
 * A couple functions to find a specific namespace record. We'll return
 * the pointer to that namespace record.
 *
 * Find the namespace corresponding to the mnt_suffx in a Namespace struct,
 * which corresponds with a "namespace" managed by fuse.  We might
 * pontentially have many namespaces (should be cheap to have as many as
 * you want), and this lookup is done for every fuse call (and in parallel
 * from pftool).  Also done every time we parse an object-ID xattr!  Thus,
 * this should eventually be made efficient.
 * 
 * One way to make this fast would be to look through all the namespaces
 * and identify the places where a path diverges for different namespaces.
 * This becomes a series of hardcoded substring-ops, on the path.  Each one
 * identifies the next suffix in a suffix tree.  (Attractive Chaos has an
 * open source suffix-array impl).  The leaves would be pointers to
 * Namespaces.
 * 
 * NOTE: If the fuse mount-point (i.e. the configured "mnt_top") is "/A/B",
 * and you provide a path like "/A/B/C", then the "path" seen in fuse
 * callbacks is "/C".  In other words, we should never see MarFS_mnt_top,
 * as part of the incoming path.
 * 
 * For a quick first-cut, there's only one namespace.  Your path is either
 * in it or fails.
 *
 ****************************************************************************/
MarFS_Namespace_Ptr find_namespace_by_name( const char *name ) {

  const size_t     name_len = strlen( name );

  MarFS_Namespace* ns = NULL;
  NSIterator       it = namespace_iterator();
  while ((ns = namespace_next(&it))) {
     if ((ns->name_len == name_len)
         && (! strcmp( ns->name, name ))) {
        return ns;
    }
  }
  return NULL;
}

/*****************************************************************************
 *
 * The path that is passed into this function always starts with the
 * "/" character. That character and any others up to the next "/"
 * character are the namespace's mnt_suffix. A namespace's mnt_suffix
 * must begin with the "/" character and not contain any other "/"
 * characters after the initial one by definition. It is the FUSE
 * mount point and we'll always use a one-level mount point.
 *
 ****************************************************************************/
MarFS_Namespace_Ptr find_namespace_by_mnt_path( const char *mnt_path ) {

   //  int i;
  char *path_dup;
  char *path_dup_token;
  size_t path_dup_len;


  path_dup = strdup( mnt_path );
  path_dup_token = strtok( path_dup, "/" );
  path_dup_len = strlen( path_dup );

/*
 * At this point path_dup will include the leading "/" and any other
 * characters up to, but not including, the next "/" character in
 * path. This includes path_dup being able to be "/" (the root
 * namespace).
 */

  MarFS_Namespace* ns = NULL;
  NSIterator       it = namespace_iterator();
  while ((ns = namespace_next(&it))) {
    if (( ns->mnt_path_len == path_dup_len )	&&
        (! strcmp( ns->mnt_path, path_dup ))) {
      free( path_dup );
      return ns;
    }
  }

  free( path_dup );
  return NULL;
}

/*****************************************************************************
 * Given the full MDFS path to a MD file, return the corresponding NS.
 * This is useful for utilities that are scanning inodes, seeing MDFS
 * paths.
 *
 * We assume that no NS has NS.md_path that is a prefix of another
 * NS.md_path.  This could be ensured by calling this function with each
 * NS.md_path, for each NS, in validate_configuration().  If any such call
 * returns non-NULL, then the given NS is misconfigured.
 ****************************************************************************/

MarFS_Namespace_Ptr find_namespace_by_mdfs_path( const char *mdfs_path ) {

   const size_t mdfs_path_len = strlen(mdfs_path);

   MarFS_Namespace* ns = NULL;
   NSIterator       it = namespace_iterator();
   while ((ns = namespace_next(&it))) {
      if (( ns->md_path_len == mdfs_path_len )	&&
          (! strcmp( ns->md_path, mdfs_path ))) {
         return ns;
      }
   }

   return NULL;
}

/*****************************************************************************
 *
 * A couple functions to find a specific repo record. We'll return
 * the pointer to that repo record.
 *
 * Given a file-size, find the corresponding repo that is the namespace's
 * repository for files of this size.
 *
 ****************************************************************************/
MarFS_Repo_Ptr find_repo_by_range (MarFS_Namespace_Ptr	namespacePtr,
                                   size_t                file_size  ) {   
  int i;

  if ( namespacePtr != NULL ) {
    for ( i = 0; i < namespacePtr->repo_range_list_count; i++ ) {
#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "File size sought is %d.\n", file_size );
  LOG( LOG_INFO, "Namespace is \"%s\"\n", namespacePtr->name );
  LOG( LOG_INFO, "Namespace's repo_range_list[%d]->min_size is %d.\n",
       i, namespacePtr->repo_range_list[i]->min_size );
  LOG( LOG_INFO, "Namespace's repo_range_list[%d]->max_size is %d.\n",
       i, namespacePtr->repo_range_list[i]->max_size );
#endif

      if ((( file_size >= namespacePtr->repo_range_list[i]->min_size ) &&

          (( file_size <= namespacePtr->repo_range_list[i]->max_size ) ||
           ( namespacePtr->repo_range_list[i]->max_size == -1 )))) {

#ifdef _DEBUG_MARFS_CONFIGURATION
         LOG( LOG_INFO, "Repo pointer for this range is '%s'\n",
              namespacePtr->repo_range_list[i]->repo_ptr->name );
#endif

        return namespacePtr->repo_range_list[i]->repo_ptr;
      }
    }
  }
#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "Repo pointer for this range not found. Returning NULL.\n" );
#endif

  return NULL;
}

MarFS_Repo_Ptr find_repo_by_name( const char* name ) {

  MarFS_Repo*   repo = NULL;
  RepoIterator  it = repo_iterator();
  while ((repo = repo_next(&it))) {
    if ( ! strcmp( repo->name, name )) {
      return repo;
    }
  }
  return NULL;
}

/*****************************************************************************
 *
 * This function converts the string that is passed to it to be
 * all upper case. It is done in place. That is, the string itself
 * is modified.
 *
static void stringToUpper( char *stringToConvert ) {

  int k, slen;


  slen = (int) strlen( stringToConvert );
  for ( k = 0; k < slen; k++ ) {
    stringToConvert[k] = toupper( stringToConvert[k] );
  }
}
 ****************************************************************************/

/*****************************************************************************
 *
 * This function sets index to where c is first found in s, and returns 0
 * (zero). If c is not found in s, then -1 is returned.
 *
 ****************************************************************************/
static int find_code_index_in_string( char *s, char c, int *index ) {

  char *ptr = strchr( s, c );
  if ( ptr ) {
    *index = ((int) ( ptr - s ));
  } else {
    return -1;
  }

  return 0;
}

/*****************************************************************************
 *
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
 *
 ****************************************************************************/

int lookup_boolean( const char* str, MarFS_Bool *enumeration ) {

  if ( ! strcasecmp( str, "NO" )) {
    *enumeration = _FALSE;
  } else if ( ! strcasecmp( str, "YES" )) {
    *enumeration = _TRUE;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/

// Find the config-file strings that would corresponding with a given setting.
static const char* accessmethod_str[] = {
   "DIRECT",
   "SEMI_DIRECT",
   "CDMI",
   "SPROXYD",
   "S3",
   "S3_SCALITY",
   "S3_EMC",
   NULL
};
const char* accessmethod_string( MarFS_AccessMethod method) {
   int i;
   for (i=0; accessmethod_str[i]; ++i) {
      if (i == method)
         return accessmethod_str[i];
   }
   return NULL;
}

int lookup_accessmethod( const char* str, MarFS_AccessMethod *enumeration ) {

  if ( ! strcasecmp( str, "DIRECT" )) {
    *enumeration = ACCESSMETHOD_DIRECT;
  } else if ( ! strcasecmp( str, "SEMI_DIRECT" )) {
    *enumeration = ACCESSMETHOD_SEMI_DIRECT;
  } else if ( ! strcasecmp( str, "CDMI" )) {
    *enumeration = ACCESSMETHOD_CDMI;
  } else if ( ! strcasecmp( str, "SPROXYD" )) {
    *enumeration = ACCESSMETHOD_SPROXYD;
  } else if ( ! strcasecmp( str, "S3" )) {
    *enumeration = ACCESSMETHOD_S3;
  } else if ( ! strcasecmp( str, "S3_SCALITY" )) {
    *enumeration = ACCESSMETHOD_S3_SCALITY;
  } else if ( ! strcasecmp( str, "S3_EMC" )) {
    *enumeration = ACCESSMETHOD_S3_EMC;
  } else {
    return -1;
  }

  return 0;
}



/****************************************************************************/

// Find the config-file strings that would corresponding with a given setting.
static const char* securitymethod_str[] = {
   "NONE",
   "S3_AWS_USER",
   "S3_AWS_MASTER",
   "S3_PER_OBJ",
   "HTTP_DIGEST",
   NULL
};
const char* securitymethod_string( MarFS_SecurityMethod method ) {
   int i;
   for (i=0; securitymethod_str[i]; ++i) {
      if (i == method)
         return securitymethod_str[i];
   }
   return NULL;
}

int lookup_securitymethod( const char* str, MarFS_SecurityMethod *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = SECURITYMETHOD_NONE;
  } else if ( ! strcasecmp( str, "S3_AWS_USER" )) {
    *enumeration = SECURITYMETHOD_S3_AWS_USER;
  } else if ( ! strcasecmp( str, "S3_AWS_MASTER" )) {
    *enumeration = SECURITYMETHOD_S3_AWS_MASTER;
  } else if ( ! strcasecmp( str, "S3_PER_OBJ" )) {
    *enumeration = SECURITYMETHOD_S3_PER_OBJ;
  } else if ( ! strcasecmp( str, "HTTP_DIGEST" )) {
    *enumeration = SECURITYMETHOD_HTTP_DIGEST;
  } else {
    return -1;
  }

  return 0;
}


/****************************************************************************/

static char enc_type_index[] = "_";

int lookup_enctype( const char* str, MarFS_EncryptType *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = ENCRYPTTYPE_NONE;
  } else {
    return -1;
  }

  return 0;
}

int encode_enctype( MarFS_EncryptType enumeration, char *code ) {

  if (( enumeration >= ENCRYPTTYPE_NONE )	&&
      ( enumeration <= ENCRYPTTYPE_NONE )) {

    *code = enc_type_index[enumeration];
  } else {
    return -1;
  }

  return 0;
}

int decode_enctype( char code, MarFS_EncryptType *enumeration ) {

  int index;
  if ( ! find_code_index_in_string( enc_type_index, code, &index )) {
    *enumeration = (MarFS_EncryptType) index;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/

static char comptype_index[] = "_";

int lookup_comptype( const char* str, MarFS_CompType *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = COMPTYPE_NONE;
  } else {
    return -1;
  }

  return 0;
}

int encode_comptype( MarFS_CompType enumeration, char *code ) {

  if (( enumeration >= COMPTYPE_NONE )	&&
      ( enumeration <= COMPTYPE_NONE )) {

    *code = comptype_index[enumeration];
  } else {
    return -1;
  }

  return 0;
}

int decode_comptype( char code, MarFS_CompType *enumeration ) {

  int index;
  if ( ! find_code_index_in_string( comptype_index, code, &index )) {
    *enumeration = (MarFS_CompType) index;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/

static char correcttype_index[] = "_";

int lookup_correcttype( const char* str, MarFS_CorrectType *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = CORRECTTYPE_NONE;
  } else {
    return -1;
  }

  return 0;
}

int encode_correcttype( MarFS_CorrectType enumeration, char *code ) {

  if (( enumeration >= CORRECTTYPE_NONE )	&&
      ( enumeration <= CORRECTTYPE_NONE )) {

    *code = correcttype_index[enumeration];
  } else {
    return -1;
  }

  return 0;
}

int decode_correcttype( char code, MarFS_CorrectType *enumeration ) {

  int index;


  if ( ! find_code_index_in_string( correcttype_index, code, &index )) {
    *enumeration = (MarFS_CorrectType) index;
  } else {
    return -1;
  }

  return 0;
}



/*****************************************************************************
 *
 * The parser generated by PA2X from blueprints, which is found in
 * libconfig, just returns string values that are parsed out of the config
 * file.  That parser has no idea of the meaning of fields in the
 * config-file.  For example, repo.host_count is just the string value
 * found in the "<host_count>" tag, inside the "<repo>" tag.
 *
 * Here, we translate those values into corresponding structures with typed
 * values, to be used inside libmarfs.
 *
 ****************************************************************************/


// helper-function to translate the DAL/MDAL "options" parsed by PA2X into
// a vector of xDALConfigOpts.  In the case of no DAL entry, or a DAL with
// no options, the parser gives us opts == 0.  Non-empty options will
// appear as a vector of pointers to "struct opt" (defined in the PA2X
// blueprint), with the last slot NULL.
//
// caller gives us a ptr to their variable which is to hold a vector of ptrs
// (which we will allocate).  Hence, triple indirection.
//
// Even though we are returning the size of the vector, we also allocate an
// extra NULL pointer beyond the end, so traversal can just iterate until
// it finds a null pointer.
    
ssize_t parse_xdal_config_options(xDALConfigOpt*** result_ptr,
                                  struct opt**     opts) {
  *result_ptr = NULL;

  // count the number of options
  ssize_t opt_count = 0;
  if (opts) {
    while (opts[opt_count])
      ++ opt_count;
  }
    
  // generate vector of corresponding xDALConfigOpts
  if (opt_count) {

     *result_ptr = (xDALConfigOpt**)malloc((opt_count +1) * sizeof(xDALConfigOpt*));
    if (! *result_ptr) {
      LOG( LOG_ERR, "Couldn't allocate %ld DAL-option-ptrs.\n", opt_count);
      return -1;
    }
    xDALConfigOpt** result = *result_ptr; /* shorthand */

    // initialize the null-pointer at the tail of the vector
    result[opt_count] = NULL;

    // translate the "opt" structs (from PA2X) into xDALConfigOpt structs
    int   i;
    char* ptr;
    for (i=0; i<opt_count; ++i) {
      char* key = NULL;
      char* val = NULL;

      // configuration-file had a "key_val" option, here?
      if (opts[i]->key_val) {

        // parse "key"
        ptr = opts[i]->key_val;
        while (*ptr && *ptr == ' ')
          ++ ptr;
        if (! *ptr) {
          LOG( LOG_ERR, "Error parsing key in DAL-option %d.\n", i);
          return -1;
        }

        key = ptr;
        size_t key_len = strcspn(key, " :");
        ptr = key + key_len;
        if (! *ptr) {
          LOG( LOG_ERR, "Error parsing value in DAL-option %d.\n", i);
          return -1;
        }

        *ptr = '\0';
        ++ ptr;
        if (! *ptr) {
          LOG( LOG_ERR, "Error parsing value in DAL-option %d\n", i);
          return -1;
        }

        // parse "val" (might have multiple tokens)
        size_t gap_len = strspn(ptr, " :");
        ptr += gap_len;
        val = ptr;
      }

      // configuration-file had a "value" option here.
      // We'll just create an xDALConfigOpt with no key.
      else {

        // parse "value"
        char* ptr = opts[i]->value;
        while (*ptr && *ptr == ' ')
          ++ ptr;
        val = ptr;
      }

      // trim trailing whitespace from <val>
      size_t val_len = strlen(val);
      ptr = val + val_len -1;
      while ((ptr >= val) && (*ptr == ' '))
        *ptr-- = '\0';

      // create xDALConfigOpt
      result[i] = (xDALConfigOpt*)malloc(sizeof(xDALConfigOpt));
      if (! opts[i]) {
        LOG( LOG_ERR, "Couldn't allocate xDALConfigOpt %d\n", i);
        return -1;
      }

      result[i]->key            = (key ? strdup(key) : NULL);
      result[i]->val.value.str  = (val ? strdup(val) : NULL);
      result[i]->val.type       = GVTS_STRING;
    }
  }

  return opt_count;
}


// The "TYPE" arg to EXTRACT() must match the "foo" in some foo_MAX defn in limits.h
// e.g. UCHAR, SHRT, UINT, etc
#include <limits.h>

#define EXTRACT(TYPE, DEST, SRC)                                    \
{ errno = 0;                                                        \
  unsigned long temp = strtoul( SRC, NULL, 10 );                    \
  if ( errno ) {                                                    \
    LOG( LOG_ERR, "Invalid host_offset value of \"%s\".\n", #SRC ); \
    return NULL;                                                    \
  }                                                                 \
  else if (temp > TYPE##_MAX) {                                     \
    LOG( LOG_ERR, "Invalid host_offset value of \"%s\".\n", #SRC ); \
    return NULL;                                                    \
  }                                                                 \
  DEST = (uint8_t)temp;                                             \
}


static MarFS_Config_Ptr read_configuration_internal() {

   struct namespace**     namespaceList;
   struct repo**          repoList;
   int                    i, j, k;
   int                    slen;
   int                    repoRangeCount;
   char*                  perms_dup;
   char*                  tok;
   int                    pd_index;
   MarFS_Repo_Range_List  marfs_repo_range_list;

   struct config* config = read_PA2X_config();
   if (! config) {
      LOG(LOG_ERR, "read_PA2X_config() failed\n");
      return NULL;
   }

   marfs_config = (MarFS_Config_Ptr) malloc( sizeof(MarFS_Config));
   if ( marfs_config == NULL) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS config structure.\n");
      return NULL;
   }
   memset(marfs_config, 0x00, sizeof(MarFS_Config));


  /* REPOS ----------------------------------------------------------------- */
  repoList = (struct repo **) config->repo;
  if (repoList == NULL) {
    LOG( LOG_ERR, "No MarFS repo list structure: config->repo == NULL\n");
    return NULL;
  }


  // count repos
  j = 0;
  while ( repoList[j] != (struct repo *) NULL ) {
#ifdef _DEBUG_MARFS_CONFIGURATION
    // LOG( LOG_INFO, "repoList[%d]->name is \"%s\".\n", j, repoList[j]->name );
#endif
    j++;
  }
  repoCount = j;
  LOG( LOG_INFO, "parser gave us a list of %d repos.\n", repoCount );

  
  // allocate space for marfs_repos
  marfs_repo_list = (MarFS_Repo_List) malloc( sizeof( MarFS_Repo_Ptr ) * ( repoCount + 1 ));
  if ( marfs_repo_list == NULL) {
    LOG( LOG_ERR, "Error allocating memory for the MarFS repo list structure.\n");
    return NULL;
  }
  marfs_repo_list[repoCount] = NULL;


  
  // initialize marfs_repos corresponding to PA2X parsed repos
  for ( j = 0; j < repoCount; j++ ) {
    marfs_repo_list[j] = (MarFS_Repo_Ptr) malloc( sizeof( MarFS_Repo ));
    if ( marfs_repo_list[j] == NULL) {
      LOG( LOG_ERR, "marfs_configuration.c: Error allocating memory for the MarFS repo structure.\n");
      return NULL;
    }
    memset(marfs_repo_list[j], 0, sizeof(MarFS_Repo));

    LOG( LOG_INFO, "processing parsed-repo \"%s\".\n", repoList[j]->name );


    // shorthand
    struct repo*    p_repo = repoList[j];       /* parsed version. All strings */
    MarFS_Repo_Ptr  m_repo = marfs_repo_list[j]; /* marfs version.  To be initialized */

    /*
     * Anything that isn't staying a string and can't be converted
     * to its new type directly needs to be upper-case for easy comparison.
     */

    if (! p_repo->name) {
      LOG( LOG_ERR, "Found an empty Repo.name.\n" );
      return NULL;
    }
    m_repo->name     = strdup( p_repo->name );
    m_repo->name_len = strlen( p_repo->name );


    if (! p_repo->host) {
      LOG( LOG_ERR, "Repo '%s' has an empty <host>.\n", p_repo->name );
      return NULL;
    }
    m_repo->host     = strdup( p_repo->host );
    m_repo->host_len = strlen( p_repo->host );


    if (p_repo->host_offset) {
#if 0
       errno = 0;
       unsigned long temp = strtoul( p_repo->host_offset, NULL, 10 );
       if ( errno ) {
          LOG( LOG_ERR, "Invalid host_offset value of \"%s\".\n", p_repo->host_offset );
          return NULL;
       }
       else if (temp > 255) { // m_repo->host_offset is uint8_t
          LOG( LOG_ERR, "Invalid host_offset value of \"%s\".\n", p_repo->host_offset );
          return NULL;
       }
       m_repo->host_offset = (uint8_t)temp;
#else
       EXTRACT(UCHAR, m_repo->host_offset, p_repo->host_offset);
#endif
    }
    else
       m_repo->host_offset = 0;

    if (p_repo->host_count) {
       errno = 0;
       unsigned long temp = strtoul( p_repo->host_count, NULL, 10 );
       if ( errno ) {
          LOG( LOG_ERR, "Invalid host_count value of \"%s\".\n", p_repo->host_count );
          return NULL;
       }
       else if (temp > 255) { // m_repo->host_count is uint8_t
          LOG( LOG_ERR, "Invalid host_count value of \"%s\".\n", p_repo->host_count );
          return NULL;
       }
       m_repo->host_count = (uint8_t)temp;

       // if user specifies a host_count > 1, then host must be a format-string
       if (temp > 1) {
          if (! strstr(m_repo->host, "%d")) {
             LOG( LOG_ERR,
                  "If host_count>1, then host must include '%%d'.  " 
                  "This will be used to print a randomized per-thread host-name, something like "
                  "'sprintf(host_name, config.host, "
                  "config.host_offset + (rand() %% config.host_count))'\n");
             return NULL;
          }
       }
    }
    else
       m_repo->host_count = 1;


    // .................................................................
    // DAL
    // PA2X parsed the DAL type, as well as any options.
    // Convert those options to xDALConfigOpt, and invoke DAL.config()
    // .................................................................

    // default to OBJ, for backward-compatibility.  If repo-spec has no DAL
    // entry, we'll see (p_repo->dal.type == 0), from the parser.  In that
    // case we default to an OBJECT DAL.  (Note that the object DAL can be
    // safely configured with empty configuration options).
    const char* dal_name = NULL;
    if ( ! p_repo->dal.type ) {
       LOG( LOG_INFO, "MarFS repo '%s' has no DAL. Defaulting to OBJ\n",
            p_repo->name );
       dal_name = strdup("OBJECT");
    }
    else
       dal_name = strdup(p_repo->dal.type);

    // Go find the built-in DAL matching the type
    // Then make a copy to be private to this repo.
    DAL* dal = get_DAL(dal_name);
    if (! dal) {
      LOG( LOG_ERR, "Couldn't find DAL named '%s' for repo '%s'.\n",
           dal_name, p_repo->name);
      return NULL;
    }
    DAL* dal_copy = (DAL*)malloc(sizeof(DAL));
    *dal_copy = *dal;


    // convert any DAL options from PA2X strings to xDALConfigOpt
    xDALConfigOpt** opts = NULL; /* array of ptrs */
    ssize_t         opt_count = parse_xdal_config_options(&opts, p_repo->dal.opt);
    if (opt_count < 0) {
      LOG(LOG_ERR, "Couldn't parse DAL-options for repo '%s'\n", p_repo->name);
      return NULL;
    }
    LOG( LOG_INFO, "parser gave us a list of %ld DAL options, for repo '%s'.\n",
         opt_count, p_repo->name );

    // install the options, parsed out above.
    if ((*dal_copy->config)(dal_copy, opts, opt_count)) {
      LOG( LOG_ERR, "DAL_config failed for repo '%s'.\n",
           p_repo->name);
      return NULL;
    }
    m_repo->dal = dal_copy;


    // .................................................................
    // convert remaining PA2X strings into marfs_repo member values.
    // .................................................................

    if ( lookup_boolean( p_repo->update_in_place, &( m_repo->update_in_place ))) {
      LOG( LOG_ERR, "Invalid update_in_place value of \"%s\".\n", p_repo->update_in_place );
      return NULL;
    }

    if ( lookup_boolean( p_repo->ssl, &( m_repo->ssl ))) {
      LOG( LOG_ERR, "Invalid ssl value of \"%s\".\n", p_repo->ssl );
      return NULL;
    }

    if ( lookup_accessmethod( p_repo->access_method, &( m_repo->access_method ))) {
      LOG( LOG_ERR, "Invalid access_method value of \"%s\".\n", p_repo->access_method );
      return NULL;
    }


    if (! p_repo->chunk_size) {
       LOG( LOG_ERR, "Repo '%s' has no chunk_size.\n", p_repo->name );
      return NULL;
    }
    errno = 0;
    m_repo->chunk_size = strtoull( p_repo->chunk_size, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid chunk_size value of \"%s\".\n", p_repo->chunk_size );
      return NULL;
    }

    // repo.max_get_size was added in version 0.2.
    // Allow old configurations to be parsed by assigning a default (of 0)
    m_repo->max_get_size = 0;
    if (p_repo->max_get_size) {
       errno = 0;
       m_repo->max_get_size = strtoull( p_repo->max_get_size, (char **) NULL, 10 );
       if ( errno ) {
          LOG( LOG_ERR, "Invalid max_get_size value of \"%s\".\n", p_repo->max_get_size );
          return NULL;
       }
    }

#if 0
    if (! p_repo->pack_size) {
       LOG( LOG_ERR, "Repo '%s' has no pack_size.\n", p_repo->name );
      return NULL;
    }
    errno = 0;
    m_repo->pack_size = strtoull( p_repo->pack_size, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid pack_size value of \"%s\".\n", p_repo->pack_size );
      return NULL;
    }
#else

    // max number of files to be allowed in a packed file.
    // -1 means no maximum.  0 means no packing.
    if (! p_repo->max_pack_file_count) {
       LOG( LOG_ERR, "Repo '%s' has no max_pack_file_count.\n",
            p_repo->name );
      return NULL;
    }
    errno = 0;
    m_repo->max_pack_file_count
       = strtoll( p_repo->max_pack_file_count, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid max_pack_file_count value of \"%s\".\n",
           p_repo->max_pack_file_count );
      return NULL;
    }


    // if max_pack_file_count = 0, packing is disabled, and we ignore all
    // other packing-related parameters.  This means you don't have to make
    // up meaningless values for fields that are going to be ignored.
    if (! m_repo->max_pack_file_count) {

       // packing is disabled.  Allow config to ignore other
       // packing-related parameters.
       m_repo->min_pack_file_count = -1;
       m_repo->min_pack_file_size = -1;
       m_repo->max_pack_file_size = -1;
    }
    else {

       // packing is enabled.  The other packing-related parameters will be
       // parssed and validated.

       if (! p_repo->min_pack_file_count) {
          LOG( LOG_ERR, "Repo '%s' has no min_pack_file_count.\n",
               p_repo->name );
          return NULL;
       }
       errno = 0;
       m_repo->min_pack_file_count
          = strtoll( p_repo->min_pack_file_count, (char **) NULL, 10 );
       if ( errno ) {
          LOG( LOG_ERR, "Invalid min_pack_file_count value of \"%s\".\n",
               p_repo->min_pack_file_count );
          return NULL;
       }


       if (! p_repo->max_pack_file_size) {
          LOG( LOG_ERR, "Repo '%s' has no max_pack_file_size.\n",
               p_repo->name );
          return NULL;
       }
       errno = 0;
       m_repo->max_pack_file_size
          = strtoll( p_repo->max_pack_file_size, (char **) NULL, 10 );
       if ( errno ) {
          LOG( LOG_ERR, "Invalid max_pack_file_size value of \"%s\".\n",
               p_repo->max_pack_file_size );
          return NULL;
       }


       if (! p_repo->min_pack_file_size) {
          LOG( LOG_ERR, "Repo '%s' has no min_pack_file_size.\n", p_repo->name );
          return NULL;
       }
       errno = 0;
       m_repo->min_pack_file_size
          = strtoll( p_repo->min_pack_file_size, (char **) NULL, 10 );
       if ( errno ) {
          LOG( LOG_ERR, "Invalid min_pack_file_size value of \"%s\".\n",
               p_repo->min_pack_file_size );
          return NULL;
       }

    }

#endif

    if ( lookup_securitymethod( p_repo->security_method, &( m_repo->security_method ))) {
      LOG( LOG_ERR, "Invalid security_method value of \"%s\".\n", p_repo->security_method );
      return NULL;
    }

    if ( lookup_enctype( p_repo->enc_type, &( m_repo->enc_type ))) {
      LOG( LOG_ERR, "Invalid enc_type value of \"%s\".\n", p_repo->enc_type );
      return NULL;
    }

    if ( lookup_comptype( p_repo->comp_type, &( m_repo->comp_type ))) {
      LOG( LOG_ERR, "Invalid comp_type value of \"%s\".\n", p_repo->comp_type );
      return NULL;
    }

    if ( lookup_correcttype( p_repo->correct_type, &( m_repo->correct_type ))) {
      LOG( LOG_ERR, "Invalid correct_type value of \"%s\".\n", p_repo->correct_type );
      return NULL;
    }

    m_repo->online_cmds = NULL;
    m_repo->online_cmds_len = 0;

    if (! p_repo->latency) {
       LOG( LOG_ERR, "Repo '%s' has no latency.\n", p_repo->name );
      return NULL;
    }
    errno = 0;
    m_repo->latency = strtoull( p_repo->latency, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid latency value of \"%s\".\n", p_repo->latency );
      return NULL;
    }

    // default Repo.write_timeout = 0 means we will fall back to constants
    // hardcoded in object_stream.c
    // TBD: Use a constant defined in a header file, and put that in here.
    errno = 0;
    unsigned long wr_timeout = (p_repo->write_timeout
                                ? strtoul( p_repo->write_timeout, (char **) NULL, 10 )
                                : 0);
    if ( errno || (wr_timeout > (uint16_t)-1)) {
      LOG( LOG_ERR, "Invalid write_timeout value \"%s\".\n", p_repo->write_timeout );
      return NULL;
    }
    m_repo->write_timeout = wr_timeout;


    // default Repo.read_timeout = 0 means we will fall back to constants
    // hardcoded in object_stream.c
    // TBD: Use a constant defined in a header file, and put that in here.
    errno = 0;
    unsigned long rd_timeout = (p_repo->read_timeout
                                ? strtoul( p_repo->read_timeout, (char **) NULL, 10 )
                                : 0);
    if ( errno || (rd_timeout > (uint16_t)-1)) {
      LOG( LOG_ERR, "Invalid read_timeout value \"%s\".\n", p_repo->read_timeout );
      return NULL;
    }
    m_repo->read_timeout = rd_timeout;
  }
  free( repoList );






  /* NAMESPACE ----------------------------------------------------------------- */

  namespaceList = (struct namespace **) config->namespace;

  // count namespaces
  j = 0;
  while ( namespaceList[j] != (struct namespace *) NULL ) {
#ifdef _DEBUG_MARFS_CONFIGURATION
     // LOG( LOG_INFO, "namespaceList[%d]->name is \"%s\".\n", j, namespaceList[j]->name );
#endif
    j++;
  }
  namespaceCount = j;
  LOG( LOG_INFO, "parser gave us a list of %d namespaces.\n", namespaceCount );

  marfs_namespace_list = (MarFS_Namespace_List) malloc( sizeof( MarFS_Namespace_Ptr ) * ( namespaceCount + 1 ));
  if ( marfs_namespace_list == NULL) {
    LOG( LOG_ERR, "Error allocating memory for the MarFS namespace list structure.\n");
    return NULL;
  }
  marfs_namespace_list[namespaceCount] = NULL;


  for ( j = 0; j < namespaceCount; j++ ) {
    marfs_namespace_list[j] = (MarFS_Namespace_Ptr) malloc( sizeof( MarFS_Namespace ));
    if ( ! marfs_namespace_list[j] ) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS namespace structure.\n");
      return NULL;
    }
    memset(marfs_namespace_list[j], 0, sizeof(MarFS_Namespace));

    LOG( LOG_INFO, "processing parsed-NS \"%s\".\n", namespaceList[j]->name );


    // shorthand
    struct namespace*    p_ns = namespaceList[j];        /* parsed version. All strings */
    MarFS_Namespace_Ptr  m_ns = marfs_namespace_list[j]; /* marfs version.  To be initialized */


    if (! p_ns->name) {
      LOG( LOG_ERR, "Found an empty Namespace.name.\n" );
      return NULL;
    }
    m_ns->name = strdup( p_ns->name );
    m_ns->name_len = strlen( p_ns->name );

    if ( ! p_ns->alias ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no alias.\n", p_ns->name);
       return NULL;
    }
    m_ns->alias = strdup( p_ns->alias );
    m_ns->alias_len = strlen( p_ns->alias );

    if ( ! p_ns->mnt_path ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no mnt_path.\n", p_ns->name);
      return NULL;
    }
    m_ns->mnt_path = strdup( p_ns->mnt_path );
    m_ns->mnt_path_len = strlen( p_ns->mnt_path );

/*
 * Because strtok destroys the string on which it operates, we're going to
 * make a copy of it first. But, we want to make sure it does not have any
 * whitespace in it so that we get the permission mnemonics without
 * whitespace that are delimited by a comma.
 *
 * Also, the parser does not allow tags without any contents, but we want to allow
 * perms to be empty.  Therefore, we allow the value "NONE" to appear, which
 * we treat as a no-op.
 */

    slen = (int) strlen( p_ns->bperms );
    perms_dup = (char *) malloc( slen + 1 );
    pd_index = 0;

    for ( k = 0; k < slen; k++ ) {
      if ( ! isspace( p_ns->bperms[k] )) {
        perms_dup[pd_index] = p_ns->bperms[k];
        pd_index++;
      }
    }
    perms_dup[pd_index] = '\0';

    tok = strtok( perms_dup, "," );
    while ( tok != NULL ) {
      if ( ! strcasecmp( tok, "RM" )) {
        m_ns->bperms |= R_META;
      } else if ( ! strcasecmp( tok, "WM" )) {
        m_ns->bperms |= W_META;
      } else if ( ! strcasecmp( tok, "RD" )) {
        m_ns->bperms |= R_DATA;
      } else if ( ! strcasecmp( tok, "WD" )) {
        m_ns->bperms |= W_DATA;
      } else if ( ! strcasecmp( tok, "TD" )) {
        m_ns->bperms |= T_DATA;
      } else if ( ! strcasecmp( tok, "UD" )) {
        m_ns->bperms |= U_DATA;
      } else if ( strcasecmp( tok, "NONE" )) {
        LOG( LOG_ERR, "Invalid bperms value of \"%s\".\n", tok );
        return NULL;
      }

      tok = strtok( NULL, "," );
    }

    free( perms_dup );

    slen = (int) strlen( p_ns->iperms );
    perms_dup = (char *) malloc( slen + 1 );
    pd_index = 0;

    for ( k = 0; k < slen; k++ ) {
      if ( ! isspace( p_ns->iperms[k] )) {
        perms_dup[pd_index] = p_ns->iperms[k];
        pd_index++;
      }
    }
    perms_dup[pd_index] = '\0';

    tok = strtok( perms_dup, "," );
    while ( tok != NULL ) {
      if ( ! strcasecmp( tok, "RM" )) {
        m_ns->iperms |= R_META;
      } else if ( ! strcasecmp( tok, "WM" )) {
        m_ns->iperms |= W_META;
      } else if ( ! strcasecmp( tok, "RD" )) {
        m_ns->iperms |= R_DATA;
      } else if ( ! strcasecmp( tok, "WD" )) {
        m_ns->iperms |= W_DATA;
      } else if ( ! strcasecmp( tok, "TD" )) {
        m_ns->iperms |= T_DATA;
      } else if ( ! strcasecmp( tok, "UD" )) {
        m_ns->iperms |= U_DATA;
      } else {
        LOG( LOG_ERR, "Invalid iperms value of \"%s\".\n", tok );
        return NULL;
      }

      tok = strtok( NULL, "," );
    }

    free( perms_dup );

    if ( ! p_ns->md_path ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no md_path.\n", p_ns->md_path);
      return NULL;
    }
    m_ns->md_path = strdup( p_ns->md_path );
    m_ns->md_path_len = strlen( p_ns->md_path );


    /* iwrite_repo */
    m_ns->iwrite_repo = find_repo_by_name( p_ns->iwrite_repo_name );

    if (! m_ns->iwrite_repo) {
        LOG( LOG_ERR, "Couldn't find iwrite_repo named \"%s\", for namespace \"%s\".\n",
             p_ns->iwrite_repo_name,
             m_ns->name );
        return NULL;
    }

/*
 * For now we'll set this to one (1). Once the configuration parser is fixed we can
 * potentially have more than one range per namespace.
 *
 * 9/22/15: We now have the ability to access a list within the namespace list.
 *          So, this kludge of forcing one repo range per namespace is being
 *          commented out and replaced with the repo range code that follows
 *          this comment block.

    repoRangeCount = 1;

    marfs_repo_range_list = (MarFS_Repo_Range_List) malloc( sizeof( MarFS_Repo_Range_Ptr ) * ( repoRangeCount + 1 ));
    if ( ! marfs_repo_range_list ) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS repo range list structure.\n");
      return NULL;
    }
    marfs_repo_range_list[repoRangeCount] = NULL;

    marfs_repo_range_list[0] = (MarFS_Repo_Range_Ptr) malloc( sizeof( MarFS_Repo_Range ));
    if ( ! marfs_repo_range_list[0] ) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS repo range structure.\n");
      return NULL;
    }

    marfs_repo_range_list[0]->min_size = atoi( p_ns->range[0]->min_size );
    marfs_repo_range_list[0]->max_size = atoi( p_ns->range[0]->max_size );
    marfs_repo_range_list[0]->repo_ptr = find_repo_by_name( p_ns->range[0]->repo_name );
 */

/*
 * The configuration parser allows access to the list of repo ranges for a namespace.
 * Here we build the list that is assigned to the MarFS namespace structure.
 */

    k = 0;
    while ( p_ns->range[k] != (struct range *) NULL ) {
      k++;
    }
    repoRangeCount = k;
    LOG( LOG_INFO, "parser gave us a list of %d ranges, for namespace \"%s\".\n",
         repoRangeCount, p_ns->name );

    marfs_repo_range_list = (MarFS_Repo_Range_List) malloc( sizeof( MarFS_Repo_Range_Ptr ) * ( repoRangeCount + 1 ));
    if ( ! marfs_repo_range_list ) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS repo range list structure.\n");
      return NULL;
    }
    marfs_repo_range_list[repoRangeCount] = NULL;

    for ( k = 0; k < repoRangeCount; k++ ) {
      marfs_repo_range_list[k] = (MarFS_Repo_Range_Ptr) malloc( sizeof( MarFS_Repo_Range ));
      if ( ! marfs_repo_range_list[k] ) {
        LOG( LOG_ERR, "Error allocating memory for the MarFS repo range structure.\n");
        return NULL;
      }
      memset(marfs_repo_range_list[k], 0, sizeof(MarFS_Repo_Range));
    
      marfs_repo_range_list[k]->min_size = atoi( p_ns->range[k]->min_size );
      marfs_repo_range_list[k]->max_size = atoi( p_ns->range[k]->max_size );
      marfs_repo_range_list[k]->repo_ptr = find_repo_by_name( p_ns->range[k]->repo_name );

      if (! marfs_repo_range_list[k]->repo_ptr ) {
        LOG( LOG_ERR, "Couldn't find iwrite_repo named \"%s\", "
             "for range[%d] in namespace \"%s\".\n",
             p_ns->iwrite_repo_name,
             k,
             m_ns->name );
        return NULL;
      }

    }

    m_ns->repo_range_list = marfs_repo_range_list;
    m_ns->repo_range_list_count = repoRangeCount;

    if ( ! p_ns->trash_md_path ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no trash_md_path.\n", p_ns->name);
      return NULL;
    }
    m_ns->trash_md_path = strdup( p_ns->trash_md_path );
    m_ns->trash_md_path_len = strlen( p_ns->trash_md_path );


    if ( ! p_ns->fsinfo_path ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no fsinfo_path.\n", p_ns->name);
      return NULL;
    }
    m_ns->fsinfo_path = strdup( p_ns->fsinfo_path );
    m_ns->fsinfo_path_len = strlen( p_ns->fsinfo_path );


    if ( ! p_ns->quota_space ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no quota_space.\n", p_ns->name );
      return NULL;
    }
    errno = 0;
    m_ns->quota_space = strtoll( p_ns->quota_space, (char **) NULL, 10 );   if ( errno ) {
      LOG( LOG_ERR, "Invalid quota_space value of \"%s\".\n", p_ns->quota_space );
      return NULL;
    }


    if ( ! p_ns->quota_names ) {
       LOG( LOG_ERR, "MarFS namespace '%s' has no quota_names.\n", p_ns->name );
      return NULL;
    }
    errno = 0;
    m_ns->quota_names = strtoll( p_ns->quota_names, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid quota_names value of \"%s\".\n", p_ns->quota_names );
      return NULL;
    }



#if 0
    // default to POSIX, for backward-compatibility
    // [co-maintain with file_MDAL tests, below]
    const char* dir_MDAL_name = strdup("POSIX");
    if ( ! p_ns->dir_MDAL ) {
       LOG( LOG_INFO, "MarFS namespace '%s' has no dir_MDAL. Defaulting to POSIX\n",
            p_ns->name );
    }
    else
       dir_MDAL_name = strdup(p_ns->dir_MDAL);

    m_ns->dir_MDAL_name = dir_MDAL_name;
    m_ns->dir_MDAL      = NULL; // see validate_config() in libmarfs
#endif


#if 0
    // default to POSIX, for backward-compatibility
    // [co-maintain with dir_MDAL tests, above]
    const char* file_MDAL_name = strdup("POSIX");
    if ( ! p_ns->file_MDAL ) {
       LOG( LOG_INFO, "MarFS namespace '%s' has no file_MDAL. Defaulting to POSIX\n",
            p_ns->name );
    }
    else
       file_MDAL_name = strdup(p_ns->file_MDAL);

    m_ns->file_MDAL_name = file_MDAL_name;
    m_ns->file_MDAL      = NULL; // see validate_config() in libmarfs
#endif



    // .................................................................
    // MDAL
    // PA2X parsed the MDAL type, as well as any options.
    // Convert those options to xDALConfigOpt, and invoke MDAL.config()
    // .................................................................

    // same operations for both f_ns->f_mdal, m_ns->d_mdal
    for (i=0; i<2; ++i) {

       // generic vars allow re-using code for file/dir MDALs
       char*         iteration_name = NULL;
       MDAL**        m_ns_mdal      = NULL;
       char*         p_ns_mdal_type = NULL;
       struct opt**  p_ns_mdal_opt  = NULL;

       if (! i) {
          iteration_name = "file";
          m_ns_mdal      = &m_ns->file_MDAL;
          p_ns_mdal_type = p_ns->f_mdal.type;
          p_ns_mdal_opt  = p_ns->f_mdal.opt;
       }
       else {
          iteration_name = "dir";
          m_ns_mdal      = &m_ns->dir_MDAL;
          p_ns_mdal_type = p_ns->d_mdal.type;
          p_ns_mdal_opt  = p_ns->d_mdal.opt;
       }

       // default to POSIX, for backward-compatibility.  If ns-spec has no
       // MDAL entry, we'll see (p_ns->mdal.type == 0), from the parser.  In
       // that case we default to a POSIX MDAL (for both file and dir).  (Note
       // that the POSIX MDAL can be safely configured with empty
       // configuration options).
       const char* mdal_name = NULL;
       if ( ! p_ns_mdal_type ) {
          LOG( LOG_INFO, "MarFS ns '%s' has no %s-MDAL. Defaulting to POSIX\n",
               p_ns->name, iteration_name );
          mdal_name = strdup("POSIX");
       }
       else
          mdal_name = strdup(p_ns_mdal_type);

       // Go find the built-in MDAL matching the type
       // Then make a copy to be private to this ns.
       MDAL* mdal = get_MDAL(mdal_name);
       if (! mdal) {
          LOG( LOG_ERR, "Couldn't find %s-MDAL named '%s' for ns '%s'.\n",
               iteration_name, mdal_name, p_ns->name);
          return NULL;
       }
       MDAL* mdal_copy = (MDAL*)malloc(sizeof(MDAL));
       *mdal_copy = *mdal;


       // convert any MDAL options from PA2X strings to xDALConfigOpt
       xDALConfigOpt** opts = NULL; /* array of ptrs */
       ssize_t         opt_count = parse_xdal_config_options(&opts, p_ns_mdal_opt);
       if (opt_count < 0) {
          LOG(LOG_ERR, "Couldn't parse %s-MDAL-options for ns '%s'\n",
              iteration_name, p_ns->name);
          return NULL;
       }
       LOG( LOG_INFO, "parser gave us a list of %ld %s-MDAL options, for namespace '%s'.\n",
            opt_count, iteration_name, p_ns->name );

       // install the options, parsed out above.
       if ((*mdal_copy->config)(mdal_copy, opts, opt_count)) {
          LOG( LOG_ERR, "%s MDAL_config failed for ns '%s'.\n",
               iteration_name, p_ns->name);
          return NULL;
       }
       *m_ns_mdal = mdal_copy;

    }

  }
  free( namespaceList );






  /* CONFIG ----------------------------------------------------------------- */

  if ( ! config->name ) {
     LOG( LOG_ERR, "MarFS config has no name.\n" );
     return NULL;
  }
  marfs_config->name     = strdup( config->name );
  marfs_config->name_len = strlen( config->name );

  if ( ! config->version ) {
     LOG( LOG_ERR, "MarFS config '%s' has no version.\n", config->name);
     return NULL;
  }
  char* version_str      = strdup(config->version);
  char* version_tok      = strtok(version_str, ".");
  marfs_config->version_major = ((version_tok) ? strtol( version_tok, NULL, 10) : 0);
  version_tok            = strtok(NULL, ".");
  marfs_config->version_minor = ((version_tok) ? strtol( version_tok, NULL, 10) : 0);
  free(version_str);

  // make sure this SW handles this config-file version
  if ((   marfs_config->version_major != MARFS_CONFIG_MAJOR)
      || (marfs_config->version_minor >  MARFS_CONFIG_MINOR)) {
     LOG( LOG_ERR, "Config-reader %d.%d found incompatible config vers %d.%d\n",
          MARFS_CONFIG_MAJOR, MARFS_CONFIG_MINOR,
          marfs_config->version_major, marfs_config->version_minor);
     return NULL;
  }


  if ( ! config->mnt_top ) {
     LOG( LOG_ERR, "MarFS config '%s' has no mnt_top.\n", config->name);
     return NULL;
  }
  if ( config->mnt_top[0] != '/' ) {
     LOG( LOG_ERR, "MarFS config '%s' mnt_top should start with '/'.\n", config->name);
     return NULL;
  }
  size_t mnt_top_len = strlen( config->mnt_top );
  if ( config->mnt_top[ mnt_top_len -1 ] == '/' ) {
     LOG( LOG_ERR, "MarFS config '%s' mnt_top should not have final '/'.\n", config->name);
     return NULL;
  }
  marfs_config->mnt_top     = strdup( config->mnt_top );
  marfs_config->mnt_top_len = strlen( config->mnt_top );


  // if you really don't want to protect yourself, put something in mdfs_top
  // that will never occur at the front of your MDFS paths.
  if ( ! config->mdfs_top ) {
     LOG( LOG_ERR, "MarFS config '%s' has no mdfs_top.\n", config->name);
     return NULL;
  }
  if ( config->mdfs_top[0] != '/' ) {
     LOG( LOG_ERR, "MarFS config '%s' mdfs_top should start with '/'.\n", config->name);
     return NULL;
  }
  size_t mdfs_top_len = strlen( config->mdfs_top );
  if ( config->mnt_top[ mdfs_top_len -1 ] == '/' ) {
     LOG( LOG_ERR, "MarFS config '%s' mdfs_top should not have final '/'.\n", config->name);
     return NULL;
  }
  marfs_config->mdfs_top     = strdup( config->mdfs_top );
  marfs_config->mdfs_top_len = mdfs_top_len;

  // marfs_config->namespace_list = marfs_namespace_list;
  // marfs_config->namespace_count = namespaceCount;

#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "\n" );
  LOG( LOG_INFO, "The members of the config structure are:\n" );
  LOG( LOG_INFO, "\tconfig name            : %s\n", marfs_config->name );
  LOG( LOG_INFO, "\tconfig version         : %d.%d\n", marfs_config->version_major, marfs_config->version_minor );
  LOG( LOG_INFO, "\tconfig mnt_top         : %s\n", marfs_config->mnt_top );
  LOG( LOG_INFO, "\tconfig mdfs_top        : %s\n", marfs_config->mdfs_top );
  // LOG( LOG_INFO, "\tconfig namespace count : %lu\n", marfs_config->namespace_count );
  LOG( LOG_INFO, "\tconfig repo count      : %d\n", repoCount );
  fflush( stdout );
#endif

/*
 * This seems to generate an error indicating we're freeing a pointer not allocated.
 *
 *  freeConfigStructContent( config );
 */

  return marfs_config;
}

// return 0 for success, non-zero for failure
// We assume that no config is a failure.
//
int read_configuration() {
   marfs_config = read_configuration_internal();
   return ((marfs_config) ? 0 : 1);
}




/*****************************************************************************
 *
 * When the user is done with the configuration, for example if run-time
 * updates to a configuration are allowed, the memory must be returned
 * to the system to avoid memory leaks. This function properly frees all
 * the dynamically allocated memory to the system.
 *
 * -1 is returned if there is an error freeing the config, otherwise 0
 * (zero) is returned on success. The config will be set to NULL.
 *
 ****************************************************************************/

static int free_configuration_internal( MarFS_Config_Ptr *config ) {

  int j, k;


/*
 * First free the repos from the global list. Before freeing the actual repo
 * we first free the repo members that were dynamically allocated, then
 * we free the repo. Once we've freed each of the repos, we free the list.
 */

  for ( j = 0; j < repoCount; j++ ) {
    free( marfs_repo_list[j]->name );
    free( marfs_repo_list[j]->host );
    free( marfs_repo_list[j]->online_cmds );
    free( marfs_repo_list[j] );
  }
  free( marfs_repo_list );
  marfs_repo_list = NULL;

  for ( j = 0; j < namespaceCount; j++ ) {
    free( marfs_namespace_list[j]->name );
    free( marfs_namespace_list[j]->mnt_path );
    free( marfs_namespace_list[j]->md_path );

/*
 * The only dynamically allocated part of a namespace_repo_range_list
 * is a pointer to the repo itself. The repos were all freed in the
 * prior loop, so we just need to free the pointers and the list
 * itself.
 */

    k = 0;
    while( marfs_namespace_list[j]->repo_range_list[k] != NULL ) {
      free( marfs_namespace_list[j]->repo_range_list[k] );
      k++;
    }
    free( marfs_namespace_list[j]->repo_range_list );

    free( marfs_namespace_list[j]->trash_md_path );
    free( marfs_namespace_list[j]->fsinfo_path );
    free( marfs_namespace_list[j] );
  }
  free( marfs_namespace_list );
  marfs_namespace_list = NULL;

  free( marfs_config->name );
  free( marfs_config->mnt_top );

/*
 * The marfs_namespace_list was assigned to point to the
 * marfs_namespace_list. It is now a dangling pointer, as all it was
 * pointing at was freed. It would be an error to free it at this
 * point, so we'll just note that here and move on.
 *
 * free( marfs_config->namespace_list );
 */

  free( marfs_config );
  marfs_config = NULL;
  *config = marfs_config;

  return 0;
}

int free_configuration() {
   return free_configuration_internal(&marfs_config);
}




// ---------------------------------------------------------------------------
// run-time configuration
// ---------------------------------------------------------------------------

// e.g. set_runtime(MARFS_INTERACTIVE,1);
int  set_runtime_config(MarFS_RunTime_Flag flag, int value) {
   if (! marfs_config) {
      LOG(LOG_ERR, "No marfs_config\n");
      return -1;
   }
   else if (value)
      marfs_config->runtime.flags |= flag;
   else
      marfs_config->runtime.flags &= ~flag;

   return 0;
}

int  get_runtime_config(MarFS_RunTime_Flag flag) {
   if (! marfs_config) {
      LOG(LOG_ERR, "No marfs_config\n");
      return -1;
   }
   else
      return ((marfs_config->runtime.flags & flag) != 0);
}



// ---------------------------------------------------------------------------
// diagnostics
// ---------------------------------------------------------------------------


int debug_range_list( MarFS_Repo_Range** range_list,
                     int                 range_list_count ) {
   int i;
   for (i=0; i<range_list_count; ++i) {
      fprintf( stdout, "\t\t[%d] (min: %d, max: %d) -> %s\n",
               i,
               range_list[i]->min_size,
               range_list[i]->max_size,
               (range_list[i]->repo_ptr ? range_list[i]->repo_ptr->name : "NULL") );
   }
   return 0;
}


int debug_xdal_config_options(xDALConfigOpt** opt) {
   while (opt && *opt) {
      xDALConfigOpt* o   = *opt;
      const char*    key = (o->key ? o->key : "");

      // want quotes around the string itself, with padding afterwards, but not truncation
      const int      target_field_width = 14; // pad key field with blanks, out to this size
      size_t         key_str_len = strlen(key);
      int            pad_size = ((key_str_len > target_field_width)
				? 0 : (target_field_width - key_str_len));

      fprintf(stdout, "\t\tkey: '%s'%*s val: '%s'\n",
              key, pad_size, "",
              ((o->val.type == GVTS_STRING) ? o->val.value.str : "UNK"));
      ++ opt;
   }
   return 0;
}

int debug_namespace( MarFS_Namespace* ns ) {
   fprintf(stdout, "Namespace\n");
   fprintf(stdout, "\tname               %s\n",   ns->name );
   fprintf(stdout, "\tname_len           %ld\n",  ns->name_len);
   fprintf(stdout, "\tmnt_path           %s\n",   ns->mnt_path);
   fprintf(stdout, "\tmnt_path_len       %ld\n",  ns->mnt_path_len);
   fprintf(stdout, "\tbperms             0x%x\n", ns->bperms);
   fprintf(stdout, "\tiperms             0x%x\n", ns->iperms);
   fprintf(stdout, "\tmd_path            %s\n",   ns->md_path);
   fprintf(stdout, "\tmd_path_len        %ld\n",  ns->md_path_len);
   fprintf(stdout, "\tiwrite_repo        %s\n",   ns->iwrite_repo->name);

   fprintf(stdout, "\trepo_range_list\n");
   debug_range_list(ns->repo_range_list, ns->repo_range_list_count);

   fprintf(stdout, "\ttrash_md_path      %s\n",   ns->trash_md_path);
   fprintf(stdout, "\ttrash_md_path_len  %ld\n",  ns->trash_md_path_len);
   fprintf(stdout, "\tfsinfo_path        %s\n",   ns->fsinfo_path);
   fprintf(stdout, "\tfsinfo_path_len    %ld\n",  ns->fsinfo_path_len);
   fprintf(stdout, "\tquota_space        %lld\n", ns->quota_space);

   fprintf(stdout, "\tdir_MDAL           %s\n",   (ns->dir_MDAL  ? ns->dir_MDAL->name  : "NULL"));
   // We're assuming read_configuration() has already configured the MDAL in question
   if (ns->dir_MDAL && (ns->dir_MDAL->config == &default_mdal_config)) {
      debug_xdal_config_options(ns->dir_MDAL->global_state);
   }

   fprintf(stdout, "\tfile_MDAL          %s\n",   (ns->file_MDAL ? ns->file_MDAL->name : "NULL"));
   // We're assuming read_configuration() has already configured the MDAL in question
   if (ns->file_MDAL && (ns->file_MDAL->config == &default_mdal_config)) {
      debug_xdal_config_options(ns->file_MDAL->global_state);
   }

   return 0;
}



int debug_repo (MarFS_Repo* repo ) {
   fprintf(stdout, "Repo\n");
   fprintf(stdout, "\tname                %s\n",   repo->name);
   fprintf(stdout, "\tname_len            %ld\n",  repo->name_len);
   fprintf(stdout, "\thost                %s\n",   repo->host);
   fprintf(stdout, "\thost_len            %ld\n",  repo->host_len);
   fprintf(stdout, "\thost_offset         %d\n",   repo->host_offset);
   fprintf(stdout, "\thost_count          %d\n",   repo->host_count);
   fprintf(stdout, "\tupdate_in_place     %d\n",   repo->update_in_place);
   fprintf(stdout, "\tssl                 %d\n",   repo->ssl);
   fprintf(stdout, "\tis_online           %d\n",   repo->is_online);
   fprintf(stdout, "\taccess_method       %s\n",   accessmethod_string(repo->access_method));
   fprintf(stdout, "\tchunk_size          %ld\n",  repo->chunk_size);
   fprintf(stdout, "\tsecurity_method     %s\n",   securitymethod_string(repo->security_method));
   fprintf(stdout, "\tenc_type            %d\n",   repo->enc_type);
   fprintf(stdout, "\tcomp_type           %d\n",   repo->comp_type);
   fprintf(stdout, "\tcorrect_type        %d\n",   repo->correct_type);
   fprintf(stdout, "\tmax_pack_file_count %ld\n",  repo->max_pack_file_count);
   fprintf(stdout, "\tmin_pack_file_count %ld\n",  repo->min_pack_file_count);
   fprintf(stdout, "\tmax_pack_file_size  %ld\n",  repo->max_pack_file_size);
   fprintf(stdout, "\tmin_pack_file_size  %ld\n",  repo->min_pack_file_size);

   fprintf(stdout, "\tDAL                 %s\n",   (repo->dal ? repo->dal->name : "N/A"));
   // We're assuming read_configuration() has already configured the DAL in question
   if (repo->dal && (repo->dal->config == &default_dal_config)) {
      debug_xdal_config_options(repo->dal->global_state);
   }

   fprintf(stdout, "\tonline_cmds         %s\n",   repo->online_cmds);
   fprintf(stdout, "\tonline_cmds_len     %ld\n",  repo->online_cmds_len);
   fprintf(stdout, "\tlatency             %llu\n", repo->latency);

   return 0;
}
