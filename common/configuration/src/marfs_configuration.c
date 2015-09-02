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

#include <string.h> /* memset needs this */
#include <stdlib.h> /* malloc and strto* needs this */
#include <stdio.h>  /* *printf, etc. need this */
#include <ctype.h>  /* toupper needs this */
#include <errno.h>  /* checking errno needs this */
#include <unistd.h> /* access */

#include "logging.h"
#include "marfs_configuration.h"
#include "parse-inc/config-structs.h"
#include "confpars-structs.h"
#include "confpars.h"
#include "parse-types.h"
#include "parsedata.h"


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
 ****************************************************************************/
static MarFS_Config_Ptr marfs_configPtr = NULL;
static MarFS_Repo_List marfs_repoList = NULL;
static MarFS_Namespace_List marfs_namespaceList = NULL;

static int namespaceCount = 0, repoRangeCount = 0, repoCount = 0;


/*****************************************************************************
 *
 * FUNCTION IMPLEMENTATIONS
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
 * NOTE: If the fuse mount-point is "/A/B", and you provide a path like
 * "/A/B/C", then the "path" seen by fuse callbacks is "/C".  In
 * otherwords, we should never see MarFS_mnt_top, as part of the
 * incoming path.
 * 
 * For a quick first-cut, there's only one namespace.  Your path is either
 * in it or fails.
 *
 ****************************************************************************/
extern MarFS_Namespace_Ptr find_namespace_by_name( const char *name ) {

  int i;
  size_t name_len = strlen( name );

  for ( i = 0; i < namespaceCount; ++i ) {
    if (( marfs_namespaceList[i]->name_len == name_len ) &&
        ( ! strcmp( marfs_namespaceList[i]->name, name ))) {
      return marfs_namespaceList[i];
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
extern MarFS_Namespace_Ptr find_namespace_by_mntpath( const char *mntpath ) {

  int i;
  char *path_dup;
  char *path_dup_token;
  size_t path_dup_len;


  path_dup = strdup( mntpath );
  path_dup_token = strtok( path_dup, "/" );
  path_dup_len = strlen( path_dup );

/*
 * At this point path_dup will include the leading "/" and any other
 * characters up to, but not including, the next "/" character in
 * path. This includes path_dup being able to be "/" (the root
 * namespace).
 */

  for ( i = 0; i < namespaceCount; i++ ) {
    if (( marfs_namespaceList[i]->mntpath_len == path_dup_len )	&&
        ( ! strcmp( marfs_namespaceList[i]->mntpath, path_dup ))) {
      free( path_dup );
      return marfs_namespaceList[i];
    }
  }

  free( path_dup );
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
extern MarFS_Repo_Ptr find_repo_by_range	(
                 MarFS_Namespace_Ptr	namespacePtr,
                 size_t                 file_size  ) {   

  int i;


  if ( namespacePtr != NULL ) {
    for ( i = 0; i < namespacePtr->repo_range_list_count; i++ ) {
#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "File size sought is %d.\n", file_size );
  LOG( LOG_INFO, "Namespace is \"%s\"\n", namespacePtr->name );
  LOG( LOG_INFO, "Namespace's repo_range_list[%d]->minsize is %d.\n", i, namespacePtr->repo_range_list[i]->minsize );
  LOG( LOG_INFO, "Namespace's repo_range_list[%d]->maxsize is %d.\n", i, namespacePtr->repo_range_list[i]->maxsize );
#endif

      if ((( file_size >= namespacePtr->repo_range_list[i]->minsize ) &&

          (( file_size <= namespacePtr->repo_range_list[i]->maxsize ) ||
           ( namespacePtr->repo_range_list[i]->maxsize == -1 )))) {

#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "Repo pointer for this range found and being returned.\n" );
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

extern MarFS_Repo_Ptr find_repo_by_name( const char* name ) {

  int i;

  for ( i = 0; i < repoCount; i++ ) {
    if ( ! strcmp( marfs_repoList[i]->name, name )) {
      return marfs_repoList[i];
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

  char *ptr;


  ptr = strchr( s, c );
  if ( ptr != NULL ) {
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

extern int lookup_boolean( const char* str, MarFS_Boolean *enumeration ) {

  if ( ! strcasecmp( str, "NO" )) {
    *enumeration = FALSE;
  } else if ( ! strcasecmp( str, "YES" )) {
    *enumeration = TRUE;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/

extern int lookup_accessmethod( const char* str, MarFS_AccessMethod *enumeration ) {

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

extern int lookup_securitymethod( const char* str, MarFS_SecurityMethod *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = SECURITYMETHOD_NONE;
  } else if ( ! strcasecmp( str, "S3_AWS_USER" )) {
    *enumeration = SECURITYMETHOD_S3_AWS_USER;
  } else if ( ! strcasecmp( str, "S3_AWS_MASTER" )) {
    *enumeration = SECURITYMETHOD_S3_AWS_MASTER;
  } else if ( ! strcasecmp( str, "S3_PER_OBJ" )) {
    *enumeration = SECURITYMETHOD_S3_PER_OBJ;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/

static char sectype_index[] = "_";

extern int lookup_sectype( const char* str, MarFS_SecType *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = SECTYPE_NONE;
  } else {
    return -1;
  }

  return 0;
}

extern int encode_sectype( MarFS_SecType enumeration, char *code ) {

  if (( enumeration >= SECTYPE_NONE )	&&
      ( enumeration <= SECTYPE_NONE )) {

    *code = sectype_index[enumeration];
  } else {
    return -1;
  }

  return 0;
}

extern int decode_sectype( char code, MarFS_SecType *enumeration ) {

  int index;


  if ( ! find_code_index_in_string( sectype_index, code, &index )) {
    *enumeration = (MarFS_SecType) index;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/

static char comptype_index[] = "_";

extern int lookup_comptype( const char* str, MarFS_CompType *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = COMPTYPE_NONE;
  } else {
    return -1;
  }

  return 0;
}

extern int encode_comptype( MarFS_CompType enumeration, char *code ) {

  if (( enumeration >= COMPTYPE_NONE )	&&
      ( enumeration <= COMPTYPE_NONE )) {

    *code = comptype_index[enumeration];
  } else {
    return -1;
  }

  return 0;
}

extern int decode_comptype( char code, MarFS_CompType *enumeration ) {

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

extern int lookup_correcttype( const char* str, MarFS_CorrectType *enumeration ) {

  if ( ! strcasecmp( str, "NONE" )) {
    *enumeration = CORRECTTYPE_NONE;
  } else {
    return -1;
  }

  return 0;
}

extern int encode_correcttype( MarFS_CorrectType enumeration, char *code ) {

  if (( enumeration >= CORRECTTYPE_NONE )	&&
      ( enumeration <= CORRECTTYPE_NONE )) {

    *code = correcttype_index[enumeration];
  } else {
    return -1;
  }

  return 0;
}

extern int decode_correcttype( char code, MarFS_CorrectType *enumeration ) {

  int index;


  if ( ! find_code_index_in_string( correcttype_index, code, &index )) {
    *enumeration = (MarFS_CorrectType) index;
  } else {
    return -1;
  }

  return 0;
}

/****************************************************************************/


/*****************************************************************************
 *
 * This function returns the configuration after reading the configuration
 * file. The configuration file is found by searching in this order:
 *
 * 1) Translating the MARFSCONFIGRC environment variable.
 *   or
 * 2) Looking for it in $HOME/.marfsconfigrc.
 *  or
 * 3) Looking for it in /etc/marfsconfigrc.
 *
 * If none of those are found, NULL is returned.
 *
 ****************************************************************************/
extern MarFS_Config_Ptr read_configuration() {

  struct line h_page, pseudo_h, fld_nm_lst;        // for internal use
  struct config *config = NULL;                    // always need one of these

  struct namespace *namespacePtr, **namespaceList;
  struct repo_range *repoRangePtr, **repoRangeList;
  struct repo *repoPtr, **repoList;
  int j, k, slen;
  char *perms_dup, *tok, *envVal, *path;
  int pd_index;
  MarFS_Repo_Range_List marfs_repo_rangeList;


  envVal = getenv( "MARFSCONFIGRC" );
  if ( envVal == NULL ) {
    envVal = getenv( "HOME" );
    path = (char *) malloc( strlen( envVal ) + strlen( "/.marfsconfigrc" ) + 1 );
    path = strcpy( path, envVal );
    path = strcat( path, "/.marfsconfigrc" );

    if ( access( path, R_OK ) != -1 ) {
      ; // We found it in $HOME, but path is already set, so there's nothing to do.
    } else if ( access( "/etc/marfsconfigrc", R_OK ) != -1 ) {
      path = strdup( "/etc/marfsconfigrc" );
    } else {
      free( path );
      LOG( LOG_ERR, "The MarFS configuration RC file is not found in its 3 locations.\n" );
      return NULL;
    }
  } else {
    path = strdup( envVal ); // We found it with the MARFSCONFIGRC env variable.
  }

#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "Found the MarFS configuration file at \"%s\".\n", path );
#endif

  memset(&h_page,     0x00, sizeof(struct line));  // clear header page
  memset(&pseudo_h,   0x00, sizeof(struct line));  // clear pseudo headers
  memset(&fld_nm_lst, 0x00, sizeof(struct line));  // clear field names list

/*
 * Use Parser to read the configuration file and get all the
 * components out of it.
 */

  config = (struct config *) malloc( sizeof( struct config ));
  if (config == NULL) {
    free( path );
    LOG( LOG_ERR, "Error allocating memory for the config structure.\n");
    return NULL;
  }

  marfs_configPtr = (MarFS_Config_Ptr) malloc( sizeof( MarFS_Config ));
  if ( marfs_configPtr == NULL) {
    free( path );
    LOG( LOG_ERR, "Error allocating memory for the MarFS config structure.\n");
    return NULL;
  }

  parseConfigFile( path, CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, config, QUIET );
  freeHeaderFile(h_page.next);

/*
 * We're done with the path now. Give the memory back to the system.
 */

#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "Freeing path after parsing the MarFS configuration file.\n" );
#endif

  free( path );

#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "Freed path after parsing the MarFS configuration file.\n" );
#endif

  repoList = (struct repo **) listObjByName( "repo", config );
  j = 0;
  while ( repoList[j] != (struct repo *) NULL ) {
//#ifdef _DEBUG_MARFS_CONFIGURATION
//    LOG( LOG_INFO, "repoList[%d]->name is \"%s\".\n", j, repoList[j]->name );
//#endif
    j++;
  }
  repoCount = j;

  marfs_repoList = (MarFS_Repo_List) malloc( sizeof( MarFS_Repo_Ptr ) * ( repoCount + 1 ));
  if ( marfs_repoList == NULL) {
    LOG( LOG_ERR, "Error allocating memory for the MarFS repo list structure.\n");
    return NULL;
  }
  marfs_repoList[repoCount] = NULL;

  for ( j = 0; j < repoCount; j++ ) {
    marfs_repoList[j] = (MarFS_Repo_Ptr) malloc( sizeof( MarFS_Repo ));
    if ( marfs_repoList[j] == NULL) {
      LOG( LOG_ERR, "marfs_configuration.c: Error allocating memory for the MarFS repo structure.\n");
      return NULL;
    }

/*
 * Anything that isn't staying a string and can't be converted
 * to its new type directly needs to be upper case for easy comparison.
 */

    marfs_repoList[j]->name = strdup( repoList[j]->name );
    marfs_repoList[j]->name_len = strlen( repoList[j]->name );

    marfs_repoList[j]->host = strdup( repoList[j]->host );
    marfs_repoList[j]->host_len = strlen( repoList[j]->host );

    if ( lookup_boolean( repoList[j]->updateinplace, &( marfs_repoList[j]->updateinplace ))) {
      LOG( LOG_ERR, "Invalid updateinplace value of \"%s\".\n", repoList[j]->updateinplace );
      return NULL;
    }

    if ( lookup_boolean( repoList[j]->ssl, &( marfs_repoList[j]->ssl ))) {
      LOG( LOG_ERR, "Invalid ssl value of \"%s\".\n", repoList[j]->ssl );
      return NULL;
    }

    if ( lookup_accessmethod( repoList[j]->accessmethod, &( marfs_repoList[j]->accessmethod ))) {
      LOG( LOG_ERR, "Invalid accessmethod value of \"%s\".\n", repoList[j]->accessmethod );
      return NULL;
    }

    errno = 0;
    marfs_repoList[j]->chunksize = strtoull( repoList[j]->chunksize, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid chunksize value of \"%s\".\n", repoList[j]->chunksize );
      return NULL;
    }

    errno = 0;
    marfs_repoList[j]->packsize = strtoull( repoList[j]->packsize, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid packsize value of \"%s\".\n", repoList[j]->packsize );
      return NULL;
    }

    if ( lookup_securitymethod( repoList[j]->securitymethod, &( marfs_repoList[j]->securitymethod ))) {
      LOG( LOG_ERR, "Invalid securitymethod value of \"%s\".\n", repoList[j]->securitymethod );
      return NULL;
    }

    if ( lookup_sectype( repoList[j]->sectype, &( marfs_repoList[j]->sectype ))) {
      LOG( LOG_ERR, "Invalid sectype value of \"%s\".\n", repoList[j]->sectype );
      return NULL;
    }

    if ( lookup_comptype( repoList[j]->comptype, &( marfs_repoList[j]->comptype ))) {
      LOG( LOG_ERR, "Invalid comptype value of \"%s\".\n", repoList[j]->comptype );
      return NULL;
    }

    if ( lookup_correcttype( repoList[j]->correcttype, &( marfs_repoList[j]->correcttype ))) {
      LOG( LOG_ERR, "Invalid correcttype value of \"%s\".\n", repoList[j]->correcttype );
      return NULL;
    }

    marfs_repoList[j]->onoffline = NULL;
    marfs_repoList[j]->onoffline_len = 0;

    errno = 0;
    marfs_repoList[j]->latency = strtoull( repoList[j]->latency, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid latency value of \"%s\".\n", repoList[j]->latency );
      return NULL;
    }
  }
  free( repoList );

  namespaceList = (struct namespace **) listObjByName( "namespace", config );

  j = 0;
  while ( namespaceList[j] != (struct namespace *) NULL ) {
//#ifdef _DEBUG_MARFS_CONFIGURATION
//    LOG( LOG_INFO, "namespaceList[%d]->name is \"%s\".\n", j, namespaceList[j]->name );
//#endif
    j++;
  }
  namespaceCount = j;

  marfs_namespaceList = (MarFS_Namespace_List) malloc( sizeof( MarFS_Namespace_Ptr ) * ( namespaceCount + 1 ));
  if ( marfs_namespaceList == NULL) {
    LOG( LOG_ERR, "Error allocating memory for the MarFS namespace list structure.\n");
    return NULL;
  }
  marfs_namespaceList[namespaceCount] = NULL;

  for ( j = 0; j < namespaceCount; j++ ) {
    marfs_namespaceList[j] = (MarFS_Namespace_Ptr) malloc( sizeof( MarFS_Namespace ));
    if ( marfs_namespaceList[j] == NULL) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS namespace structure.\n");
      return NULL;
    }

    marfs_namespaceList[j]->name = strdup( namespaceList[j]->name );
    marfs_namespaceList[j]->name_len = strlen( namespaceList[j]->name );

    marfs_namespaceList[j]->mntpath = strdup( namespaceList[j]->mntpath );
    marfs_namespaceList[j]->mntpath_len = strlen( namespaceList[j]->mntpath );

/*
 * Because strtok destroys the string on which it operates, we're going to
 * make a copy of it first. But, we want to make sure it does not have any
 * whitespace in it so that we get the permission mnemonics without
 * whitespace that are delimited by a comma.
 */

    slen = (int) strlen( namespaceList[j]->bperms );
    perms_dup = (char *) malloc( slen + 1 );
    pd_index = 0;

    for ( k = 0; k < slen; k++ ) {
      if ( ! isspace( namespaceList[j]->bperms[k] )) {
        perms_dup[pd_index] = namespaceList[j]->bperms[k];
        pd_index++;
      }
    }
    perms_dup[pd_index] = '\0';

    tok = strtok( perms_dup, "," );
    while ( tok != NULL ) {
      if ( ! strcasecmp( tok, "RM" )) {
        marfs_namespaceList[j]->bperms |= R_META;
      } else if ( ! strcasecmp( tok, "WM" )) {
        marfs_namespaceList[j]->bperms |= W_META;
      } else if ( ! strcasecmp( tok, "RD" )) {
        marfs_namespaceList[j]->bperms |= R_DATA;
      } else if ( ! strcasecmp( tok, "WD" )) {
        marfs_namespaceList[j]->bperms |= W_DATA;
      } else if ( ! strcasecmp( tok, "TD" )) {
        marfs_namespaceList[j]->bperms |= T_DATA;
      } else if ( ! strcasecmp( tok, "UD" )) {
        marfs_namespaceList[j]->bperms |= U_DATA;
      } else {
        LOG( LOG_ERR, "Invalid bperms value of \"%s\".\n", tok );
        return NULL;
      }

      tok = strtok( NULL, "," );
    }

    free( perms_dup );

    slen = (int) strlen( namespaceList[j]->iperms );
    perms_dup = (char *) malloc( slen + 1 );
    pd_index = 0;

    for ( k = 0; k < slen; k++ ) {
      if ( ! isspace( namespaceList[j]->iperms[k] )) {
        perms_dup[pd_index] = namespaceList[j]->iperms[k];
        pd_index++;
      }
    }
    perms_dup[pd_index] = '\0';

    tok = strtok( perms_dup, "," );
    while ( tok != NULL ) {
      if ( ! strcasecmp( tok, "RM" )) {
        marfs_namespaceList[j]->iperms |= R_META;
      } else if ( ! strcasecmp( tok, "WM" )) {
        marfs_namespaceList[j]->iperms |= W_META;
      } else if ( ! strcasecmp( tok, "RD" )) {
        marfs_namespaceList[j]->iperms |= R_DATA;
      } else if ( ! strcasecmp( tok, "WD" )) {
        marfs_namespaceList[j]->iperms |= W_DATA;
      } else if ( ! strcasecmp( tok, "TD" )) {
        marfs_namespaceList[j]->iperms |= T_DATA;
      } else if ( ! strcasecmp( tok, "UD" )) {
        marfs_namespaceList[j]->iperms |= U_DATA;
      } else {
        LOG( LOG_ERR, "Invalid iperms value of \"%s\".\n", tok );
        return NULL;
      }

      tok = strtok( NULL, "," );
    }

    free( perms_dup );

    marfs_namespaceList[j]->mdpath = strdup( namespaceList[j]->mdpath );
    marfs_namespaceList[j]->mdpath_len = strlen( namespaceList[j]->mdpath );

/*
 * For now we'll set this to one (1). Once the configuration parser is fixed we can
 * potentially have more than one range per namespace.
 */

    repoRangeCount = 1;

    marfs_repo_rangeList = (MarFS_Repo_Range_List) malloc( sizeof( MarFS_Repo_Range_Ptr ) * ( repoRangeCount + 1 ));
    if ( marfs_repo_rangeList == NULL) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS repo range list structure.\n");
      return NULL;
    }
    marfs_repo_rangeList[repoRangeCount] = NULL;

    marfs_repo_rangeList[0] = (MarFS_Repo_Range_Ptr) malloc( sizeof( MarFS_Repo_Range ));
    if ( marfs_repo_rangeList[0] == NULL) {
      LOG( LOG_ERR, "Error allocating memory for the MarFS repo range structure.\n");
      return NULL;
    }

    marfs_repo_rangeList[0]->minsize = atoi( namespaceList[j]->minsize );
    marfs_repo_rangeList[0]->maxsize = atoi( namespaceList[j]->maxsize );
    marfs_repo_rangeList[0]->repo_ptr = find_repo_by_name( namespaceList[j]->repo_name );

    marfs_namespaceList[j]->repo_range_list = marfs_repo_rangeList;
    marfs_namespaceList[j]->repo_range_list_count = repoRangeCount;

    marfs_namespaceList[j]->trashmdpath = strdup( namespaceList[j]->trashmdpath );
    marfs_namespaceList[j]->trashmdpath_len = strlen( namespaceList[j]->trashmdpath );

    marfs_namespaceList[j]->fsinfopath = strdup( namespaceList[j]->fsinfopath );
    marfs_namespaceList[j]->fsinfopath_len = strlen( namespaceList[j]->fsinfopath );

    errno = 0;
    marfs_namespaceList[j]->quota_space = strtoll( namespaceList[j]->quota_space, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid quota_space value of \"%s\".\n", namespaceList[j]->quota_space );
      return NULL;
    }

    errno = 0;
    marfs_namespaceList[j]->quota_names = strtoll( namespaceList[j]->quota_names, (char **) NULL, 10 );
    if ( errno ) {
      LOG( LOG_ERR, "Invalid quota_names value of \"%s\".\n", namespaceList[j]->quota_names );
      return NULL;
    }

/*
    marfs_namespaceList[j]->namespaceshardp = strdup( namespaceList[j]->namespaceshardp );
    marfs_namespaceList[j]->namespaceshardp_len = strlen( namespaceList[j]->namespaceshardp );
*/
    marfs_namespaceList[j]->namespaceshardp = NULL;
    marfs_namespaceList[j]->namespaceshardp_len = 0;

/*
    errno = 0;
    marfs_namespaceList[j]->namespaceshardpnum = strtoull( namespaceList[j]->namespaceshardpnum, (char **) NULL, 10 );
*/

    marfs_namespaceList[j]->namespaceshardpnum = 0;

/*
    if ( errno ) {
      LOG( LOG_ERR, "Invalid namespaceshardpnum value of \"%s\".\n", namespaceList[j]->namespaceshardpnum );
      return NULL;
    }
*/
  }
  free( namespaceList );

  marfs_configPtr->name = strdup( config->config_name );
  marfs_configPtr->name_len = strlen( config->config_name );

  marfs_configPtr->version = strtod( config->config_version, (char **) NULL );

  marfs_configPtr->mnttop = strdup( config->mnttop );
  marfs_configPtr->mnttop_len = strlen( config->mnttop );

  marfs_configPtr->namespace_list = marfs_namespaceList;
  marfs_configPtr->namespace_count = namespaceCount;

#ifdef _DEBUG_MARFS_CONFIGURATION
  LOG( LOG_INFO, "\n" );
  LOG( LOG_INFO, "The members of the config structure are:\n" );
  LOG( LOG_INFO, "\tconfig name            : %s\n", marfs_configPtr->name );
  LOG( LOG_INFO, "\tconfig version         : %f\n", marfs_configPtr->version );
  LOG( LOG_INFO, "\tconfig mnttop          : %s\n", marfs_configPtr->mnttop );
  LOG( LOG_INFO, "\tconfig namespace count : %lu\n", marfs_configPtr->namespace_count );
  LOG( LOG_INFO, "\tconfig repo count      : %d\n", repoCount );
  fflush( stdout );
#endif

/*
 * This seems to generate an error indicating we're freeing a pointer not allocated.
 *
 *  freeConfigStructContent( config );
 */

  return marfs_configPtr;
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
extern int free_configuration( MarFS_Config_Ptr *config ) {

  int j, k;


/*
 * First free the repos from the global list. Before freeing the actual repo
 * we first free the repo members that were dynamically allocated, then
 * we free the repo. Once we've freed each of the repos, we free the list.
 */

  for ( j = 0; j < repoCount; j++ ) {
    free( marfs_repoList[j]->name );
    free( marfs_repoList[j]->host );
    free( marfs_repoList[j]->onoffline );
    free( marfs_repoList[j] );
  }
  free( marfs_repoList );
  marfs_repoList = NULL;

  for ( j = 0; j < namespaceCount; j++ ) {
    free( marfs_namespaceList[j]->name );
    free( marfs_namespaceList[j]->mntpath );
    free( marfs_namespaceList[j]->mdpath );

/*
 * The only dynamically allocated part of a namespace_repo_range_list
 * is a pointer to the repo itself. The repos were all freed in the
 * prior loop, so we just need to free the pointers and the list
 * itself.
 */

    k = 0;
    while( marfs_namespaceList[j]->repo_range_list[k] != NULL ) {
      free( marfs_namespaceList[j]->repo_range_list[k] );
      k++;
    }
    free( marfs_namespaceList[j]->repo_range_list );

    free( marfs_namespaceList[j]->trashmdpath );
    free( marfs_namespaceList[j]->fsinfopath );
    free( marfs_namespaceList[j]->namespaceshardp );
    free( marfs_namespaceList[j] );
  }
  free( marfs_namespaceList );
  marfs_namespaceList = NULL;

  free( marfs_configPtr->name );
  free( marfs_configPtr->mnttop );

/*
 * The marfs_namespace_list was assigned to point to the
 * marfs_namespaceList. It is now a dangling pointer, as all it was
 * pointing at was freed. It would be an error to free it at this
 * point, so we'll just note that here and move on.
 *
 * free( marfs_configPtr->namespace_list );
 */

  free( marfs_configPtr );
  marfs_configPtr = NULL;
  *config = marfs_configPtr;

  return 0;
}

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
extern NSIterator namespace_iterator() {

  return (NSIterator) { .pos = 0 };
}

extern MarFS_Namespace_Ptr namespace_next( NSIterator *nsIterator ) {

  if ( nsIterator->pos >= namespaceCount ) {
    return NULL;
  } else {
    return marfs_configPtr->namespace_list[nsIterator->pos++];
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
extern RepoIterator repo_iterator() {

  return (RepoIterator) { .pos = 0 };
}

extern MarFS_Repo_Ptr repo_next( RepoIterator *repoIterator ) {

  if ( repoIterator->pos >= repoCount ) {
    return NULL;
  } else {
    return marfs_repoList[repoIterator->pos++];
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
extern int iterate_marfs_list( void **marfs_list, int ( *marfsPtrCallback )( void *marfsPtr )) {

  int k = 0, retVal;


  while( marfs_list[k] != NULL ) {
    retVal = marfsPtrCallback( marfs_list[k] );
// Check the return value and exit if not 0 (zero)
    k++;
  }
}
 *
 ****************************************************************************/
