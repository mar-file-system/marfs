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


#include <stdio.h>

#include "logging.h"
#include "marfs_configuration.h"

int main( int argc, char *argv[] ) {

  MarFS_Config_Ptr marfs_config;
  char sectype_code = '_';
  MarFS_SecType marfs_sectype;
  char compType_code = '_';
  MarFS_CompType marfs_comptype;
  char comptype_code = '_';
  MarFS_CorrectType marfs_correcttype;
  char code;
  int ret_val;
  MarFS_Namespace_Ptr namespacePtr;
  MarFS_Repo_Ptr repoPtr;


  INIT_LOG();
  fprintf( stdout, "\n" );

  marfs_config = read_configuration();

  if ( marfs_config != NULL ) {
    fprintf( stdout, "CORRECT: The members of the MarFS config structure are:\n" );
    fprintf( stdout, "\tconfig name            : %s\n", marfs_config->marfs_config_name );
    fprintf( stdout, "\tconfig version         : %f\n", marfs_config->marfs_config_version );
    fprintf( stdout, "\tconfig mnttop          : %s\n", marfs_config->marfs_mnttop );
    fprintf( stdout, "\tconfig namespace count : %lu\n", marfs_config->marfs_namespace_count );
  } else {
    fprintf( stderr, "ERROR: Reading MarFS configuration failed.\n" );
    return 1;
  }

  fprintf( stdout, "\n" );

  ret_val = free_configuration( &marfs_config );
  if ( ! ret_val ) {
    fprintf( stdout, "CORRECT: free_configuration returned %d\n", ret_val );
    if ( marfs_config == NULL ) {
      fprintf( stdout, "CORRECT: We freed the MarFS configuration and it is now NULL.\n" );
    } else {
      fprintf( stderr, "ERROR: free_configuration did not set the MarFS configuration to NULL.\n" );
    }
  } else {
    fprintf( stderr, "ERROR: free_configuration returned %d\n", ret_val );
  }

  fprintf( stdout, "\n" );

  fprintf( stdout, "Re-reading the configuration to continue testing...\n" );
  marfs_config = read_configuration(); 

  if ( marfs_config == NULL ) {
    fprintf( stderr, "ERROR: Reading MarFS configuration failed.\n" );
    return 1;
  }

  fprintf( stdout, "\n" );

  if ( lookup_sectype( "none", &marfs_sectype )) {
    fprintf( stderr, "ERROR: Invalid sectype value of \"%s\".\n", "none" );
  } else {
    fprintf( stdout, "CORRECT: SecType value of \"%s\" translates to %d.\n", "none", marfs_sectype );
  }

  if ( encode_sectype( SECTYPE_NONE, &code )) {
    fprintf( stderr, "ERROR: Invalid enumeration value of %d.\n", SECTYPE_NONE );
  } else {
    fprintf( stdout, "CORRECT: Encode value of %d is \"%c\".\n", SECTYPE_NONE, code );
  }

  if ( decode_sectype( '_', &marfs_sectype )) {
    fprintf( stderr, "ERROR: Invalid code of \"%c\".\n", '_' );
  } else {
    fprintf( stdout, "CORRECT: Decode code of \"%c\" is %d.\n", '_',marfs_sectype ); 
  }

  fprintf( stdout, "\n" );

  if ( lookup_comptype( "none", &marfs_comptype )) {
    fprintf( stderr, "ERROR: Invalid comptype value of \"%s\".\n", "none" );
  } else {
    fprintf( stdout, "CORRECT: CompType value of \"%s\" translates to %d.\n", "none", marfs_comptype );
  }

  if ( encode_comptype( COMPTYPE_NONE, &code )) {
    fprintf( stderr, "ERROR: Invalid enumeration value of %d.\n", COMPTYPE_NONE );
  } else {
    fprintf( stdout, "CORRECT: Encode value of %d is \"%c\".\n", COMPTYPE_NONE, code );
  }

  if ( decode_comptype( '_', &marfs_comptype )) {
    fprintf( stderr, "ERROR: Invalid code of \"%c\".\n", '_' );
  } else {
    fprintf( stdout, "CORRECT: Decode code of \"%c\" is %d.\n", '_',marfs_comptype ); 
  }

  fprintf( stdout, "\n" );

  if ( lookup_correcttype( "none", &marfs_correcttype )) {
    fprintf( stderr, "ERROR: Invalid correcttype value of \"%s\".\n", "none" );
  } else {
    fprintf( stdout, "CORRECT: CorrectType value of \"%s\" translates to %d.\n", "none", marfs_correcttype );
  }

  if ( encode_correcttype( CORRECTTYPE_NONE, &code )) {
    fprintf( stderr, "ERROR: Invalid enumeration value of %d.\n", CORRECTTYPE_NONE );
  } else {
    fprintf( stdout, "CORRECT: Encode value of %d is \"%c\".\n", CORRECTTYPE_NONE, code );
  }

  if ( decode_correcttype( '_', &marfs_correcttype )) {
    fprintf( stderr, "ERROR: Invalid code of \"%c\".\n", '_' );
  } else {
    fprintf( stdout, "CORRECT: Decode code of \"%c\" is %d.\n", '_',marfs_correcttype ); 
  }

  fprintf( stdout, "\n" );

  namespacePtr = find_namespace_by_name( "BoGuS" );
  if ( namespacePtr == NULL ) {
    fprintf( stdout, "CORRECT: Namespace \"BoGuS\" does not exist.\n" );
  } else {
    fprintf( stderr, "ERROR: Namespace \"BoGuS\" does not exist and was found.\n" );
  }

  fprintf( stdout, "\n" );

  namespacePtr = find_namespace_by_name( "s3" );
  if ( namespacePtr != NULL ) {
    fprintf( stdout, "CORRECT: Namespace \"s3\" does exist and has mntpath \"%s\".\n", namespacePtr->namespace_mntpath );
  } else {
    fprintf( stderr, "ERROR: Namespace \"s3\" does exist and was not found.\n" );
  }

  fprintf( stdout, "\n" );

  namespacePtr = find_namespace_by_mntpath( "/BoGuS" );
  if ( namespacePtr == NULL ) {
    fprintf( stdout, "CORRECT: Mntpath \"/BoGuS\" does not exist.\n" );
  } else {
    fprintf( stderr, "ERROR: Mntpath \"/BoGuS\" does not exist and was found.\n" );
  }

  fprintf( stdout, "\n" );

  namespacePtr = find_namespace_by_mntpath( "/s3" );
  if ( namespacePtr != NULL ) {
    fprintf( stdout, "CORRECT: Mntpath \"/s3\" does exist and has name \"%s\".\n", namespacePtr->namespace_name );
  } else {
    fprintf( stderr, "ERROR: Mntpath \"/s3\" does exist and was not found.\n" );
  }

  fprintf( stdout, "\n" );

  repoPtr = find_repo_by_range( namespacePtr, 38 );
  if ( repoPtr != NULL ) {
    fprintf( stdout, "CORRECT: Namespace \"%s\" has a repo \"%s\" for files of size 38.\n",
			namespacePtr->namespace_name,
			repoPtr->repo_name );
  } else {
    fprintf( stderr, "ERROR: Namespace \"%s\" should have a repo for files of size 38.\n",
			namespacePtr->namespace_name );
  }

/*
 * Since the file_size argument is size_t, that is unsigned and negative numbers
 * are not allowed.
 *
  fprintf( stdout, "\n" );

  repoPtr = find_repo_by_range( namespacePtr, -2 );
  if ( repoPtr == NULL ) {
    fprintf( stdout, "CORRECT: Namespace \"%s\" should not have a repo for files of size -2.\n",
			namespacePtr->namespace_name );
  } else {
    fprintf( stderr, "ERROR: Namespace \"%s\" incorrectly has a repo \"%s\" for files of size -2.\n",
			namespacePtr->namespace_name,
                        repoPtr->repo_name );
  }
 */

  fprintf( stdout, "\n" );

  repoPtr = find_repo_by_range( NULL, 38 );
  if ( repoPtr == NULL ) {
    fprintf( stdout, "CORRECT: A NULL namespace should not have a repo for files of size 38.\n" );
  } else {
    fprintf( stderr, "ERROR: A NULL namespace incorrectly has a repo \"%s\" for files of size 38.\n",
			repoPtr->repo_name );
  }

  fprintf( stdout, "\n" );

  repoPtr = find_repo_by_name( "BoGuS" );
  if ( repoPtr == NULL ) {
    fprintf( stdout, "CORRECT: Repo name \"BoGuS\" does not exist.\n" );
  } else {
    fprintf( stderr, "ERROR: Repo name \"BoGuS\" does not exist and was found.\n" );
  }

  fprintf( stdout, "\n" );

  repoPtr = find_repo_by_name( "emcS3_00" );
  if ( repoPtr != NULL ) {
    fprintf( stdout, "CORRECT: Repo name \"emcS3_00\" does exist and has host \"%s\".\n", repoPtr->repo_host );
  } else {
    fprintf( stderr, "ERROR: Repo name \"emcS3_00\" does exist and was not found.\n" );
  }

  fprintf( stdout, "\n" );

  NSIterator nit = namespace_iterator();
  while (( namespacePtr = namespace_next( &nit )) != NULL ) {
    fprintf( stdout, "CORRECT: Namespace name is \"%s\"\n", namespacePtr->namespace_name );
  }

  fprintf( stdout, "\n" );

  RepoIterator rit = repo_iterator();
  while (( repoPtr = repo_next( &rit )) != NULL ) {
    fprintf( stdout, "CORRECT: Repo name is \"%s\"\n", repoPtr->repo_name );
  }

  return 0;
}
