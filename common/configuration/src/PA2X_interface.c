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

#include "PA2X_interface.h"

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


// STRING() transforms a command-line -D argument-value into a string
// For example, if we are given -DBUILD_INC_DIR=/usr/include, and we
// want "/usr/include".  It requires two steps, like this:

#define STRING2(X) #X
#define STRING(X) STRING2(X)




/*****************************************************************************
 *
 * This function returns the version of the configuration parsed by PA2X.
 * The structs are defined in config-structs.h, generated from the
 * blueprint file, and all their elements are char* values, parsed from the
 * config-file.
 *
 * It remains for libmarfs to translate these config structs into
 * corresponding real structs used internally by libmarfs.  We used to do
 * that here in libconfig, but that meant the "real" headers had to live
 * here, as well.  We've now refactored, so that this library only produces
 * the string-value structs from PA2X, so the libmarfs stuff can remain
 * with libmarfs.
 *
 * The configuration file is found by searching in this order:
 *
 * 1) Translating the MARFSCONFIGRC environment variable.
 * 2) Looking for it in $HOME/.marfsconfigrc.
 * 3) Looking for it in /etc/marfsconfigrc.
 *
 * If none of those are found, NULL is returned.
 ****************************************************************************/


// new requirement for parseConfigFile [2015-09-23]
static struct varNameTypeList vNTL;



struct config* read_PA2X_config() {

  char *envVal, *path;
  struct line h_page, pseudo_h, fld_nm_lst;        // for internal use
  struct config *config = NULL;                    // always need one of these

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

  config = (struct config *) malloc( sizeof(struct config) );
  if (config == NULL) {
    free( path );
    LOG( LOG_ERR, "Error allocating memory for the config structure.\n");
    return NULL;
  }
  memset(config,      0x00, sizeof(struct config));

  // Ron's parser tries to read "./parse-inc/config-structs.h", at
  // run-time.  The build now copies parse-inc/config-structs.h from
  // the PA2X source to the MarFS build-destination, with the same
  // directory-path, under there.  The BUILD_INC_DIR define is
  // provided by the build, so that we can chdir() to there, while
  // running Ron's code.
  { const size_t MAX_WD = 2048;
     char orig_wd[MAX_WD];
     if (! getcwd(orig_wd, MAX_WD)) {
        LOG( LOG_ERR, "Couldn't capture CWD.\n");
        return NULL;
     }
     LOG( LOG_INFO, "original working-dir: '%s'.\n", orig_wd);

     if (chdir(STRING(BUILD_INC_DIR))) {
        LOG( LOG_ERR, "Couldn't set CWD to '%s'.\n", STRING(BUILD_INC_DIR));
        return NULL;
     }
     LOG( LOG_INFO, "new working-dir:      '%s'.\n", STRING(BUILD_INC_DIR));

     // Ron's most-recent commits [2015-09-21] use this, but listObjByName() is broken
     LOG( LOG_INFO, "calling parseConfigFile(...)\n", path);
     parseConfigFile( path, CREATE_STRUCT_PATHS, &h_page, &fld_nm_lst, config, &vNTL, QUIET );

     if (chdir(orig_wd)) {
        LOG( LOG_ERR, "Couldn't restore CWD to '%s'.\n", orig_wd);
        return NULL;
     }
     LOG( LOG_INFO, "working-dir restored: '%s'.\n", orig_wd);
  }
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

  return config;
}



int free_PA2X_config(struct config* cfg) {
   // TBD ...
}
