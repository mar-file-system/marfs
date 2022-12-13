#ifndef _CONFIG_H
#define _CONFIG_H
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

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "hash/hash.h"
#include "mdal/mdal.h"
#include <ne.h>

#define CONFIG_CTAG_LENGTH 32

typedef struct marfs_repo_struct marfs_repo;
typedef struct marfs_namespace_struct marfs_ns;

typedef enum {
   NS_NOACCESS = 0,    // 0  = 0b0000 -- No access at all
   NS_READMETA,        // 1  = 0b0001 -- Read access to metadata   ( readdir / stat / etc. )
   NS_WRITEMETA,       // 2  = 0b0010 -- Write access to metadata  ( open / mkdir / etc. )
   NS_RWMETA,          // 3  = 0b0011 -- Read and Write access to metadata
   NS_READDATA,        // 4  = 0b0100 -- Read access to data
   NS_WRITEDATA = 8,   // 8  = 0b1000 -- Write access to data
   NS_RWDATA = 12,     // 12 = 0b1100 -- Read and Write access to data
   NS_FULLACCESS = 15  // 15 = 0b1111 -- Read and Write for data and metadata
} ns_perms;


typedef struct marfs_namespace_struct {
   char*       idstr;        // unique (per-repo) ID of this namespace
   size_t      fquota;       // file quota of the namespace ( zero if no limit )
   size_t      dquota;       // data quota of the namespace ( zero if no limit )
   ns_perms    iperms;       // interactive access perms for this namespace
   ns_perms    bperms;       // batch access perms for this namespace
   marfs_repo* prepo;        // reference to the repo containing this namespace
   marfs_ns*   pnamespace;   // reference to the parent of this namespace
   HASH_TABLE  subspaces;    // subspace hash table, referencing namespaces below this one
   HASH_NODE*  subnodes;     // subnode list reference ( shared with table ) for safe iter
   size_t      subnodecount; // count of subnode references
   // GhostNS-specific info
   marfs_ns*   ghtarget;     // target NS of this ghost ( NULL for non-ghost NS )
   marfs_ns*   ghsource;     // reference to the original ghost NS instance ( NULL for all but active ghosts )
} marfs_ns;
// NOTE -- namespaces will be wrapped in HASH_NODES for use in HASH_TABLEs
//         the HASH_NODE struct will provide the name string of the namespace


typedef struct marfs_datascheme_struct {
   ne_erasure protection;    // erasure defintion for writing out objects
   ne_ctxt    nectxt;        // LibNE context reference for data access
   size_t     objfiles;      // maximum count of files per data object (zero if no limit)
   size_t     objsize;       // maximum data object size (zero if no limit)
   HASH_TABLE podtable;      // hash table for object POD postion
   HASH_TABLE captable;      // hash table for object CAP position
   HASH_TABLE scattertable;  // hash table for object SCATTER position
} marfs_ds;


typedef struct marfs_metadatascheme_struct {
   MDAL       mdal;          // MDAL reference for metadata access
   char       directread;    // flag indicating support for data read from metadata files
   int        refbreadth;    // breadth of reference trees
   int        refdepth;      // depth of reference trees
   int        refdigits;     // digits of reference trees
   HASH_TABLE reftable;      // hash table for determining reference path
   HASH_NODE* refnodes;      // reference node list ( shared with table ) for safe iter
   size_t     refnodecount;  // count of reference nodes
   int        nscount;       // count of the namespaces directly referenced by this repo
   HASH_NODE* nslist;        // array of namespaces directly referenced by this repo
} marfs_ms;


typedef struct marfs_repo_struct {
   char*     name;        // name of this repo
   marfs_ds  datascheme;  // struct defining the data structure of this repo
   marfs_ms  metascheme;  // struct defining the metadata structure of this repo
} marfs_repo;


typedef struct marfs_config_struct {
   char*       version;
   char*       mountpoint;
   char*       ctag;
   marfs_ns*   rootns;
   int         repocount;
   marfs_repo* repolist;
} marfs_config;

typedef struct marfs_position_struct {
   marfs_ns* ns;
   unsigned int depth;
   MDAL_CTXT ctxt;
} marfs_position;

/**
 * Initialize memory structures based on the given config file
 * @param const char* cpath : Path of the config file to be parsed
 * @return marfs_config* : Reference to the newly populated config structures
 */
marfs_config* config_init( const char* cpath );

/**
 * Destroy the given config structures
 * @param marfs_config* config : Reference to the config to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int config_term( marfs_config* config );

/**
 * Duplicate the reference to a given NS
 * @param marfs_ns* ns : NS ref to duplicate
 * @return marfs_ns* : Duplicated ref, or NULL on error
 */
marfs_ns* config_duplicatensref( marfs_ns* ns );

/**
 * Potentially free the given NS ( only if it is an allocated ghostNS )
 * @param marfs_ns* ns : Namespace to be freed
 */
void config_destroynsref( marfs_ns* ns );

/**
 * Create a fresh marfs_position struct, targetting the MarFS root
 * @param marfs_position* pos : Reference to the position to be initialized,
 * @param marfs_config* config : Reference to the config to be used
 * @return int : Zero on success, or -1 on failure
 */
int config_establishposition( marfs_position* pos, marfs_config* config );

/**
 * Duplicate the given source position into the given destination position
 * @param marfs_position* srcpos : Reference to the source position
 * @param marfs_position* destpos : Reference to the destination position
 * @return int : Zero on success, or -1 on failure
 */
int config_duplicateposition( marfs_position* srcpos, marfs_position* destpos );

/**
 * Establish a CTXT for the given position, if it is lacking one
 * @param marfs_position* pos : Reference to the position
 * @return int : Zero on success ( ctxt established or already present ),
 *               or -1 on failure
 */
int config_fortifyposition( marfs_position* pos );

/**
 * Terminate a marfs_position struct
 * @param marfs_position* pos : Position to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int config_abandonposition( marfs_position* pos );

/**
 * Generate a new NS reference HASH_TABLE, used to identify the reference location of MarFS metadata files
 * @param HASH_NODE** refnodes : Reference to be populated with the HASH_NODE list of the produced table
 * @param size_t* refnodecount : Reference to be populated with the length of the above HASH_NODE list
 * @param size_t refbreadth : Breadth value of the NS reference tree
 * @param size_t refdepdth : Depth value of the NS reference tree
 * @param size_t refdigits : Included digits value of the NS reference tree
 * @return HASH_TABLE : The produced reference HASH_TABLE
 */
HASH_TABLE config_genreftable( HASH_NODE** refnodes, size_t* refnodecount, size_t refbreadth, size_t refdepth, size_t refdigits );

/**
 * Verifies the LibNE Ctxt of every repo, creates every namespace, creates all
 *  reference dirs in the given config, and verifies the LibNE CTXT
 * @param marfs_config* config : Reference to the config to be validated
 * @param const char* tgtNS : Path of the NS to be verified
 * @param char MDALcheck : If non-zero, the MDAL security and reference dirs of each encountered NS will be verified
 * @param char NEcheck : If non-zero, the LibNE ctxt of each encountered NS will be verified
 * @param char recurse : If non-zero, children of the target NS will also be verified
 * @param char fix : If non-zero, attempt to correct any problems encountered
 * @return int : A count of uncorrected errors encountered, or -1 if a failure occurred
 */
int config_verify( marfs_config* config, const char* tgtNS, char MDALcheck, char NEcheck, char recurse, char fix );

/**
 * Traverse the given path, idetifying a final NS target and resulting subpath
 * @param marfs_config* config : Config reference
 * @param marfs_position* pos : Reference populated with the initial position value
 *                              This will be updated to reflect the resulting position
 *                              NOTE -- pos->ctxt may be destroyed; see below
 * @param char** subpath : Relative path from the tgtns
 *                         This will be updated to reflect the resulting subpath from
 *                         the new tgtns
 *                         NOTE -- this function may completely replace the
 *                         string reference
 * @param char linkchk : If zero, this function will not check for symlinks in the path.
 *                          All path componenets are assumed to be directories.
 *                       If non-zero, this function will perform a readlink() op on all
 *                          path components, substituting in the targets of all symlinks.
 * @return int : The depth of the path from the resulting NS target, 
 *               or -1 if a failure occurred
 * NOTE -- If the returned depth == 0 ( path directly targets a NS ), it is possible for 
 *         the position value to have its MDAL_CTXT destroyed ( set to NULL ).  This is 
 *         because many ops ( such as stat() ) which directly target a NS do not require 
 *         an MDAL_CTXT for that NS.  In fact, certain permissions ( no execute ) may 
 *         disallow the creation of an MDAL_CTXT for the NS, but still allow the op itself.
 *         Therefore, it is up to the caller to identify if an NS ctxt is required, and to 
 *         create such an MDAL_CTXT themselves.
 */
int config_traverse( marfs_config* config, marfs_position* pos, char** subpath, char linkchk );

/**
 * Idetify the repo and NS path of the given NS ID string reference
 * @param const char* nsidstr : Reference to the NS ID string for which to retrieve info
 * @param char** repo : Reference to be populated with the name of the NS repo
 *                      NOTE -- it is the caller's responsibility to free this string
 * @param char** path : Reference to be populated with the path of the NS
 *                      NOTE -- it is the caller's responsibility to free this string
 * @return int : Zero on success;
 *               One, if the NS path is invalid ( likely means NS has no parent );
 *               -1 on failure.
 */
int config_nsinfo( const char* nsidstr, char** repo, char** path );

#endif // _CONFIG_H

