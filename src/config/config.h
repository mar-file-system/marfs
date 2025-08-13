#ifndef _CONFIG_H
#define _CONFIG_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "hash/hash.h"
#include "mdal/mdal.h"
#include "ne/ne.h"

#define CONFIG_CTAG_LENGTH 32

typedef struct marfs_repo_struct marfs_repo;
typedef struct marfs_namespace_struct marfs_ns;
typedef struct marfs_config_struct marfs_config;

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


typedef struct marfs_cache_struct {
   char*      idstr;         // unique (per-cache) ID of this cache
   MDAL       mdal;          // MDAL reference for metadata access in this cache
} marfs_cache;

typedef struct marfs_dataclient_struct {
   char*      idstr;         // ID of a cache in the global cache list
   char*      ctag;          // regex used to match a client tag
} marfs_dc;

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
   char*      datatgt;       // indicates the type of underlying data storage (NULL - object, non-NULL - cache id)	
   ne_erasure protection;    // erasure defintion for writing out objects
   ne_ctxt    nectxt;        // LibNE context reference for data access
   size_t     objfiles;      // maximum count of files per data object (zero if no limit)
   size_t     objsize;       // maximum data object size (zero if no limit)
   HASH_TABLE podtable;      // hash table for object POD postion
   HASH_TABLE captable;      // hash table for object CAP position
   HASH_TABLE scattertable;  // hash table for object SCATTER position
} marfs_ds;


typedef struct marfs__struct {
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
   char*         name;        // name of this repo
   marfs_ds      datascheme;  // struct defining the data structure of this repo
   marfs_ms      metascheme;  // struct defining the metadata structure of this repo
   marfs_config* pconfig;     // a pointer back to the overall config structure that contains this repo
} marfs_repo;


typedef struct marfs_config_struct {
   char*        version;
   char*        mountpoint;
   char*        ctag;
   marfs_ns*    rootns;
   int          cachecount;
   marfs_cache* cachelist;
   int          repocount;
   marfs_repo*  repolist;
} marfs_config;

typedef struct marfs_position_struct {
   marfs_ns* ns;
   unsigned int depth;
   MDAL_CTXT ctxt;
} marfs_position;

// flags for config_verify
enum {
   CFG_FIX          = 0x1,  // fix problems found with the config
   CFG_OWNERCHECK   = 0x2,  // check owner of MDAL "security directory"
   CFG_MDALCHECK    = 0x4,  // check MDAL
   CFG_DALCHECK     = 0x8,  // check NE (DAL)
   CFG_RECURSE      = 0x10, // recursively check children of the namespace
};

/**
 * Initialize memory structures based on the given config file
 * @param const char* cpath : Path of the config file to be parsed
 * @param const char* clienttag : a client identifer, used to form object/files/etc in
 *                                the MarFS system.
 * @param pthread_mutex_t* erasurelock : Reference to the libne erasure synchronization lock
 * @return marfs_config* : Reference to the newly populated config structures
 */
marfs_config* config_init( const char* cpath, const char* clienttag, pthread_mutex_t* erasurelock );

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
 * Returns the MDAL of the global cache map specified by the given mapid
 * @param marfs_config* config : Reference to the config to be used
 * @param char *mapid : Map ID string of the desired MDAL
 * @return MDAL : valid pointer to an MDAL on success. NULL if not found
 */
MDAL config_getcachemdal( marfs_config* config, char* mapid );

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
 * @param int flags : flags to control behavior of the verification
 * @return int : A count of uncorrected errors encountered, or -1 if a failure occurred
 */
int config_verify( marfs_config* config, const char* tgtNS, int flags );

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

