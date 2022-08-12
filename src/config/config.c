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


/*
 * EXAMPLE CONFIG -- excluding XML header
 * <marfs_config version="1.10">
 *    <!-- Mount Point -->
 *    <mnt_top>/campaign</mnt_top>
 *
 *    <!-- Host Definitions ( ignored by this code ) -->
 *    <hosts> ... </hosts>
 *
 *    <!-- Repo Definition -->
 *    <repo name="mc10+2">
 *
 *       <!-- Per-Repo Data Scheme -->
 *       <data>
 *
 *          <!-- Erasure Protection -->
 *          <protection>
 *             <N>10</N>
 *             <E>2</E>
 *             <PSZ>1024</PSZ>
 *          </protection>
 *
 *          <!-- Packing -->
 *          <packing enabled="yes">
 *             <max_files>4096</max_files>
 *          </packing>
 *
 *          <!-- Chunking -->
 *          <chunking enabled="yes">
 *             <max_size>1G</max_size>
 *          </chunking>
 *
 *          <!-- Object Distribution -->
 *          <distribution>
 *             <pods dweight=2>4:0=1,3=5</pods>
 *             <caps>5:2=0</caps>
 *             <scatters>4096</scatters>
 *          </distribution>
 *
 *          <!-- DAL Definition ( ignored by this code ) -->
 *          <DAL type="posix"> ... </DAL>
 *
 *       </data>
 *
 *       <!-- Per-Repo Metadata Scheme -->
 *       <meta>
 *
 *          <!-- Namespace Definitions -->
 *          <namespaces rbreadth="10" rdepth="3" rdigits="3">
 *          
 *             <ns name="gransom-allocation">
 *
 *                <!-- Quota Limits for this NS -->
 *                <quotas>
 *                   <files></files>  <!-- no file count limit -->
 *                   <data>10P</data> <!-- 10 Pibibyte data size limit -->
 *                </quotas>
 *
 *                <!-- Permission Settings for this NS -->
 *                <perms>
 *                   <!-- metadata only inter access -->
 *                   <interactive>RM,WM</interactive>
 *                   <!-- full batch program access -->
 *                   <batch>RM,WM,RD,WD</batch>
 *                </perms>
 *
 *                <!-- Subspace Definition -->
 *                <ns name="read-only-reference">
 *                   <!-- no quota definition implies no limits -->
 *
 *                   <!-- perms for read only -->
 *                   <perms>
 *                      <interactive>RM,RD</interactive>
 *                      <batch>RM,RD</batch>
 *                   </perms>
 *                </ns>
 *
 *                <!-- Remote Subspace Definition -->
 *                <!-- Note : This repo def is not included -->
 *                <rns name="heavily-protected-data" repo="mc3+2"/>
 *
 *             </ns>
 *
 *
 *          </namespaces>
 *
 *          <!-- Direct Data -->
 *          <direct read="yes" write="yes"/>
 *
 *          <!-- MDAL Definition ( ignored by this code ) -->
 *          <MDAL type="posix"> ... </MDAL>
 *
 *       </meta>
 *
 *    </repo>
 *
 * </marfs_config>
 *
 */

#include "marfs_auto_config.h"
#ifdef DEBUG_CONFIG
#define DEBUG DEBUG_CONFIG
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "config"

#include <logging.h>
#include "config.h"
#include "general_include/numdigits.h"
#include "general_include/restrictedchars.h"

#include <libxml/tree.h>

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif


//   -------------   INTERNAL DEFINITIONS    -------------

/**
 * Traverse backwards, identifying the previous element of a given path
 * @param char* path : Reference to the head of the path string
 * @param char* curpath : Reference to the current path element
 * @return char* : Reference to the previous path element, or NULL if none exists
 */
char* config_prevpathelem( char* path, char* curpath ) {
   while ( curpath != path ) {
      // skip any trailing '/' chars
      curpath--;
      if ( *curpath != '/' ) { break; }
   }
   while ( curpath != path ) {
      // reverse, until we hit another '/' char
      curpath--;
      if ( *curpath == '/' ) { curpath++; break; }
   }
   if ( curpath == path ) {
      // we are at the foremost path element already
      return NULL;
   }
   return curpath;
}

/**
 * Update the given position to reference a new NS
 * NOTE -- This exists primarily due to the complexity of Ghost namespace transitions
 * @param marfs_position* pos : Reference to the current position
 *                              NOTE -- may be abandoned by this func ( see config_abandonposition() )
 * @param marfs_ns* nextns : Reference to the NS to enter
 * @param const char* relpath : Relative path component which resulted in entering this NS
 * @param char updatectxt : Flag value indicating if an updated MDAL_CTXT is needed
 *                          Zero -> Position CTXT will be destroyed
 *                          Non-Zero -> Position CTXT will be updated to reference the new tgt NS
 * @return int : Zero on success, or -1 on failure
 */
int config_enterns( marfs_position* pos, marfs_ns* nextns, const char* relpath, char updatectxt ) {
   // create some shorthand refs
   marfs_ns* curns = pos->ns;
   char ascending = ( nextns == curns->pnamespace ) ? 1 : 0; // useful for quick checks of traversal direction
   // check for expected case first - currently targetting a standard NS
   if ( curns->ghsource == NULL ) {
      // simplest case first, no ghostNS involved
      if ( nextns->ghtarget == NULL ) {
         // update the NS
         pos->ns = nextns;
         // update or destroy any existing ctxt
         if ( curns->prepo == nextns->prepo  &&  updatectxt ) {
            // shifting NS within a repo means we can directly update
            if ( curns->prepo->metascheme.mdal->setnamespace( pos->ctxt, relpath ) ) {
               LOG( LOG_ERR, "Failed to update CTXT via relative path value: \"%s\"\n", relpath );
               config_abandonposition( pos );
               return -1;
            }
         }
         else {
            // destroy the current context
            if ( pos->ctxt  &&  curns->prepo->metascheme.mdal->destroyctxt( pos->ctxt ) ) {
               // nothing to do but complain
               LOG( LOG_WARNING, "Failed to destroy MDAL_CTXT for NS \"%s\"\n", curns->idstr );
            }
            pos->ctxt = NULL;
            // potentially recreate
            if ( updatectxt ) {
               // create a fresh ctxt
               if ( config_fortifyposition( pos ) ) {
                  LOG( LOG_ERR, "Failed to establish a fresh ctxt for next NS: \"%s\"\n", nextns->idstr );
                  config_abandonposition( pos );
                  return -1;
               }
               LOG( LOG_INFO, "Established a fresh MDAL_CTXT following cross-repo NS transition\n" );
            }
         }
         LOG( LOG_INFO, "Performed simple transition from NS \"%s\" to NS \"%s\"\n", curns->idstr, nextns->idstr );
         return 0;
      }

      // we are moving into a new ghost ( nextns->ghtarget != NULL )
      marfs_ns* tgtns = NULL;

      // sanity check : should be impossible for a standard NS to be a child of a ghost
      if ( ascending ) {
         LOG( LOG_ERR, "Cannot ascend from a standard NS into a GhostNS\n" );
         config_abandonposition( pos );
         return -1;
      }
      // need to allocate a fresh NS struct to hold ghost state
      tgtns = malloc( sizeof( struct marfs_namespace_struct ) );
      if ( tgtns == NULL ) {
         LOG( LOG_ERR, "Failed to allocate space for a new GhostNS\n" );
         config_abandonposition( pos );
         return -1;
      }
      // Store the original GhostNS as our source ( will never change for this copy )
      tgtns->ghsource = nextns;
      // Target of the copy should match that of the Ghost itself
      tgtns->ghtarget = nextns->ghtarget;
      // Parent NS of the copy should match that of the Ghost
      tgtns->pnamespace = nextns->pnamespace;
      // Parent repo of the copy should match that of the target
      tgtns->prepo = nextns->ghtarget->prepo;
      // ID String becomes a copy of the GhostNS itself ( for now )
      tgtns->idstr = strdup( nextns->idstr );
      if ( tgtns->idstr == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate tgtNS ID string for copy of GhostNS: \"%s\"\n", nextns->idstr );
         free( tgtns );
         config_abandonposition( pos );
         return -1;
      }
      // Ghost copy inherits most of the GhostTgtNS values with slight modification
      // Quotas become the 'lesser' values, between the Ghost and its target
      //    ( with zero considered the 'greatest', unlimited, value )
      tgtns->fquota = nextns->fquota;
      if ( nextns->ghtarget->fquota  &&  (nextns->ghtarget->fquota < tgtns->fquota  ||  tgtns->fquota == 0 ) ) {
         tgtns->fquota = nextns->ghtarget->fquota;
      }
      tgtns->dquota = nextns->dquota;
      if ( nextns->ghtarget->dquota  &&  (nextns->ghtarget->dquota < tgtns->dquota  ||  tgtns->dquota == 0 ) ) {
         tgtns->dquota = nextns->ghtarget->dquota;
      }
      // Permissions become the most restrictive set, between the Ghost at its target
      tgtns->iperms = ( nextns->iperms & nextns->ghtarget->iperms );
      tgtns->bperms = ( nextns->bperms & nextns->ghtarget->bperms );
      // Subspaces become a copy of the Ghost's target NS subspaces, EXCLUDING ALL GHOSTS
      //    ( Ghost inclusion implies possible FS loop )
      HASH_NODE* parsenode = nextns->ghtarget->subnodes;
      size_t parsecount = 0;
      tgtns->subnodecount = 0;
      // count up non-ghost children of the target
      for ( ; parsecount < nextns->ghtarget->subnodecount; parsecount++ ) {
         if ( ( (marfs_ns*)parsenode->content )->ghtarget == NULL ) { tgtns->subnodecount++; }
         parsenode++;
      }
      tgtns->subspaces = NULL;
      tgtns->subnodes = NULL;
      if ( tgtns->subnodecount ) {
         // allocate subspace list
         tgtns->subnodes = malloc( sizeof( HASH_NODE ) * tgtns->subnodecount );
         if ( tgtns->subnodes == NULL ) {
            LOG( LOG_ERR, "Failed to allocate subnode list for copy of GhostNS: \"%s\"\n", nextns->idstr );
            free( tgtns->idstr );
            free( tgtns );
            config_abandonposition( pos );
            return -1;
         }
         // copy non-GhostNS nodes
         parsenode = nextns->ghtarget->subnodes;
         size_t nodeindex = 0;
         for ( parsecount = 0; parsecount < nextns->ghtarget->subnodecount; parsecount++ ) {
            if ( ( (marfs_ns*)parsenode->content )->ghtarget == NULL ) {
               HASH_NODE* editnode = tgtns->subnodes + nodeindex;
               nodeindex++;
               // just directly copy hash node values, rather than duplicating
               editnode->name = parsenode->name;
               editnode->weight = parsenode->weight;
               editnode->content = parsenode->content;
               if ( nodeindex == tgtns->subnodecount ) { break; } // potentially exit early
            }
            parsenode++;
         }
         // establish the subspace table
         tgtns->subspaces = hash_init( tgtns->subnodes, tgtns->subnodecount, 1 );
         if ( tgtns->subspaces == NULL ) {
            LOG( LOG_ERR, "Failed to initialize subspace table for copy of GhostNS: \"%s\"\n", nextns->idstr );
            free( tgtns->subnodes );
            free( tgtns->idstr );
            free( tgtns );
            config_abandonposition( pos );
            return -1;
         }
      }
      // provide the new, dynamically allocated NS target
      pos->ns = tgtns;
      // GhostNS contexts must always be freshly created
      if ( pos->ctxt  &&  curns->prepo->metascheme.mdal->destroyctxt( pos->ctxt ) ) {
         // nothing to do but complain
         LOG( LOG_WARNING, "Failed to destroy MDAL_CTXT for NS \"%s\"\n", curns->idstr );
      }
      pos->ctxt = NULL;
      if ( updatectxt ) {
         // generate a new 'split' ctxt
         if ( config_fortifyposition( pos ) ) {
            LOG( LOG_ERR, "Failed to generate split ctxt for new GhostNS copy: \"%s\"\n", tgtns->idstr );
            config_abandonposition( pos );
            return -1;
         }
      }
      LOG( LOG_INFO, "Entered newly created copy of GhostNS \"%s\" from parent NS \"%s\"\n", tgtns->idstr, curns->idstr );
      return 0;
   }

   // we are moving within a GhostNS ( curns->ghsource != NULL )

   // sanity check : ghost to ghost transitions should be impossible
   if ( nextns->ghtarget != NULL ) {
      LOG( LOG_ERR, "Detected theoretically impossible Ghost-to-Ghost transition from \"%s\" to \"%s\"\n",
           curns->idstr, nextns->idstr );
      config_abandonposition( pos );
      return -1;
   }
   // check if we're exiting the active Ghost, by traversing up to its parent NS
   //    NOTE -- The parent of the inital ghost may be the target of that ghost, making this check more complex
   if ( ascending  &&  curns->ghtarget == curns->ghsource->ghtarget  &&  curns->ghsource->pnamespace == nextns ) {
      // we are exiting the ghost dimension!
      pos->ns = nextns;
      // GhostNS contexts must always be destroyed
      if ( pos->ctxt  &&  curns->ghtarget->prepo->metascheme.mdal->destroyctxt( pos->ctxt ) ) {
         // nothing to do but complain
         LOG( LOG_WARNING, "Failed to destroy MDAL_CTXT for active copy of GhostNS \"%s\"\n", curns->idstr );
      }
      pos->ctxt = NULL;
      // potentially recreate
      if ( updatectxt ) {
         // create a fresh ctxt
         if ( config_fortifyposition( pos ) ) {
            LOG( LOG_ERR, "Failed to establish a fresh ctxt for next NS: \"%s\"\n", nextns->idstr );
            config_abandonposition( pos );
            config_destroynsref( curns );
            return -1;
         }
         LOG( LOG_INFO, "Established a fresh MDAL_CTXT after exiting the ghost dimension\n" );
      }
      LOG( LOG_INFO, "Exited GhostNS \"%s\" and entered parent: \"%s\"\n", curns->ghsource->idstr, nextns->idstr );
      config_destroynsref( curns );
      return 0;
   }
   // Target the new NS
   curns->ghtarget = nextns;
   // Parent NS of the copy should match that of the target in most cases
   curns->pnamespace = nextns->pnamespace;
   if ( nextns == curns->ghsource->ghtarget ) {
      // special case, reentering the origianl NS target means we must inherit the original Ghost's parent
      //    We're redirecting, such that further ascent will exit the Ghost rather than continue up the tree
      LOG( LOG_INFO, "Redirecting parent of active Ghost copy \"%s\" to \"%s\"\n",
                     curns->idstr, curns->ghsource->pnamespace->idstr );
      curns->pnamespace = curns->ghsource->pnamespace;
      // sanity check : this should only ever happen when ascending
      if ( !(ascending) ) {
         LOG( LOG_ERR, "Encountered original target NS \"%s\" of GhostNS \"%s\" when NOT ascending\n", nextns->idstr, curns->ghsource->idstr );
         config_abandonposition( pos );
         return -1;
      }
   }
   // Parent repo always matches the target
   curns->prepo = nextns->prepo;
   // ID String manipulation depends on direction
   char* repostr;
   char* nspath;
   if ( config_nsinfo( curns->idstr, &(repostr), &(nspath) ) ) {
      LOG( LOG_ERR, "Failed to identify NS path value of active Ghost copy: \"%s\"\n", curns->idstr );
      config_abandonposition( pos );
      return -1;
   }
   if ( ascending ) {
      // we need to strip off the last path component
      char* parsepath = nspath;
      char* prevelem = NULL;
      while ( *parsepath != '\0' ) {
         if ( *parsepath == '/' ) {
            prevelem = parsepath;
            while ( *parsepath == '/' ) { parsepath++; } // skip any repeated slashes
            continue;
         }
         parsepath++;
      }
      if ( prevelem == NULL ) {
         LOG( LOG_ERR, "Failed to identify final path component of NS string: \"%s\"\n", nspath );
         free( repostr );
         free( nspath );
         config_abandonposition( pos );
         return -1;
      }
      // just truncate the string directly
      *prevelem = '\0';
      // allocate a new ID string
      size_t newstrlen = strlen(repostr) + 1 + strlen(nspath) + 1;
      char* tmpidstr = malloc( sizeof( char ) * newstrlen );
      if ( tmpidstr == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a new ID string (ascending)\n" );
         free( repostr );
         free( nspath );
         config_abandonposition( pos );
         return -1;
      }
      if ( snprintf( tmpidstr, newstrlen, "%s|%s", repostr, nspath ) < 2 ) {
         LOG( LOG_ERR, "Failed to populate new ID string (ascending)\n" );
         free( tmpidstr );
         free( repostr );
         free( nspath );
         config_abandonposition( pos );
         return -1;
      }
      free( repostr );
      free( nspath );
      free( curns->idstr );
      curns->idstr = tmpidstr;
   }
   else {
      // we need to append the next relative path component
      size_t newstrlen = strlen(repostr) + 1 + strlen(nspath) + 1 + strlen(relpath) + 1;
      char* tmpidstr = malloc( sizeof(char) * newstrlen );
      if ( tmpidstr == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a new ID string (descending)\n" );
         free( repostr );
         free( nspath );
         config_abandonposition( pos );
         return -1;
      }
      if ( snprintf( tmpidstr, newstrlen, "%s|%s/%s", repostr, nspath, relpath ) < 3 ) {
         LOG( LOG_ERR, "Failed to populate new ID string (descending)\n" );
         free( tmpidstr );
         free( repostr );
         free( nspath );
         config_abandonposition( pos );
         return -1;
      }
      free( repostr );
      free( nspath );
      free( curns->idstr );
      curns->idstr = tmpidstr;
   }
   // Ghost copy inherits most of the new NS values with slight modification
   // Quotas become the 'lesser' values, between the original Ghost and new target
   //    ( with zero considered the 'greatest', unlimited, value )
   curns->fquota = curns->ghsource->fquota;
   if ( nextns->fquota  &&  (nextns->fquota < curns->fquota  ||  curns->fquota == 0 ) ) {
      curns->fquota = nextns->fquota;
   }
   curns->dquota = curns->ghsource->dquota;
   if ( nextns->dquota  &&  (nextns->dquota < curns->dquota  ||  curns->dquota == 0 ) ) {
      curns->dquota = nextns->dquota;
   }
   // Permissions become the most restrictive set, between the Ghost at its target
   curns->iperms = ( curns->ghsource->iperms & nextns->iperms );
   curns->bperms = ( curns->ghsource->bperms & nextns->bperms );
   // clear out the subspace allocations of the copy
   if ( curns->subspaces ) { hash_term( curns->subspaces, NULL, NULL ); }
   if ( curns->subnodes ) { free( curns->subnodes ); }
   // Subspaces become a copy of the target NS subspaces, EXCLUDING ALL GHOSTS
   //    ( Ghost inclusion implies possible FS loop )
   HASH_NODE* parsenode = nextns->subnodes;
   size_t parsecount = 0;
   curns->subnodecount = 0;
   // count up non-ghost children of the target
   for ( ; parsecount < nextns->subnodecount; parsecount++ ) {
      if ( ( (marfs_ns*)parsenode->content )->ghtarget == NULL ) { curns->subnodecount++; }
      parsenode++;
   }
   curns->subspaces = NULL;
   curns->subnodes = NULL;
   if ( curns->subnodecount ) {
      // allocate subspace list
      curns->subnodes = malloc( sizeof( HASH_NODE ) * curns->subnodecount );
      if ( curns->subnodes == NULL ) {
         LOG( LOG_ERR, "Failed to allocate subnode list for active GhostNS copy: \"%s\"\n", curns->idstr );
         config_abandonposition( pos );
         return -1;
      }
      // copy non-GhostNS nodes
      parsenode = nextns->subnodes;
      size_t nodeindex = 0;
      for ( parsecount = 0; parsecount < nextns->subnodecount; parsecount++ ) {
         if ( ( (marfs_ns*)parsenode->content )->ghtarget == NULL ) {
            HASH_NODE* editnode = curns->subnodes + nodeindex;
            nodeindex++;
            // just directly copy hash node values, rather than duplicating
            editnode->name = parsenode->name;
            editnode->weight = parsenode->weight;
            editnode->content = parsenode->content;
            if ( nodeindex == curns->subnodecount ) { break; } // potentially exit early
         }
         parsenode++;
      }
      // establish the subspace table
      curns->subspaces = hash_init( curns->subnodes, curns->subnodecount, 1 );
      if ( curns->subspaces == NULL ) {
         LOG( LOG_ERR, "Failed to initialize subspace table for active GhostNS copy: \"%s\"\n", curns->idstr );
         config_abandonposition( pos );
         return -1;
      }
   }
   // NOTE -- No need to update NS pointer

   // GhostNS contexts must always be freshly created
   if ( pos->ctxt  &&  curns->ghtarget->prepo->metascheme.mdal->destroyctxt( pos->ctxt ) ) {
      // nothing to do but complain
      LOG( LOG_WARNING, "Failed to destroy MDAL_CTXT for active Ghost NS copy \"%s\"\n", curns->idstr );
   }
   pos->ctxt = NULL;
   if ( updatectxt ) {
      // generate a new 'split' ctxt
      if ( config_fortifyposition( pos ) ) {
         LOG( LOG_ERR, "Failed to generate split ctxt for active GhostNS copy: \"%s\"\n", curns->idstr );
         config_abandonposition( pos );
         return -1;
      }
   }
   LOG( LOG_INFO, "Moved within GhostNS \"%s\" to new target: \"%s\"\n", curns->ghsource->idstr, curns->ghtarget->idstr );
   return 0;
}

/**
 * Identify if a given path enters (or reenters) the MarFS mountpoint
 * @param marfs_config* config : Config reference
 * @param marfs_ns* tgtns : Reference populated with the initial NS value
 *                          NOTE -- The value of this reference is expected to be 
 *                          either NULL ( external to MarFS, no current NS ) or 
 *                          to match the root NS of the config.
 * @param char* subpath : Relative path from the tgtns, or absolute path from NULL tgtns
 * @return char* : Reference to the path element at which the subpath enters the MarFS 
 *                 mount, or NULL if the path references any non-MarFS element
 */
char* config_validatemnt( marfs_config* config, marfs_ns* tgtns, char* subpath ) {
   // the postion of our comparison pointer depends on our current NS
   char* mntpoint = strdup( config->mountpoint );
   char* mntparse = mntpoint;
   char* parsepath = subpath;
   if ( tgtns != NULL ) {
      LOG( LOG_INFO, "Checking for path re-entry: \"%s\"\n", subpath );
      // we need to compare against the deepest path element of the mountpoint
      mntparse = mntpoint + strlen(mntpoint);
      char* prevelem = config_prevpathelem( mntpoint, mntparse );
      // if there is no previous path element, leave mntparse at end of string
      // NOTE -- this should only occur when MarFS is mounted at the global FS root
      //      -- see 'special case check' below
      // otherwise, adjust our parse position to the deepest path element
      if ( prevelem != NULL ) { mntparse = prevelem; }
   }
   else {
      LOG( LOG_INFO, "Checking if path reaches MarFS mount: \"%s\"\n", subpath );
      // skip any leading '/' chars in both paths
      while ( *mntparse == '/' ) { mntparse++; }
      while ( *parsepath == '/' ) { parsepath++; }
   }
   // produce a string reference for the current mountpoint path element
   char* mntelem = mntparse;
   while ( *mntparse != '\0'  &&  *mntparse != '/' ) { mntparse++; }
   char replacemnt = 0;
   if ( *mntparse == '/' ) { replacemnt = 1; *mntparse = '\0'; }
   // parse over the path string
   char* pathelem = parsepath;
   while ( *parsepath != '\0' ) {
      // move the parse pointer ahead to the next '/' char
      while ( *parsepath != '\0'  &&  *parsepath != '/' ) { parsepath++; }
      // replace any '/' char with a NULL
      char replacechar = 0;
      if ( *parsepath == '/' ) { replacechar = 1; *parsepath = '\0'; }
      // identify the current path element
      if ( strcmp( pathelem, ".." ) == 0 ) {
         // restore the mntpath
         if ( replacemnt ) { replacemnt = 0; *mntparse = '/'; }
         // identify the previous element of the mount path
         mntparse = config_prevpathelem( mntpoint, mntelem );
         if ( mntparse == NULL ) {
            if ( replacechar ) { *parsepath = '/'; }
            free( mntpoint );
            LOG( LOG_ERR, "Path extends above global root: \"%s\"\n", subpath );
            errno = EINVAL;
            return NULL;
         }
         mntelem = mntparse;
         // terminate the current mount element
         while ( *mntparse != '\0'  &&  *mntparse != '/' ) { mntparse++; }
         if ( *mntparse == '/' ) { replacemnt = 1; *mntparse = '\0'; }
      }
      else if ( strcmp( pathelem, "." ) ) {  // ignore any './' path elements
         // special case check -- mounted at global FS root
         if ( *mntelem == '\0' ) {
            if ( replacechar ) { *parsepath = '/'; }
            free( mntpoint );
            // just entered the root NS
            // return the current path element reference
            LOG( LOG_INFO, "Subpath inherently below a global MarFS: \"%s\"\n", pathelem );
            return pathelem;
         }
         // verify this element matches the current mount path element
         if ( strcmp( pathelem, mntelem ) ) {
            LOG( LOG_ERR, "Encountered non-MarFS-mount path element: \"%s\"\n", pathelem );
            if ( replacechar ) { *parsepath = '/'; }
            free( mntpoint );
            errno = EINVAL;
            return NULL;
         }
         // match; move on to the next mount path element
         if ( replacemnt ) { replacemnt = 0; *mntparse = '/'; }
         while ( *mntparse == '/' ) { mntparse++; }
         mntelem = mntparse;
         if ( *mntelem == '\0' ) {
            // the path has traversed into the MarFS mount, so we are done
            if ( replacechar ) { *parsepath = '/'; }
            free( mntpoint );
            // just entered the root NS
            // progress to the next path element, and return that reference
            while ( *parsepath == '/' ) { parsepath++; }
            LOG( LOG_INFO, "Path within MarFS mount: \"%s\"\n", parsepath );
            return parsepath;
         }
         // terminate the current mount element
         while ( *mntparse != '\0'  &&  *mntparse != '/' ) { mntparse++; }
         if ( *mntparse == '/' ) { replacemnt = 1; *mntparse = '\0'; }
      }
      // undo any previous path edit
      if ( replacechar ) { *parsepath = '/'; }
      // move our parse pointer ahead to the next path component
      while ( *parsepath == '/' ) { parsepath++; }
      // update our element pointer to the next path element
      pathelem = parsepath;
   }
         
   // if we've gotten this far, the path terminates before re-entering the mountpoint
   free( mntpoint );
   LOG( LOG_ERR, "Path terminates above MarFS mount: \"%s\"\n", subpath );
   errno = EINVAL;
   return NULL;
}

/**
 * Traverse the given relative path, idetifying a NS target and the resulting subpath
 * NOTE -- This function will only traverse the given path until it exits the config
 *         namespace structure.  At that point, the path will be truncated, and the
 *         resulting NS target set.
 *         This is required, as path elements within a NS may be symlinks or have
 *         restrictive perms.  Such non-NS paths must actually be traversed.
 *         Example, original values:
 *            tgtns == rootns
 *            return == "subspace/somedir/../../../target"
 *         Result:
 *            tgtns == subspace                     (NOT rootns)
 *            return == "somedir/../../../target"   (NOT "target")
 * @param marfs_config* config : Config to traverse
 * @param marfs_ns** tgtns : Reference populated with the initial NS value
 *                           This will be updated to reflect the resulting NS value
 * @param char* subpath : Relative path from the current tgtns
 * @return char* : Reference to the start of the NS subpath, or NULL on failure
 */
char* config_shiftns( marfs_config* config, marfs_position* pos, char* subpath ) {
   // traverse the subpath, identifying any NS elements
   char* parsepath = subpath;
   char* pathelem = parsepath;
   MDAL mdal = pos->ns->prepo->metascheme.mdal;
   while ( *parsepath != '\0' ) {
      // move the parse pointer ahead to the next '/' char
      while ( *parsepath != '\0'  &&  *parsepath != '/' ) { parsepath++; }
      // identify the start of the next path component in advance
      char* nextpelem = parsepath;
      while ( *nextpelem == '/' ) { nextpelem++; }
      // replace any '/' char with a NULL
      char replacechar = 0;
      if ( *parsepath == '/' ) { replacechar = 1; *parsepath = '\0'; }
      // identify the current path element
      if ( strcmp( pathelem, ".." ) == 0 ) {
         // parent ref, move the tgtns up one level
         // undo any previous path edit
         if ( replacechar ) { *parsepath = '/'; }
         replacechar = 0;
         if ( pos->ns->pnamespace == NULL ) {
            // we can't move up any further
            // check if this relative path reenters the MarFS mountpoint
            parsepath = config_validatemnt( config, pos->ns, nextpelem );
            if ( parsepath == NULL ) {
               LOG( LOG_ERR, "Path extends beyond config root: \"%s\"\n", pathelem );
               errno = EINVAL;
               return NULL;
            }
            // update our 'nextpelem' var
            nextpelem = parsepath;
            while ( *nextpelem == '/' ) { nextpelem++; }
            // back to the rootNS at this point, so no need to adjust position vals
         }
         else {
            // update our position to the new NS
            if ( config_enterns( pos, pos->ns->pnamespace, "..", 1 ) ) {
               LOG( LOG_ERR, "Failed to enter parent NS\n" );
               return NULL;
            }
            // update our MDAL quick ref
            mdal = pos->ns->prepo->metascheme.mdal;
         }
      }
      else if ( strcmp( pathelem, "." ) ) {
         // ignore "." references
         // lookup all others in the current subspace table
         HASH_NODE* resnode = NULL;
         if ( pos->ns->subspaces == NULL  ||  
              hash_lookup( pos->ns->subspaces, pathelem, &(resnode) ) ) {
            // this is not a NS path
            // check if the MDAL permits this path element
            if ( mdal->pathfilter( pathelem ) ) {
               LOG( LOG_ERR, "NS subpath element rejected by MDAL: \"%s\"\n", pathelem );
               if ( replacechar ) { *parsepath = '/'; }
               errno = EPERM;
               return NULL;
            }
            if ( replacechar ) { *parsepath = '/'; }
            break;
         }
         // this is a NS path
         if ( *nextpelem != '\0' ) {
            // we will be traversing within the NS; update our position and ctxt
            if ( config_enterns( pos, (marfs_ns*)(resnode->content), pathelem, 1 ) ) {
               LOG( LOG_ERR, "Failed to enter subspace \"%s\"\n", pathelem );
               return NULL;
            }
          }
         else {
            // the path is targetting the NS itself
            // NOTE -- We need to explicitly note this special case.  We don't want to 
            //         move our ctxt to reference the new NS, as that may be unnecessary 
            //         for many ops ( stating the NS itself ).
            //         Additionally, under some circumstances ( no execute perms ), it 
            //         may not be possible to set our ctxt to the NS, but still possible 
            //         to perform the required op ( stat(), for example ).
            if ( config_enterns( pos, (marfs_ns*)(resnode->content), pathelem, 0 ) ) {
               LOG( LOG_ERR, "Failed to enter subspace (without ctxt) \"%s\"\n", pathelem );
               return NULL;
            }
         }
         mdal = pos->ns->prepo->metascheme.mdal;
      }
      // undo any previous path edit
      if ( replacechar ) { *parsepath = '/'; }
      // move our parse pointer ahead to the next path component
      parsepath = nextpelem;
      // update our element pointer to the next path element
      pathelem = parsepath;
   }
   return pathelem;
}


//   -------------   INTERNAL FUNCTIONS    -------------


/**
 * Count the number of nodes with the given name at this level of the libxml tree structure
 * @param xmlNode* root : XML Tree node at which to begin the scan
 * @param const char* tag : String name to search for
 * @return int : Count of nodes with name 'tag' at this level, beginning at 'root'
 */
int count_nodes( xmlNode* root, const char* tag ) {
   // iterate over all nodes at this level, checking for instances of 'tag'
   int tagcnt = 0;
   for ( ; root; root = root->next ) {
      if ( root->type == XML_ELEMENT_NODE  &&  strncmp( (char*)root->name, tag, strlen(tag) ) == 0 ) {
         tagcnt++;
      }
   }
   return tagcnt;
}

/**
 * Scan through the list of HASH_NODEs, searching for a reference to the given namespace name
 * NOTE -- used to identify the targets of remote namespace references
 * @param HASH_NODE* nslist : List of HASH_NODEs referencing namespaces
 * @param size_t nscount : Count of namespaces in the provided list
 * @param const char* nsname : String name of the namespace to search for
 * @return HASH_NODE* : Reference to the HASH_NODE match, or NULL if no match is found
 */
HASH_NODE* find_namespace( HASH_NODE* nslist, size_t nscount, const char* nsname ) {
   // iterate over the elements of the nslist, searching for a matching name
   size_t index;
   for( index = 0; index < nscount; index++ ) {
      if( strncmp( nslist[index].name, nsname, strlen(nsname) ) == 0 ) {
         return ( nslist + index );
      }
   }
   return NULL;
}

/**
 * Scan through the list of repos, searching for one with the given name
 * NOTE -- used to identify the targets of remote namespace references
 * @param marfs_repo* repolist : List of repos to be searched
 * @param int repocount : Count of repos in the provided list
 * @param const char* reponame : String name of the repo to search for
 * @return marfs_repo* : Reference to the matching repo, or NULL if no match is found
 */
marfs_repo* find_repo( marfs_repo* repolist, int repocount, const char* reponame ) {
   // iterate over the elements of the repolist, searching for a matching name
   int index;
   for ( index = 0; index < repocount; index++ ) {
      if ( strncmp( repolist[index].name, reponame, strlen(reponame) ) == 0 ) {
         return ( repolist + index );
      }
   }
   return NULL;
}

/**
 * Parse the givne permroot xmlNode, populating either the provided iperms or bperms sets
 * @param ns_perms* iperms : Interactive perms, to be populated
 * @param ns_perms* bperms : Batch perms, to be populated
 * @param xmlNode* permroot : Reference to the permission xml root node
 * @return int : Zero on success, or -1 on failure
 */
int parse_perms( ns_perms* iperms, ns_perms* bperms, xmlNode* permroot ) {
   // define an unused character value and use as an indicator of a completed perm string
   char eoperm = 'x';

   // iterate over nodes at this level
   for ( ; permroot; permroot = permroot->next ) {
      // check for unkown xml node type
      if ( permroot->type != XML_ELEMENT_NODE ) {
         // skip any comment nodes
         if ( permroot->type == XML_COMMENT_NODE ) { continue; }
         // don't know what this is supposed to be
         LOG( LOG_ERR, "encountered unknown tag within 'perms' definition\n" );
         return -1;
      }

      // determine if we're parsing iteractive or batch perms
      char ptype = '\0';
      if ( strncmp( (char*)permroot->name, "interactive", 12 ) == 0 ) {
         if ( *iperms != NS_NOACCESS ) {
            // this is a duplicate 'interactive' def
            LOG( LOG_ERR, "encountered duplicate 'interactive' perm set\n" );
            return -1;
         }
         ptype = 'i';
      }
      else if ( strncmp( (char*)permroot->name, "batch", 6 ) == 0 ) {
         if ( *bperms != NS_NOACCESS ) {
            // this is a duplicate 'batch' def
            LOG( LOG_ERR, "encountered duplicate 'batch' perm set\n" );
            return -1;
         }
         ptype = 'b';
      }
      else {
         LOG( LOG_ERR, "encountered unexpected perm node: \"%s\"\n", (char*)permroot->name );
         return -1;
      }

      // check for unknown child node or bad content
      if ( permroot->children == NULL  ||  
           permroot->children->type != XML_TEXT_NODE  ||  
           permroot->children->content == NULL ) {
         LOG( LOG_ERR, "encountered an unrecognized xml node type within the perms definition\n" );
         return -1;
      }

      // parse the actual permission set
      char* permstr = (char*)permroot->children->content;
      char fchar = '\0';
      ns_perms tmpperm = NS_NOACCESS;
      for ( ; *permstr != '\0'; permstr++ ) {
         switch ( *permstr ) {
            case 'R':
            case 'W':
               // 'R'/'W' are only acceptable as the first character of a pair
               if ( fchar != '\0' ) {
                  if ( fchar == eoperm ) { LOG( LOG_ERR, "trailing '%c' character is unrecognized\n", *permstr ); }
                  else { LOG( LOG_ERR, "perm string '%c%c' is unrecognized\n", fchar, *permstr ); }
                  return -1;
               }
               fchar = *permstr;
               break;
            
            case 'M':
            case 'D':
               // 'R' and 'W' must preceed 'M' or 'D'
               if ( fchar == '\0' ) {
                  LOG( LOG_ERR, "perm field cannot begin with '%c'\n", *permstr );
                  return -1;
               }
               else if ( fchar == eoperm ) {
                  LOG( LOG_ERR, "trailing '%c' character is unrecognized\n", *permstr );
                  return -1;
               }
               // remember our original perm value, to check for duplicate permstrings
               ns_perms operm = tmpperm;
               if ( fchar == 'R' ) {
                  if ( *permstr == 'M' ) tmpperm |= NS_READMETA;
                  else tmpperm |= NS_READDATA;
               }
               else if ( fchar == 'W' ) {
                  if ( *permstr == 'M' ) tmpperm |= NS_WRITEMETA;
                  else tmpperm |= NS_WRITEDATA;
               }
               else {
                  LOG( LOG_ERR, "perm string '%c%c' is unrecognized\n", fchar, *permstr );
                  return -1;
               }
               // verify that this permision string actually resulted in a change
               if ( operm == tmpperm ) {
                  LOG( LOG_WARNING, "detected repeated '%c%c' perm string\n", fchar, *permstr );
               }
               fchar = eoperm; // set to unused character val, indicating a complete permstring
               break;

            case ',':
               fchar = '\0';
               break;

            default:
               LOG( LOG_ERR, "encountered unrecognized character value '%c'\n", *permstr );
               return -1;
         }
      }
      if ( ptype == 'i' )
         *iperms = tmpperm;
      else
         *bperms = tmpperm;
   }
   // we have iterated over all perm nodes
   return 0;
}

/**
 * Parse the content of an xmlNode to populate a size value
 * @param size_t* target : Reference to the value to populate
 * @param xmlNode* node : Node to be parsed
 * @return int : Zero on success, -1 on error
 */
int parse_size_node( size_t* target, xmlNode* node ) {
   // check for unexpected node format
   if ( node->children == NULL  ||  node->children->type != XML_TEXT_NODE ) {
      LOG( LOG_ERR, "unexpected format of size node: \"%s\"\n", (char*)node->name );
      return -1;
   }
   // check for an included value
   if ( node->children->content != NULL ) {
      char* valuestr = (char*)node->children->content;
      size_t unitmult = 1;
      char* endptr = NULL;
      unsigned long long parsevalue = strtoull( valuestr, &(endptr), 10 );
      // check for any trailing unit specification
      if ( *endptr != '\0' ) {
         if ( *endptr == 'K' ) { unitmult = 1024ULL; }
         else if ( *endptr == 'M' ) { unitmult = 1048576ULL; }
         else if ( *endptr == 'G' ) { unitmult = 1073741824ULL; }
         else if ( *endptr == 'T' ) { unitmult = 1099511627776ULL; }
         else if ( *endptr == 'P' ) { unitmult = 1125899906842624ULL; }
         else {
            LOG( LOG_ERR, "encountered unrecognized character in \"%s\" value: \"%c\"", (char*)node->name, *endptr );
            return -1;
         }
         // check for unacceptable trailing characters
         endptr++;
         if ( *endptr != '\0' ) {
            LOG( LOG_ERR, "encountered unrecognized trailing character in \"%s\" value: \"%c\"", (char*)node->name, *endptr );
            return -1;
         }
      }
      if ( (parsevalue * unitmult) >= SIZE_MAX ) {  // check for possible overflow
         LOG( LOG_ERR, "specified \"%s\" value is too large to store: \"%s\"\n", (char*)node->name, valuestr );
         return -1;
      }
      // actually store the value
      LOG( LOG_INFO, "detected value of %llu with unit of %zu for \"%s\" node\n", parsevalue, unitmult, (char*)node->name );
      *target = (parsevalue * unitmult);
      return 0;
   }
   // allow empty string to indicate zero value
   *target = 0;
   return 0;
}

/**
 * Parse the content of an xmlNode to populate an int value
 * @param int* target : Reference to the value to populate
 * @param xmlNode* node : Node to be parsed
 * @return int : Zero on success, -1 on error
 */
int parse_int_node( int* target, xmlNode* node ) {
   // check for an included value
   if ( node->children != NULL  &&
        node->children->type == XML_TEXT_NODE  &&
        node->children->content != NULL ) {
      char* valuestr = (char*)node->children->content;
      char* endptr = NULL;
      unsigned long long parsevalue = strtoull( valuestr, &(endptr), 10 );
      // check for any trailing unit specification
      if ( *endptr != '\0' ) {
         LOG( LOG_ERR, "encountered unrecognized trailing character in \"%s\" value: \"%c\"", (char*)node->name, *endptr );
         return -1;
      }
      if ( parsevalue >= INT_MAX ) {  // check for possible overflow
         LOG( LOG_ERR, "specified \"%s\" value is too large to store: \"%s\"\n", (char*)node->name, valuestr );
         return -1;
      }
      // actually store the value
      *target = parsevalue;
      return 0;
   }
   LOG( LOG_ERR, "failed to identify a value string within the \"%s\" definition\n", (char*)node->name );
   return -1;
}

/**
 * Parse the given quota node, populating the provided values
 * @param size_t* fquota : File count quota value to be populated
 * @param size_t* dquota : Data quota value to be populated
 * @param xmlNode* quotaroot : Quota node to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int parse_quotas( size_t* fquota, size_t* dquota, xmlNode* quotaroot ) {
   // iterate over nodes at this level
   for ( ; quotaroot; quotaroot = quotaroot->next ) {
      // check for unknown node type
      if ( quotaroot->type != XML_ELEMENT_NODE ) {
         // ignore all comment nodes
         if ( quotaroot->type == XML_COMMENT_NODE ) { continue; }
         // don't know what this is supposed to be
         LOG( LOG_ERR, "encountered unknown tag within 'quota' definition\n" );
         return -1;
      }

      // determine if we're parsing file or data quotas
      if ( strncmp( (char*)quotaroot->name, "files", 6 ) == 0 ) {
         if ( *fquota != 0 ) {
            // this is a duplicate 'files' def
            LOG( LOG_ERR, "encountered duplicate 'files' quota set\n" );
            return -1;
         }
         // parse the value and set quota enforcement
         if ( parse_size_node( fquota, quotaroot ) ) {
            LOG( LOG_ERR, "failed to parse 'files' quota value\n" );
            return -1;
         }
      }
      else if ( strncmp( (char*)quotaroot->name, "data", 5 ) == 0 ) {
         if ( *dquota != 0 ) {
            // this is a duplicate 'data' def
            LOG( LOG_ERR, "encountered duplicate 'data' quota set\n" );
            return -1;
         }
         // parse the value and set quota enforcement
         if ( parse_size_node( dquota, quotaroot ) ) {
            LOG( LOG_ERR, "failed to parse 'data' quota value\n" );
            return -1;
         }
      }
      else {
         LOG( LOG_ERR, "encountered unexpected quota sub-node: \"%s\"\n", (char*)quotaroot->name );
         return -1;
      }
   }
   // we have iterated over all perm nodes
   return 0;
}

/**
 * Free a namespace and other references of the given hash node
 * @param HASH_NODE* nsnode : Reference to the namespace hash node to be freed
 * @return int : Zero on success, or -1 on failure
 */
int free_namespace( HASH_NODE* nsnode ) {
   // we are assuming this function will ONLY be called on completed NS structures
   //   As in, this function will not check for certain NULL pointers, like name
   marfs_ns* ns = (marfs_ns*)nsnode->content;
   char* nsname = nsnode->name;
   int retval = 0;
   if ( ns  &&  ns->subspaces ) {
      // need to properly free the subspace hash table
      HASH_NODE* subspacelist;
      size_t subspacecount;
      if ( hash_term( ns->subspaces, &(subspacelist), &(subspacecount) ) ) {
         LOG( LOG_WARNING, "failed to free the subspace table of NS \"%s\"\n", nsname );
         retval = -1;
      }
      else {
         // free all subspaces
         size_t subspaceindex = 0;
         for( ; subspaceindex < subspacecount; subspaceindex++ ) {
            // recursively free this subspace
            if ( free_namespace( subspacelist+subspaceindex ) ) {
               LOG( LOG_WARNING, "failed to free subspace of NS \"%s\"\n", nsname );
               retval = -1;
            }
         }
         // regardless, we need to free the list of HASH_NODEs
         free( subspacelist );
         if ( subspacelist != ns->subnodes ) {
            LOG( LOG_ERR, "Detected mismatch between HASH_TABLE subspace list and NS ref\n" );
            retval = -1;
         }
      }
   }
   // free NS componenets
   if ( ns ) {
      LOG( LOG_INFO, "Freeing NS: \"%s\"\n", nsname );
      // free the namespace id string
      free( ns->idstr );
      // free the namespace itself
      free( ns );
   }
   else {
      LOG( LOG_INFO, "Freeing NS stub: \"%s\"\n", nsname );
   }
   // free the namespace name
   free( nsname );
   return retval;
}

/**
 * Allocate a new namespace, based on the given xmlNode and parent references
 * @param HASH_NODE* nsnode : HASH_NODE to be populated with NS info
 * @param marfs_ns* pnamespace : Parent namespace reference of the new namespace
 * @param marfs_repo* prepo : Parent repo reference of the new namespace
 * @param xmlNode* nsroot : Xml node defining the new namespace
 * @return int : Zero on success, or -1 on failure
 */
int create_namespace( HASH_NODE* nsnode, marfs_ns* pnamespace, marfs_repo* prepo, xmlNode* nsroot ) {
   // need to check if this is a real NS or a remote/ghost reference
   char rns = 0;
   char gns = 0;
   if ( strncmp( (char*)nsroot->name, "ns", 3 ) ) {
      if ( strncmp( (char*)nsroot->name, "gns", 4 ) ) {
         if ( strncmp( (char*)nsroot->name, "rns", 4 ) ) {
            LOG( LOG_ERR, "received an unexpected node as NSroot: \"%s\"\n", (char*)nsroot->name );
            errno = EINVAL;
            return -1;
         }
         // this is a remote namespace
         rns = 1;
         // do not permit remote namespaces at the root of a repo
         if ( pnamespace == NULL ) {
            LOG( LOG_ERR, "Detected a remote namespace at the root of the \"%s\" repo namespace defs\n", prepo->name );
            errno = EINVAL;
            return -1;
         }
      }
      else {
         // this is a ghost namespace
         gns = 1;
         // do not permit ghost namespaces at the root of a repo
         if ( pnamespace == NULL ) {
            LOG( LOG_ERR, "Detected a ghost namespace at the root of the \"%s\" repo namespace defs\n", prepo->name );
            errno = EINVAL;
            return -1;
         }
      }
   }

   // find the name of this namespace
   char* nsname = NULL;
   xmlAttr* attr = nsroot->properties;
   for ( ; attr; attr = attr->next ) {
      if ( attr->type == XML_ATTRIBUTE_NODE ) {
         if ( strncmp( (char*)attr->name, "name", 5 ) == 0 ) {
            if ( nsname != NULL ) {
               // we already found a 'name'
               LOG( LOG_WARNING, "encountered a duplicate 'name' value for NS \"%s\"\n", nsname );
               continue;
            }
            // we've found our 'name' attribute
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               nsname = strdup( (char*)attr->children->content );
            }
            else {
               LOG( LOG_WARNING, "encountered NS name attribute with unrecognized value\n" );
            }
         }
      }
   }

   // make sure we now have a name
   if ( nsname == NULL ) {
      LOG( LOG_ERR, "found a namespace without a 'name' value\n" );
      errno = EINVAL;
      return -1;
   }

   // make sure we don't have any forbidden name characters
   if ( restrictedchars_check( nsname ) ) {
      LOG( LOG_ERR, "Namespace has disallowed chars: \"%s\"\n", nsname );
      errno = EINVAL;
      free( nsname );
      return -1;
   }
   // make sure this is not a remote NS reference to the root NS ( would create FS loop )
   if ( rns  &&  strcmp( nsname, "root" ) == 0 ) {
      LOG( LOG_ERR, "found remote NS reference to the root NS\n" );
      errno = EINVAL;
      free( nsname );
      return -1;
   }

   // iterate over all attributes again, looking for errors and remote/ghost namespace values
   char* reponame = NULL;
   char* nstgt = NULL;
   for( attr = nsroot->properties; attr; attr = attr->next ) {
      if ( attr->type == XML_ATTRIBUTE_NODE ) {
         if ( ( rns  ||  gns )  &&  strncmp( (char*)attr->name, "repo", 5 ) == 0 ) {
            if ( reponame ) {
               // we already found a 'repo'
               LOG( LOG_WARNING, "encountered a duplicate 'repo' value on NS \"%s\"\n", nsname );
               continue;
            }
            // we've found our 'repo' attribute
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               // explicitly disable remote NS links to the same repo ( no purpose, possible FS loop )
               if ( rns  &&  strcmp( (char*)attr->children->content, prepo->name ) == 0 ) {
                  LOG( LOG_ERR, "encountered remote namespace linked to the same repo: \"%s\"\n" );
                  errno = EINVAL;
                  free( nsname );
                  return -1;
               }
               reponame = strdup( (char*)attr->children->content );
            }
            else {
               LOG( LOG_WARNING, "encountered unrecognized 'repo' attribute value for NS \"%s\"\n", nsname );
            }
         }
         else if ( gns  &&  strncmp( (char*)attr->name, "nstgt", 6 ) == 0 ) {
            if ( nstgt ) {
               // we already found a 'nstgt'
               LOG( LOG_WARNING, "encountered a duplicate 'nstgt' value on NS \"%s\"\n", nsname );
               continue;
            }
            // we've found our target
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               nstgt = strdup( (char*)attr->children->content );
            }
            else {
               LOG( LOG_WARNING, "encountered unrecognized 'nstgt' attribute value for NS \"%s\"\n", nsname );
            }
         }
         else if ( strncmp( (char*)attr->name, "name", 5 ) ) {
            LOG( LOG_WARNING, "encountered unrecognized \"%s\" attribute of NS \"%s\"\n", (char*)attr->name, nsname );
         }
      }
      else {
         LOG( LOG_WARNING, "encountered unrecognized tag on NS \"%s\"\n", nsname );
      }
   }

   // additional checks for remote/ghost namespaces
   if ( rns  ||  gns ) {
      // make sure we actually found a 'repo' name
      if ( reponame == NULL ) {
         LOG( LOG_ERR, "%s namespace \"%s\" is missing a 'repo' value\n", (rns) ? "remote":"ghost", nsname );
         if ( nstgt ) { free( nstgt ); }
         free( nsname );
         errno = EINVAL;
         return -1;
      }
      // and nstgt, if applicable
      if ( gns  &&  nstgt == NULL ) {
         LOG( LOG_ERR, "ghost namespace \"%s\" is missing a 'nstgt' value\n", nsname );
         free( reponame );
         free( nsname );
         errno = EINVAL;
         return -1;
      }
   }

   // time to populate the NS name
   nsnode->name = nsname;

   // weight of these nodes will always be zero
   nsnode->weight = 0;

   // finally time to allocate the 'real' namespace struct
   nsnode->content = malloc( sizeof( marfs_ns ) );
   if ( nsnode->content == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for namespace \"%s\"\n", nsname );
      if ( reponame ) { free( reponame ); }
      if ( nstgt ) { free( nstgt ); }
      free( nsname );
      if ( rns ) free( reponame );
      return -1;
   }
   marfs_ns* ns = (marfs_ns*)nsnode->content; //shorthand ref

   // set some default namespace values
   ns->fquota = 0;
   ns->dquota = 0;
   ns->iperms = NS_NOACCESS;
   ns->bperms = NS_NOACCESS;
   ns->subspaces = NULL;
   ns->subnodes = NULL;
   ns->subnodecount = 0;
   ns->ghtarget = NULL;
   ns->ghsource = NULL;

   // set parent values
   ns->prepo = prepo;
   ns->pnamespace = pnamespace;
   if ( rns ) {
      // we'll indicate this is a remote namespace by NULLing out the prepo ref
      ns->prepo = NULL;
      // we need to know what repo to look for the full namespace def in; so jam it in as the id string
      ns->idstr = reponame;
      // make sure there aren't any other XML nodes lurking below here
      if ( nsroot->children ) {
         LOG( LOG_WARNING, "ignoring all xml elements below remote NS \"%s\"\n", nsname );
      }
      // for remote namespaces, we're done; no other values are relevant
      return 0;
   }
   size_t idstrlen;
   if ( gns ) {
      // we'll indicate this is a ghost namespace via a self-referential ghtarget ( real tgt determined later )
      ns->ghtarget = ns;
      // stash the ghost target info into the id string
      idstrlen = strlen( reponame ) + 2 + strlen( nstgt );
   }
   else {
      // populate the namespace id string
      idstrlen = strlen(nsname) + 2; // nsname plus '|' prefix and NULL terminator
      if ( pnamespace ) { idstrlen += strlen( pnamespace->idstr ); } // prepend to parent name
      else { idstrlen += strlen( prepo->name ); } // or parent repo name, if we are at the top
   }
   ns->idstr = malloc( sizeof(char) * idstrlen );
   if ( ns->idstr == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for the id string of NS \"%s\"\n", nsname );
      if ( nstgt ) { free( nstgt ); }
      free( nsname );
      free( ns );
      return -1;
   }
   int prres = 0;
   if ( gns ) {
      prres = snprintf( ns->idstr, idstrlen, "%s|%s", reponame, nstgt );
      // done with these strings
      free( nstgt );
      free( reponame );
   }
   else if ( pnamespace ) {
      prres = snprintf( ns->idstr, idstrlen, "%s|%s", pnamespace->idstr, nsname );
   }
   else {
      prres = snprintf( ns->idstr, idstrlen, "%s|%s", prepo->name, nsname );
   }
   // check for successful print (probably unnecessary)
   if ( prres != (idstrlen - 1) ) {
      LOG( LOG_ERR, "failed to populate id string of NS \"%s\"\n", nsname );
      free( ns->idstr );
      free( nsname );
      free( ns );
      return -1;
   }

   // real namespaces may have additional namespace defs below them
   int subspcount = count_nodes( nsroot->children, "ns" );
   subspcount += count_nodes( nsroot->children, "rns" );
   subspcount += count_nodes( nsroot->children, "gns" );
   HASH_NODE* subspacelist = NULL;
   if ( subspcount ) {
      // subspaces are forbidden for ghosts
      if ( gns ) {
         LOG( LOG_ERR, "GhostNS with forbidden child namespaces: \"%s\"\n", nsname );
         free( ns->idstr );
         free( nsname );
         free( ns );
         return -1;
      }
      subspacelist = malloc( sizeof( HASH_NODE ) * subspcount );
      if ( subspacelist == NULL ) {
         LOG( LOG_ERR, "failed to allocate space for subspace list of NS \"%s\"\n", nsname );
         free( ns->idstr );
         free( nsname );
         free( ns );
         return -1;
      }
   }

   // iterate over children, populating perms, quotas, and subspaces
   xmlNode* subnode = nsroot->children;
   size_t allocsubspaces = 0;
   int retval = 0;
   for ( ; subnode; subnode = subnode->next ) {
      if ( subnode->type == XML_ELEMENT_NODE ) {
         if ( strncmp( (char*)subnode->name, "quotas", 7 ) == 0 ) {
            // doesn't work for ghosts
            if ( gns ) {
               LOG( LOG_WARNING, "Skipping non-functional quota definition for GhostNS: \"%s\"\n", nsname );
               continue;
            }
            // parse NS quota info
            if( parse_quotas( &(ns->fquota), &(ns->dquota), subnode->children ) ) {
               LOG( LOG_ERR, "failed to parse quota info for NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
         }
         else if ( strncmp( (char*)subnode->name, "perms", 6 ) == 0 ) {
            // parse NS perm info
            if ( parse_perms( &(ns->iperms), &(ns->bperms), subnode->children ) ) {
               LOG( LOG_ERR, "failed to parse perm info for NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
         }
         else if ( strncmp( (char*)subnode->name, "ns", 3 ) == 0  ||  
                   strncmp( (char*)subnode->name, "rns", 4 ) == 0  ||
                   strncmp( (char*)subnode->name, "gns", 4 ) == 0 ) {
            // ensure we haven't encountered more nodes than expected
            if ( allocsubspaces >= subspcount ) {
               LOG( LOG_ERR, "insufficient subspace allocation for NS \"%s\"\n", nsname );
               retval = -1;
               errno = EFAULT;
               break;
            }
            // allocate a subspace
            HASH_NODE* subnsnode = subspacelist + allocsubspaces;
            if ( create_namespace( subnsnode, ns, prepo, subnode ) ) {
               LOG( LOG_ERR, "failed to initialize subspace of NS \"%s\"\n", nsname );
               retval = -1;
               break;
            }
            // ensure we don't have any name collisions
            if ( find_namespace( subspacelist, allocsubspaces, subnsnode->name ) ) {
               LOG( LOG_ERR, "encountered subspace name collision: \"%s\"\n", subnsnode->name );
               free_namespace( subnsnode );
               retval = -1;
               break;
            }
            allocsubspaces++;
         }
         else {
            LOG( LOG_ERR, "encountered unrecognized \"%s\" child node of NS \"%s\"\n", (char*)subnode->name, nsname );
            retval = -1;
            errno = EINVAL;
            break;
         }
      }
      else if ( subnode->type != XML_COMMENT_NODE ) { // ignore comment nodes
         LOG( LOG_ERR, "encountered unrecognized xml child node of NS \"%s\": \"%s\"\n", nsname, (char*)(subnode->name) );
         retval = -1;
         errno = EINVAL;
         break;
      }
   }

   // ensure we found all of the subspaces we allocated references for
   if ( allocsubspaces != subspcount ) {
      LOG( LOG_ERR, "encountered a reduced count of subspaces for NS \"%s\"\n", nsname );
      retval = -1;
      errno = EFAULT;
   }

   // allocate our subspace hash table only if we aren't already aborting and have subspaces
   if ( retval == 0  &&  allocsubspaces ) {
      ns->subspaces = hash_init( subspacelist, subspcount, 1 );
      if ( ns->subspaces == NULL ) {
         LOG( LOG_ERR, "failed to create the subspace table of NS \"%s\"\n", nsname );
         retval = -1;
      }
   }

   // catch any failure cases, and free all memory
   if ( retval != 0 ) {
      while ( allocsubspaces ) {
         allocsubspaces--;
         if ( free_namespace( subspacelist + allocsubspaces ) ) {
            // nothing to do besides complain; we're already failing out
            LOG( LOG_WARNING, "failed to free subspace %d of NS \"%s\"\n", nsname );
         }
      }
      if ( subspacelist ) { free( subspacelist ); }
      free( ns->idstr );
      free( nsname );
      free( ns );
      return retval;
   }

   // store the node list, for later iteration
   ns->subnodes = subspacelist;
   ns->subnodecount = subspcount;

   return 0;
}

/**
 * Create a new HASH_TABLE, based on the content of the given distribution node
 * @param int* count : Integer to be populated with the count of distribution targets
 * @param xmlNode* distroot : Xml node containing distribution info
 * @return HASH_TABLE : Newly created HASH_TABLE, or NULL if a failure occurred
 */
HASH_TABLE create_distribution_table( int* count, xmlNode* distroot ) {
   // iterate over attributes, looking for cnt and dweight values
   int dweight = 1;
   size_t nodecount = 0;
   xmlAttr* attr = distroot->properties;
   for ( ; attr; attr = attr->next ) {
      char*  attrtype = NULL;
      char* attrval = NULL;
      if ( attr->type == XML_ATTRIBUTE_NODE ) {
         // attempt to stash the attribute type and value, for later parsing
         attrtype = (char*)attr->name;
         if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
            attrval = (char*)attr->children->content;
         }
         else {
            LOG( LOG_ERR, "encountered an unrecognized '%s' value for %s distribution\n", attrtype, (char*)distroot->name );
            errno = EINVAL;
            return NULL;
         }

         // perform some checks, specific to the attribute type
         if ( strncmp( attrtype, "cnt", 4 ) == 0 ) {
            if ( nodecount != 0 ) {
               // we already found a 'cnt'
               LOG( LOG_ERR, "encountered a duplicate 'cnt' value for %s distribution\n", (char*)distroot->name );
               errno = EINVAL;
               return NULL;
            }
         }
         else if ( strncmp( attrtype, "dweight", 8 ) == 0 ) {
            if ( dweight != 1 ) {
               // we already found a 'dweight' ( note - this will fail to detect prior dweight='1' )
               LOG( LOG_ERR, "encountered a duplicate 'dweight' value for %s distribution\n", (char*)distroot->name );
               errno = EINVAL;
               return NULL;
            }
         }
         else {
            // reject any unrecognized attributes
            LOG( LOG_WARNING, "ignoring unrecognized '%s' attr of %s distribution\n", attrtype, (char*)distroot->name );
            attrval = NULL; // don't try to interpret
         }
      }
      else {
         // not even certain this is possible; warn just in case our config is in a very odd state
         LOG( LOG_WARNING, "encountered an unrecognized property of %s distribution\n", (char*)distroot->name );
      }

      // if we found a new attribute value, parse it
      if ( attrval ) {
         char* endptr = NULL;
         unsigned long long parseval = strtoull( attrval, &(endptr), 10 );
         if ( *endptr != '\0' ) {
            LOG( LOG_ERR, "detected a trailing '%c' character in '%s' value for %s distribution\n", *endptr, attrtype, (char*)distroot->name );
            errno = EINVAL;
            return NULL;
         }
         // check for possible value overflow
         if ( parseval >= INT_MAX ) {
            LOG( LOG_ERR, "%s distribution has a '%s' value which exceeds memory bounds: \"%s\"\n", (char*)distroot->name, attrtype, attrval );
            errno = EINVAL;
            return NULL;
         }

         // assign value to the appropriate var
         // Note -- this check can be lax, as we already know attrype == "dweight" OR "cnt"
         if ( *attrtype == 'c' ) {
            nodecount = parseval;
         }
         else {
            dweight = parseval;
         }
      }
   }
   // all attributes of this distribution have been parsed
   // make sure we have the required 'cnt' value
   if ( nodecount == 0 ) {
      LOG( LOG_ERR, "failed to identify a valid 'cnt' of %s distribution\n", (char*)distroot->name );
      errno = EINVAL;
      return NULL;
   }

   // allocate space for all hash nodes
   HASH_NODE* nodelist = malloc( sizeof(HASH_NODE) * nodecount );
   if ( nodelist == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for hash nodes of %s distribution\n", (char*)distroot->name );
      return NULL;
   }

   // populate all node names and set weights to default
   size_t curnode;
   for ( curnode = 0; curnode < nodecount; curnode++ ) {
      nodelist[curnode].content = NULL; // we don't care about content
      nodelist[curnode].weight = dweight; // every node gets the default weight, for now
      // identify the number of characers needed for this nodename via some cheeky use of snprintf
      int namelen = snprintf( NULL, 0, "%zu", curnode );
      nodelist[curnode].name = malloc( sizeof(char) * (namelen + 1) );
      if ( nodelist[curnode].name == NULL ) {
         LOG( LOG_ERR, "failed to allocate space for node names of %s distribution\n", (char*)distroot->name );
         break;
      }
      if ( snprintf( nodelist[curnode].name, namelen + 1, "%zu", curnode ) > namelen ) {
         LOG( LOG_ERR, "failed to populate nodename \"%zu\" of %s distribution\n", curnode, (char*)distroot->name );
         errno = EFAULT;
         free( nodelist[curnode].name );
         break;
      }
   }
   // check for any 'break' conditions and clean up our allocated memory
   if ( curnode != nodecount ) {
      // free all name strings
      size_t freenode;
      for ( freenode = 0; freenode < curnode; freenode++ ) {
         free( nodelist[freenode].name );
      }
      // free our nodelist and terminate
      free( nodelist );
      return NULL;
   }

   // now, we need to check for specifically defined weight values
   char errorflag = 0;
   if ( distroot->children != NULL  &&
        distroot->children->type == XML_TEXT_NODE  &&
        distroot->children->content != NULL ) {
      char* weightstr = (char*)distroot->children->content;
      size_t tgtnode = nodecount;
      while ( *weightstr != '\0' ) {
         char* endptr;
         unsigned long long parseval = strtoull( weightstr, &(endptr), 10 );
         // perform checks for invalid trailing characters
         if ( tgtnode == nodecount  &&  *endptr != '=' ) {
            LOG( LOG_ERR, "improperly formatted node definition for %s distribution\n", (char*)distroot->name );
            errno = EINVAL;
            errorflag = 1;
            break;
         }
         else if ( tgtnode != nodecount  &&  *endptr != ','  &&  *endptr != '\0' ) {
            LOG( LOG_ERR, "improperly formatted weight value of node %zu for %s distribution\n", tgtnode, (char*)distroot->name );
            errno = EINVAL;
            errorflag = 1;
            break;
         }

         // attempt to assign our parsed numeric value to the proper var
         if ( tgtnode == nodecount ) {
            // this is a definition of target node
            // check for an out of bounds value
            if ( parseval >= nodecount ) {
               LOG( LOG_ERR, "%s distribution has a node value which exceeds the defined limit of %zu\n", (char*)distroot->name, nodecount );
               errno = EINVAL;
               errorflag = 1;
               break;
            }
            tgtnode = parseval; // assign the tgtnode value
         }
         else {
            // this is a weight definition
            // check for an out of bounds value
            if ( parseval >= INT_MAX ) {
               LOG( LOG_ERR, "%s distribution has a weight value for node %zu which exceeds memory limits\n", (char*)distroot->name, tgtnode );
               errno = EINVAL;
               errorflag = 1;
               break;
            }
            nodelist[tgtnode].weight = parseval; // assign the specified weight to the target node
            tgtnode = nodecount; // reset our target, so we know to expect a new one
         }

         // abort if we've reached the end of the string
         if ( *endptr == '\0' ) { break; }
         // if we've gotten here, endptr either references '=' or ';'
         // either way, we need to go one character further to find our next def
         weightstr = (endptr + 1);
      }
      if ( tgtnode != nodecount ) {
         LOG( LOG_ERR, "%s distribution has node %zu reference, but no defined weight value\n", (char*)distroot->name, tgtnode );
         errno = EINVAL;
         errorflag = 1;
      }
   }
   // check if we hit any error
   if ( errorflag ) {
      // free all name strings
      size_t freenode;
      for ( freenode = 0; freenode < nodecount; freenode++ ) {
         free( nodelist[freenode].name );
      }
      // free our nodelist and terminate
      free( nodelist );
      return NULL;
   }

   // finally, initialize the hash table
   HASH_TABLE table = hash_init( nodelist, nodecount, 0 ); // NOT a lookup table
   // verify success
   if ( table == NULL ) {
      LOG( LOG_ERR, "failed to initialize hash table for %s distribution\n", (char*)distroot->name );
      // free all name strings
      size_t freenode;
      for ( freenode = 0; freenode < nodecount; freenode++ ) {
         free( nodelist[freenode].name );
      }
      // free our nodelist and terminate
      free( nodelist );
      return NULL;
   }

   // populate the provided count value
   *count = (int)nodecount;
   // return the created table
   return table;
}

/**
 * Free the given repo structure
 * @param marfs_repo* repo : Reference to the repo to be freed
 * @return int : Zero on success, or -1 on failure
 */
int free_repo( marfs_repo* repo ) {
   // note any errors
   int retval = 0;

   // free metadata scheme components
   if ( repo->metascheme.mdal ) {
      if ( repo->metascheme.mdal->cleanup( repo->metascheme.mdal ) ) {
         LOG( LOG_WARNING, "failed to terminate MDAL of \"%s\" repo\n", repo->name );
         retval = -1;
      }
   }
   size_t nodecount;
   HASH_NODE* nodelist;
   if ( repo->metascheme.reftable ) {
      if ( hash_term( repo->metascheme.reftable, &(nodelist), &(nodecount) ) ) {
         LOG( LOG_WARNING, "failed to free the reference path hash table of \"%s\" repo\n", repo->name );
         retval = -1;
      }
      else {
         // free all hash nodes ( no content for reference path nodes )
         size_t nodeindex = 0;
         for( ; nodeindex < nodecount; nodeindex++ ) {
            free( nodelist[nodeindex].name );
         }
         if ( nodelist != repo->metascheme.refnodes ) {
            LOG( LOG_ERR, "Reference table node list differs from expected value\n" );
            retval = -1;
         }
         free( nodelist );
      }
   }
   int nsindex;
   for ( nsindex = 0; nsindex < repo->metascheme.nscount; nsindex++ ) {
      // recursively free all namespaces below this repo
      if ( free_namespace( repo->metascheme.nslist + nsindex ) ) {
         LOG( LOG_WARNING, "failed to free NS %d of \"%s\" repo\n", nsindex, repo->name );
         retval = -1;
      }
   }
   if ( repo->metascheme.nslist ) { free( repo->metascheme.nslist ); }

   // free data scheme components
   if ( repo->datascheme.nectxt ) {
      if ( ne_term( repo->datascheme.nectxt ) ) {
         LOG( LOG_WARNING, "failed to terminate NE context of \"%s\" repo\n", repo->name );
         retval = -1;
      }
   }
   int target;
   for( target = 0; target < 3; target++ ) {
      HASH_TABLE ttable;
      if ( target == 0 ) { ttable = repo->datascheme.podtable; }
      else if ( target == 1 ) { ttable = repo->datascheme.captable; }
      else { ttable = repo->datascheme.scattertable; }
      // skip this table if it was never allocated
      if ( ttable == NULL ) { continue; }
      // otherwise, free the table
      if ( hash_term( ttable, &(nodelist), &(nodecount) ) ) {
         LOG( LOG_WARNING, "failed to free a distribution hash table of \"%s\" repo\n", repo->name );
         retval = -1;
      }
      else {
         // free all hash nodes
         size_t nodeindex = 0;
         for( ; nodeindex < nodecount; nodeindex++ ) {
            free( nodelist[nodeindex].name );
         }
         free( nodelist );
      }
   }

   free( repo->name );

   return retval;
}

/**
 * Parse the given datascheme xml node to populate the given datascheme structure
 * @param marfs_ds* ds : Datascheme to be populated
 * @param xmlNode* dataroot : Xml node to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int parse_datascheme( marfs_ds* ds, xmlNode* dataroot ) {
   xmlNode* dalnode = NULL;
   ne_location maxloc = { .pod = 0, .cap = 0, .scatter = 0 };
   // iterate over nodes at this level
   for ( ; dataroot; dataroot = dataroot->next ) {
      // check for unknown xml node type
      if ( dataroot->type != XML_ELEMENT_NODE ) {
         // skip comment nodes
         if ( dataroot->type == XML_COMMENT_NODE ) { continue; }
         // don't know what this is supposed to be
         LOG( LOG_ERR, "encountered unknown node within a 'data' definition\n" );
         return -1;
      }

      // first, check for a DAL definition
      if ( strncmp( (char*)dataroot->name, "DAL", 4 ) == 0 ) {
         if ( dalnode ) {
            LOG( LOG_ERR, "detected duplicate DAL definition\n" );
            return -1;
         }
         dalnode = dataroot; // save our DAL node reference for later
         continue;
      }
      // check for an 'enabled' attr
      xmlAttr* attr = dataroot->properties;
      char enabled = 2; // assume enabled, but track if we actually get a value
      if ( attr ) {
         if ( attr->type == XML_ATTRIBUTE_NODE ) {
            if ( strncmp( (char*)attr->name, "enabled", 8 ) == 0 ) {
               if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
                  if ( strncmp( (char*)attr->children->content, "no", 3 ) == 0 ) {
                     enabled = 0;
                  }
                  else if ( strncmp( (char*)attr->children->content, "yes", 4 ) == 0 ) {
                     enabled = 1;
                  }
               }
            }
         }
         // if we found any attribute value, ensure it made sense
         if ( enabled == 2 ) {
            LOG( LOG_ERR, "encountered an unrecognized attribute of a '%s' node within a 'data' definition\n", (char*)dataroot->name );
            return -1;
         }
         // make sure this is the only attribute
         if ( attr->next ) {
            LOG( LOG_ERR, "detected trailing attributes of a '%s' node within a 'data' definition\n", (char*)dataroot->name );
            return -1;
         }
         // if this node has been disabled, don't even bother parsing it
         if ( !(enabled) ) { continue; }
      }
      // determine what definition we are parsing
      xmlNode* subnode = dataroot->children;
      if ( strncmp( (char*)dataroot->name, "protection", 11 ) == 0 ) {
         // iterate over child nodes, populating N/E/PSZ
         char haveN = 0;
         char haveE = 0;
         char haveP = 0;
         for( ; subnode; subnode = subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               // skip comment nodes
               if ( subnode->type == XML_COMMENT_NODE ) { continue; }
               LOG( LOG_ERR, "encountered unknown node within a 'protection' definition\n" );
               return -1;
            }
            if ( strncmp( (char*)subnode->name, "N", 2 ) == 0 ) {
               haveN = 1;
               if( parse_int_node( &(ds->protection.N), subnode ) ) {
                  LOG( LOG_ERR, "failed to parse 'N' value within a 'protection' definition\n" );
                  return -1;
               }
            }
            else if ( strncmp( (char*)subnode->name, "E", 2 ) == 0 ) {
               haveE = 1;
               if( parse_int_node( &(ds->protection.E), subnode ) ) {
                  LOG( LOG_ERR, "failed to parse 'E' value within a 'protection' definition\n" );
                  return -1;
               }
            }
            else if ( strncmp( (char*)subnode->name, "PSZ", 4 ) == 0 ) {
               haveP = 1;
               if( parse_size_node( &(ds->protection.partsz), subnode ) ) {
                  LOG( LOG_ERR, "failed to parse 'PSZ' value within a 'protection' definition\n" );
                  return -1;
               }
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'protection' definition\n", (char*)subnode->name );
               return -1;
            }
         }
         // verify that all expected values were populated
         if ( !(haveN)  ||  !(haveE)  ||  !(haveP) ) {
            LOG( LOG_ERR, "encountered a 'protection' definition without all N/E/PSZ values\n" );
            return -1;
         }
      }
      else if ( strncmp( (char*)dataroot->name, "packing", 8 ) == 0 ) {
         // iterate over child nodes, populating max_files
         char haveM = 0;
         for( ; subnode; subnode = subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               // skip comment nodes
               if ( subnode->type == XML_COMMENT_NODE ) { continue; }
               LOG( LOG_ERR, "encountered unknown node within a 'packing' definition\n" );
               return -1;
            }
            if ( strncmp( (char*)subnode->name, "max_files", 10 ) == 0 ) {
               haveM = 1;
               if( parse_size_node( &(ds->objfiles), subnode ) ) {
                  LOG( LOG_ERR, "failed to parse 'max_files' value within a 'packing' definition\n" );
                  return -1;
               }
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'packing' definition\n", (char*)subnode->name );
               return -1;
            }
         }
         // verify that all expected values were populated
         if ( !(haveM) ) {
            LOG( LOG_ERR, "encountered a 'packing' definition without a 'max_files' value\n" );
            return -1;
         }
      }
      else if ( strncmp( (char*)dataroot->name, "chunking", 9 ) == 0 ) {
         // iterate over child nodes, populating max_size
         char haveM = 0;
         for( ; subnode; subnode = subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               // skip comment nodes
               if ( subnode->type == XML_COMMENT_NODE ) { continue; }
               LOG( LOG_ERR, "encountered unknown node within a 'chunking' definition\n" );
               return -1;
            }
            if ( strncmp( (char*)subnode->name, "max_size", 9 ) == 0 ) {
               haveM = 1;
               if( parse_size_node( &(ds->objsize), subnode ) ) {
                  LOG( LOG_ERR, "failed to parse 'max_size' value within a 'chunking' definition\n" );
                  return -1;
               }
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'chunking' definition\n", (char*)subnode->name );
               return -1;
            }
         }
         // verify that all expected values were populated
         if ( !(haveM) ) {
            LOG( LOG_ERR, "encountered a 'chunking' definition without a 'max_size' value\n" );
            return -1;
         }
      }
      else if ( strncmp( (char*)dataroot->name, "distribution", 13 ) == 0 ) {
         // iterate over child nodes, creating our distribution tables
         for( ; subnode; subnode = subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               // skip comment nodes
               if ( subnode->type == XML_COMMENT_NODE ) { continue; }
               LOG( LOG_ERR, "encountered unknown node within a 'distribution' definition\n" );
               return -1;
            }
            if ( strncmp( (char*)subnode->name, "pods", 5 ) == 0 ) {
               if ( (ds->podtable = create_distribution_table( &(maxloc.pod), subnode )) == NULL ) {
                  LOG( LOG_ERR, "failed to create 'pods' distribution table\n" );
                  return -1;
               }
               maxloc.pod--; // decrement node count to get actual max value
            }
            else if ( strncmp( (char*)subnode->name, "caps", 5 ) == 0 ) {
               if ( (ds->captable = create_distribution_table( &(maxloc.cap), subnode )) == NULL ) {
                  LOG( LOG_ERR, "failed to create 'caps' distribution table\n" );
                  return -1;
               }
               maxloc.cap--; // decrement node count to get actual max value
            }
            else if ( strncmp( (char*)subnode->name, "scatters", 9 ) == 0 ) {
               if ( (ds->scattertable = create_distribution_table( &(maxloc.scatter), subnode )) == NULL ) {
                  LOG( LOG_ERR, "failed to create 'scatters' distribution table\n" );
                  return -1;
               }
               maxloc.scatter--; // decrement node count to get actual max value
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a 'distribution' definition\n", (char*)subnode->name );
               return -1;
            }
         }
      }
      else {
         LOG( LOG_ERR, "encountered unexpected 'data' sub-node: \"%s\"\n", (char*)dataroot->name );
         return -1;
      }
   }
   // now that we're done iterating through our config, make sure we found a DAL definition
   if ( !(dalnode) ) {
      LOG( LOG_ERR, "failed to locate a DAL definition\n" );
      return -1;
   }
   // attempt to create our NE context
   if ( (ds->nectxt = ne_init( dalnode, maxloc, ds->protection.N + ds->protection.E )) == NULL ) {
      LOG( LOG_ERR, "failed to initialize an NE context\n" );
      return -1;
   }

   return 0;
}

/**
 * Parse the given metascheme xml node to populate the given metascheme structure
 * @param marfs_repo* repo : Repo, with metascheme to be populated
 * @param xmlNode* metaroot : Xml node to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int parse_metascheme( marfs_repo* repo, xmlNode* metaroot ) {
   marfs_ms* ms = &(repo->metascheme);
   xmlNode* mdalnode = NULL;
   // iterate over nodes at this level
   for ( ; metaroot; metaroot = metaroot->next ) {
      // make sure this is a real ELEMENT_NODE
      if ( metaroot->type != XML_ELEMENT_NODE ) {
         // skip comment nodes
         if ( metaroot->type == XML_COMMENT_NODE ) { continue; }
         LOG( LOG_ERR, "detected unrecognized node within a 'meta' definition\n" );
         return -1;
      }
      // first, check for an MDAL definition
      if ( strncmp( (char*)metaroot->name, "MDAL", 5 ) == 0 ) {
         if ( mdalnode ) {
            LOG( LOG_ERR, "detected duplicate MDAL definition\n" );
            return -1;
         }
         mdalnode = metaroot; // save our MDAL node reference for later
         continue;
      }
      // determine what definition we are parsing
      xmlAttr* attr = metaroot->properties;
      xmlNode* subnode = metaroot->children;
      if ( strncmp( (char*)metaroot->name, "namespaces", 11 ) == 0 ) {
         // check if we have already created a reference table, as this indicates a duplicate node
         if ( ms->reftable ) {
            LOG( LOG_ERR, "detected a duplicate 'namespaces' definition\n" );
            return -1;
         }
         int refbreadth = 0;
         int refdepth = 0;
         int refdigits = 0;
         for ( ; attr; attr = attr->next ) {
            int attrvalue = 0;
            if ( attr->type == XML_ATTRIBUTE_NODE ) {
               if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
                  // all attributes of this node should be integer strings
                  char* endptr = NULL;
                  unsigned long long parseval = strtoull( (char*)attr->children->content, &(endptr), 10 );
                  if ( parseval >= INT_MAX ) {
                     LOG( LOG_ERR, "value of \"%s\" attribute is to large to store\n", (char*)attr->name );
                     return -1;
                  }
                  if ( *endptr != '\0' ) {
                     LOG( LOG_ERR, "detected trailing '%c' character in value of \"%s\" attribute\n", (char*)attr->name );
                     return -1;
                  }
                  attrvalue = parseval;
               }
            }
            // check that we have a sensible attribute value
            if ( attrvalue <= 0 ) {
               LOG( LOG_ERR, "inappropriate value for a \"%s\" attribute\n", (char*)attr->name );
               return -1;
            }
            // check which value this attribute provides
            if ( strncmp( (char*)attr->name, "rbreadth", 9 ) == 0 ) {
               refbreadth = attrvalue;
            }
            else if ( strncmp( (char*)attr->name, "rdepth", 7 ) == 0 ) {
               refdepth = attrvalue;
            }
            else if ( strncmp( (char*)attr->name, "rdigits", 8 ) == 0 ) {
               refdigits = attrvalue;
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized attribute of a 'namespaces' node: \"%s\"\n", (char*)attr->name );
               return -1;
            }
         }
         // verify that we found the required reference dimensions
         if ( refbreadth < 1  ||  refdepth < 1 ) {
            LOG( LOG_ERR, "failed to locate required 'rbreadth' and/or 'rdepth' values for a 'namespaces' node\n" );
            return -1;
         }
         // create a string to hold temporary reference paths
         int breadthdigits = numdigits_unsigned( (unsigned long long) refbreadth );
         if ( refdigits > breadthdigits ) { breadthdigits = refdigits; }
         size_t rpathlen = ( refdepth * (breadthdigits + 1) ) + 1;
         char* rpathtmp = malloc( sizeof(char) * rpathlen ); // used to populate node name strings
         if ( rpathtmp == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for namespace refpaths\n" );
            return -1;
         }
         // create an array of integers to hold reference indexes
         int* refvals = malloc( sizeof(int) * refdepth );
         if ( refvals == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for namespace reference indexes\n" );
            free( rpathtmp );
            return -1;
         }
         // create an array of hash nodes
         size_t rnodecount = 1;
         int curdepth = refdepth;
         while ( curdepth ) { rnodecount *= refbreadth; curdepth--; } // equiv of breadth to the depth power
         HASH_NODE* rnodelist = malloc( sizeof(struct hash_node_struct) * rnodecount );
         if ( rnodelist == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for namespace reference hash nodes\n" );
            free( rpathtmp );
            free( refvals );
            return -1;
         }
         // populate all hash nodes
         size_t curnode;
         for ( curnode = 0; curnode < rnodecount; curnode++ ) {
            // populate the index for each rnode, starting at the depest level
            size_t tmpnode = curnode;
            for ( curdepth = refdepth; curdepth; curdepth-- ) {
               refvals[curdepth-1] = tmpnode % refbreadth; // what is our index at this depth
               tmpnode /= refbreadth; // find how many groups we have already traversed at this depth
            }
            // now populate the reference pathname
            char* outputstr = rpathtmp;
            int pathlenremaining = rpathlen;
            for ( curdepth = 0; curdepth < refdepth; curdepth++ ) {
               int prlen = snprintf( outputstr, pathlenremaining, "%.*d/", breadthdigits, refvals[curdepth] );
               if ( prlen <= 0  ||  prlen >= pathlenremaining ) {
                  LOG( LOG_ERR, "failed to generate reference path string\n" );
                  free( rpathtmp );
                  free( refvals );
                  free( rnodelist );
                  return -1;
               }
               pathlenremaining -= prlen;
               outputstr += prlen;
            }
            // copy the reference pathname into the hash node
            rnodelist[curnode].name = strndup( rpathtmp, rpathlen );
            if ( rnodelist[curnode].name == NULL ) {
               LOG( LOG_ERR, "failed to allocate reference path hash node name\n" );
               free( rpathtmp );
               free( refvals );
               free( rnodelist );
               return -1;
            }
            rnodelist[curnode].weight = 1;
            rnodelist[curnode].content = NULL;
            LOG( LOG_INFO, "created ref node: \"%s\"\n", rnodelist[curnode].name );
         }
         // free data structures which we no longer need
         free( rpathtmp );
         free( refvals );
         // create the reference tree hash table
         ms->reftable = hash_init( rnodelist, rnodecount, 0 );
         if ( ms->reftable == NULL ) {
            LOG( LOG_ERR, "failed to create reference path table\n" );
            free( rnodelist );
            return -1;
         } // can't free the node list now, as it is in use by the hash table
         ms->refnodes = rnodelist;
         ms->refnodecount = rnodecount;
         // count all subnodes
         ms->nscount = 0;
         int subspaces = 0;
         for( ; subnode; subnode = subnode->next ) {
            if ( subnode->type != XML_ELEMENT_NODE ) {
               // skip comment nodes
               if ( subnode->type == XML_COMMENT_NODE ) { continue; }
               LOG( LOG_ERR, "encountered unknown node within a 'namespaces' definition\n" );
               return -1;
            }
            // only 'ns' nodes are acceptible; for now, just assume every node is a NS def
            subspaces++;
         }
         // if we have no subspaces at all, just warn and continue on
         if ( !(subspaces) ) {
            LOG( LOG_WARNING, "metascheme has no namespaces defined\n" );
            continue;
         }
         // allocate an array of namespace nodes
         ms->nslist = malloc( sizeof(HASH_NODE) * subspaces );
         if ( ms->nslist == NULL ) {
            LOG( LOG_ERR, "failed to allocate space for repo namespaces\n" );
            return -1;
         }
         // parse and allocate all subspaces
         for( subnode = metaroot->children; subnode; subnode = subnode->next ) {
            if ( subnode->type == XML_ELEMENT_NODE ) {
               if ( strncmp( (char*)(subnode->name), "ns", 3 ) ) {
                  if ( strncmp( (char*)(subnode->name), "rns", 4 ) == 0 ) {
                     LOG( LOG_ERR, "Remote NS def found at the root of a 'namespaces' definition\n" );
                  }
                  else {
                     LOG( LOG_ERR, "Encountered unrecognized element in a 'namespaces' definition: \"%s\"\n", (char*)(subnode->name) );
                  }
                  return -1;
               }
               // set default namespace values
               HASH_NODE* subspace = ms->nslist + ms->nscount;
               subspace->name = NULL;
               subspace->weight = 0;
               subspace->content = NULL;
               if ( create_namespace( subspace, NULL, repo, subnode ) ) {
                  LOG( LOG_ERR, "failed to create subspace %d\n", ms->nscount );
                  return -1;
               }
               // ensure we don't have any name collisions
               if ( find_namespace( ms->nslist, ms->nscount, subspace->name ) ) {
                  LOG( LOG_ERR, "encountered subspace name collision: \"%s\"\n", subspace->name );
                  free_namespace( subspace );
                  return -1;
               }
               ms->nscount++;
            }
         }
      }
      else if ( strncmp( (char*)metaroot->name, "direct", 7 ) == 0 ) {
         // parse through attributes, looking for a read attr with yes/no values
         for ( ; attr; attr = attr->next ) {
            char enabled = -1;
            if ( attr->type == XML_ATTRIBUTE_NODE ) {
               if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
                  if ( strncmp( (char*)attr->children->content, "no", 3 ) == 0 ) {
                     enabled = 0;
                  }
                  else if ( strncmp( (char*)attr->children->content, "yes", 4 ) == 0 ) {
                     enabled = 1;
                  }
               }
            }
            // check that we have a sensible attribute value
            if ( enabled < 0 ) {
               LOG( LOG_ERR, "inappropriate value for a \"%s\" attribute\n", (char*)attr->name );
               return -1;
            }
            // check which value this attribute provides
            if ( strncmp( (char*)attr->name, "read", 5 ) == 0 ) {
               ms->directread = enabled;
            }
            else {
               LOG( LOG_ERR, "encountered an unrecognized attribute of a 'direct' node: \"%s\"\n", (char*)attr->name );
               return -1;
            }
         }
      }
      else {
         LOG( LOG_ERR, "encountered unexpected meta sub-node: \"%s\"\n", (char*)metaroot->name );
         return -1;
      }
   }
   // ensure we found an MDAL def
   if ( !(mdalnode) ) {
      LOG( LOG_ERR, "failed to locate MDAL definition\n" );
      return -1;
   }
   // initialize the MDAL
   if ( (ms->mdal = init_mdal( mdalnode )) == NULL ) {
      LOG( LOG_ERR, "failed to initialize MDAL\n" );
      return -1;
   }

   return 0;
}

/**
 * Parse the given repo xml node and populate the given marfs_repo reference
 * @param marfs_repo* repo : Reference to the marfs_repo to be populated
 * @param xmlNode* reporoot : Xml node to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int create_repo( marfs_repo* repo, xmlNode* reporoot ) {
   // check for a name attribute
   xmlAttr* attr = reporoot->properties;
   for ( ; attr; attr = attr->next ) {
      if ( strncmp( (char*)attr->name, "name", 5 ) == 0 ) {
         if ( attr->type == XML_ATTRIBUTE_NODE ) {
            if ( attr->children->type == XML_TEXT_NODE  &&  attr->children->content != NULL ) {
               if ( repo->name ) {
                  LOG( LOG_ERR, "detected a repeated 'name' attribute for repo \"%s\"\n", repo->name );
                  return -1;
               }
               repo->name = strdup( (char*)attr->children->content );
            }
         }
      }
      else {
         LOG( LOG_ERR, "encountered an unrecognized attribute of a 'repo' node: \"%s\"\n", (char*)attr->name );
         return -1;
      }
   }
   // verify that we did find a name value
   if ( !(repo->name) ) {
      LOG( LOG_ERR, "failed to identify name value for repo\n" );
      return -1;
   }
   // populate some default repo values
   repo->datascheme.protection.N = 1;
   repo->datascheme.protection.E = 0;
   repo->datascheme.protection.O = 0;
   repo->datascheme.protection.partsz = 1024;
   repo->datascheme.nectxt = NULL;
   repo->datascheme.objfiles = 1;
   repo->datascheme.objsize = 0;
   repo->datascheme.podtable = NULL;
   repo->datascheme.captable = NULL;
   repo->datascheme.scattertable = NULL;
   repo->metascheme.mdal = NULL;
   repo->metascheme.directread = 0;
   repo->metascheme.reftable = NULL;
   repo->metascheme.refnodes = NULL;
   repo->metascheme.refnodecount = 0;
   repo->metascheme.nscount = 0;
   repo->metascheme.nslist = NULL;
   // iterate over child nodes, looking for 'data' and 'meta' defs
   xmlNode* children = reporoot->children;
   for ( ; children != NULL; children = children->next ) {
      // verify that this is a node of the proper type
      if ( children->type != XML_ELEMENT_NODE ) {
         // skip comment nodes
         if ( children->type == XML_COMMENT_NODE ) { continue; }
         LOG( LOG_WARNING, "Encountered unrecognized subnode of \"%s\" repo\n", repo->name );
         continue;
      }
      // check node names
      if ( strncmp( (char*)(children->name), "data", 5 ) == 0 ) {
         if ( parse_datascheme( &(repo->datascheme), children->children ) ) {
            LOG( LOG_ERR, "Failed to parse the 'data' subnode of the \"%s\" repo\n", repo->name );
            break;
         }
      }
      else if ( strncmp( (char*)(children->name), "meta", 5 ) == 0 ) {
         if ( parse_metascheme( repo, children->children ) ) {
            LOG( LOG_ERR, "Failed to parse the 'meta' subnode of the \"%s\" repo\n", repo->name );
            break;
         }
      }
      else {
         LOG( LOG_WARNING, "Encountered unrecognized \"%s\" subnode of \"%s\" repo\n", children->name, repo->name );
      }
   }
   if ( repo->datascheme.nectxt == NULL  ||  repo->metascheme.mdal == NULL ) {
      LOG( LOG_ERR, "\"%s\" repo is missing required data/meta definitions\n", repo->name );
      free_repo( repo );
      return -1;
   }

   return 0;
}

/**
 * Link the given remote NS entry to its actual NS target
 * @param HASH_NODE* rnsnode : HASH_NODE referencing the 'dummy' remote NS entry
 * @param marfs_ns* parent : Reference to the parent NS of the remote NS entry
 * @param marfs_config* config : Reference to the MarFS config
 * @return int : Zero on succes, or -1 on failure
 */
int linkremotens( HASH_NODE* rnsnode, marfs_ns* parent, marfs_config* config ) {
   marfs_ns* rns = (marfs_ns*)(rnsnode->content);
   // the remote NS idstring should contain the name of the target repo
   marfs_repo* tgtrepo = find_repo( config->repolist, config->repocount, rns->idstr );
   if ( tgtrepo == NULL ) {
      LOG( LOG_ERR, "Remote NS \"%s\" of repo \"%s\" references non-existent target repo: \"%s\"\n", rnsnode->name, parent->prepo->name, rns->idstr );
      return -1;
   }
   // now determine the target NS in the target repo
   HASH_NODE* tgtnsnode = find_namespace( tgtrepo->metascheme.nslist, tgtrepo->metascheme.nscount, rnsnode->name );
   if ( tgtnsnode == NULL ) {
      LOG( LOG_ERR, "Remote NS \"%s\" of repo \"%s\" does not have a valid NS def in target repo \"%s\"\n", rnsnode->name, parent->prepo->name, rns->idstr );
      return -1;
   }
   marfs_ns* tgtns = (marfs_ns*)(tgtnsnode->content);
   if ( tgtns == NULL ) {
      LOG( LOG_ERR, "Remote NS \"%s\" of repo \"%s\" references a NS node with NULL content\n", rnsnode->name );
      return -1;
   }
   if ( tgtns->pnamespace ) {
      LOG( LOG_ERR, "Remote NS \"%s\" of repo \"%s\" references a NS which has already been linked\n", rnsnode->name );
      return -1;
   }
   // we have to clear out the remote NS ref and replace it with the actual NS
   free( rns->idstr );
   free( rns );
   rnsnode->content = (void*)tgtns;
   tgtns->pnamespace = parent;
   tgtnsnode->content = NULL; // remove the previous NS ref
   return 0;
}

/**
 * Traverse the namespaces of the given config, establishing remote NS refs
 * @param marfs_config* config : Reference to the config to traverse
 * @return int : Zero on success, or -1 on failure
 */
int establish_nsrefs( marfs_config* config ) {

   // iterate over top level namespaces of every repo, looking for the root NS
   int currepo = 0;
   for ( ; currepo < config->repocount; currepo++ ) {
      // check for the root ns in the current repo
      marfs_repo* repo = config->repolist + currepo;
      HASH_NODE* rootnsnode = find_namespace( repo->metascheme.nslist, repo->metascheme.nscount, "root" );
      if ( rootnsnode != NULL ) {
         marfs_ns* rootns = (marfs_ns*)(rootnsnode->content);
         if ( config->rootns != NULL ) {
            LOG( LOG_ERR, "encountered two 'root' namespaces in the same config\n" );
            return -1;
         }
         config->rootns = rootns;
      }
   }
   if ( config->rootns == NULL ) {
      LOG( LOG_ERR, "Failed to identify the root NS\n" );
      return -1;
   }

   // replace the ID string of the root NS
   char* previd = config->rootns->idstr;
   size_t idlen = strlen(config->rootns->prepo->name) + 1 + 2;
   config->rootns->idstr = malloc( sizeof(char) * (idlen + 1) );
   if ( config->rootns->idstr == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new rootNS ID string\n" );
      config->rootns->idstr = previd;
      return -1;
   }
   if ( snprintf( config->rootns->idstr, idlen + 1, "%s|/.",
                  config->rootns->prepo->name ) != idlen ) {
      LOG( LOG_ERR, "Failed to populate new ID string for rootNS\n" );
      free( config->rootns->idstr );
      config->rootns->idstr = previd;
      return -1;
   }
   free( previd ); // done with original ID
                  
   // traverse the entire NS hierarchy, replacing remote NS refs with actual NS pointers
   marfs_ns* curns = config->rootns;
   size_t nscount = 0;
   size_t rnscount = 0;
   while ( curns ) {
      
      // iterate over subspaces
      HASH_NODE* subnode = NULL;
      int iterres = 0;
      if ( curns->subspaces  &&  (iterres = hash_iterate( curns->subspaces, &(subnode) )) > 0 ) {
         marfs_ns* subspace = (marfs_ns*)(subnode->content);
         if ( subspace == NULL ) { LOG( LOG_ERR, "NULL subspace ref\n" ); return -1; }
         // check for a ghost NS
         if ( subspace->ghtarget ) {
            // the ghost NS idstring should contain both the repo and NS of the target
            char* tgtnsname;
            if ( config_nsinfo( subspace->idstr, NULL, &tgtnsname ) < 0 ) {
               LOG( LOG_ERR, "Failed to parse GhostNS \"%s\" target info: \"%s\"\n", subnode->name, subspace->idstr );
               return -1;
            }
            // parse of the nspath to find our target
            HASH_NODE* tgtnode = NULL;
            marfs_ns* tgtns = config->rootns; // start at the root
            char* nsparse = tgtnsname;
            char* nssegment = tgtnsname;
            char substitute = 0;
            size_t len = 0;
            while ( *nsparse != '\0' ) {
               // identify the next path segment
               do {
                  if ( *nsparse == '/' ) {
                     if ( len == 0 ) {
                        // skip leading '/'
                        nsparse++;
                        nssegment = nsparse;
                        continue;
                     }
                     else {
                        *nsparse = '\0';
                        substitute = 1;
                        break;
                     }
                  }
                  nsparse++;
                  len++;
               } while ( *nsparse != '\0' );
               // possibly exit early
               if ( len == 0 ) { break; }
               // identify the NS subdir ( can not use hash_lookup, which would throw off iteration )
               tgtnode = NULL;
               size_t iter = 0;
               for ( ; iter < tgtns->subnodecount; iter++ ) {
                  if ( strcmp( nssegment, (tgtns->subnodes + iter)->name ) == 0 ) {
                     tgtnode = tgtns->subnodes + iter;
                     break;
                  }
               }
               if ( tgtnode == NULL ) {
                  LOG( LOG_ERR, "Failed to identify tgt subspace \"%s\" of NS \"%s\" for GhostNS: \"%s\"\n",
                       nssegment, tgtns->idstr, subnode->name );
                  free( tgtnsname );
                  errno = EINVAL;
                  return -1;
               }
               tgtns = (marfs_ns*)tgtnode->content;
               // possibly reverse our substitution
               if ( substitute ) {
                  *nsparse = '/';
                  nsparse++;
                  substitute = 0;
               }
               nssegment = nsparse;
               len = 0;
            }
            free( tgtnsname ); // finally done parsing over this
            // check for NULL subnode content, indicating a remotely reference NS which has been linked
            if ( tgtns == NULL ) {
               LOG( LOG_ERR, "GhostNS \"%s\" targets a remotely referenced NS ( \"%s\" ) which has already been linked\n",
                             subnode->name, tgtnode->name );
               errno = EINVAL;
               return -1;
            }
            // if we're linking to a remoteNS ref, we'll need to replace that first
            if ( tgtns->prepo == NULL ) {
               if ( linkremotens( tgtnode, tgtns->pnamespace, config ) ) {
                  LOG( LOG_ERR, "Failed to link remoteNS target of GhostNS \"%s\"\n", subnode->name );
                  return -1;
               }
               rnscount++;
               tgtns = (marfs_ns*)tgtnode->content; // need to update to real NS target
            }
            // perform a final check for incompatible MDALs
            if ( strcmp( tgtns->prepo->metascheme.mdal->name, subspace->prepo->metascheme.mdal->name ) ) {
               LOG( LOG_ERR, "Target of GhostNS \"%s\" has an incompatible MDAL: \"%s\"\n", subnode->name, tgtns->idstr );
               errno = EINVAL;
               return -1;
            }
            // populate the ghost NS with the appropriate final target
            LOG( LOG_INFO, "Target of GhostNS \"%s\" identified as \"%s\"\n", subnode->name, tgtns->idstr );
            subspace->ghtarget = tgtns;
         }
         // check for a remote NS reference
         if ( subspace->prepo == NULL ) {
            // replace the 'dummy' remote NS entry with a real reference
            if ( linkremotens( subnode, curns, config ) ) {
               LOG( LOG_ERR, "Failed to link remote NS \"%s\"\n", subnode->name );
               return -1;
            }
            subspace = (marfs_ns*)(subnode->content);
            rnscount++; // we've replaced a remote NS ref
         }

         // replace the ID string of this subspace with a complete path
         char* sprevid = subspace->idstr;
         char* parentpath = NULL;
         if ( config_nsinfo( curns->idstr, NULL, &(parentpath) ) ) {
            LOG( LOG_ERR, "Failed to identify path of NS \"%s\"\n", curns->idstr );
            return -1;
         }
         if ( curns->pnamespace == NULL ) {
            // root NS special case ( empty string will result in '/' leading char only )
            *parentpath = '\0';
         }
         size_t sidlen = strlen(subspace->prepo->name) + 1 + strlen(parentpath) + 1 + strlen(subnode->name);
         subspace->idstr = malloc( sizeof(char) * (sidlen + 1) );
         if ( subspace->idstr == NULL ) {
            LOG( LOG_ERR, "Failed to allocate a new ID string for NS: \"%s\"\n", sprevid );
            subspace->idstr = sprevid;
            free( parentpath );
            return -1;
         }
         if ( snprintf( subspace->idstr, sidlen + 1, "%s|%s/%s",
                        subspace->prepo->name,
                        parentpath,
                        subnode->name ) != sidlen ) {
            LOG( LOG_ERR, "Failed to populate a new ID string for NS: \"%s\"\n", sprevid );
            free( subspace->idstr );
            subspace->idstr = sprevid;
            free( parentpath );
            errno = EFAULT;
            return -1;
         }
         LOG( LOG_INFO, "Updated NS ID \"%s\" to \"%s\"\n", sprevid, subspace->idstr );
         free( sprevid );
         free( parentpath );

         // continue into the subspace
         curns = subspace;
         continue;
      }
      // verify that we didn't hit some oddball iterate error
      if ( iterres < 0 ) {
         LOG( LOG_ERR, "Failed to iterate over subspace table of a NS in repo \"%s\"\n", curns->prepo->name );
         return -1;
      }
      // we've iterated over all subspaces of this NS and can progress back up to the parent
      curns = curns->pnamespace;
      nscount++; // we've traversed this NS completely
   }
   // we've finally traversed the entire NS tree
   LOG( LOG_INFO, "Traversed %zu namespaces and replaced %zu remote NS definitions\n", nscount, rnscount );
   return 0;
}


//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Initialize memory structures based on the given config file
 * @param const char* cpath : Path of the config file to be parsed
 * @return marfs_config* : Reference to the newly populated config structures
 */
marfs_config* config_init( const char* cpath ) {
   // Initialize the libxml library and check potential API mismatches between 
   // the version it was compiled for and the actual shared library used.
   LIBXML_TEST_VERSION

   // attempt to parse the given config file into an xmlDoc
   xmlDoc* doc = xmlReadFile( cpath, NULL, XML_PARSE_NOBLANKS );
   if ( doc == NULL ) {
      LOG( LOG_ERR, "Failed to parse the given XML config file: \"%s\"\n", cpath );
      xmlCleanupParser();
      errno = EINVAL;
      return NULL;
   }
   // get the root element node
   xmlNode* root_element = xmlDocGetRootElement(doc);

   // skip any number of comment nodes
   while ( root_element  &&  root_element->type == XML_COMMENT_NODE ) {
      root_element = root_element->next;
   }

   // verify we actually found a root element
   if ( root_element == NULL ) {
      LOG( LOG_ERR, "Failed to locate non-comment root element of the given XML config\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // verify the marfs_config root element
   if ( root_element->type != XML_ELEMENT_NODE  ||  strcmp( (char*)(root_element->name), "marfs_config" ) ) {
      LOG( LOG_ERR, "Root element of config file is not 'marfs_config'\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // locate version info
   xmlAttr* rootattr = root_element->properties;
   if ( rootattr == NULL  ||  rootattr->type != XML_ATTRIBUTE_NODE  ||  strcmp((char*)(rootattr->name), "version" ) ) {
      LOG( LOG_ERR, "Failed to locate expected marfs_config 'version' attribute\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }
   xmlNode* vertxt = rootattr->children;
   if ( vertxt == NULL  ||  vertxt->type != XML_TEXT_NODE  ||  vertxt->content == NULL ) {
      LOG( LOG_ERR, "Unrecognized 'version' attribute content\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // locate the mnt_top node
   xmlNode* mnttxt = root_element->children;
   for ( ; mnttxt != NULL; mnttxt = mnttxt->next ) {
      if ( strcmp( (char*)(mnttxt->name), "mnt_top" ) == 0 ) {
         // we've found the mnt_top node, and need its content
         mnttxt = mnttxt->children;
         break;
      }
   }
   if ( mnttxt == NULL  ||  mnttxt->type != XML_TEXT_NODE  ||  mnttxt->content == NULL ) {
      LOG( LOG_ERR, "Failed to locate the 'mnt_top' value\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // count up the number of repo nodes
   int repocnt = count_nodes( root_element->children, "repo" );
   if ( repocnt < 1 ) {
      LOG( LOG_ERR, "Failed to locate any repo definitions in config file\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // allocate the top-level config struct
   marfs_config* config = malloc( sizeof( struct marfs_config_struct ) );
   if ( config == NULL ) {
      LOG( LOG_ERR, "Failed to allocate a new marfs_config struct\n" );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // populate config string values and allocate repolist
   config->version = strdup( (char*)(vertxt->content) );
   config->mountpoint = strdup( (char*)(mnttxt->content) );
   config->ctag = strdup( "UNKNOWN" ); // default to unknown client
   config->repolist = malloc( sizeof( struct marfs_repo_struct ) * repocnt );
   if ( config->version == NULL  ||  config->mountpoint == NULL  ||  config->ctag == NULL  ||  config->repolist == NULL ) {
      LOG( LOG_ERR, "Failed to allocate required config values\n" );
      if ( config->version ) { free( config->version ); }
      if ( config->mountpoint ) { free( config->mountpoint ); }
      if ( config->ctag ) { free( config->ctag ); }
      if ( config->repolist ) { free( config->repolist ); }
      free( config );
      xmlFreeDoc(doc);
      xmlCleanupParser();
      return NULL;
   }

   // populate some initial config vals
   config->rootns = NULL;

   // allocate and populate all repos
   xmlNode* reponode = root_element->children;
   for ( config->repocount = 0; reponode; reponode = reponode->next ) {
      // parse all repo nodes, skip all others
      if ( strcmp( (char*)(reponode->name), "repo" ) == 0 ) {
         // NULL out the repo's name value, to indicate an initial parse
         ( config->repolist + config->repocount )->name = NULL;
         if ( create_repo( config->repolist + config->repocount, reponode ) ) {
            LOG( LOG_ERR, "Failed to parse repo %d\n", config->repocount );
            config_term( config );
            xmlFreeDoc(doc);
            xmlCleanupParser();
            return NULL;
         }
         config->repocount++;
         if ( config->repocount == repocnt ) { break; }
      }
   }

   /* Free the xml Doc */
   xmlFreeDoc(doc);
   /*
   *Free the global variables that may
   *have been allocated by the parser.
   */
   xmlCleanupParser();

   // iterate over all namespaces and establish hierarchy
   if ( establish_nsrefs( config ) ) {
      LOG( LOG_ERR, "Failed to establish all NS references\n" );
      config_term( config );
      return NULL;
   }

   return config;
}

/**
 * Destroy the given config structures
 * @param marfs_config* config : Reference to the config to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int config_term( marfs_config* config ) {
   // check for NULL config ref
   if ( config == NULL ) {
      LOG( LOG_ERR, "Received a NULL config reference\n" );
      return -1;
   }
   // free all repos
   int retval = 0;
   for ( ; config->repocount > 0; config->repocount-- ) {
      if ( free_repo( config->repolist + (config->repocount - 1) ) ) {
         LOG( LOG_ERR, "Failed to free repo %d\n", config->repocount - 1 );
         retval = -1;
      }
   }
   free( config->repolist );
   // free all string values
   free( config->ctag );
   free( config->mountpoint );
   free( config->version );
   // free the config itself
   free( config );
   return retval;
}

/**
 * Duplicate the reference to a given NS
 * @param marfs_ns* ns : NS ref to duplicate
 * @return marfs_ns* : Duplicated ref, or NULL on error
 */
marfs_ns* config_duplicatensref( marfs_ns* ns ) {
   // check for NULL arg
   if ( ns == NULL ) {
      LOG( LOG_ERR, "Received a NULL namespace ref\n" );
      errno = EINVAL;
      return NULL;
   }
   // copy the NS
   if ( !(ns->ghsource) ) {
      // super simple for non-ghosts
      return ns;
   }
   // for ghosts, we must create a dynamically allocated NS copy
   marfs_ns* ghcopy = malloc( sizeof( struct marfs_namespace_struct ) );
   if ( ghcopy == NULL ) {
      LOG( LOG_ERR, "Failed to allocate GhostNS copy\n" );
      return NULL;
   }
   ghcopy->idstr = strdup( ns->idstr );
   if ( ghcopy->idstr == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate GhostNS idstring from source\n" );
      free( ghcopy );
      return NULL;
   }
   ghcopy->fquota = ns->fquota;
   ghcopy->dquota = ns->dquota;
   ghcopy->iperms = ns->iperms;
   ghcopy->bperms = ns->bperms;
   ghcopy->prepo = ns->prepo;
   ghcopy->pnamespace = ns->pnamespace;
   ghcopy->subnodecount = ns->subnodecount;
   ghcopy->ghtarget = ns->ghtarget;
   ghcopy->ghsource = ns->ghsource;
   ghcopy->subnodes = malloc( sizeof( HASH_NODE ) * ghcopy->subnodecount );
   if ( ghcopy->subnodes == NULL ) {
      LOG( LOG_ERR, "Failed to allocate GhostNS copy subnodes\n" );
      free( ghcopy->idstr );
      free( ghcopy );
      return NULL;
   }
   size_t nodeindex = 0;
   for ( ; nodeindex < ghcopy->subnodecount; nodeindex++ ) {
      HASH_NODE* curnode = ghcopy->subnodes + nodeindex;
      curnode->name = (ns->subnodes + nodeindex)->name;
      curnode->weight = (ns->subnodes + nodeindex)->weight;
      curnode->content = (ns->subnodes + nodeindex)->content;
   }
   ghcopy->subspaces = hash_init( ghcopy->subnodes, ghcopy->subnodecount, 1 );
   if ( ghcopy->subspaces == NULL ) {
      LOG( LOG_ERR, "Failed to initialize subspaces table of GhostNS copy\n" );
      free( ghcopy->subnodes );
      free( ghcopy->idstr );
      free( ghcopy );
      return NULL;
   }
   // finally, provide the copy
   return ghcopy;
}

/**
 * Potentially free the given NS ( only if it is an allocated ghostNS )
 * @param marfs_ns* ns : Namespace to be freed
 */
void config_destroynsref( marfs_ns* ns ) {
   if ( ns  &&  ns->ghsource ) {
      if ( ns->subspaces ) { hash_term( ns->subspaces, NULL, NULL ); }
      if ( ns->subnodes ) { free( ns->subnodes ); }
      if ( ns->idstr ) { free( ns->idstr ); }
      free( ns );
   }
}

/**
 * Create a fresh marfs_position struct, targeting the MarFS root
 * @param marfs_position* pos : Reference to the position to be initialized,
 * @param marfs_config* config : Reference to the config to be used
 * @return int : Zero on success, or -1 on failure
 */
int config_establishposition( marfs_position* pos, marfs_config* config ) {
   // check for NULL args
   if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos->ns != NULL ) {
      LOG( LOG_ERR, "Received an active pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( config == NULL ) {
      LOG( LOG_ERR, "Received a NULL config arg\n" );
      errno = EINVAL;
      return -1;
   }
   // populate the root position values
   pos->ctxt = config->rootns->prepo->metascheme.mdal->newctxt( "/.", config->rootns->prepo->metascheme.mdal->ctxt );
   if ( pos->ctxt == NULL ) {
      LOG( LOG_ERR, "Failed to create a root MDAL_CTXT\n" );
      return -1;
   }
   pos->ns = config->rootns;
   pos->depth = 0;
   return 0;
}

/**
 * Duplicate the given source position into the given destination position
 * @param marfs_position* srcpos : Reference to the source position
 * @param marfs_position* destpos : Reference to the destination position
 * @return int : Zero on success, or -1 on failure
 */
int config_duplicateposition( marfs_position* srcpos, marfs_position* destpos ) {
   // check for NULL args
   if ( srcpos == NULL  ||  destpos == NULL ) {
      LOG( LOG_ERR, "Received a NULL pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( srcpos->ns == NULL ) {
      LOG( LOG_ERR, "Received an inactive src pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( destpos->ns != NULL ) {
      LOG( LOG_ERR, "Received an active dest pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   // copy the NS
   destpos->ns = config_duplicatensref( srcpos->ns );
   if ( destpos->ns == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate NS ref\n" );
      return -1;
   }
   // depth and CTXT are much more straightforward
   destpos->depth = srcpos->depth;
   if ( srcpos->ctxt ) {
      destpos->ctxt = srcpos->ns->prepo->metascheme.mdal->dupctxt( srcpos->ctxt );
      if ( destpos->ctxt == NULL ) {
         LOG( LOG_ERR, "Failed to duplicate MDAL_CTXT of source position\n" );
         destpos->depth = 0;
         config_destroynsref( destpos->ns );
         destpos->ns = NULL;
         return -1;
      }
   }
   else {
      destpos->ctxt = NULL;
   }
   LOG( LOG_INFO, "Position values successfully copied\n" );
   return 0;
}

/**
 * Establish a CTXT for the given position, if it is lacking one
 * @param marfs_position* pos : Reference to the position
 * @return int : Zero on success ( ctxt established or already present ),
 *               or -1 on failure
 */
int config_fortifyposition( marfs_position* pos ) {
   // check for NULL arg
   if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos->ns == NULL ) {
      LOG( LOG_ERR, "Received an inactive pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   // check if the position is lacking a ctxt
   if ( pos->ctxt ) {
      LOG( LOG_INFO, "Position already has a MDAL_CTXT\n" );
      return 0;
   }
   // generation behavior differs for ghosts
   if ( pos->ns->ghsource ) {

      // identify paths for both the ghost and target NS
      char* ghostpath;
      if ( config_nsinfo( pos->ns->idstr, NULL, &(ghostpath) ) ) {
         LOG( LOG_ERR, "Failed to identify path of GhostNS copy: \"%s\"\n", pos->ns->idstr );
         return -1;
      }
      char* tgtpath;
      if ( config_nsinfo( pos->ns->ghtarget->idstr, NULL, &(tgtpath) ) ) {
         LOG( LOG_ERR, "Failed to identify path of GhostNS copy target: \"%s\"\n", pos->ns->ghtarget->idstr );
         free( ghostpath );
         return -1;
      }
      // generate a new 'split' ctxt, using the Ghost postion for user paths and the tgt postion for reference paths
      pos->ctxt = pos->ns->ghtarget->prepo->metascheme.mdal->newsplitctxt( ghostpath,
                                                                           pos->ns->ghsource->prepo->metascheme.mdal->ctxt,
                                                                           tgtpath,
                                                                           pos->ns->ghtarget->prepo->metascheme.mdal->ctxt );
      free( tgtpath );
      free( ghostpath );
      if ( pos->ctxt == NULL ) {
         LOG( LOG_ERR, "Failed to generate split ctxt for GhostNS copy: \"%s\"\n", pos->ns->idstr );
         return -1;
      }
      LOG( LOG_INFO, "Generated new split ctxt for GhostNS copy: \"%s\"\n", pos->ns->idstr );
      return 0;
   }
   // determine the NS path
   char* nspath;
   if ( config_nsinfo( pos->ns->idstr, NULL, &(nspath) ) ) {
      LOG( LOG_ERR, "Failed to identify the nspath of NS: \"%s\"\n", pos->ns->idstr );
      return -1;
   }
   // create a fresh ctxt
   pos->ctxt = pos->ns->prepo->metascheme.mdal->newctxt( nspath, pos->ns->prepo->metascheme.mdal->ctxt );
   free( nspath );
   if ( pos->ctxt == NULL ) {
      LOG( LOG_ERR, "Failed to establish a fresh ctxt for NS: \"%s\"\n", pos->ns->idstr );
      return -1;
   }
   LOG( LOG_INFO, "Generated new ctxt for NS: \"%s\"\n", pos->ns->idstr );
   return 0;
}

/**
 * Terminate a marfs_position struct
 * @param marfs_position* pos : Position to be destroyed
 * @return int : Zero on success, or -1 on failure
 */
int config_abandonposition( marfs_position* pos ) {
   // check for NULL arg
   if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos->ns == NULL ) {
      LOG( LOG_ERR, "Received an inactive pos arg\n" );
      errno = EINVAL;
      return -1;
   }
   int retval = pos->ns->prepo->metascheme.mdal->destroyctxt( pos->ctxt );
   config_destroynsref( pos->ns );
   pos->ns = NULL;
   pos->depth = 0;
   pos->ctxt = NULL;
   return retval;
}

/**
 * Verifies the LibNE Ctxt of every repo, creates every namespace, creates all 
 *  reference dirs in the given config, and verifies the LibNE CTXT
 * @param marfs_config* config : Reference to the config to be validated
 * @param const char* tgtNS : Path of the NS to be verified
 * @param char MDALcheck : If non-zero, the MDAL security of each encountered NS will be verified
 * @param char NEcheck : If non-zero, the LibNE ctxt of each encountered NS will be verified
 * @param char recurse : If non-zero, children of the target NS will also be verified
 * @param char fix : If non-zero, attempt to correct any problems encountered
 * @return int : A count of uncorrected errors encountered, or -1 if a failure occurred
 */
int config_verify( marfs_config* config, const char* tgtNS, char MDALcheck, char NEcheck, char recurse, char fix ) {

   // check for NULL refs
   if ( config == NULL ) {
      LOG( LOG_ERR, "Received a NULL config reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( tgtNS == NULL ) {
      LOG( LOG_ERR, "Received a NULL NS target\n" );
      errno = EINVAL;
      return -1;
   }

   // establish a NS string we can manipulate
   char* NSpath = strdup( tgtNS );
   if ( NSpath == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate path of NS target: \"%s\"\n", tgtNS );
      return -1;
   }

   // establish a default position value, from which we'll traverse to our targetNS
   MDAL rootmdal = config->rootns->prepo->metascheme.mdal;
   marfs_position pos = {
      .ns = NULL,
      .depth = 0,
      .ctxt = NULL
   };
   if ( config_establishposition( &pos, config ) ) {
      // failed to establish a root position
      if ( errno == ENOENT  &&  fix ) {
         // very likely need to create the rootNS
         LOG( LOG_INFO, "Attempting to create rootNS\n" );
         if ( rootmdal->createnamespace( rootmdal->ctxt, "/." )  ) {
            LOG( LOG_ERR, "Failed to create rootNS (%s)\n", strerror(errno) );
            return -1;
         }
         if ( config_establishposition( &pos, config ) ) {
            LOG( LOG_ERR, "Failed to establish a rootNS position, even after creation\n" );
            return -1;
         }
      }
      else {
         LOG( LOG_ERR, "Failed to establish a rootNS MDAL_CTXT\n" );
         return -1;
      }
   }

   // traverse the config, identifying info for our NS target
   int tgtdepth = config_traverse( config, &(pos), &(NSpath), 0 );
   if ( pos.ctxt ) { pos.ns->prepo->metascheme.mdal->destroyctxt( pos.ctxt );  pos.ctxt = NULL; } // ctxt not required
   if ( tgtdepth != 0 ) {
      if ( tgtdepth < 0 ) {
         LOG( LOG_ERR, "Failed to identify the specified target: \"%s\"\n", NSpath );
      }
      else {
         LOG( LOG_ERR, "The specified target is not a NS: \"%s\"\n", NSpath );
      }
      free( NSpath );
      return -1;
   }
   free( NSpath ); // done with NS path

   // track verfied repos ( by pointer value, which should be safe )
   marfs_repo** vrepos = calloc( sizeof(marfs_repo*), config->repocount );
   if ( vrepos == NULL ) {
      LOG( LOG_ERR, "Failed to allocate verified repos list\n" );
      return -1;
   }
   size_t vrepocnt = 0;

   // create a dynamic array, holding the current NS index at each depth
   size_t curiteralloc = 1024;
   size_t* nsiterlist = calloc( sizeof(size_t), curiteralloc );
   if ( nsiterlist == NULL ) {
      LOG( LOG_ERR, "Failed to allocate NS iterator list\n" );
      free( vrepos );
      return -1;
   }

   // zero out our umask value, to avoid improper perms
   mode_t oldmask = umask(0);

   // traverse the entire NS hierarchy, creating any missing NSs and reference dirs
   int errcount = 0;
   size_t curdepth = 1;
   size_t nscount = 0;
   char createcurrent = 1;
   while ( curdepth ) {
      if ( createcurrent ) {
         nscount++;
         int olderr = errno;
         errno = 0;
         MDAL curmdal = pos.ns->prepo->metascheme.mdal;
         MDAL_CTXT nsctxt = NULL;
         // potentially verify the MDAL / libNE context of this NS's parent repo
         char checkmdalsec = MDALcheck;
         char checklibne = NEcheck;
         size_t repoiter = 0;
         for ( ; ( repoiter < vrepocnt )  &&  ( checkmdalsec  ||  checklibne ); repoiter++ ) {
            if ( pos.ns->prepo == vrepos[repoiter] ) { // don't reverify a repo we've already seen
               checkmdalsec = 0;
               checklibne = 0;
            }
         }
         if ( checkmdalsec ) {
            int verres = curmdal->checksec( curmdal->ctxt, fix );
            if ( verres < 0 ) {
               LOG( LOG_ERR, "Failed to verify the MDAL security of repo: \"%s\" (%s)\n",
                             pos.ns->prepo->name, strerror(errno) );
               free( vrepos );
               free( nsiterlist );
               return -1;
            }
            else if ( verres ) {
               LOG( LOG_INFO, "MDAL of repo \"%s\" has %d uncorrected security errors\n",
                              pos.ns->prepo->name );
               errcount++;
            }
         }
         if ( checklibne ) {
            int verres = ne_verify( pos.ns->prepo->datascheme.nectxt, fix );
            if ( verres < 0 ) {
               LOG( LOG_ERR, "Failed to verify ne_ctxt of repo: \"%s\" (%s)\n",
                             pos.ns->prepo->name, strerror(errno) );
               free( vrepos );
               free( nsiterlist );
               return -1;
            }
            else if ( verres ) {
               LOG( LOG_INFO, "ne_ctxt of repo \"%s\" encountered %d errors\n",
                              pos.ns->prepo->name, verres );
               errcount++;
            }
         }
         // get the path of this NS
         char* nspath = NULL;
         if ( config_nsinfo( pos.ns->idstr, NULL, &(nspath) ) ) {
            LOG( LOG_ERR, "Failed to retrieve path of NS: \"%s\"\n", pos.ns->idstr );
            errcount++;
         }
         // attempt to create the current NS ( if 'fix' is specified )
         else if ( fix  &&  curmdal->createnamespace( curmdal->ctxt, nspath )  &&
                   errno != EEXIST ) {
            LOG( LOG_ERR, "Failed to create NS: \"%s\" (%s)\n", pos.ns->idstr, strerror(errno) );
            errcount++;
         }
         // attempt to create a CTXT for that NS ( skipped for ghosts )
         else if ( !(pos.ns->ghsource)  &&  (nsctxt = curmdal->newctxt( nspath, curmdal->ctxt )) == NULL ) {
            LOG( LOG_ERR, "Failed to create MDAL_CTXT for NS: \"%s\"\n", nspath );
            errcount++;
         }
         // verify all reference dirs of this NS
         //    skip this if we're in ThE gHoSt DiMeNsIoN!!!!
         else if ( !(pos.ns->ghsource) ){
            size_t curref = 0;
            char anyerror = 0;
            for ( ; curref < pos.ns->prepo->metascheme.refnodecount; curref++ ) {
               int mkdirres = 0;
               char* rparse = strdup( pos.ns->prepo->metascheme.refnodes[curref].name );
               char* rfullpath = rparse;
               if ( rparse == NULL ) {
                  LOG( LOG_ERR, "Failed to duplicate reference string: \"%s\"\n", pos.ns->prepo->metascheme.refnodes[curref].name );
                  anyerror = 1;
                  continue;
               }
               LOG( LOG_INFO, "Verifying refdir: \"%s\"\n", rfullpath );
               errno = 0;
               while ( mkdirres == 0  &&  rparse != NULL ) {
                  // iterate ahead in the stream, tokenizing into intermediate path components
                  while ( 1 ) {
                     // cut string to next dir comp
                     if ( *rparse == '/' ) {
                        *rparse = '\0';
                        // ignore any final, empty path component
                        if ( *(rparse + 1) == '\0' ) { rparse = NULL; }
                        break;
                     }
                     // end of str, prepare to exit
                     if ( *rparse == '\0' ) { rparse = NULL; break; }
                     rparse++;
                  }
                  if ( fix ) {
                     // isssue the createrefdir op
                     if ( rparse ) {
                        // create all intermediate dirs with global execute access
                        mkdirres = curmdal->createrefdir( nsctxt, rfullpath, S_IRWXU | S_IXOTH );
                     }
                     else {
                        // create the final dir with full global access
                        mkdirres = curmdal->createrefdir( nsctxt, rfullpath, S_IRWXU | S_IWOTH | S_IXOTH );
                     }
                     // ignore any EEXIST errors, at this point
                     if ( mkdirres  &&  errno == EEXIST ) { mkdirres = 0; errno = 0; }
                  }
                  else {
                     // stat the reference dir
                     struct stat stval;
                     mkdirres = curmdal->statref( nsctxt, rfullpath, &(stval) );
                     if ( mkdirres == 0 ) {
                        if ( rparse ) {
                           // check for any group/other perms besides global execute
                           if ( stval.st_mode & S_IRWXG  ||
                                stval.st_mode & S_IROTH  ||
                                stval.st_mode & S_IWOTH  ||
                                !(stval.st_mode & S_IXOTH) ) {
                              LOG( LOG_ERR, "Intermediate dir has unexpected perms: \"%s\"\n", rfullpath );
                              mkdirres = -1;
                           }
                        }
                        else {
                           // check for write/execute global perms
                           if ( stval.st_mode & S_IROTH  ||
                                !(stval.st_mode & S_IWOTH)  ||
                                !(stval.st_mode & S_IXOTH) ) {
                              LOG( LOG_ERR, "Terminating dir has unexpected perms: \"%s\"\n", rfullpath );
                              mkdirres = -1;
                           }
                        }
                     }
                  }
                  // if we cut the string short, we need to undo that and progress to the next str comp
                  if ( rparse ) { *rparse = '/'; rparse++; }
               }
               if ( mkdirres ) { // check for error conditions ( except EEXIST )
                  LOG( LOG_ERR, "Failed to verify refdir: \"%s\"\n", rfullpath );
                  anyerror = 1;
               }
               // cleanup after ourselves
               free( rfullpath ); // done with this reference path
            }
            if ( anyerror ) {
               errcount++;
               LOG( LOG_ERR, "Failed to create all ref dirs for NS: \"%s\"\n", nspath );
            }
            else { errno = olderr; }
         }
         // cleanup after ourselves
         if ( nsctxt  &&  curmdal->destroyctxt( nsctxt ) ) {
            // just warn if we can't clean this up
            LOG( LOG_WARNING, "Failed to destory MDAL_CTXT of NS \"%s\"\n", nspath );
         }
         free( nspath );
      }

      // quit out here, if not recursing
      if ( !(recurse) ) { break; }

      // identify next NS target
      if ( nsiterlist[curdepth - 1] < pos.ns->subnodecount ) {
         // proceed to the next subspace of the current NS
         marfs_ns* newnstgt = (marfs_ns*)( pos.ns->subnodes[ nsiterlist[curdepth - 1] ].content );
         if ( config_enterns( &pos, newnstgt, pos.ns->subnodes[ nsiterlist[curdepth - 1] ].name, 0 ) ) {
            LOG( LOG_ERR, "Failed to transition position into subspace: \"%s\"\n", newnstgt->idstr );
            umask(oldmask); // reset umask
            free( vrepos );
            free( nsiterlist );
            return -1;
         }
         LOG( LOG_INFO, "Incrementing iterator for parent ( index = %zu / iter = %zu )\n",
                        curdepth - 1, nsiterlist[ curdepth - 1 ] + 1 );
         nsiterlist[ curdepth - 1 ]++; // increment our iterator at this depth
         curdepth++;
         if ( curdepth >= curiteralloc ) {
            nsiterlist = realloc( nsiterlist, sizeof(size_t) * ( curiteralloc + 1024 ) );
            if ( nsiterlist == NULL ) {
               LOG( LOG_ERR, "Failed to allocate extended NS iterator list\n" );
               umask(oldmask); // reset umask
               free( vrepos );
               return -1;
            }
            curiteralloc += 1024;
         }
         nsiterlist[ curdepth - 1 ] = 0; // zero out our next iterator
         createcurrent = 1; // the subspace has yet to be verified
      }
      else {
         // proceed back up to the parent of this space
         if ( pos.ns->pnamespace  &&  config_enterns( &pos, pos.ns->pnamespace, "..", 0 ) ) {
            LOG( LOG_ERR, "Failed to transition to the parent of current NS\n" );
            umask(oldmask); // reset umask
            free( vrepos );
            free( nsiterlist );
            return -1;
         }
         curdepth--;
         createcurrent = 0; // the parent space has already been verified
      }
   }
   // we've finally traversed the entire NS tree
   LOG( LOG_INFO, "Traversed %zu namespaces with %zu encountered errors\n", nscount, errcount );

   free( nsiterlist );
   free( vrepos );
   umask(oldmask); // reset umask
   // abandon our position
   config_abandonposition( &pos );
   return errcount;
}

/**
 * Traverse the given path, idetifying a final NS target and resulting subpath
 * @param marfs_config* config : Config reference
 * @param marfs_position* pos : Reference populated with the initial position value
 *                              This will be updated to reflect the resulting position
 * @param char** subpath : Relative path from the tgtns
 *                         This will be updated to reflect the resulting subpath from
 *                         the new tgtns
 *                         NOTE -- this function may completely replace the
 *                         string reference
 * @param char linkchk : If zero, this function will not check for symlinks in the path.
 *                          All path componenets are assumed to be directories.
 *                       If one, this function will perform a readlink() op on all
 *                          path components, substituting in the targets of all symlinks.
 *                       If greater than one, this function will perform a readlink() op 
 *                          and substitute targets ( as above ) for all path components
 *                          EXCEPT the final target.
 *                          This behavior allows for ops to target symlinks directly.
 * @return int : The depth of the path from the resulting NS target, 
 *               or -1 if a failure occurred
 */
int config_traverse( marfs_config* config, marfs_position* pos, char** subpath, char linkchk ) {
   // check for NULL refs
   if ( config == NULL ) {
      LOG( LOG_ERR, "Received a NULL config reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos == NULL ) {
      LOG( LOG_ERR, "Received a NULL position reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( pos->ns == NULL  ||  pos->ctxt == NULL ) {
      LOG( LOG_ERR, "Received a position reference with NULL content\n" );
      errno = EINVAL;
      return -1;
   }
   if ( subpath == NULL  ||  (*subpath) == NULL ) {
      LOG( LOG_ERR, "Received a NULL subpath reference\n" );
      errno = EINVAL;
      return -1;
   }
   // setup some references
   char* parsepath = *subpath;
   MDAL mdal = pos->ns->prepo->metascheme.mdal;
   if ( *parsepath == '/' ) {
      // verify that the abspath contains the required mountpoint prefix
      parsepath = config_validatemnt( config, NULL, parsepath );
      if ( parsepath == NULL ) {
         LOG( LOG_ERR, "Non-MarFS path: \"%s\"\n", *subpath );
         errno = EINVAL;
         return -1;
      }
      // NOTE -- now targetting the rootNS
      if ( pos->ns != config->rootns  ||  pos->depth != 0 ) {
         // we need to create a fresh MDAL_CTXT, referencing the rootNS
         if ( config_abandonposition( pos ) ) {
            // nothing to do, besides complain
            LOG( LOG_WARNING, "Failed to abandon existing position prior to establishing new one\n" );
         }
         if ( config_establishposition( pos, config ) ) {
            LOG( LOG_ERR, "Failed to initialize a new rootNS position\n" );
            return -1;
         }
         mdal = pos->ns->prepo->metascheme.mdal;
      }
      // check for special case, targeting the rootNS directly
      if ( *parsepath == '\0' ) {
         parsepath = *subpath;
         *subpath = strdup( "." );
         if ( *subpath == NULL ) {
            LOG( LOG_ERR, "Failed to allocate space for '.' path reference\n" );
            *subpath = parsepath;
            return -1;
         }
         free( parsepath );
         return 0;
      }
   }
   // NOTE -- path is now relative to the current 'pos' values
   // traverse the subpath, keeping track of depth from the current NS
   char* pathelem = parsepath;
   char* relpath = parsepath; // relative path of current target from pos->ctxt
   int depth = pos->depth; // depth of current traversal
   char nonNS = 0;
   while ( *parsepath != '\0' ) {
      // move the parse pointer ahead to the next '/' char
      while ( *parsepath != '\0'  &&  *parsepath != '/' ) { parsepath++; }
      // replace any '/' char with a NULL
      char replacechar = 0;
      char finalpathcomp = 0;
      if ( *parsepath == '/' ) {
         // check if the path continues beyond this component
         int lookahead = 1;
         while ( *(parsepath + lookahead) == '/' ) { lookahead++; } // skip over repeated '/' chars
         if ( *(parsepath + lookahead) == '\0' ) { finalpathcomp = 1; } // check for end of path
         // replace '/'
         replacechar = 1;
         *parsepath = '\0';
      }
      else { finalpathcomp = 1; } // no trailing '/' means this is the end of the path
      // identify the current path element
      if ( strcmp( pathelem, ".." ) == 0 ) {
         // parent ref
         if ( depth == 0 ) {
            // we can't move up any further
            if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
            replacechar = 0;
            // traverse the NS paths
            parsepath = config_shiftns( config, pos, pathelem );
            if ( parsepath == NULL  ||  parsepath == pathelem ) {
               LOG( LOG_ERR, "Failed to identify NS target following parent ref\n" );
               return -1;
            }
            // NS target identified; update our refs and continue
            nonNS = 1; // don't recheck if this path elem is a NS
            pos->depth = 0;
            relpath = parsepath;
            pathelem = parsepath;
            mdal = pos->ns->prepo->metascheme.mdal;
            continue;
         }
         depth--; // one level up
      }
      else if ( strcmp( pathelem, "." ) ) { // ignore "." references
         if ( depth == 0  &&  nonNS == 0 ) {
            // check for NS shift at root of NS
            if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
            char* shiftres = config_shiftns( config, pos, pathelem );
            if ( shiftres == NULL ) {
               LOG( LOG_ERR, "Failed to identify potential subspace: \"%s\"\n", pathelem );
               return -1;
            }
            if ( shiftres != pathelem ) {
               // NS target identified; update our refs and continue
               nonNS = 1; // don't recheck if this path elem is a NS
               pos->depth = 0;
               relpath = shiftres;
               parsepath = shiftres;
               pathelem = parsepath;
               mdal = pos->ns->prepo->metascheme.mdal;
               continue;
            }
            // this is not a subspace reference, continue processing
            if ( replacechar ) { *parsepath = '\0'; } // redo any previous edit
         }
         // assume all others are either dirs or links
         // possibly substitute in symlink target ( if requested, and not skipping final path comp )
         if ( linkchk == 1  ||  ( linkchk > 1  &&  !(finalpathcomp) ) ) {
            // stat the current path element
            errno = 0;
            struct stat linkst = {
               .st_mode = 0,
               .st_size = 0
            };
            // NOTE -- stat() failure with ENOENT is acceptable, as a non-existent files
            //         are definitely not symlinks
            if ( mdal->stat( pos->ctxt, relpath, &(linkst), AT_SYMLINK_NOFOLLOW )  &&
                 errno != ENOENT ) {
               LOG( LOG_ERR, "Failed to stat relative path: \"%s\"\n", relpath );
               if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
               return -1;
            }
            if ( S_ISLNK(linkst.st_mode) ) {
               size_t linksize = linkst.st_size;
               LOG( LOG_INFO, "Subpath is a symlink: \"%s\"\n", relpath );
               if ( linksize == 0 ) {
                  LOG( LOG_ERR, "Empty content of link: \"%s\"\n", relpath );
                  if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
                  errno = ENOENT;
                  return -1;
               }
               if ( linksize > INT_MAX ) {
                  LOG( LOG_ERR, "Link content of %zu bytes exceeds memory limits\n", linksize );
                  if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
                  errno = ENAMETOOLONG;
                  return -1;
               }
               // this is a real link, so we need to follow it
               char* linkbuf = malloc( sizeof(char) * (linksize + 1) );
               if ( linkbuf == NULL ) {
                  LOG( LOG_ERR, "Failed to allocate space for content of link: \"%s\"\n", relpath );
                  if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
                  return -1;
               }
               ssize_t readres2 = mdal->readlink( pos->ctxt, relpath, linkbuf, linksize );
               if ( readres2 != linksize ) {
                  LOG( LOG_ERR, "State of link changed during traversal: \"%s\"\n", relpath );
                  if ( replacechar ) { *parsepath = '/'; } // undo any previous edit
                  free( linkbuf );
                  errno = EBUSY;
                  return -1;
               }
               *(linkbuf + linksize) = '\0'; // manually insert NULL-term because readlink is a stupid function that doesn't bother to make any such guarantee itself
               LOG( LOG_INFO, "Encountered symlink \"%s\" with target \"%s\"\n",
                              relpath, linkbuf );
               // restore the orignal string structure
               if ( replacechar ) { *parsepath = '/'; }
               replacechar = 0;
               // calculate string length necessary to store updated path
               size_t remainlen = strlen(parsepath);  // remaining, unparsed, string length
               size_t elemlen = parsepath - pathelem; // path element len (to discard)
               size_t prefixlen = pathelem - relpath; // len of relpath to this element
               size_t headerlen = relpath - (*subpath); // unneeded NS prefix (to discard)
               LOG( LOG_INFO, "LINK_INFO ( remain=%zu, elemlen=%zu, prefix=%zu, header=%zu )\n", remainlen, elemlen, prefixlen, headerlen );
               char* oldpath = *subpath;
               size_t requiredchars = remainlen + linksize + prefixlen;
               if ( *linkbuf == '/' ) {
                  // absolute link path makes the relpath prefix irrelevant
                  requiredchars -= prefixlen;
               }
               size_t origstrlen = headerlen + prefixlen + elemlen + remainlen;
               // check if we can fit the modified string into the current buffer
               if ( origstrlen < requiredchars ) {
                  LOG( LOG_INFO, "Allocating new path buffer of length %zu\n", requiredchars+1);
                  // must allocate a new string buffer to hold required chars
                  *subpath = malloc( sizeof(char) * (requiredchars + 1) );
                  if ( *subpath == NULL ) {
                     LOG( LOG_ERR, "Failed to alloc new subpath buffer\n" );
                     free( linkbuf );
                     *subpath = oldpath;
                     return -1;
                  }
               }
               // update the string with link content
               char* output = *subpath;
               if ( *linkbuf != '/' ) {
                  // non-abspath requires the relative path prefix
                  // NOTE -- I am using memcpy, as strncpy() would needlessly traverse
                  //         the entire input string for each call.  We already know the 
                  //         lengths of all strings, making this a slight waste of time.
                  memcpy( output, relpath, sizeof(char) * prefixlen );
                  output += prefixlen; // update output location
               }
               char* parsestart = output; // parsing begins with link content
               memcpy( output, linkbuf, linksize ); // insert link content
               output += linksize; // update output location
               free( linkbuf ); // finally done with link content buffer
               memcpy( output, parsepath, sizeof(char) * remainlen ); // append remaining
               output += remainlen;
               *output = '\0'; // ensure NULL-terminated ( should be unnecessary )
               if ( oldpath != (*subpath) ) { free( oldpath ); } // done with the orig
               LOG( LOG_INFO, "Link substitution result: \"%s\"\n", *subpath );
               // restart our parsing with updated string
               relpath = *subpath; // update relpath to the new prefix position
               if ( *parsestart == '/' ) {
                  // absolute path means we must re-verify the mountpoint
                  parsestart = config_validatemnt( config, NULL, parsestart );
                  if ( parsestart == NULL ) {
                     LOG( LOG_ERR, "Non-MarFS absolute link target: \"%s\"\n", *subpath );
                     errno = EINVAL;
                     return -1;
                  }
                  // NOTE -- now targetting the rootNS
                  pos->depth = 0;
                  if ( pos->ns != config->rootns ) {
                     // we need to create a fresh MDAL_CTXT, referencing the rootNS
                     if ( mdal->destroyctxt( pos->ctxt ) ) {
                        // nothing to do, besides complain
                        LOG( LOG_WARNING, "Failed to destory MDAL_CTXT of NS \"%s\"\n", pos->ns->idstr );
                     }
                     pos->ns = config->rootns;
                     mdal = pos->ns->prepo->metascheme.mdal;
                     pos->ctxt = mdal->newctxt( "/.", mdal->ctxt );
                     if ( pos->ctxt == NULL ) {
                        LOG( LOG_ERR, "Failed to initialize a new rootNS MDAL_CTXT, post link\n" );
                        return -1;
                     }
                  }
                  depth = 0; // now at the root of the rootNS
                  relpath = parsestart; // update relpath, as our NS has shifted
               }
               // update all of our references
               nonNS = 0; // new path element, may be a potential subspace
               parsepath = parsestart;
               pathelem = parsestart;
               continue;
            }
         }
         depth++; // if we get here, assume this is a subdir
      }
      // undo any previous path edit
      if ( replacechar ) { *parsepath = '/'; }
      // move our parse pointer ahead to the next path component
      while ( *parsepath == '/' ) { parsepath++; }
      // update our element pointer to the next path element
      pathelem = parsepath;
      nonNS = 0; // new path element, may be a potential subspace
   }
   // truncate our subpath down to a relative path from the current position
   if ( *subpath != relpath ) {
      // manually shift relpath to the front of the subpath string
      // NOTE -- sprintf could do this, but results in overlapping memcpy errors
      int newlen = 0;
      char* dest = *subpath;
      char* src = relpath;
      while ( *src != '\0' ) {
         *dest = *src;
         dest++;
         src++;
         newlen++;
      }
      *dest = '\0'; // be *certain* to append NULL terminator
      bzero( (*subpath)+newlen+1, relpath-((*subpath)+1) );
   }

   return depth;
}

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
int config_nsinfo( const char* nsidstr, char** repo, char** path ) {
   // check for NULL references
   if ( nsidstr == NULL ) {
      LOG( LOG_ERR, "Received a NULL NS ID string\n" );
      errno = EINVAL;
      return -1;
   }
   // parse over the ID string
   char* idcpy = strdup( nsidstr );
   if ( idcpy == NULL ) {
      LOG( LOG_ERR, "Failed to copy NS ID string of \"%s\"\n", nsidstr );
      return -1;
   }
   char nopath = 1;
   char* parse = idcpy;
   while ( *parse != '\0' ) {
      // iterate over the string, looking for the '|' seperator
      parse++;
      if ( *parse == '|' ) { *parse = '\0'; parse++; nopath = 0; break; }
   }
   if ( nopath ) {
      LOG( LOG_ERR, "Failed to identify NS path of \"%s\"\n", nsidstr );
      free( idcpy );
      errno = EINVAL;
      return -1;
   }

   // create output strings, if requested
   if ( repo ) {
      *repo = strdup( idcpy );
      if ( *repo == NULL ) {
         LOG( LOG_ERR, "Failed to create duplicate string of repo name: \"%s\"\n", idcpy );
         free( idcpy );
         return -1;
      }
   }
   if ( path ) {
      *path = strdup( parse );
      if ( *path == NULL ) {
         LOG( LOG_ERR, "Failed to create duplicate string of path name: \"%s\"\n", parse );
         free( idcpy );
         if ( repo ) { free( *repo ); *repo = NULL; } // don't leave one output populated
         return -1;
      }
   }

   // determine if path header is valid
   int retval = 0;
   if ( *parse != '/' ) {
      LOG( LOG_INFO, "Received an NS path string which has not been properly linked: \"%s\"\n", parse );
      retval = 1;
   }
   free( idcpy );
   return retval;
}


