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
#include <libxml/tree.h>

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif

typedef struct marfs_repo_struct marfs_repo;
typedef struct marfs_namespace_struct marfs_ns;

typedef enum {
   NS_NOACCESS = 0,    // 0  = 0b00000 -- No access at all
   NS_READMETA,        // 1  = 0b00001 -- Read access to metadata   ( readdir / stat / etc. )
   NS_WRITEMETA,       // 2  = 0b00010 -- Write access to metadata  ( open / mkdir / etc. )
   NS_RWMETA,          // 3  = 0b00011 -- Read and Write access to metadata
   NS_READDATA,        // 4  = 0b00100 -- Read access to data
   NS_WRITEDATA = 8,   // 8  = 0b01000 -- Write access to data
   NS_RWDATA = 12,     // 12 = 0b01100 -- Read and Write access to data
   NS_UNLINK = 16      // 16 = 0b10000 -- Ability to unlink existing files / dirs
} ns_perms;


typedef struct marfs_namespace_struct {
   size_t      fquota;      // file quota of the namespace
   size_t      dquota;      // data quota of the namespace ( in bytes )
   HASH_TABLE  subspaces;   // subspace hash table, referencing namespaces below this one
   marfs_repo* prepo;       // reference to the repo containing this namespace
   marfs_ns*   pnamespace;  // reference to the parent namespace of this one
} marfs_ns;
// NOTE -- namespaces will be wrapped in HASH_NODES for use in HASH_TABLEs
//         the HASH_NODE struct will provide the name string of the namespace


typedef struct marfs_datascheme_struct {
   ne_erasure epat;
   ne_ctxt    nectxt;
   size_t     objfiles;
   size_t     objsize;
   HASH_TABLE podtable;
   HASH_TABLE captable;
   HASH_TABLE scattertable;
} marfs_ds;


typedef struct marfs_metadatascheme_struct {
   char       directread;
   char       directwrite;
   int        nscount;
   HASH_NODE* nslist;
   MDAL       mdal;
} marfs_ms;


typedef struct marfs_repo_struct {
   char*     name;
   marfs_ds  datascheme;
   marfs_ms  metascheme;
} marfs_repo;


typedef struct marfs_config_struct {
   char*       mountpoint;
   int         repocount;
   marfs_repo* repolist;
} marfs_config;


marfs_config* config_init( const char* cpath );
int config_term( marfs_config* config );
int config_shiftns( marfs_config* config, marfs_ns** curns, char** newns, char** subpath );
int config_abspath( marfs_config* config, marfs_ns* curns, char** subpath );

#endif // _CONFIG_H

