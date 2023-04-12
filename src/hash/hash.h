#ifndef _HASH_H
#define _HASH_H
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

#include <stdlib.h>

typedef struct hash_table_struct* HASH_TABLE;

typedef struct hash_node_struct {
   char* name;
   int         weight;
   void*       content;
} HASH_NODE;

/**
 * Produces a randomized integer value, between zero and maxval-1 (inclusive), 
 * which can be reproducibly generated from the given string and max values.
 * @param const char* string : String seed value for the randomization
 * @param int maxval : Maximum integer value ( produced value will be < maxval )
 * @return int : Randomized integer result
 */
int hash_rangevalue( const char* string, int maxval );

/**
 * Create a HASH_TABLE
 * @param HASH_NODE* nodes : List of hash nodes to be included in the table
 * @param size_t count : Count of HASH_NODEs in the 'nodes' arg
 * @param char directlookup : With a zero value ( no lookups ), HASH_NODES with a zero weight 
 *                            value will be omitted from the produced table ( no lookup will 
 *                            ever produce a reference to that node ).
 *                            With a non-zero value ( lookups ), HASH_NODES with a zero weight 
 *                            value will be included in the produced table in a single lookup 
 *                            location, which *exactly* maps to the name of the node.
 *                            To put it another way, a hash_lookup() of a string matching the 
 *                            node name will exactly map to that node ( hash_lookup() == 0 ).
 * @return HASH_TABLE : Reference to the newly produced HASH_TABLE, or NULL if a failure occurred.
 * Note -- The expected use case of a HASH_TABLE is either of the following:
 *          DirectLookup Table:
 *             - 'directlookup' arg == 1, and *all* HASH_NODEs have a zero weight
 *             - In this configuration, the HASH_TABLE can be used efficiently determine the 
 *               presence ( hash_lookup() == 0 ) or absence ( hash_lookup() == 1 ) of given node 
 *               names.
 *          Distribution Table:
 *             - 'directlookup' arg == 0, and all relevant HASH_NODEs have non-zero weights
 *               ( zero weight HASH_NODEs are excluded from lookups )
 *             - In this configuration, the HASH_TABLE can be used to produce a uniform 
 *               distribution of HASH_NODE results ( in accordance with their relative weights ) 
 *               for arbitrary string targets ( hash_lookup() == 1 or 0, not relevant which ).
 */
HASH_TABLE hash_init( HASH_NODE* nodes, size_t count, char directlookup );

/**
 * Destroy the given HASH_TABLE, producing a reference to the original HASH_NODE list
 * @param HASH_TABLE table : HASH_TABLE to be destroyed
 * @param HASH_NODE** nodes : Reference to a HASH_NODE* to be populated with the original 
 *                            HASH_NODE list
 * @param size_t* count : Reference to a size_t value to be populated with the length of 
 *                        the original HASH_NODE list
 * @return int : Zero on success, or -1 if a failure occurred
 */
int hash_term( HASH_TABLE table, HASH_NODE** nodes, size_t* count );

/**
 * Lookup the HASH_NODE corresponding to the given string target value
 * @param HASH_TABLE table : HASH_TABLE to perform the lookup within
 * @param const char* target : String target of the lookup
 * @param HASH_NODE** node : Reference to a HASH_NODE* to be populated with the corresponding 
 *                           HASH_NODE reference
 *                           Note -- editing the referenced HASH_NODE, prior to destorying the 
 *                           HASH_TABLE, will result in undefined behavior.
 * @return int : 0, if the corresponding HASH_NODE is an exact match for the target string;
 *               1, if the corresponding HASH_NODE is an approximate match;
 *               -1, if a failure occurred
 * Note -- Assuming no errors, every lookup will result in *some* corresponding HASH_NODE.
 *         Only a return value of zero indicates that the name value of the returned node is an 
 *         exact match of the target string.
 *         Additionally, if the weight of a node is non-zero and/or the 'lookup' arg used to 
 *         generate this HASH_TABLE was zero ( NO support for direct lookups ), there is no 
 *         guarantee that a hash_lookup() of a string matching the node name will map to that 
 *         same node.
 */
int hash_lookup( HASH_TABLE table, const char* target, HASH_NODE** node );

/**
 * From the most recently accessed HASH_NODE, iterate over all remaining HASH_NODE
 * entries in the given table
 * WARNING : This function is NOT thread safe
 * @param HASH_TABLE table : Table on which to iterate
 * @param HASH_NODE** node : Reference to a HASH_NODE* to be populated with the
 *                           corresponding HASH_NODE reference
 * @return int : 1, if a new HASH_NODE reference was produced
 *               0, if no HASH_NODE references remain
 *               -1, if a failure occurred
 */
int hash_iterate( HASH_TABLE table, HASH_NODE** node );

/**
 * Reset the iteration values of the given table, allowing a subsequent iteration to fully traverse it
 * WARNING : This function is NOT thread safe
 * @param HASH_TABLE table : Table to be reset
 * @return int : 0 on success, or -1 on failure
 */
int hash_reset( HASH_TABLE table );

#endif // _HASH_H

