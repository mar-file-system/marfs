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

#include "hashtable.h"
#include "mustang_logging.h"
#include <errno.h>

/** 
 * Internal hashing function: MurmurHash3_x64_128 by Austin Appleby. See
 * release notes and full implementation, including helper utilities and
 * macros, below.
 *
 * Duplicated from the MarFS public repository at:
 * https://github.com/mar-file-system/marfs/blob/master/src/hash/hash.c
 */
void MurmurHash3_x64_128( const void* key, const int len, const uint32_t seed,
        void* out );

/**
 * A wrapper around the MurmurHash3 calls to return a "friendly" 
 * capacity-aligned index.
 */
uint64_t hashcode(hashtable* table, char* name) {
    uint64_t murmur_result[2];
    MurmurHash3_x64_128(name, strlen(name), KEY_SEED, murmur_result);
    return murmur_result[0] % (table->capacity);
}

/**
 * An internal "private" function which initializes a separately chained node 
 * for a hashtable's linked list of chained nodes.
 *
 * NOTE: users should **never** call this directly, instead relying on the 
 * higher-level public wrappers related to hashtable initialization and put
 * operations (in this case, put()).
 */
hashnode_link* hn_link_init(char* new_data) {
    hashnode_link* new_link = (hashnode_link*) calloc(1, sizeof(hashnode_link));

    if (new_link == NULL) {
        return NULL;
    }

    new_link->data = strdup(new_data);
    new_link->next = NULL;
    return new_link;
}

/**
 * An internal "private" function which cleans up a separately chained node 
 * within a hashnode's linked list, freeing the chained node (the "hashnode 
 * link") and its associated state.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this case, 
 * hashtable_destroy()).
 */
int hn_link_destroy(hashnode_link* hn_link) {
    if (hn_link == NULL) {
        errno = EINVAL;
        return -1;
    }

    free(hn_link->data);
    free(hn_link);
    return 0;
}

/**
 * An internal "private" function which initializes a hashtable node and 
 * prepares the node for separate chaining operations related to hashtable 
 * inserts. 
 *
 * NOTE: as a private function, users should **never** call this directly,
 * instead relying on higher-level public wrappers (in this case, 
 * hashtable_init()).
 */
hashnode* hashnode_init(void) {
    hashnode* new_node = (hashnode*) calloc(1, sizeof(hashnode));

    if (new_node == NULL) {
        return NULL;
    }
    
    new_node->linked = 0;
    new_node->hn_links = NULL;
    return new_node;
}

/**
 * An internal "private" function to check whether a string value which hashes 
 * to the index where the current node is stored is a duplicate of any string 
 * values currently stored in the node's separately chained data linked list.
 *
 * If the data is found to be a duplicate, the calling function (the public 
 * put()) will simply "drop" the data and not add it to the table. If the data
 * is original, put() will invoke the proper functionality to link the data to 
 * the hashnode.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this, case, put()).
 */
int verify_original(hashnode* node, char* new_data) {
    hashnode_link* current_link = node->hn_links;

    for (size_t i = 0; i < node->linked; i += 1) {
        if (strncmp(current_link->data, new_data, strlen(current_link->data) + 1) == 0) {
            return 0;
        }

        current_link = current_link->next;
    }

    return 1;
}

/**
 * An internal "private" function to chain new data to a particular hashonde's 
 * separate chaining linked list. This function makes the necessary calls to 
 * other private functions to initialize and prepare associated state for the 
 * new data, then properly link it to the node's list.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this case, put()).
 */
int hashnode_chain(hashnode* node, char* new_link_data) {
    hashnode_link* new_link = hn_link_init(new_link_data);

    if (new_link == NULL) {
        return -1;
    }

    if (node->hn_links == NULL) {
        node->hn_links = new_link;
    } else {
        hashnode_link* tail_link = node->hn_links;

        for (size_t i = 0; i < (node->linked - 1); i += 1) {
            tail_link = tail_link->next;
        }

        tail_link->next = new_link;
    }

    node->linked += 1;
    return 0;
}

/**
 * An internal "private" function to clean up a particular hashnode and its 
 * associated state, including its separately chained data linked list.
 *
 * NOTE: as a private function, users should **never** call this directly,
 * instead relying on higher-level public wrappers (in this case, 
 * hashtable_destroy()).
 */
int hashnode_destroy(hashnode* node) {
    if (node == NULL) {
        errno = EINVAL;
        return -1;
    }

    hashnode_link* current_link = node->hn_links;

    for (size_t i = 0; i < node->linked; i += 1) {
        hashnode_link* next_link = current_link->next;
        hn_link_destroy(current_link);
        current_link = next_link;
    }

    node->linked = 0;
    node->hn_links = NULL;
    free(node);

    return 0;
}

/**
 * Initialize a hashtable on the heap, including space for all `new_capacity`
 * hashnodes. As part of the table initialization, this function invokes 
 * necessary functionality to allocate state for each hashnode and prepare 
 * separate chaining.
 */
hashtable* hashtable_init(size_t new_capacity) {
    hashtable* new_table = calloc(1, sizeof(hashtable));

    new_table->stored_nodes = (hashnode**) calloc(new_capacity, sizeof(hashnode*));

    if (new_table == NULL) {
        return NULL;
    }
    
    new_table->capacity = new_capacity;

    for (size_t node_index = 0; node_index < new_capacity; node_index += 1) {
        new_table->stored_nodes[node_index] = hashnode_init();
    }

    return new_table;
}

/**
 * Destroy a hashtable on the heap, freeing all memory associated with the
 * table including the space for all `table->capacity` nodes and their 
 * associated state of their linked lists maintained for separate chaining.
 */
void hashtable_destroy(hashtable* table) {
    for (size_t node_index = 0; node_index < table->capacity; node_index += 1) {
        hashnode_destroy((table->stored_nodes)[node_index]);
    }
    
    free(table->stored_nodes);
    free(table); 
}

/** 
 * The public function to insert an object name into a particular hash table.
 *
 * For a given name key, compute the hashcode and insert the name at the
 * appropriate index within the hashtable.
 *
 * NOTE: in a case of application-specific behavior, this function always
 * succeeds (and thus does not return anything). The function will either
 * insert the object name into the table (including through internal separate 
 * chaining functionality) if it is not present at the computed index or will 
 * simply return without inserting upon encountering a duplicate.
 */
void put(hashtable* table, char* new_object_name) {
    // Compute the hash to see which index (and, therefore, which relevant
    // node) to insert at
    
    uint64_t mapped_hashcode = hashcode(table, new_object_name);

    if (verify_original((table->stored_nodes)[mapped_hashcode], new_object_name)) {
        hashnode_chain((table->stored_nodes)[mapped_hashcode], new_object_name);
    }
}

/** 
 * An internal "private" function to print a hashnode's full contents,
 * including the data for all nodes in the hashnode's separately chained data
 * linked list, to the specified stream.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this case, 
 * hashtable_dump()).
 */
int hashnode_dump(hashnode* node, FILE* output) { 
    if ((node == NULL) || (output == NULL)) { 
        errno = EINVAL; 
        return -1; 
    }

    if (node->hn_links == NULL) {
        return 0;
    }

    hashnode_link* current_link = node->hn_links;

    for (size_t i = 0; i < node->linked; i += 1) {
        fprintf(output, "%s\n", current_link->data);
        current_link = current_link->next;
    }

    return 0;
}

/** 
 * A helper function to print the contents of non-empty hashnodes, including 
 * the contents of their linked lists maintained for separate chaining, in a
 * hashtable to the file referenced by the pointer `output`.
 *
 * NOTE: this function assumes any synchronization measures (locking an
 * associated mutex for hashtable `table`, etc.) have already been taken. In
 * mustang, the engine (main routine) locks and unlocks the associated mutex
 * around a call to this function to satisfy the synchronization assumptions.
 * Do not otherwise call this function without appropriately synchronizing on
 * the hashtable.
 */
int hashtable_dump(hashtable* table, FILE* output) {
    for (size_t index = 0; index < table->capacity; index += 1) {
        hashnode_dump((table->stored_nodes)[index], output);
    }

    return fclose(output);
}

/* ----- END HASHTABLE IMPLEMENTATION ----- */

// The following implementation of MurmurHash was retrieved from --
//    https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
// 
// The release info for the code has been reproduced below.
//
//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
//


#define BIG_CONSTANT(x) (x##LLU)
#define FORCE_INLINE inline __attribute__((always_inline))
#define ROTL64(x,y) rotl64(x,y)


static FORCE_INLINE uint64_t rotl64 ( uint64_t x, int8_t r )
{
  return (x << r) | (x >> (64 - r));
}

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here
#define getblock64(p,i) (p[i])

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche
static FORCE_INLINE uint64_t fmix64 ( uint64_t k )
{
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;

  return k;
}


void MurmurHash3_x64_128 ( const void * key, const int len,
                           const uint32_t seed, void * out )
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 16;

  uint64_t h1 = seed;
  uint64_t h2 = seed;

  const uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  const uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

  //----------
  // body

  const uint64_t * blocks = (const uint64_t *)(data);

  int i = 0;
  for(; i < nblocks; i++)
  {
    uint64_t k1 = getblock64(blocks,i*2+0);
    uint64_t k2 = getblock64(blocks,i*2+1);

    k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;

    h1 = ROTL64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

    k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;

    h2 = ROTL64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
  }

  //----------
  // tail

  const uint8_t * tail = (const uint8_t*)(data + nblocks*16);

  uint64_t k1 = 0;
  uint64_t k2 = 0;

  // switch statement written to implicitly allow fallthroughs.
  // To silence gcc warnings with `-Wextra` about this, use 
  // "// fall through" magic comments. Yes, there are better ways of
  // silencing compiler warnings, but this is the funniest.
  switch(len & 15)
  {
  case 15: k2 ^= ((uint64_t)tail[14]) << 48; // fall through
  case 14: k2 ^= ((uint64_t)tail[13]) << 40; // fall through
  case 13: k2 ^= ((uint64_t)tail[12]) << 32; // fall through
  case 12: k2 ^= ((uint64_t)tail[11]) << 24; // fall through
  case 11: k2 ^= ((uint64_t)tail[10]) << 16; // fall through
  case 10: k2 ^= ((uint64_t)tail[ 9]) << 8; // fall through
  case  9: k2 ^= ((uint64_t)tail[ 8]) << 0; 
           k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2; // fall through

  case  8: k1 ^= ((uint64_t)tail[ 7]) << 56; // fall through
  case  7: k1 ^= ((uint64_t)tail[ 6]) << 48; // fall through
  case  6: k1 ^= ((uint64_t)tail[ 5]) << 40; // fall through
  case  5: k1 ^= ((uint64_t)tail[ 4]) << 32; // fall through
  case  4: k1 ^= ((uint64_t)tail[ 3]) << 24; // fall through
  case  3: k1 ^= ((uint64_t)tail[ 2]) << 16; // fall through
  case  2: k1 ^= ((uint64_t)tail[ 1]) << 8; // fall through
  case  1: k1 ^= ((uint64_t)tail[ 0]) << 0; 
           k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1; // fall through
  };

  //----------
  // finalization

  h1 ^= len; h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  h2 += h1;

  ((uint64_t*)out)[0] = h1;
  ((uint64_t*)out)[1] = h2;
}


