#ifndef _MC_HASH_H
#define _MC_HASH_H
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

#include <stdint.h>

// If this is defined then use a 128 bit identifiers, otherwise use 64 bits.
#define ID128
// these seeds were chosen arbitratily
#define NODE_SEED      119
#define KEY_SEED       17
#define MAX_CAP_NAME   256
// I have done several experiments where I simulate filling a pod with
// four capacity units to 95% capacity to investigate the distribution
// of keys over capacity units. With `DEFAULT_WEIGHT=4000` I found
// that once in 10000 trials a capacity unit may be filled to 99%
// capacity. 4000 also seems to provide a decent balance poiunt for
// uniformity vs. performance: for a 16 node ring with an uneven
// distribution this value yeilds 370ms to create the ring and 0.27
// microseconds to compute the hash of a single key.
#define DEFAULT_WEIGHT 2800

typedef struct node {
   const char *name;
   uint64_t    id[2];
   int         ticket_number;
} node_t;

typedef struct ring {
   size_t   num_nodes;          /* The number of "real" nodes */
   int     *weights;            /* The weight for each of those nodes */
   char   **nodes;              /* The names of the "real" nodes */
   node_t  *virtual_nodes;      /* Sorted array of virtual nodes */
   int      total_tickets;      /* total number of virtual nodes */
} ring_t;

typedef struct node_list {
   node_t           *head;
   struct node_list *tail;
   int               length;
} node_list_t;

typedef struct node_iterator {
   node_list_t *position;
} node_iterator_t;

typedef struct successor_iterator {
   node_list_t  *visited;
   node_t       *position;
   ring_t       *ring;
   unsigned int  start_index;
} successor_it_t;

// The same migration function can be used for both _join and _leave.
// For join a key requires migration if it hashes to a different
// node. For a leave the same is true, but since we are iterating over
// only the keys on the node that is leaving this will be true for
// every key. So this should work for both:
//
// for each node in `from`
//    for each key on node
//       if succ(key) != node
//          do migration function
//       end if
//    end for
// end for
//
// The node list contains nodes that may have keys taken from them.
typedef int (*migration_fn_t)(node_list_t *from, ring_t *new_ring);

// Create a new ring.
//
// The nodes in the ring are specified in the node_names array.
//
// The node_weights array should be the same size as the names array
// and contains the percentages of the tickets that should be assigned
// to each node. If node_weights == NULL the tickets are distributed
// evenly (or as close to evenly as possible) to the nodes.
//
// num_nodes gives the size of the array(s).
ring_t *new_ring(const char *node_names[],
                 const int node_weights[],
                 size_t num_nodes);

// clean up and free allocated memory for this ring.
void destroy_ring(ring_t *ring);

// Join a new node to the ring.
//
// new_node - the name of the node to join.
//
// weight   - the weight of the new node, or 0 to use the default weight
//
// migrate  - function for migrating keys to the new node.
int ring_join(ring_t          *ring,
               const char     *new_node,
               int             weight,
               migration_fn_t  migrate);

// Remove `node` from the ring.
//
// name    - the node to remove from the ring
//
// migrate - function for migrating keys off of `node` and onto the
//           nodes that remain.
int ring_leave(ring_t         *ring,
                const char     *name,
                migration_fn_t  migrate);

// get the successor to the given key in the ring.
node_t *successor(ring_t *ring, const char *key);

// get an iterator that will move clockwise around the ring begining
// with the successor to the specified key. The iterator is exhausted
// once every node in the ring has been returned.
successor_it_t *successor_iterator(ring_t *ring, const char *key);

// get the next node from the successor iterator. Returns NULL if the
// iterator is exhausted.
node_t *next_successor(successor_it_t *it);

// Free memory associated with the successor iterator
void destroy_successor_iterator(successor_it_t *it);

// utility functions for working with node lists. These functions
// provide authors of migration functions with the ability to iterate
// through the node lists.
node_list_t *new_node_list();
node_t *node_pop(node_list_t *list);
int node_push(node_list_t *list, node_t *node);
void destroy_node_list(node_list_t *list);
node_iterator_t *node_iterator(node_list_t *list);
inline void destroy_node_iterator(node_iterator_t *it);
const char *next_node(node_iterator_t *it);
int contains(node_list_t *list, node_t *node);

// Computes a good, uniform, hash of the string.
//
// Treats each character in the length n string as a coefficient of a
// degree n polynomial.
//
// f(x) = string[n -1] + string[n - 2] * x + ... + string[0] * x^(n-1)
//
// The hash is computed by evaluating the polynomial for x=33 using
// Horner's rule.
//
// Reference: http://cseweb.ucsd.edu/~kube/cls/100/Lectures/lec16/lec16-14.html
uint64_t polyhash(const char* string);

// compute the hash function h(x) = (a*x) >> 32. This family of hash
// functions is 2-universal so using them you should see a good
// uniform distribution if a is selected at random.
uint64_t h_a(const uint64_t key, uint64_t a);

#endif // _MC_HASH_H
