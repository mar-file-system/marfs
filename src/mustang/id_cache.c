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

#include "id_cache.h"
#include <string.h>
#include <errno.h>

/**** Prototypes for private functions ****/
id_cachenode* cachenode_init(char* new_id);
void update_tail(id_cache* cache);
void cachenode_destroy(id_cachenode* node);
void pluck_node(id_cache* cache, id_cachenode* node);

/**** Public interface implementation ****/

/**
 * Allocate space for, and return a pointer to, a new id_cache struct on the 
 * heap according to a specified capacity.
 *
 * Returns: valid pointer to id_cache struct on success, or NULL on failure.
 */
id_cache* id_cache_init(size_t new_capacity) {
    id_cache* new_cache = (id_cache*) calloc(1, sizeof(id_cache));

    if (new_cache == NULL) {
        return NULL;
    }

    // Use the capacity argment in a "constructor" usage pattern
    new_cache->capacity = new_capacity;

    // Use "defaults" for other state
    new_cache->size = 0;
    new_cache->head = NULL;
    new_cache->tail = NULL;

    return new_cache;
}

/** 
 * Create a new cache node to store `new_id`, then place that node at the head
 * of the cache to indicate that the `new_id` is the most-recently-used ID in
 * the cache. If the cache is at capacity, silently evict the tail node in the
 * cache to make room for the new node.
 *
 * Returns: 0 on success (node could be created and the list was successfully
 * modified), or -1 on failure (node could not be allocated).
 */
int id_cache_add(id_cache* cache, char* new_id) {
    // Internally create space for new node based on ID
    id_cachenode* new_node = cachenode_init(new_id);

    // Indicates ENOMEM or similar critical error condition
    if (new_node == NULL) {
        return -1;
    }

    // If cache is empty, no need to "link" new node to any existing nodes.
    // If cache is not empty, perform "linking" to any existing nodes as 
    // applicable in addition to placing the new node at the head position.
    if (cache->size == 0) {
        cache->head = new_node;
    } else {
        new_node->next = cache->head;
        cache->head->prev = new_node;
        cache->head = new_node;
    }

    cache->size += 1;

    // New nodes are essentially "pushed" to the top of the cache in a 
    // stack-like usage pattern, so update the tail node manually.
    update_tail(cache);

    // If cache is at capacity, evict (pop and destroy) the tail node, which
    // corresponds to the least-recently-used object ID.
    if (cache->size > cache->capacity) {
        cachenode_destroy(cache->tail);
        cache->size -= 1;
        update_tail(cache); // Tail was just evicted, so find new tail node
    }

    return 0;
}

/** 
 * Check an id_cache struct for a node which stores ID `searched_id`. If the ID
 * is stored, silently "bump" the node storing the searched ID to the head of 
 * the cache to indicate that the ID has been the most recently used in the
 * cache.
 *
 * Returns: 1 if a node was found which stored an id matching `stored_id`, or 0
 * if no such node was present.
 */
int id_cache_probe(id_cache* cache, char* searched_id) {
    id_cachenode* searched_node = cache->head;

    // Perform a linear traversal of the cache to attempt to find a node with
    // matching data
    while (searched_node != NULL) {
        // If the queried ID hits against this cache...
        if (strncmp(searched_node->id, searched_id, strlen(searched_node->id) + 1) == 0) {
            // Move the node with matching data to the head position to 
            // indicate it is the most recently used in the cache
            pluck_node(cache, searched_node);

            // This may possibly move the tail node to the head node, so 
            // ensure that the tail node is properly recorded for this 
            // cache.
            update_tail(cache);

            return 1;
        }

        searched_node = searched_node->next;
    }

    return 0;
}

/**
 * Destroy the given id_cache struct and free the memory associated with it.
 */
void id_cache_destroy(id_cache* cache) {
    if (cache == NULL) {
        return;
    }

    id_cachenode* to_destroy = cache->head;

    // In a simple linear traversal, destroy each node.
    while (to_destroy != NULL) {
        id_cachenode* next_node = to_destroy->next;
        cachenode_destroy(to_destroy);
        to_destroy = next_node;
    }

    // Invalidate cache-specific state: head node, tail node, size.
    cache->size = 0;
    cache->head = NULL;
    cache->tail = NULL;
    free(cache);
}

/**** Private functions ****/

/**
 * An internal "private" function to initialize an individual cache "node"
 * (i.e., an doubly-linked list node that is a constituent of the cache).
 *
 * NOTE: as a private function, users should **never** call this directly,
 * instead relying on higher-level public wrappers (in this case, 
 * id_cache_add()).
 */
id_cachenode* cachenode_init(char* new_id) {
    id_cachenode* new_node = calloc(1, sizeof(id_cachenode));

    if (new_node == NULL) {
        return NULL;
    }
    
    // Dup string memory to separate concerns of argument and memory storage.
    // In effect, follow a "constructor" pattern.
    char* duped_id = strdup(new_id);
    if (duped_id == NULL) {
        free(new_node);
        return NULL;
    }

    new_node->id = duped_id;
    new_node->prev = NULL;
    new_node->next = NULL;

    return new_node;
}

/**
 * An internal "private" function to search the cache for the "canonical" tail
 * node (i.e., the node whose ->next field is a NULL pointer) and ensure that
 * the cache properly records this node as the tail pointer in its ->tail 
 * field.
 *
 * NOTE: as a private function, users should **never** call this directly,
 * instead relying on higher-level public wrappers (in this case, 
 * id_cache_add() and id_cache_probe()).
 */
void update_tail(id_cache* cache) {
    if (cache == NULL) {
        return;
    }

    id_cachenode* current_node = cache->head;

    while (current_node != NULL) {
        if (current_node->next == NULL) {
            cache->tail = current_node;
        }

        current_node = current_node->next;
    }
}

/** 
 * An internal "private" function to free an individual node in the ID cache 
 * and its associated state.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this case, 
 * id_cache_add() and id_cache_destroy()).
 */
void cachenode_destroy(id_cachenode* node) {
    if (node == NULL) {
        return;
    }

    // "Reach into" next node if valid and redirect prev pointer
    if (node->next != NULL) {
        node->next->prev = node->prev;
    }

    // "Reach into" prev node if valid and redirect next pointer
    if (node->prev != NULL) {
        node->prev->next = node->next;
    }

    // Completely unlink this node from others for safety
    node->prev = NULL;
    node->next = NULL;
    free(node->id);
    free(node);
}

/** 
 * An internal "private" function to move a node from an arbitrary position in
 * the ID cache to the head position, then update the cache's state accordingly
 * to reflect that the node is now the head ndoe.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this case, 
 * id_cache_probe()).
 */
void pluck_node(id_cache* cache, id_cachenode* node) {
    if (cache == NULL || node == NULL) {
        return;
    }

    // If node is already at the head of the cache, no work needs to be done to
    // rearrange node to head position in cache
    if (cache->head == node) {
        return;
    }

    // If node has a valid previous node, "reach into" previous node and 
    // redirect its next pointer
    if (node->prev != NULL) {
        node->prev->next = node->next;
    }

    // If node has a valid next node, "reach into" next node and redirect its
    // prev pointer
    if (node->next != NULL) {
        node->next->prev = node->prev;
    }

    // Move node to head of cache: make prev NULL (no node precedes it), 
    // link new node to current head, and record head as new node
    node->prev = NULL;
    node->next = cache->head;
    cache->head->prev = node;
    cache->head = node;
}
