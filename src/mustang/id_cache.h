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

#ifndef __ID_CACHE_H__
#define __ID_CACHE_H__

#include <stdlib.h>

typedef struct id_cachenode_struct id_cachenode;

typedef struct id_cachenode_struct {
    char* id;
    id_cachenode* prev;
    id_cachenode* next;
} id_cachenode;

typedef struct id_cache_struct id_cache;

typedef struct id_cache_struct {
    size_t size;
    size_t capacity;
    id_cachenode* head;
    id_cachenode* tail;
} id_cache;

/**
 * Allocate space for, and return a pointer to, a new id_cache struct on the 
 * heap according to a specified capacity.
 *
 * Returns: valid pointer to id_cache struct on success, or NULL on failure.
 */
id_cache* id_cache_init(size_t new_capacity);

/** 
 * Create a new cache node to store `new_id`, then place that node at the head
 * of the cache to indicate that the `new_id` is the most-recently-used ID in
 * the cache. If the cache is at capacity, silently evict the tail node in the
 * cache to make room for the new node.
 *
 * Returns: 0 on success (node could be created and the list was successfully
 * modified), or -1 on failure (node could not be allocated).
 */
int id_cache_add(id_cache* cache, char* new_id);

/** 
 * Check an id_cache struct for a node which stores ID `searched_id`. If the ID
 * is stored, silently "bump" the node storing the searched ID to the head of 
 * the cache to indicate that the ID has been the most recently used in the
 * cache.
 *
 * Returns: 1 if a node was found which stored an id matching `stored_id`, or 0
 * if no such node was present.
 */
int id_cache_probe(id_cache* cache, char* searched_id);

/**
 * Destroy the given id_cache struct and free the memory associated with it.
 */
void id_cache_destroy(id_cache* cache);

#endif
