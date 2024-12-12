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

#ifndef __ID_LIST_H__
#define __ID_LIST_H__

#include <stdlib.h>

#include "hashtable.h"

typedef struct id_listnode_struct id_listnode;

typedef struct id_listnode_struct {
    char* name;
    hashtable* idlist;
    id_listnode* prev;
    id_listnode* next;
} id_listnode;

typedef struct id_list_struct id_list;

typedef struct id_list_struct {
    size_t size;
    size_t capacity;
    id_listnode* head;
    id_listnode* tail;
} id_list;

/**
 * Allocate space for, and return a pointer to, a new id_list struct on the 
 * heap according to a specified capacity.
 *
 * Returns: valid pointer to id_list struct on success, or NULL on failure.
 */
id_list* id_list_init(size_t new_capacity);

/** 
 * Adds a new_id to the named table (i.e. namespace table). If the named
 * table does not exist in the id_list, then it is added to the id_list
 * struct, prior to adding the new_id to tha appropriate table.
 *
 * Returns: 0 on success (node could be created and the list was successfully
 * modified), or -1 on failure (node could not be allocated).
 */
int id_list_add(id_list* list, char* tabname, char* new_id);

/** 
 * Check an id_list struct for a node which has an id list by the given
 * serched_name. This routine returned a pointer to the hashtabled list of
 * object IDs.
 *
 * Returns: pointer to the hashtable if a node was found, or NULL
 * if there was an error.
 */
hashtable* id_list_probe(id_list* list, char* searched_name);

/**
 * Destroy the given id_list struct and free the memory associated with it.
 */
void id_list_destroy(id_list* list);

#endif
