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
#include <string.h>
#include <errno.h>

#include "id_list.h"

/**** Prototypes for private functions ****/
id_listnode* listnode_init(char* new_name, size_t tabsize);
id_listnode* listnode_get(id_list* list, char* name);
void update_list_tail(id_list* list);
void listnode_destroy(id_listnode* node);

/**** Public interface implementation ****/

/**
 * Allocate space for, and return a pointer to, a new id_list struct on the 
 * heap according to a specified capacity.
 * @param new_capacity : Size of the object ID hashtable for new nodes
 *
 * Returns: valid pointer to id_list struct on success, or NULL on failure.
 */
id_list* id_list_init(size_t new_capacity) {
    id_list* new_list = (id_list*) calloc(1, sizeof(id_list));

    if (new_list == NULL) {
        return NULL;
    }

    // Use the capacity argment in a "constructor" usage pattern
    new_list->capacity = new_capacity;

    // Use "defaults" for other state
    new_list->size = 0;
    new_list->head = NULL;
    new_list->tail = NULL;

    return new_list;
}

/** 
 * Adds a new_id to the named table (i.e. namespace table). If the named
 * table does not exist in the id_list, then it is added to the id_list
 * struct, prior to adding the new_id to tha appropriate table.
 * @param id_list* list : List to update or add to
 * @param char* tabname : Name of the table to add the new_id to
 * @param char* new_id : The new data to add to the name hash table 
 *                       (i.e. Object ID)
 *
 * Returns: 0 on success (node could be created and the list was successfully
 * modified), or -1 on failure (node could not be allocated).
 */
int id_list_add(id_list* list, char* tabname, char* new_id) {
    // Get the right hashtable, based on tabname
    id_listnode* tab_node = listnode_get(list, tabname);

    // Indicates problems finding the right hashtable
    if (!tab_node) return -1;

    // Add the new id to the table
    put(tab_node->idlist, new_id);
    return 0;
}

/** 
 * Check an id_list struct for a node which has an id list by the given
 * serched_name. This routine returned a pointer to the hashtabled list of
 * object IDs.
 * @param id_list* list : List to search
 * @param char* searched_name : Name of the table to find
 *
 * Returns: pointer to the hashtable if a node was found, or NULL
 * if there was an error.
 */
hashtable* id_list_probe(id_list* list, char* searched_name) {
    id_listnode* found_node = listnode_get(list, searched_name);	

    return found_node->idlist;
}

/**
 * Destroy the given id_list struct and free the memory associated with it.
 * @param list: List to deallocate
 */
void id_list_destroy(id_list* list) {
    if (list == NULL) {
        return;
    }

    id_listnode* to_destroy = list->head;

    // In a simple linear traversal, destroy each node.
    while (to_destroy != NULL) {
        id_listnode* next_node = to_destroy->next;
        listnode_destroy(to_destroy);
        to_destroy = next_node;
    }

    // Invalidate list-specific state: head node, tail node, size.
    list->size = 0;
    list->head = NULL;
    list->tail = NULL;
    free(list);
}

/**** Private functions ****/

/**
 * An internal "private" function to initialize an individual list "node"
 * (i.e., an doubly-linked list node that is a constituent of the list).
 * @param new_name : Name for the new hashtable
 * @param tabsize : The capacity of the new hashtable
 *
 * NOTE: as a private function, users should **never** call this directly,
 * instead relying on higher-level public wrappers (in this case, 
 * id_list_add()).
 */
id_listnode* listnode_init(char* new_name, size_t tabsize) {
    id_listnode* new_node = calloc(1, sizeof(id_listnode));

    if (new_node == NULL) {
        return NULL;
    }
    
    // Dup string memory to separate concerns of argument and memory storage.
    // In effect, follow a "constructor" pattern.
    char* duped_id = strdup(new_name);
    if (duped_id == NULL) {
        free(new_node);
        return NULL;
    }

    new_node->name = duped_id;
    new_node->idlist = hashtable_init(tabsize);
    new_node->prev = NULL;
    new_node->next = NULL;

    return new_node;
}

/**
 * Returns the id_listnode* associated with the table name. If
 * the node does not exist, it is created.
 * @param id_list* list : List to update or add to
 * @param char* name : Name of the table to add the new_id to
 *
 * @returns : a id_listnode* for the given name. If NULL
 *            is returned, then an error occured - most
 *            likely some issue with system resources
 *            (i.e. ENOMEM)
 */
id_listnode* listnode_get(id_list* list, char* name) {
    id_listnode* lptr = list->head;    // pointer to move through the list

    // Look for the correct id_listnode in the list
    while ( lptr ) {
        if (!strcmp(name,lptr->name)) break;
        lptr = lptr->next;	
    }

    // id_list is empty, or name is not found,
    // add new node with the new name
    if (!lptr) {
        // Internally create space for new node based on ID
        id_listnode* new_node = listnode_init(name, list->capacity);	    

        // Indicates ENOMEM or similar critical error condition
        if (!new_node) return (id_listnode*)NULL;

        // If list is empty, no need to "link" new node to any existing nodes.
        // If list is not empty, perform "linking" to any existing nodes as 
        // applicable in addition to placing the new node at the head position.
        if (list->size == 0) {
            list->head = new_node;
        } else {
            new_node->next = list->head;
            list->head->prev = new_node;
            list->head = new_node;
        }

        list->size += 1;

        // New nodes are essentially "pushed" to the top of the list in a 
        // stack-like usage pattern, so update the tail node manually.
        update_list_tail(list);
	lptr = new_node;
    }

    return lptr;    
}

/**
 * An internal "private" function to search the list for the "canonical" tail
 * node (i.e., the node whose ->next field is a NULL pointer) and ensure that
 * the list properly records this node as the tail pointer in its ->tail 
 * field.
 *
 * NOTE: as a private function, users should **never** call this directly,
 * instead relying on higher-level public wrappers (in this case, 
 * id_list_add() and id_list_probe()).
 */
void update_list_tail(id_list* list) {
    if (list == NULL) {
        return;
    }

    id_listnode* current_node = list->head;

    while (current_node != NULL) {
        if (current_node->next == NULL) {
            list->tail = current_node;
        }

        current_node = current_node->next;
    }
}

/** 
 * An internal "private" function to free an individual node in the ID list 
 * and its associated state.
 *
 * NOTE: as a private function, users should **never** call this directly, 
 * instead relying on higher-level public wrappers (in this case, 
 * id_list_add() and id_list_destroy()).
 */
void listnode_destroy(id_listnode* node) {
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
    free(node->name);
    hashtable_destroy(node->idlist);
    free(node);
}

