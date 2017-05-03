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
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "mc_hash.h"
#include "murmur_hash.h"

#ifndef ID128
#include <spooky-c.h>
#endif

////////// Hash functions

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
uint64_t polyhash(const char* string) {
   // According to http://www.cse.yorku.ca/~oz/hash.html
   // 33 is a magical number that inexplicably works the best.
   const int salt = 33;
   char c;
   uint64_t h = *string++;
   while((c = *string++))
      h = abs(salt * h + c);
   return h;
}

// compute the hash function h(x) = (a*x) >> 32
uint64_t h_a(const uint64_t key, uint64_t a) {
   return ((a * key) >> 32);
}

////////// Ring API

static void identifier128(const char *key, uint64_t *id) {
   //spooky_hash128(key, strlen(key), &id[0], &id[1]);
   MurmurHash3_x86_128(key, strlen(key), KEY_SEED, id);
}

static void node_identifier128(const char *key, uint64_t *id) {
   MurmurHash3_x86_128(key, strlen(key), NODE_SEED, id);
}

static void identifier(const char *key, uint64_t *id) {
   identifier128(key, id);
#ifndef ID128
   id[1] = 0; // jsut use 64 bits by masking out 
#endif
}

static void node_identifier(const char *key, uint64_t *id) {
   node_identifier128(key, id);
#ifndef ID128
   id[1] = 0; // just using 64 bits
#endif
}

static inline int compare_id(uint64_t *id_a, uint64_t *id_b) {
   if(id_a[0] > id_b[0]) {
      return 1;
   }
   else if(id_a[0] < id_b[0]) {
      return -1;
   }
   else if(id_a[0] == id_b[0]) {
      if(id_a[1] > id_b[1]) {
         return 1;
      }
      else if(id_a[1] < id_b[1]) {
         return -1;
      }
      else if(id_a[1] == id_b[1]) {
         return 0;
      }
   }
}

// comparisong function for ues by qsort.
static int compare_nodes(const void *a, const void *b) {
   node_t *node_a = (node_t*)a;
   node_t *node_b = (node_t*)b;
   int comparison_result = compare_id(node_a->id, node_b->id);
   if(comparison_result < 0)
      return -1;
   else if(comparison_result > 0)
      return 1;
   else if(comparison_result == 0) {
      // impose a total ordering on nodes based first on id.  if ids
      // are equal then compare ticket numbers. if ticket number are
      // equal then compare names.
      if(node_a->ticket_number < node_b->ticket_number)
         return -1;
      else if(node_b->ticket_number == node_a->ticket_number)
         // as a last result do a lexical comparison on the node names.
         return strncmp(node_a->name, node_b->name, MAX_CAP_NAME);
      else
         return 1;
   }
}

static inline void vnode_name(char *vname, node_t *node) {
   sprintf(vname, "%d-%s", node->ticket_number, node->name);
}

ring_t *_new_ring(const char *node_names[],
                   const int node_weights[],
                   size_t num_nodes, size_t tickets);

ring_t *new_ring(const char *node_names[], const int node_weights[],
                 size_t num_nodes) {
   return _new_ring(node_names, node_weights, num_nodes, DEFAULT_WEIGHT);
}


ring_t *_new_ring(const char *node_names[],
                   const int node_weights[],
                   size_t num_nodes, size_t virtual_nodes) {
   int tickets_per_node = 0;

   ring_t *ring = malloc(sizeof(ring_t));
   if(ring == NULL) {
      return NULL;
   }
   ring->total_tickets = 0;
   ring->num_nodes = num_nodes;

   // distribute tickets evenly
   if(node_weights == NULL) {
      tickets_per_node = virtual_nodes;
      ring->total_tickets = tickets_per_node * num_nodes;
   }
   else {
      int i;
      for(i = 0; i < num_nodes; i++) {
         ring->total_tickets += node_weights[i] * virtual_nodes;
      }
   }

   ring->virtual_nodes = (node_t *)malloc(sizeof(node_t) * ring->total_tickets);
   if(ring->virtual_nodes == NULL) {
      free(ring);
      return NULL;
   }
   ring->nodes = malloc(sizeof(char *) * num_nodes);
   if(ring->nodes == NULL) {
      free(ring->virtual_nodes);
      free(ring);
      return NULL;
   }
   ring->weights = malloc(sizeof(int) * num_nodes);
   if(ring->weights == NULL) {
      free(ring->nodes);
      free(ring->virtual_nodes);
      free(ring);
   }

   // push all the nodes into the ring->nodes array
   int i;
   int vnode = 0;
   for(i = 0; i < num_nodes; i++) {
      int tickets;
      if(!tickets_per_node) { // then we are using the list.
         tickets = node_weights[i] * virtual_nodes;
         ring->weights[i] = node_weights[i];
      }
      else {
         tickets = tickets_per_node;
         ring->weights[i] = 1;
      }

      // copy the node details to the ring_t
      ring->nodes[i] = strdup(node_names[i]);
      if(ring->nodes[i] == NULL) {
         ring->num_nodes = i-1;
         destroy_ring(ring);
         return NULL;
      }

      int j;
      for(j = 0; j < tickets; j++) {
         char vname[MAX_CAP_NAME+16];
         ring->virtual_nodes[vnode].id[0] = 0;
         ring->virtual_nodes[vnode].id[1] = 0;
         ring->virtual_nodes[vnode].ticket_number = j;
         ring->virtual_nodes[vnode].name = ring->nodes[i];
         vnode_name(vname, &ring->virtual_nodes[vnode]);
         node_identifier(vname, ring->virtual_nodes[vnode].id);
         vnode++;
      }
   }

   // sort the ring->nodes array on node.id
   qsort(ring->virtual_nodes, ring->total_tickets, sizeof(node_t), compare_nodes);

   return ring;
}

void destroy_ring(ring_t *ring) {
   int i;
   for(i = 0; i < ring->num_nodes; i++) {
      free(ring->nodes[i]);
   }
   free(ring->nodes);
   free(ring->weights);
   free(ring->virtual_nodes);
   free(ring);
}

static node_t *successor_by_id(ring_t *ring, uint64_t *key_id) {
   // perform a binary search in ring->nodes for the node that hash
   // .id >= key_id and where the previous node < key_id
   int i = ring->total_tickets/2;
   int min, max;
   min = 0;
   max = ring->total_tickets;
   while(i > 0 && i < ring->total_tickets) {
      int pred = i == 0 ? ring->total_tickets-1 : i - 1;
      int vnode_i_cmp = compare_id(ring->virtual_nodes[i].id, key_id);
      if(vnode_i_cmp >= 0
         && compare_id(ring->virtual_nodes[pred].id, key_id) < 0) {
         return &ring->virtual_nodes[i];
      }
      else if(vnode_i_cmp < 0) {
         min = i;
      }
      else { // too big
         max = i;
      }

      i = min + (max - min) / 2;

      //      if(i == ring->total_tickets-1 && ring->virtual_nodes[i].id < key_id)
      if(i == ring->total_tickets-1
         && compare_id(ring->virtual_nodes[i].id, key_id) < 0) {
         // this is the wrap-around case.
         return &ring->virtual_nodes[0];
      }
   }

   return &ring->virtual_nodes[i];
}

node_t *successor(ring_t *ring, const char *key) {
   uint64_t key_id[2];
   identifier(key, key_id);

   return successor_by_id(ring, key_id);
}

successor_it_t *successor_iterator(ring_t *ring, const char *key) {
   successor_it_t *it = malloc(sizeof(successor_it_t));
   if(it == NULL) {
      return NULL;
   }

   it->ring    = ring;
   it->visited = new_node_list();
   if(it->visited == NULL) {
      return NULL;
   }
   it->position = successor(ring, key);
   it->start_index = ((void *)it->position - (void *)it->ring->virtual_nodes)
      / sizeof(node_t);

   return it;
}

node_t *next_successor(successor_it_t *it) {
   node_t *next = it->position;

   if(next == NULL) { // the iterator has been exhausted.
      return NULL;
   }
   
   node_push(it->visited, next);
   
   if(list_length(it->visited) == it->ring->num_nodes) {
      it->position = NULL;
      return next;
   }
   
   // loop through the vnodes in the ring to set the next position.
   int index = ((void *)it->position - (void *)it->ring->virtual_nodes)
      / sizeof(node_t);
   for(index = index+1 ; index != it->start_index; index++) {
      if(index == it->ring->total_tickets) {
         index = 0; // wrap around to the "begining" of the ring.
      }
      if(!contains(it->visited, &it->ring->virtual_nodes[index])) {
         // the node has not been visited yet.
         it->position = &it->ring->virtual_nodes[index];
         break;
      }
   }

   return next;
}

void destroy_successor_iterator(successor_it_t *it) {
   destroy_node_list(it->visited);
   free(it);
}

int ring_join(ring_t     *ring,
              const char *new_node,
              int         weight,
              migration_fn_t migrate) {
   int virtual_nodes;
   if(weight == 0) {
      virtual_nodes = DEFAULT_WEIGHT;
   }
   else {
      virtual_nodes = weight * DEFAULT_WEIGHT;
   }

   int *node_weights = malloc(sizeof(int) * (ring->num_nodes + 1));
    if(node_weights == NULL) {
      return -1;
   }
   const char **node_names = malloc(sizeof(char*) * (ring->num_nodes + 1));
   if(node_names == NULL) {
      free(node_weights);
      return -1;
   }

   // copy the arrays
   int i;
   for(i = 0; i < ring->num_nodes; i++) {
      node_weights[i] = ring->weights[i];
      node_names[i] = ring->nodes[i];
   }

   node_weights[ring->num_nodes] = weight ? weight : 1;
   node_names[ring->num_nodes] = new_node;

   // Build a new ring.
   ring_t *updated_ring = new_ring(node_names,
                                   node_weights,
                                   ring->num_nodes + 1);
   if(updated_ring == NULL) {
      free(node_names);
      free(node_weights);
      return -1;
   }

   // For now a list will do.
   node_list_t *rebalance_nodes = new_node_list();
   for(i = 0; i < virtual_nodes; i++) {
      // for each virtual node id get the successor in the "old"
      // ring. add it to the list if it is not already present.
      char vnode[MAX_CAP_NAME + 16];
      node_t node;
      node.name = new_node;
      node.ticket_number = i;
      vnode_name(vnode, &node);
      node_identifier(vnode, node.id);
      node_t *succ = successor_by_id(ring, node.id);
      if(!contains(rebalance_nodes, succ)) {
         if(node_push(rebalance_nodes, succ) == -1) {
            // we could fail to push if we are out of memory.
            destroy_node_list(rebalance_nodes);
            destroy_ring(updated_ring);
            free(node_weights);
            free(node_names);
            return -1;
         }
      }
   }

   if(migrate(rebalance_nodes, updated_ring) != 0) {
      destroy_node_list(rebalance_nodes);
      destroy_ring(updated_ring);
      free(node_weights);
      free(node_names);
      return -1;
   }

   // update fields in `ring` and free resoursces associated with the
   // old ring.
   free(ring->weights);
   ring->weights = updated_ring->weights;
   for(i = 0; i < ring->num_nodes; i++) {
      free(ring->nodes[i]);
   }
   free(ring->nodes);
   ring->nodes = updated_ring->nodes;
   ring->num_nodes++;
   free(ring->virtual_nodes);
   ring->virtual_nodes = updated_ring->virtual_nodes;
   ring->total_tickets = updated_ring->total_tickets;

   free(updated_ring);
   free(node_names);
   free(node_weights);

   destroy_node_list(rebalance_nodes);
   
   return 0;
}

int ring_leave(ring_t *ring, const char *node, migration_fn_t migrate) {
   // make an array of nodes without `node`
   char **new_nodes   = malloc(sizeof(char *) * (ring->num_nodes - 1));
   int   *new_weights = malloc(sizeof(int) * ring->num_nodes);
   int    vnodes_removed;
   int    removed_weight;
   int i, j;

   if(ring->num_nodes == 1) {
      return -1; // XXX: can't have a ring with no nodes.
   }
   
   for(i = j = 0; i < ring->num_nodes; i++) {
      if(strcmp(node, ring->nodes[i])) {
         new_nodes[j] = strdup(ring->nodes[i]);
         if(new_nodes[j] == NULL) {
            for(j = j - 1; j >= 0; j--) {
               free(new_nodes[j]);
            }
            free(new_nodes);
            free(new_weights);
            return -1;
         }
         new_weights[j] = ring->weights[j];
         j++;
      }
      else {
         removed_weight = ring->weights[i];
         vnodes_removed = removed_weight * DEFAULT_WEIGHT;
      }
   }

   ring_t *updated_ring = new_ring((const char **)new_nodes,
                                   (const int *)new_weights,
                                   ring->num_nodes-1);
   if(updated_ring == NULL) {
      for(i = 0; i < ring->num_nodes-1; i++) {
         free(new_nodes[i]);
      }
      free(new_nodes);
      free(new_weights);
      return -1;
   }

   node_list_t *migration_nodes = new_node_list();
   if(migration_nodes == NULL) {
      destroy_ring(updated_ring);
      free(new_nodes);
      free(new_weights);
      return -1;
   }
   node_t n = { .name = node };
   if(node_push(migration_nodes, &n)) {
      destroy_ring(updated_ring);
      destroy_node_list(migration_nodes);
      free(new_nodes);
      free(new_weights);
      return -1;
   }
   
   // perform migration
   if(migrate(migration_nodes, updated_ring) == -1) {
      destroy_ring(updated_ring);
      destroy_node_list(migration_nodes);
      free(new_nodes);
      free(new_weights);
      return -1;
   }
   
   destroy_node_list(migration_nodes);

   // copy relevant fields from updated_ring to ring
   free(ring->nodes);
   ring->nodes = updated_ring->nodes;
   ring->num_nodes--;
   free(ring->weights);
   ring->weights = updated_ring->weights;
   free(ring->virtual_nodes);
   ring->virtual_nodes = updated_ring->virtual_nodes;
   ring->total_tickets = updated_ring->total_tickets;

   return 0;
}

////////// Node list API

node_list_t *new_node_list() {
   node_list_t *nl = malloc(sizeof(node_list_t));
   if(nl == NULL) return NULL;

   nl->head = NULL;
   nl->tail = NULL;
   nl->length = 0;

   return nl;
}

int list_length(node_list_t *list) {
   return list->length;
}

int node_push(node_list_t *list, node_t *node) {
   node_list_t *new_tail = malloc(sizeof(node_list_t));
   if(new_tail == NULL) {
      return -1;
   }
   else {
      new_tail->head = list->head;
      new_tail->tail = list->tail;
      list->head = node;
      list->tail = new_tail;
      list->length++;
   }
   return 0;
}

node_t *node_pop(node_list_t *list) {
   if(list->head == NULL) return NULL;
   node_t *h = list->head;
   node_list_t *t = list->tail;
   list->head = t->head;
   list->tail = t->tail;
   list->length--;
   free(t);
   return h;
}

node_iterator_t *node_iterator(node_list_t *list) {
   node_iterator_t *it = malloc(sizeof(node_iterator_t));
   if(it == NULL) return NULL;
   it->position = list;
   return it;
}

const char *next_node(node_iterator_t *it) {
   // if we reached the end of the list then free resources and
   // destroy the iterator.
   if(it->position == NULL || it->position->head == NULL) {
      return NULL;
   }
   const char *next = it->position->head->name;
   it->position = it->position->tail;
   return next;
}

inline void destroy_node_iterator(node_iterator_t *it) {
   free(it);
}

void destroy_node_list(node_list_t *list) {
   while(node_pop(list) != NULL) continue;
   free(list);
}

int contains(node_list_t *list, node_t *node) {
   node_iterator_t *it = node_iterator(list);
   const char *n;
   while((n = next_node(it)) != NULL) {
      if(strcmp(n, node->name) == 0) {
         destroy_node_iterator(it);
         return 1;
      }
   }
   destroy_node_iterator(it);
   return 0;
}
