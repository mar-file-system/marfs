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

#include "task_queue.h"
#include <errno.h>
#include <string.h>

/**
 * Allocate space for, and return a pointer to, a new mustang_task struct on 
 * the heap. Initialize the task with all necessary state (MarFS config, MarFS
 * position, hashtable ref, hashtable lock, task queue ref, and function 
 * pointer indicating what routine to execute).
 *
 * Returns: valid pointer to mustang_task struct on success, or NULL on 
 * failure.
 */
mustang_task* task_init(marfs_config* task_config, marfs_position* task_position, char * task_path, void* task_arg, hashtable* task_ht, pthread_mutex_t* task_ht_lock, task_queue* queue_ref, 
        void (*traversal_routine)(marfs_config*, marfs_position*, char*, void*, hashtable*, pthread_mutex_t*, task_queue*)) {
    mustang_task* new_task = (mustang_task*) calloc(1, sizeof(mustang_task));

    if (new_task == NULL) {
        return NULL;
    }

    // Initialize new task state like in a constructor based on arguments
    new_task->config = task_config;
    new_task->position = task_position;
    new_task->usrpath = task_path;
    new_task->taskarg = task_arg;
    new_task->ht = task_ht;
    new_task->ht_lock = task_ht_lock;
    new_task->queue_ptr = queue_ref;
    new_task->task_func = traversal_routine;
    new_task->prev = NULL;
    new_task->next = NULL;

    return new_task;
}

/**
 * Allocate space for, and return a pointer to, a new task_queue struct on the 
 * heap according to a specified capacity.
 *
 * Returns: valid pointer to task_queue struct on success, or NULL on failure.
 *
 * NOTE: this function may return NULL under any of the following conditions:
 * - Zero argument for capacity (errno set to EINVAL)
 * - Failure to calloc() queue space 
 * - Failure to calloc() queue mutex
 * - pthread_mutex_init() failure for queue mutex
 * - Failure to calloc() space for at least one queue condition variable
 * - pthread_cond_init() failure for at least one queue condition variable
 */
task_queue* task_queue_init(size_t new_capacity) {
    if (new_capacity == 0) {
        errno = EINVAL;
        return NULL;
    }

    task_queue* new_queue = (task_queue*) calloc(1, sizeof(task_queue));

    if (new_queue == NULL) {
        return NULL;
    }
    
    new_queue->capacity = new_capacity;
    new_queue->size = 0;
    new_queue->todos = 0;
    new_queue->head = NULL;
    new_queue->tail = NULL;

    // Heap-allocate sync primitives (lock, condition variables) for easier 
    // memory sharing since resources maintained at the process level instead
    // of in an individual thread's stack
    pthread_mutex_t* new_queue_lock = (pthread_mutex_t*) calloc(1, sizeof(pthread_mutex_t));

    if (new_queue_lock == NULL) {
        free(new_queue);
        return NULL;
    }

    // If pthread_mutex_init() fails, clean up and exit
    if (pthread_mutex_init(new_queue_lock, NULL)) {
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    new_queue->lock = new_queue_lock;

    pthread_cond_t* new_tasks_cv = (pthread_cond_t*) calloc(1, sizeof(pthread_cond_t));

    if (new_tasks_cv == NULL) {
        pthread_mutex_destroy(new_queue_lock);
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    // If pthread_cond_init() fails, clean up and exit
    if (pthread_cond_init(new_tasks_cv, NULL)) {
        free(new_tasks_cv);
        pthread_mutex_destroy(new_queue_lock);
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    new_queue->task_available = new_tasks_cv;

    pthread_cond_t* new_space_cv = (pthread_cond_t*) calloc(1, sizeof(pthread_cond_t));

    if (new_space_cv == NULL) { 
        pthread_cond_destroy(new_tasks_cv);
        free(new_tasks_cv);
        pthread_mutex_destroy(new_queue_lock);
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    // If pthread_cond_init() fails, clean up and exit
    if (pthread_cond_init(new_space_cv, NULL)) {
        free(new_space_cv);
        pthread_cond_destroy(new_tasks_cv);
        free(new_tasks_cv);
        pthread_mutex_destroy(new_queue_lock);
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    new_queue->space_available = new_space_cv;

    pthread_cond_t* new_manager_cv = (pthread_cond_t*) calloc(1, sizeof(pthread_cond_t));

    if (new_manager_cv == NULL) {
        pthread_cond_destroy(new_space_cv);
        free(new_space_cv);
        pthread_cond_destroy(new_tasks_cv);
        free(new_tasks_cv);
        pthread_mutex_destroy(new_queue_lock);
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    // If pthread_cond_init() fails, clean up and exit
    if (pthread_cond_init(new_manager_cv, NULL)) {
        free(new_manager_cv);
        pthread_cond_destroy(new_space_cv);
        free(new_space_cv);
        pthread_cond_destroy(new_tasks_cv);
        free(new_tasks_cv);
        pthread_mutex_destroy(new_queue_lock);
        free(new_queue_lock);
        free(new_queue);
        return NULL;
    }

    new_queue->manager_cv = new_manager_cv;

    return new_queue;
}

/**
 * Atomically enqueue a new task `new_task` to the given task queue `queue`, 
 * adjusting internal queue state as necessary to reflect changes (new size, 
 * new head/tail nodes in queue, etc.).
 *
 * Returns: 0 on success, or -1 on failure with errno set to EINVAL (queue
 * is NULL).
 *
 * NOTE: this function wraps a pthread_cond_wait() loop on the queue's 
 * `space_available` cv field, so callers do not need to (and, for application
 * efficiency/to minimize lock contention, should not) separately lock the 
 * queue's lock and wait on the `space_available` cv.
 */
int task_enqueue(task_queue* queue, mustang_task* new_task) {
    if ((queue == NULL) || (new_task == NULL)) {
        errno = EINVAL;
        return -1;
    }

    pthread_mutex_lock(queue->lock);

    // Sleep until the queue's size (number of currently linked nodes) is lower 
    // than its capacity.
    while (queue->size >= queue->capacity) {
        pthread_cond_wait(queue->space_available, queue->lock);
    }

    // If the queue has a valid tail, ensure that the new task is linked to that tail.
    if (queue->tail != NULL) {
        queue->tail->next = new_task;
        new_task->prev = queue->tail;
    }

    // Since the queue is a FIFO structure, the new task will begin at the tail
    // of the queue "unconditionally"
    queue->tail = new_task;
    queue->size += 1;
    queue->todos += 1;

    // Also update the head if the queue was previously empty
    if (queue->size == 1) {
        queue->head = new_task;
    }

    // Wake up other threads to dequeue the new task if they are waiting for a
    // new task to become available.
    pthread_cond_broadcast(queue->task_available);
    pthread_mutex_unlock(queue->lock);

    return 0;
}

/**
 * Atomically dequeue (unlink) and return the `mustang_task` struct at the head
 * of the task queue `queue`, adjusting internal queue state as necessary to 
 * reflect changes (new size, head/tail nodes, etc.).
 *
 * Returns: valid pointer to mustang_task struct on success, or NULL on failure
 * with errno set (EINVAL for queue == NULL).
 *
 * NOTE: in a similar fashion to task_enqueue, this function wraps a 
 * pthread_cond_wait() loop on a queue condition variable (the task_available 
 * cv in this case) before returning. Callers do not need to separately wait, 
 * and, to keep lock contention to a minimum, should not.
 */
mustang_task* task_dequeue(task_queue* queue) {
    if (queue == NULL) {
        errno = EINVAL;
        return NULL;
    }

    mustang_task* retrieved_task;

    pthread_mutex_lock(queue->lock);

    // Wait until there is at least one task in the queue
    while (queue->size == 0) {
        pthread_cond_wait(queue->task_available, queue->lock);
    }

    // Dequeue from the head to match standard FIFO queue interface behavior 
    // and to provide a constant-time retrieval operation.
    retrieved_task = queue->head;
    queue->head = queue->head->next;
    queue->size -= 1;

    // If applicable, unlink new head node from the recently dequeued node.
    if (queue->head != NULL) {
        queue->head->prev = NULL;
    }

    // Invalidate tail pointer if no nodes exist in the queue. This works 
    // because, for any nonzero queue size, there will always be a valid head
    // node and tail node (the only special case being size == 1, in which case
    // head and tail point to the same node)
    if (queue->size == 0) {
        queue->tail = NULL;
    }

    retrieved_task->next = NULL;

    // Wake up other threads to enqueue tasks if they are waiting on space.
    pthread_cond_broadcast(queue->space_available);
    pthread_mutex_unlock(queue->lock);

    return retrieved_task;
}

/**
 * Destroy the given task_queue struct and free the memory associated with it.
 */
int task_queue_destroy(task_queue* queue) {
    // Perform a final atomic check to see if other threads are currently 
    // using the queue
    pthread_mutex_lock(queue->lock);
    if (queue->size > 0) {
        pthread_mutex_unlock(queue->lock);
        errno = EBUSY;
        return -1;
    }
    pthread_mutex_unlock(queue->lock);

    // Destroy the sync primitives and free their heap memory (two steps are 
    // required since pthread_*_init() does not itself allocate heap memory).
    pthread_mutex_destroy(queue->lock);
    free(queue->lock);

    pthread_cond_destroy(queue->task_available);
    free(queue->task_available);
    
    pthread_cond_destroy(queue->space_available);
    free(queue->space_available);

    pthread_cond_destroy(queue->manager_cv);
    free(queue->manager_cv);

    free(queue);
    return 0;
}
