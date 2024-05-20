/*
Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Copyright 2015.  Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use, reproduce,
and distribute this software.  NEITHER THE GOVERNMENT NOR LOS ALAMOS NATIONAL
SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY
FOR THE USE OF THIS SOFTWARE.  If software is modified to produce derivative
works, such modified software should be clearly marked, so as not to confuse it
with the version available from LANL.

Additionally, redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
3. Neither the name of Los Alamos National Security, LLC, Los Alamos National
Laboratory, LANL, the U.S. Government, nor the names of its contributors may be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----
Although these files reside in a seperate repository, they fall under the MarFS copyright and license.

MarFS is released under the BSD license.

MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier:
LA-CC-15-039.

These erasure utilites make use of the Intel Intelligent Storage
Acceleration Library (Intel ISA-L), which can be found at
https://github.com/01org/isa-l and is under its own license.

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_DAL
#define DEBUG DEBUG_DAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "timer_dal"
#include "logging/logging.h"

#include "dal.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>

#define NODE_SIZE 1024 * 1024 // Buffer size for each timing data node

//   -------------    TIMER CONTEXT    -------------

// Linked list node containing timing data (Nodes should never be directly
// accessed)
typedef struct time_node
{
  char *data;
  int remaining;
  struct time_node *next;
} * node;

// Linked list containing timing data
typedef struct time_list
{
  node start;
  node end;
  pthread_mutex_t mtx;
} * list;

typedef struct timer_dal_context_struct
{
  DAL under_dal; // Underlying DAL
  int dump_fd;   // Directory to export timing data to upon close
  list verify;   // Linked lists containing the timing data of each DAL function
  list migrate;
  list del;
  list stat;
  list cleanup;
  list open;
  list set_meta;
  list get_meta;
  list put;
  list get;
  list abort;
  list close;
} * TIMER_DAL_CTXT;

typedef struct timer_block_context_struct
{
  TIMER_DAL_CTXT global_ctxt; // Global context
  BLOCK_CTXT bctxt;           // Block context to be passed to underlying DAL
  list set_meta;              // List containing timing data of each DAL data
  list get_meta;              // access function performed since the handle was
  list put;                   // opened.
  list get;
} * TIMER_BLOCK_CTXT;

//   -------------    TIMER INTERNAL FUNCTIONS    -------------

/** (INTERNAL HELPER FUNCTION)
 * Initializes and returns a timing data list
 * @return list : An empty list if it could be successfully initialized,
 * otherwise NULL.
 */
list list_init()
{
  // allocate space for a new list
  list lst = malloc(sizeof(struct time_list));
  if (lst == NULL)
  {
    return NULL;
  }

  // ensure that the list is empty
  lst->start = NULL;
  lst->end = NULL;

  // intitialize the list's lock
  if (pthread_mutex_init(&lst->mtx, NULL))
  {
    free(lst);
    return NULL;
  }
  return lst;
}

/** (INTERNAL HELPER FUNCTION)
 * Append data to the end of a timing data list
 * @param list lst : List to add data to
 * @param char *str : Data to be added
 * @return int : Zero if the data is added successfully, -1 otherwise
 */
int list_add(list lst, char *str)
{
  pthread_mutex_lock(&lst->mtx);

  // Allocate a first node if the list is empty before beginning
  if (lst->start == NULL)
  {
    lst->start = malloc(sizeof(struct time_node));
    if (lst->start == NULL)
    {
      pthread_mutex_unlock(&lst->mtx);
      return -1;
    }
    lst->start->data = malloc(sizeof(char) * NODE_SIZE);
    *lst->start->data = '\0';
    if (lst->start->data == NULL)
    {
      free(lst->start);
      pthread_mutex_unlock(&lst->mtx);
      return -1;
    }
    lst->start->remaining = NODE_SIZE;
    lst->start->next = NULL;
    lst->end = lst->start;
  }

  // Allocate a new node if the last node cannot fit the new data
  if (strlen(str) > lst->end->remaining)
  {
    lst->end->next = malloc(sizeof(struct time_node));
    if (lst->end->next == NULL)
    {
      pthread_mutex_unlock(&lst->mtx);
      return -1;
    }
    lst->end->next->data = malloc(sizeof(char) * NODE_SIZE);
    *lst->end->next->data = '\0';
    if (lst->end->next->data == NULL)
    {
      free(lst->end->next);
      pthread_mutex_unlock(&lst->mtx);
      return -1;
    }
    lst->end->next->remaining = NODE_SIZE;
    lst->end->next->next = NULL;
    lst->end = lst->end->next;
  }

  // Add new data to last node in list
  strcat(lst->end->data, str);
  lst->end->remaining -= strlen(str);
  pthread_mutex_unlock(&lst->mtx);

  return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Move the timing data from one list onto another, removing the data from the
 * source list
 * @param list dest : List to move the data to
 * @param list src : List to move the data from (NOTE: This list should
 * only every be accessed from one thread, as this function does not perform
 * locking on it)
 * @return int Zero if the data is migrated successfully, -1 otherwise
 */
int list_migrate(list dest, list src)
{

  // If there's nothing to migrate, return
  if (src->start == NULL)
  {
    return 0;
  }

  // Only acquire destination lock (source should be from a block and only
  // accessed by one thread)
  pthread_mutex_lock(&dest->mtx);

  // Append the source list to the end of the destination list
  if (dest->start == NULL)
  {
    dest->start = src->start;
  }
  else
  {
    dest->end->next = src->start;
  }
  dest->end = src->end;

  // Remove all nodes from source list
  src->start = NULL;
  src->end = NULL;

  pthread_mutex_unlock(&dest->mtx);
  return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Write the timing data from a list into a file, removing the data from the
 * list as it is written
 * @param list lst : List to export data from
 * @param int dir_fd : Directory that contains file where data will be written
 * @param char *fname : Path of file relative to dir_fd to write data to
 * @return int : Zero if the data is exported successfully, -1 otherwise
 */
int list_export(list lst, int dir_fd, char *fname)
{
  // Open the file we will be writing to
  int fd = openat(dir_fd, fname, O_CREAT | O_WRONLY | O_APPEND, 0666);
  if (fd < 0)
  {
    return -1;
  }

  pthread_mutex_lock(&lst->mtx);

  // If the list is empty, we have nothing to export
  if (lst->start == NULL)
  {
    pthread_mutex_unlock(&lst->mtx);
    return close(fd);
  }

  // Write out all data to the file, node by node
  int ret;
  do
  {
    ret = write(fd, lst->start->data, strlen(lst->start->data));
    if (ret < 0)
    {
      LOG(LOG_ERR, "failed to write timing data to %s (%s)\n", fname, strerror(errno));
      break;
    }

    node next = lst->start->next;
    free(lst->start->data);
    free(lst->start);
    lst->start = next;
  } while (lst->start != NULL);

  pthread_mutex_unlock(&lst->mtx);

  if (close(fd))
  {
    return -1;
  }

  return ret;
}

/** (INTERNAL HELPER FUNCTION)
 * Release all resorces associated with a list
 * @param list lst : List to be destroyed
 * @return int : Zero if list is successfully destroyed, -1 otherwise
 */
int list_destroy(list lst)
{
  pthread_mutex_lock(&lst->mtx);

  // Release every node, one at a time
  node n = lst->start;
  while (n)
  {
    node next = n->next;
    free(n->data);
    free(n);
    n = next;
  }

  // Release general resources associated with the list
  pthread_mutex_unlock(&lst->mtx);
  pthread_mutex_destroy(&lst->mtx);
  free(lst);
  return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Write out all timing data
 * @param TIMER_DAL_CTXT dctxt : Context containing timing data and export
 * location
 * @return int : Zero on success, the number of lists that could failed to be
 * written otherwise
 */
int dump_times(TIMER_DAL_CTXT dctxt)
{
  // Export every list, counting how many fail
  int ret = 0;
  if (list_export(dctxt->verify, dctxt->dump_fd, "verify") < 0)
  {
    LOG(LOG_ERR, "failed to export verify timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->migrate, dctxt->dump_fd, "migrate") < 0)
  {
    LOG(LOG_ERR, "failed to export migrate timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->del, dctxt->dump_fd, "del") < 0)
  {
    LOG(LOG_ERR, "failed to export del timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->stat, dctxt->dump_fd, "stat") < 0)
  {
    LOG(LOG_ERR, "failed to export stat timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->cleanup, dctxt->dump_fd, "cleanup") < 0)
  {
    LOG(LOG_ERR, "failed to export cleanup timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->open, dctxt->dump_fd, "open") < 0)
  {
    LOG(LOG_ERR, "failed to export open timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->set_meta, dctxt->dump_fd, "set_meta") < 0)
  {
    LOG(LOG_ERR, "failed to export set_meta timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->get_meta, dctxt->dump_fd, "get_meta") < 0)
  {
    LOG(LOG_ERR, "failed to export get_meta timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->put, dctxt->dump_fd, "put") < 0)
  {
    LOG(LOG_ERR, "failed to export put timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->get, dctxt->dump_fd, "get") < 0)
  {
    LOG(LOG_ERR, "failed to export get timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->abort, dctxt->dump_fd, "abort") < 0)
  {
    LOG(LOG_ERR, "failed to export abort timing data (%s)\n", strerror(errno));
    ret++;
  }
  if (list_export(dctxt->close, dctxt->dump_fd, "close") < 0)
  {
    LOG(LOG_ERR, "failed to export close timing data (%s)\n", strerror(errno));
    ret++;
  }
  return ret;
}

/** (INTERNAL HELPER FUNCTION)
 * Free a DAL context and any allocated resources asssociated with it.
 * @param TIMER_DAL_CTXT dctxt : Context to be freed
 */
void try_free_dctxt(TIMER_DAL_CTXT dctxt)
{
  // Free any lists that have already been initialized
  if (dctxt->verify)
  {
    list_destroy(dctxt->verify);
  }
  if (dctxt->migrate)
  {
    list_destroy(dctxt->migrate);
  }
  if (dctxt->del)
  {
    list_destroy(dctxt->del);
  }
  if (dctxt->stat)
  {
    list_destroy(dctxt->stat);
  }
  if (dctxt->cleanup)
  {
    list_destroy(dctxt->cleanup);
  }
  if (dctxt->open)
  {
    list_destroy(dctxt->open);
  }
  if (dctxt->set_meta)
  {
    list_destroy(dctxt->set_meta);
  }
  if (dctxt->get_meta)
  {
    list_destroy(dctxt->get_meta);
  }
  if (dctxt->put)
  {
    list_destroy(dctxt->put);
  }
  if (dctxt->get)
  {
    list_destroy(dctxt->get);
  }
  if (dctxt->abort)
  {
    list_destroy(dctxt->abort);
  }
  if (dctxt->close)
  {
    list_destroy(dctxt->close);
  }

  close(dctxt->dump_fd);
  dctxt->under_dal->cleanup(dctxt->under_dal);
  free(dctxt);
}

/** (INTERNAL HELPER FUNCTION)
 * Free a block context and any allocated resources asssociated with it.
 * @param TIMER_BLOCK_CTXT dctxt : Context to be freed
 */
void try_free_bctxt(TIMER_BLOCK_CTXT bctxt)
{
  // Free any lists that have already been initialized
  if (bctxt->set_meta)
  {
    list_destroy(bctxt->set_meta);
  }
  if (bctxt->get_meta)
  {
    list_destroy(bctxt->get_meta);
  }
  if (bctxt->put)
  {
    list_destroy(bctxt->put);
  }
  if (bctxt->get)
  {
    list_destroy(bctxt->get);
  }

  free(bctxt);
}

//   -------------    TIMER IMPLEMENTATION    -------------

int timer_verify(DAL_CTXT ctxt, int flags)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }

  TIMER_DAL_CTXT dctxt = (TIMER_DAL_CTXT)ctxt; // Should have been passed a timer context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = dctxt->under_dal->verify(dctxt->under_dal->ctxt, flags);

  // get end time
  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(dctxt->verify, tmp);

  return ret;
}

int timer_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }

  TIMER_DAL_CTXT dctxt = (TIMER_DAL_CTXT)ctxt; // Should have been passed a timer context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = dctxt->under_dal->migrate(dctxt->under_dal->ctxt, objID, src, dest, offline);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(dctxt->migrate, tmp);

  return ret;
}

int timer_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }

  TIMER_DAL_CTXT dctxt = (TIMER_DAL_CTXT)ctxt; // Should have been passed a timer context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = dctxt->under_dal->del(dctxt->under_dal->ctxt, location, objID);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(dctxt->del, tmp);

  return ret;
}

int timer_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return -1;
  }

  TIMER_DAL_CTXT dctxt = (TIMER_DAL_CTXT)ctxt; // Should have been passed a timer context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = dctxt->under_dal->stat(dctxt->under_dal->ctxt, location, objID);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(dctxt->stat, tmp);

  return ret;
}

int timer_cleanup(DAL dal)
{
  if (dal == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal!\n");
    return -1;
  }

  TIMER_DAL_CTXT dctxt = (TIMER_DAL_CTXT)dal->ctxt; // Should have been passed a DAL

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = dctxt->under_dal->cleanup(dctxt->under_dal);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(dctxt->cleanup, tmp);

  if (ret)
  {
    return ret;
  }

  dump_times(dctxt);

  list_destroy(dctxt->verify);
  list_destroy(dctxt->migrate);
  list_destroy(dctxt->del);
  list_destroy(dctxt->stat);
  list_destroy(dctxt->cleanup);
  list_destroy(dctxt->open);
  list_destroy(dctxt->set_meta);
  list_destroy(dctxt->get_meta);
  list_destroy(dctxt->put);
  list_destroy(dctxt->get);
  list_destroy(dctxt->abort);
  list_destroy(dctxt->close);

  close(dctxt->dump_fd);
  free(dctxt);
  free(dal);
  return 0;
}

BLOCK_CTXT timer_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL dal context!\n");
    return NULL;
  }

  TIMER_DAL_CTXT dctxt = (TIMER_DAL_CTXT)ctxt; // Should have been passed a timer context

  // Allocate space for a new block context
  TIMER_BLOCK_CTXT bctxt = malloc(sizeof(struct timer_block_context_struct));
  if (bctxt == NULL)
  {
    return NULL;
  }

  bctxt->global_ctxt = dctxt;

  // Initialize timing data lists
  if ((bctxt->set_meta = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize set_meta timing data list(%s)\n", strerror(errno));
    try_free_bctxt(bctxt);
    return NULL;
  }
  if ((bctxt->get_meta = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize get_meta timing data list(%s)\n", strerror(errno));
    try_free_bctxt(bctxt);
    return NULL;
  }
  if ((bctxt->put = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize put timing data list(%s)\n", strerror(errno));
    try_free_bctxt(bctxt);
    return NULL;
  }
  if ((bctxt->get = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize get timing data list(%s)\n", strerror(errno));
    try_free_bctxt(bctxt);
    return NULL;
  }

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  bctxt->bctxt = dctxt->under_dal->open(dctxt->under_dal->ctxt, mode, location, objID);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(dctxt->open, tmp);

  if (bctxt->bctxt == NULL)
  {
    free(bctxt);
    return NULL;
  }

  return bctxt;
}

int timer_set_meta(BLOCK_CTXT ctxt, const meta_info* source)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }

  TIMER_BLOCK_CTXT bctxt = (TIMER_BLOCK_CTXT)ctxt; // Should have been passed a timer context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = bctxt->global_ctxt->under_dal->set_meta(bctxt->bctxt, source);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(bctxt->set_meta, tmp);

  return ret;
}

int timer_get_meta(BLOCK_CTXT ctxt, meta_info* target)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }

  TIMER_BLOCK_CTXT bctxt = (TIMER_BLOCK_CTXT)ctxt; // Should have been passed a block context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  ssize_t ret = bctxt->global_ctxt->under_dal->get_meta(bctxt->bctxt, target);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(bctxt->get_meta, tmp);

  return ret;
}

int timer_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }

  TIMER_BLOCK_CTXT bctxt = (TIMER_BLOCK_CTXT)ctxt; // Should have been passed a block context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = bctxt->global_ctxt->under_dal->put(bctxt->bctxt, buf, size);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(bctxt->put, tmp);

  return ret;
}

ssize_t timer_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }

  TIMER_BLOCK_CTXT bctxt = (TIMER_BLOCK_CTXT)ctxt; // Should have been passed a block context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  ssize_t ret = bctxt->global_ctxt->under_dal->get(bctxt->bctxt, buf, size, offset);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(bctxt->get, tmp);

  return ret;
}

int timer_abort(BLOCK_CTXT ctxt)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }

  TIMER_BLOCK_CTXT bctxt = (TIMER_BLOCK_CTXT)ctxt; // Should have been passed a block context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = bctxt->global_ctxt->under_dal->abort(bctxt->bctxt);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(bctxt->global_ctxt->abort, tmp);

  if (ret)
  {
    return ret;
  }

  list_migrate(bctxt->global_ctxt->set_meta, bctxt->set_meta);
  list_migrate(bctxt->global_ctxt->get_meta, bctxt->get_meta);
  list_migrate(bctxt->global_ctxt->put, bctxt->put);
  list_migrate(bctxt->global_ctxt->get, bctxt->get);

  try_free_bctxt(bctxt);
  return 0;
}

int timer_close(BLOCK_CTXT ctxt)
{
  if (ctxt == NULL)
  {
    LOG(LOG_ERR, "received a NULL block context!\n");
    return -1;
  }

  TIMER_BLOCK_CTXT bctxt = (TIMER_BLOCK_CTXT)ctxt; // Should have been passed a block context

  // get start time
  struct timeval beg;
  gettimeofday(&beg, NULL);

  int ret = bctxt->global_ctxt->under_dal->close(bctxt->bctxt);

  // get end time
  struct timeval end;
  gettimeofday(&end, NULL);

  // add interval to list
  char tmp[20];
  sprintf(tmp, "%.6f\n", (end.tv_sec - beg.tv_sec) + (end.tv_usec + beg.tv_usec) * 1e-6);
  list_add(bctxt->global_ctxt->close, tmp);

  if (ret)
  {
    return ret;
  }

  list_migrate(bctxt->global_ctxt->set_meta, bctxt->set_meta);
  list_migrate(bctxt->global_ctxt->get_meta, bctxt->get_meta);
  list_migrate(bctxt->global_ctxt->put, bctxt->put);
  list_migrate(bctxt->global_ctxt->get, bctxt->get);

  try_free_bctxt(bctxt);
  return 0;
}

//   -------------    TIMER INITIALIZATION    -------------

DAL timer_dal_init(xmlNode *root, DAL_location max_loc)
{
  // allocate space for our context struct
  TIMER_DAL_CTXT dctxt = malloc(sizeof(struct timer_dal_context_struct));
  if (dctxt == NULL)
  {
    return NULL;
  }

  dctxt->under_dal = NULL;
  dctxt->dump_fd = -1;

  // parse configuration items from XML tree
  while (root != NULL)
  {
    if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "DAL", 4) == 0)
    {
      dctxt->under_dal = init_dal(root, max_loc);
    }
    else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "dump_path", 8) == 0)
    {
      mkdir((char *)root->children->content, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
      dctxt->dump_fd = open((char *)root->children->content, O_DIRECTORY);
    }
    root = root->next;
  }

  if (dctxt->under_dal == NULL)
  {
    free(dctxt);
    return NULL;
  }
  if (dctxt->dump_fd == -1)
  {
    LOG(LOG_ERR, "failed to parse dump path from config\n");
    dctxt->under_dal->cleanup(dctxt->under_dal);
    free(dctxt);
    return NULL;
  }

  // Initialize timing data lists
  if ((dctxt->verify = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize verify timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->migrate = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize migrate timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->del = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize dal timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->stat = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize stat timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->cleanup = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize cleanup timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->open = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize open timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->set_meta = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize set_meta timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->get_meta = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize get_meta timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->put = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize put timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->get = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize get timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->abort = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize abort timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }
  if ((dctxt->close = list_init()) == NULL)
  {
    LOG(LOG_ERR, "failed to initialize close timing data list(%s)\n", strerror(errno));
    try_free_dctxt(dctxt);
    return NULL;
  }

  // allocate and populate a new DAL structure
  DAL tdal = malloc(sizeof(struct DAL_struct));
  if (tdal == NULL)
  {
    LOG(LOG_ERR, "failed to allocate space for a DAL_struct\n");
    try_free_dctxt(dctxt);
    return NULL;
  }
  tdal->name = "timer";
  tdal->ctxt = (DAL_CTXT)dctxt;
  tdal->io_size = dctxt->under_dal->io_size;
  tdal->verify = timer_verify;
  tdal->migrate = timer_migrate;
  tdal->open = timer_open;
  tdal->set_meta = timer_set_meta;
  tdal->get_meta = timer_get_meta;
  tdal->put = timer_put;
  tdal->get = timer_get;
  tdal->abort = timer_abort;
  tdal->close = timer_close;
  tdal->del = timer_del;
  tdal->stat = timer_stat;
  tdal->cleanup = timer_cleanup;
  return tdal;
}
