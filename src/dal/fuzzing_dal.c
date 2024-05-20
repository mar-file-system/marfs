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
#define LOG_PREFIX "fuzzing_dal"
#include "logging/logging.h"

#include "dal.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#define FZ_LN 20 // Maximum number of blocks that can be fuzzed

typedef struct fuzzing_dal_context_struct
{
   DAL under_dal; // Underlying DAL
   int *verify;   // fuzzing behavior of each function. If a list for
   int *migrate;  // an operation includes -1, then the operation
   int *del;      // always fails. A positive number means always
   int *stat;     // fails for the block that corresponds to the
   int *cleanup;  // number - 1. A null pointer means do not fuzz
   int *open;     // operation.
   int *set_meta;
   int *get_meta;
   int *put;
   int *get;
   int *abort;
   int *close;
} * FUZZING_DAL_CTXT;

typedef struct fuzzing_block_context_struct
{
   FUZZING_DAL_CTXT global_ctxt; // Global context
   DAL_location loc;             // Location of block
   BLOCK_CTXT bctxt;             // Block context to be passed to underlying dal
} * FUZZING_BLOCK_CTXT;

/**
 * Checks if block needs to be fuzzed.
 * @param int* fuzz : List of block numbers to fuzz (NOTE: Each entry is 1 + the block number to fuzz)
 * @param int block : Block number to check
 * @return int : Zero if operation can proceed, and -1 if block should be fuzzed
 */
int check_fuzz(int *fuzz, int block)
{
   int i = 0;
   // Empty list means nothing to fuzz
   if (fuzz == NULL)
   {
      return 0;
   }
   // Check valid (positive) entries in list
   while (i < FZ_LN)
   {
      // Fuzz this block
      if (fuzz[i] < 0 || fuzz[i] == block + 1)
      {
         errno = -1;
         return -1;
      }
      // Run out of valid entries
      else if (fuzz[i] == 0)
      {
         return 0;
      }
      i++;
   }
   return 0;
}
/**
 * Form list of block numbers to fuzz from string
 * @param char* parse : String containing block numbers to fuzz
 * @param int** fuzz : Destination for list of block numbers
 * @return int : Zero on success and -1 on failure
 */
int parse_fuzz(char *parse, int **fuzz)
{
   // Create new list of block numbers and check for success
   int *blocks = calloc(FZ_LN, sizeof(int));
   if (blocks == NULL)
   {
      return -1;
   }

   int ind = 0;
   int tot = 0;
   int allocate = 0;
   int valid = 0;
   // Parse through string, removing whitespace and extra commas
   while (*parse != '\0')
   {
      switch (*parse)
      {
      // List will fuzz every block
      case '-':
         blocks[0] = -1;
         *fuzz = blocks;
         return 0;
         break;

      case '0' ... '9':
         tot = (tot * 10) + (*parse - '0');
         allocate++;
         valid++;
         break;

      // Append new value to list
      case ',':
         if (valid)
         {
            blocks[ind++] = tot + 1;
            tot = 0;
         }
         valid = 0;
         break;

      case ' ':
         break;

      // Invalid character
      default:
         free(blocks);
         return -1;
      }
      // String contained too many blocks
      if (ind == FZ_LN)
      {
         free(blocks);
         return -1;
      }
      parse++;
   }
   // Append last block number (if not already)
   if (valid)
   {
      blocks[ind] = tot + 1;
   }

   // Point destination to list
   if (allocate)
   {
      *fuzz = blocks;
      return 0;
   }
   free(blocks);
   return 0;
}

/**
 * Free pointer if allocated
 * @param int* ptr : Pointer to be freed
 */
void try_free(int *ptr)
{
   if (ptr != NULL)
   {
      free(ptr);
   }
}

/**
 * Free FUZZING_DAL_CTXT and any memory pointed to by its fields
 * @param FUZZING_DAL_CTXT dctxt : Context to be freed
 */
void free_fuzz(FUZZING_DAL_CTXT dctxt)
{
   try_free(dctxt->verify);
   try_free(dctxt->migrate);
   try_free(dctxt->del);
   try_free(dctxt->stat);
   try_free(dctxt->cleanup);
   try_free(dctxt->open);
   try_free(dctxt->set_meta);
   try_free(dctxt->get_meta);
   try_free(dctxt->put);
   try_free(dctxt->get);
   try_free(dctxt->abort);
   try_free(dctxt->close);
   free(dctxt);
}

//   -------------    FUZZING IMPLEMENTATION    -------------

int fuzzing_verify(DAL_CTXT ctxt, int flags)
{
   FUZZING_DAL_CTXT dctxt = (FUZZING_DAL_CTXT)ctxt;

   if (check_fuzz(dctxt->verify, -1))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing verify\n");
      return -2;
   }

   return dctxt->under_dal->verify(dctxt->under_dal->ctxt, flags);
}

int fuzzing_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
{
   FUZZING_DAL_CTXT dctxt = (FUZZING_DAL_CTXT)ctxt;

   if (check_fuzz(dctxt->migrate, src.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing migrate source block %d\n", src.block);
      return -1;
   }
   if (check_fuzz(dctxt->migrate, dest.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing migrate destination block %d\n", dest.block);
      return -1;
   }

   return dctxt->under_dal->migrate(dctxt->under_dal->ctxt, objID, src, dest, offline);
}

int fuzzing_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   FUZZING_DAL_CTXT dctxt = (FUZZING_DAL_CTXT)ctxt;

   if (check_fuzz(dctxt->del, location.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing del block %d\n", location.block);
      return -2;
   }

   return dctxt->under_dal->del(dctxt->under_dal->ctxt, location, objID);
}

int fuzzing_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   FUZZING_DAL_CTXT dctxt = (FUZZING_DAL_CTXT)ctxt;

   if (check_fuzz(dctxt->stat, location.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing stat block %d\n", location.block);
      return -2;
   }

   return dctxt->under_dal->stat(dctxt->under_dal->ctxt, location, objID);
}

int fuzzing_cleanup(DAL dal)
{
   FUZZING_DAL_CTXT dctxt = (FUZZING_DAL_CTXT)dal->ctxt;

   if (check_fuzz(dctxt->cleanup, -1))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing cleanup\n");
      return -2;
   }

   int res = dctxt->under_dal->cleanup(dctxt->under_dal);
   free_fuzz(dctxt);

   free(dal);
   return res;
}

BLOCK_CTXT fuzzing_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
   FUZZING_DAL_CTXT dctxt = (FUZZING_DAL_CTXT)ctxt;

   if (check_fuzz(dctxt->open, location.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing open block %d\n", location.block);
      return NULL;
   }

   FUZZING_BLOCK_CTXT bctxt = malloc(sizeof(struct fuzzing_block_context_struct));
   if (bctxt == NULL)
   {
      return NULL;
   }

   bctxt->global_ctxt = dctxt;
   if (memcpy(&(bctxt->loc), &location, sizeof(struct DAL_location_struct)) != &(bctxt->loc))
   {
      free(bctxt);
      return NULL;
   }
   bctxt->bctxt = dctxt->under_dal->open(dctxt->under_dal->ctxt, mode, location, objID);
   if (bctxt->bctxt == NULL)
   {
      free(bctxt);
      return NULL;
   }

   return bctxt;
}

int fuzzing_set_meta(BLOCK_CTXT ctxt, const meta_info* source)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }

   FUZZING_BLOCK_CTXT bctxt = (FUZZING_BLOCK_CTXT)ctxt;

   if (check_fuzz(bctxt->global_ctxt->set_meta, bctxt->loc.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing set_meta block %d\n", bctxt->loc.block);
      return -2;
   }

   return bctxt->global_ctxt->under_dal->set_meta(bctxt->bctxt, source);
}

int fuzzing_get_meta(BLOCK_CTXT ctxt, meta_info* target)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }

   FUZZING_BLOCK_CTXT bctxt = (FUZZING_BLOCK_CTXT)ctxt;

   if (check_fuzz(bctxt->global_ctxt->get_meta, bctxt->loc.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing get_meta block %d\n", bctxt->loc.block);
      return -2;
   }

   return bctxt->global_ctxt->under_dal->get_meta(bctxt->bctxt, target);
}

int fuzzing_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }

   FUZZING_BLOCK_CTXT bctxt = (FUZZING_BLOCK_CTXT)ctxt;

   if (check_fuzz(bctxt->global_ctxt->put, bctxt->loc.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing put block %d\n", bctxt->loc.block);
      return -2;
   }

   return bctxt->global_ctxt->under_dal->put(bctxt->bctxt, buf, size);
}

ssize_t fuzzing_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }

   FUZZING_BLOCK_CTXT bctxt = (FUZZING_BLOCK_CTXT)ctxt;

   if (check_fuzz(bctxt->global_ctxt->get, bctxt->loc.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing get block %d\n", bctxt->loc.block);
      return -2;
   }

   return bctxt->global_ctxt->under_dal->get(bctxt->bctxt, buf, size, offset);
}

int fuzzing_abort(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }

   FUZZING_BLOCK_CTXT bctxt = (FUZZING_BLOCK_CTXT)ctxt;

   if (check_fuzz(bctxt->global_ctxt->abort, bctxt->loc.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing abort block %d\n", bctxt->loc.block);
      return -2;
   }

   int res = bctxt->global_ctxt->under_dal->abort(bctxt->bctxt);
   if (res)
   {
      return res;
   }

   free(bctxt);
   return 0;
}

int fuzzing_close(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }

   FUZZING_BLOCK_CTXT bctxt = (FUZZING_BLOCK_CTXT)ctxt;

   if (check_fuzz(bctxt->global_ctxt->close, bctxt->loc.block))
   {
      LOG(LOG_ERR, "Fuzzing DAL: fuzzing close block %d\n", bctxt->loc.block);
      return -2;
   }

   int res = bctxt->global_ctxt->under_dal->close(bctxt->bctxt);
   if (res)
   {
      return res;
   }

   free(bctxt);
   return 0;
}

//   -------------    FUZZING INITIALIZATION    -------------

DAL fuzzing_dal_init(xmlNode *root, DAL_location max_loc)
{
   // allocate space for our context struct
   FUZZING_DAL_CTXT dctxt = malloc(sizeof(struct fuzzing_dal_context_struct));
   if (dctxt == NULL)
   {
      return NULL;
   }
   dctxt->verify = NULL;
   dctxt->migrate = NULL;
   dctxt->del = NULL;
   dctxt->stat = NULL;
   dctxt->cleanup = NULL;
   dctxt->open = NULL;
   dctxt->set_meta = NULL;
   dctxt->get_meta = NULL;
   dctxt->put = NULL;
   dctxt->get = NULL;
   dctxt->abort = NULL;
   dctxt->close = NULL;

   // find the fuzzing data and the sub-DAL definition
   xmlNode* fnode = NULL;
   xmlNode* dnode = NULL;
   for ( ; root != NULL; root = root->next ) {
      if ( root->type == XML_ELEMENT_NODE ) {
         if ( strncmp((char *)root->name, "fuzzing", 8) == 0 ) {
            if ( fnode ) {
               LOG( LOG_ERR, "Detected duplicate fuzzing node definition\n" );
               break;
            }
            fnode = root;
         }
         if ( strncmp((char *)root->name, "DAL", 4) == 0 ) {
            if ( dnode ) {
               LOG( LOG_ERR, "Detected duplicate sub-DAL node definition\n" );
               break;
            }
            dnode = root;
         }
      }
   }
   if (root) // catch 'break' case from above
   {
      free(dctxt);
      errno = EINVAL;
      return NULL;
   }
   if ( fnode == NULL  ||  dnode == NULL ) {
      LOG( LOG_ERR, "missing 'fuzzing' defintions and/or sub-DAL node\n" );
      free(dctxt);
      errno = EINVAL;
      return NULL;
   }

   // initialize underlying DAL
   dctxt->under_dal = init_dal(dnode, max_loc);
   if (dctxt->under_dal == NULL)
   {
      LOG( LOG_ERR, "Failed to initialize sub-DAL\n" );
      free(dctxt);
      errno = EINVAL;
      return NULL;
   }

   // parse fuzzing data to context struct
   xmlNode *child = fnode->children;
   while (child != NULL)
   {
      if (child->children == NULL || child->children->type != XML_TEXT_NODE)
      {
         LOG(LOG_ERR, "the \"%s\" node is expected to contain a fuzzing state\n", (char *)child->name);
      }
      else if (strncmp((char *)child->name, "verify", 7) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->verify))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "migrate", 8) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->migrate))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "del", 4) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->del))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "stat", 5) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->stat))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "cleanup", 8) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->cleanup))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "open", 5) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->open))
         {
            free_fuzz(dctxt);
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "set_meta", 9) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->set_meta))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "get_meta", 9) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->get_meta))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "put", 4) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->put))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "get", 4) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->get))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "abort", 6) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->abort))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "close", 6) == 0)
      {
         if (parse_fuzz((char *)child->children->content, &dctxt->close))
         {
            free_fuzz(dctxt);
            errno = EINVAL;
            return NULL;
         }
      }
      else if (strncmp((char *)child->name, "init", 5) == 0)
      {
         free_fuzz(dctxt);
         errno = -1;
         return NULL;
      }
      else
      {
         LOG(LOG_ERR, "the \"%s\" node is expected to be named after a method of the dal interface\n", (char *)child->name);
         free_fuzz(dctxt);
         errno = EINVAL;
         return NULL;
      }
      child = child->next;
   }
   // allocate and populate a new DAL structure
   DAL fdal = malloc(sizeof(struct DAL_struct));
   if (fdal == NULL)
   {
      LOG(LOG_ERR, "failed to allocate space for a DAL_struct\n");
      free_fuzz(dctxt);
      return NULL;
   }
   fdal->name = dctxt->under_dal->name;
   fdal->ctxt = (DAL_CTXT)dctxt;
   fdal->io_size = dctxt->under_dal->io_size;
   fdal->verify = fuzzing_verify;
   fdal->migrate = fuzzing_migrate;
   fdal->open = fuzzing_open;
   fdal->set_meta = fuzzing_set_meta;
   fdal->get_meta = fuzzing_get_meta;
   fdal->put = fuzzing_put;
   fdal->get = fuzzing_get;
   fdal->abort = fuzzing_abort;
   fdal->close = fuzzing_close;
   fdal->del = fuzzing_del;
   fdal->stat = fuzzing_stat;
   fdal->cleanup = fuzzing_cleanup;
   return fdal;
}
