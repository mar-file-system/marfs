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
#define LOG_PREFIX "noop_dal"
#include "logging/logging.h"

#include "dal.h"
#include "general_include/crc.c"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <pwd.h>
#include <unistd.h>
#include <stdint.h>

//   -------------    NO-OP DEFINITIONS    -------------

#define IO_SIZE 1048576 // Preferred I/O Size

static uint32_t crc32_ieee_base(uint32_t seed, uint8_t * buf, uint64_t len); // forward decl of 'base' crc32 func

//   -------------    NO-OP CONTEXT    -------------

typedef struct noop_dal_context_struct
{
   meta_info   minfo;  // meta info values to be returned to the caller
   uint32_t full_crc;  // cached CRC value associated with each 'complete' I/O
   uint32_t tail_crc;  // cached CRC value associated with a trailing 'partial' I/O ( if any )
   size_t  tail_size;  // size of any trailing 'partial' I/O ( if any )
} * NOOP_DAL_CTXT;

typedef struct noop_block_context_struct
{
   NOOP_DAL_CTXT dctxt; // Global DAL context
   DAL_MODE       mode; // Mode of this block ctxt
} * NOOP_BLOCK_CTXT;

//   -------------    NO-OP INTERNAL FUNCTIONS    -------------


/**
 * (INTERNAL HELPER FUNC)
 * Parse the content of an xmlNode to populate an int value
 * @param int* target : Reference to the value to populate
 * @param xmlNode* node : Node to be parsed
 * @return int : Zero on success, -1 on error
 */
int parse_int_node( int* target, xmlNode* node ) {
   // check for an included value
   if ( node->children != NULL  &&
        node->children->type == XML_TEXT_NODE  &&
        node->children->content != NULL ) {
      char* valuestr = (char*)node->children->content;
      char* endptr = NULL;
      unsigned long long parsevalue = strtoull( valuestr, &(endptr), 10 );
      // check for any trailing unit specification
      if ( *endptr != '\0' ) {
         LOG( LOG_ERR, "encountered unrecognized trailing character in \"%s\" value: \"%c\"", (char*)node->name, *endptr );
         return -1;
      }
      if ( parsevalue >= INT_MAX ) {  // check for possible overflow
         LOG( LOG_ERR, "specified \"%s\" value is too large to store: \"%s\"\n", (char*)node->name, valuestr );
         return -1;
      }
      // actually store the value
      *target = parsevalue;
      return 0;
   }
   LOG( LOG_ERR, "failed to identify a value string within the \"%s\" definition\n", (char*)node->name );
   return -1;
}

/**
 * (INTERNAL HELPER FUNC)
 * Parse the content of an xmlNode to populate a size value
 * @param size_t* target : Reference to the value to populate
 * @param xmlNode* node : Node to be parsed
 * @return int : Zero on success, -1 on error
 */
int parse_size_node( ssize_t* target, xmlNode* node ) {
   // check for unexpected node format
   if ( node->children == NULL  ||  node->children->type != XML_TEXT_NODE ) {
      LOG( LOG_ERR, "unexpected format of size node: \"%s\"\n", (char*)node->name );
      return -1;
   }
   // check for an included value
   if ( node->children->content != NULL ) {
      char* valuestr = (char*)node->children->content;
      size_t unitmult = 1;
      char* endptr = NULL;
      unsigned long long parsevalue = strtoull( valuestr, &(endptr), 10 );
      // check for any trailing unit specification
      if ( *endptr != '\0' ) {
         if ( *endptr == 'K' ) { unitmult = 1024ULL; }
         else if ( *endptr == 'M' ) { unitmult = 1048576ULL; }
         else if ( *endptr == 'G' ) { unitmult = 1073741824ULL; }
         else if ( *endptr == 'T' ) { unitmult = 1099511627776ULL; }
         else if ( *endptr == 'P' ) { unitmult = 1125899906842624ULL; }
         else {
            LOG( LOG_ERR, "encountered unrecognized character in \"%s\" value: \"%c\"", (char*)node->name, *endptr );
            return -1;
         }
         // check for unacceptable trailing characters
         endptr++;
         if ( *endptr != '\0' ) {
            LOG( LOG_ERR, "encountered unrecognized trailing character in \"%s\" value: \"%c\"", (char*)node->name, *endptr );
            return -1;
         }
      }
      if ( (parsevalue * unitmult) >= SSIZE_MAX ) {  // check for possible overflow
         LOG( LOG_ERR, "specified \"%s\" value is too large to store: \"%s\"\n", (char*)node->name, valuestr );
         return -1;
      }
      // actually store the value
      LOG( LOG_INFO, "detected value of %llu with unit of %zu for \"%s\" node\n", parsevalue, unitmult, (char*)node->name );
      *target = (parsevalue * unitmult);
      return 0;
   }
   // allow empty string to indicate zero value
   *target = 0;
   return 0;
}


//   -------------    NO-OP IMPLEMENTATION    -------------

int noop_verify(DAL_CTXT ctxt, int flags)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   // do nothing and assume success
   return 0;
}

int noop_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   // do nothing and assume success
   return 0;
}

int noop_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   // do nothing and assume success
   return 0;
}

int noop_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   // do nothing and assume success
   return 0;
}

int noop_cleanup(DAL dal)
{
   if (dal == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal!\n");
      return -1;
   }
   NOOP_DAL_CTXT dctxt = (NOOP_DAL_CTXT)dal->ctxt; // Should have been passed a DAL context
   // Free DAL and its context state
   free(dctxt);
   free(dal);
   return 0;
}

BLOCK_CTXT noop_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return NULL;
   }
   NOOP_DAL_CTXT dctxt = (NOOP_DAL_CTXT)ctxt; // Should have been passed a DAL context
   // attempting to read from a non-cached block is an error
   NOOP_BLOCK_CTXT bctxt = malloc(sizeof(struct noop_block_context_struct));
   if (bctxt == NULL)
   {
      LOG( LOG_ERR, "failed to allocate a new block ctxt\n" );
      return NULL;
   }
   // populate values and global ctxt reference
   bctxt->dctxt = dctxt;
   bctxt->mode = mode;
   return bctxt;
}

int noop_set_meta(BLOCK_CTXT ctxt, const meta_info* source)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      errno = EINVAL;
      return -1;
   }
   NOOP_BLOCK_CTXT bctxt = (NOOP_BLOCK_CTXT)ctxt; // Should have been passed a block context
   // validate mode
   if ( bctxt->mode != DAL_WRITE  &&  bctxt->mode != DAL_REBUILD ) {
      LOG( LOG_ERR, "received block handle has inappropriate mode\n" );
      errno = EINVAL;
      return -1;
   }
   // do nothing and assume success
   return 0;
}

int noop_get_meta(BLOCK_CTXT ctxt, meta_info* dest )
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   NOOP_BLOCK_CTXT bctxt = (NOOP_BLOCK_CTXT)ctxt; // Should have been passed a block context
   // validate mode
   if ( bctxt->mode != DAL_READ  &&  bctxt->mode != DAL_METAREAD ) {
      LOG( LOG_ERR, "received block handle has inappropriate mode\n" );
      errno = EINVAL;
      return -1;
   }
   // Return cached metadata
   cpy_minfo(dest, &(bctxt->dctxt->minfo));
   dest->crcsum = bctxt->dctxt->minfo.crcsum; // manually copy crcsum, as it is excluded from the above call
   return 0;
}

int noop_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   NOOP_BLOCK_CTXT bctxt = (NOOP_BLOCK_CTXT)ctxt; // Should have been passed a block context
   // validate mode
   if ( bctxt->mode != DAL_WRITE  &&  bctxt->mode != DAL_REBUILD ) {
      LOG( LOG_ERR, "received block handle has inappropriate mode\n" );
      errno = EINVAL;
      return -1;
   }
   // do nothing and assume success
   return 0;
}

ssize_t noop_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   NOOP_BLOCK_CTXT bctxt = (NOOP_BLOCK_CTXT)ctxt; // Should have been passed a block context
   // validate mode
   if ( bctxt->mode != DAL_READ  &&  bctxt->mode != DAL_METAREAD ) {
      LOG( LOG_ERR, "received block handle has inappropriate mode\n" );
      errno = EINVAL;
      return -1;
   }
   // Return cached data, if present
   if (bctxt->dctxt->minfo.totsz)
   {
      // validate offset
      if ( offset > bctxt->dctxt->minfo.blocksz ) {
         LOG( LOG_ERR, "offset %zd is beyond EOF at %zu\n", offset, bctxt->dctxt->minfo.blocksz );
         errno = EINVAL;
         return -1;
      }

      // copy from our primary data buff first
      size_t maxcopy = ( size > (bctxt->dctxt->minfo.blocksz - offset) ) ? (bctxt->dctxt->minfo.blocksz - offset) : size;
      size_t copied = 0;
      while ( copied < maxcopy  &&  offset < (bctxt->dctxt->minfo.blocksz - bctxt->dctxt->tail_size) ) {
         // calculate the offset and size to clear from the target buffer
         off_t suboffset = offset % bctxt->dctxt->minfo.versz; // get an offset in terms of this buffer iteration
         size_t copysize = maxcopy - copied; // start with the total remaining bytes
         if ( copysize > bctxt->dctxt->minfo.versz ) // reduce to our buffer size, at most
            copysize = bctxt->dctxt->minfo.versz;
         copysize -= suboffset; // exclude our starting offset
         // potentially zero out a portion of the target buffer
         if ( suboffset < ( bctxt->dctxt->minfo.versz - sizeof(uint32_t) ) ) {
            size_t nullsize = copysize;
            if ( nullsize + suboffset > ( bctxt->dctxt->minfo.versz - sizeof(uint32_t) ) ) {
               nullsize = ( bctxt->dctxt->minfo.versz - sizeof(uint32_t) ) - suboffset;
            }
            bzero( buf + copied, nullsize );
            copied += nullsize;
            offset += nullsize;
            suboffset += nullsize;
            copysize -= nullsize;
         }
         // potentially copy from our cached CRC value
         if ( suboffset >= ( bctxt->dctxt->minfo.versz - sizeof(uint32_t) ) ) {
            memcpy(buf + copied, ( (void*) &(bctxt->dctxt->full_crc) ) + ( suboffset - ( bctxt->dctxt->minfo.versz - sizeof(uint32_t) ) ), copysize);
            copied += copysize;
            offset += copysize;
         }
      }
      // fill any remaining from our tail buffer
      if ( copied < maxcopy ) {
         off_t suboffset = offset % bctxt->dctxt->minfo.versz; // get an offset in terms of this buffer iteration
         size_t copysize = maxcopy - copied; // start with the total remaining bytes
         if ( copysize > bctxt->dctxt->tail_size ) // reduce to our tail buffer size, at most
            copysize = bctxt->dctxt->tail_size;
         copysize -= suboffset; // exclude our starting offset
         // potentially zero out a portion of the target buffer
         if ( suboffset < ( bctxt->dctxt->tail_size - sizeof(uint32_t) ) ) {
            size_t nullsize = copysize;
            if ( nullsize + suboffset > ( bctxt->dctxt->tail_size - sizeof(uint32_t) ) ) {
               nullsize = ( bctxt->dctxt->tail_size - sizeof(uint32_t) ) - suboffset;
            }
            bzero( buf + copied, nullsize );
            copied += nullsize;
            suboffset += nullsize;
            copysize -= nullsize;
         }
         // potentially copy from our cached CRC value
         if ( suboffset >= ( bctxt->dctxt->tail_size - sizeof(uint32_t) ) ) {
            memcpy(buf + copied, ( (void*) &(bctxt->dctxt->tail_crc) ) + ( suboffset - ( bctxt->dctxt->tail_size - sizeof(uint32_t) ) ), copysize);
            copied += copysize;
         }
      }
      return copied; // return however many bytes were provided
   }
   // no cached datadata exists ( not necessarily a total failure )
   return 0;
}

int noop_abort(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   NOOP_BLOCK_CTXT bctxt = (NOOP_BLOCK_CTXT)ctxt; // Should have been passed a block context
   // Free block context
   free(bctxt);
   return 0;
}

int noop_close(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   NOOP_BLOCK_CTXT bctxt = (NOOP_BLOCK_CTXT)ctxt; // Should have been passed a block context
   // Free block context
   free(bctxt);
   return 0;
}

//   -------------    NO-OP INITIALIZATION    -------------

DAL noop_dal_init(xmlNode *root, DAL_location max_loc)
{
   // allocate space for our context struct ( initialized to zero vals, by calloc )
   NOOP_DAL_CTXT dctxt = calloc( 1, sizeof(struct noop_dal_context_struct) );
   if (dctxt == NULL)
   {
      LOG( LOG_ERR, "failed to allocate a new DAL ctxt\n" );
      return NULL;
   }
   // initialize values to indicate absence
   dctxt->minfo.N = -1;
   dctxt->minfo.E = -1;
   // initialize versz to match io size
   dctxt->minfo.versz = IO_SIZE;

   // allocate and populate a new DAL structure
   DAL ndal = malloc(sizeof(struct DAL_struct));
   if (ndal == NULL)
   {
      LOG(LOG_ERR, "failed to allocate space for a DAL_struct\n");
      free(dctxt);
      return NULL;
   }
   ndal->name = "noop";
   ndal->ctxt = (DAL_CTXT)dctxt;
   ndal->io_size = IO_SIZE;
   ndal->verify = noop_verify;
   ndal->migrate = noop_migrate;
   ndal->open = noop_open;
   ndal->set_meta = noop_set_meta;
   ndal->get_meta = noop_get_meta;
   ndal->put = noop_put;
   ndal->get = noop_get;
   ndal->abort = noop_abort;
   ndal->close = noop_close;
   ndal->del = noop_del;
   ndal->stat = noop_stat;
   ndal->cleanup = noop_cleanup;

   // loop over XML elements, checking for a read-chache source
   while ( root != NULL ) {
      // validate + parse this node
      if ( root->type != XML_ELEMENT_NODE ) {
         // skip comment nodes
         if ( root->type == XML_COMMENT_NODE ) { continue; }
         // skip text nodes ( could occur if we are passed an empty DAL tag body )
         if ( root->type == XML_TEXT_NODE ) { continue; }
         LOG( LOG_ERR, "encountered unknown node within a NoOp DAL definition\n" );
         break;
      }
      if ( strncmp( (char*)root->name, "N", 2 ) == 0 ) {
         if( parse_int_node( &(dctxt->minfo.N), root ) ) {
            LOG( LOG_ERR, "failed to parse 'N' value\n" );
            break;
         }
      }
      else if ( strncmp( (char*)root->name, "E", 2 ) == 0 ) {
         if( parse_int_node( &(dctxt->minfo.E), root ) ) {
            LOG( LOG_ERR, "failed to parse 'E' value\n" );
            break;
         }
      }
      else if ( strncmp( (char*)root->name, "PSZ", 4 ) == 0 ) {
         if( parse_size_node( &(dctxt->minfo.partsz), root ) ) {
            LOG( LOG_ERR, "failed to parse 'PSZ' value\n" );
            break;
         }
      }
      else if ( strncmp( (char*)root->name, "max_size", 9 ) == 0 ) {
         if( parse_size_node( &(dctxt->minfo.totsz), root ) ) {
            LOG( LOG_ERR, "failed to parse 'max_size' value\n" );
            break;
         }
      }
      else {
         LOG( LOG_ERR, "encountered an unrecognized \"%s\" node within a NoOp DAL definition\n", (char*)root->name );
         break;
      }
      // progress to the next element
      root = root->next;
   }
   // check for fatal error
   if ( root ) {
      noop_cleanup(ndal);
      return NULL;
   }
   // validate and ingest any cache source
   if ( dctxt->minfo.N != -1  ||  dctxt->minfo.E != -1  ||  dctxt->minfo.partsz > 0  ||  dctxt->minfo.totsz > 0 ) {
      // we're no in do-or-die mode for source caching
      // we have some values -- ensure we have all of them
      char fatalerror = 0;
      if ( dctxt->minfo.N == -1 ) {
         LOG( LOG_ERR, "missing source cache 'N' definition\n" );
         fatalerror = 1;
      }
      if ( dctxt->minfo.E == -1 ) {
         LOG( LOG_ERR, "missing source cache 'E' definition\n" );
         fatalerror = 1;
      }
      if ( dctxt->minfo.partsz <= 0 ) {
         LOG( LOG_ERR, "missing source cache 'PSZ' definition\n" );
         fatalerror = 1;
      }
      if ( dctxt->minfo.totsz <= 0 ) {
         LOG( LOG_ERR, "missing source cache 'max_size' definition\n" );
         fatalerror = 1;
      }
      if ( fatalerror ) {
         noop_cleanup(ndal);
         return NULL;
      }

      // allocate and populate our primary data buffer
      void* c_data =  calloc( 1, dctxt->minfo.versz );
      if ( c_data == NULL ) {
         LOG( LOG_ERR, "failed to allocate cached data buffer\n" );
         noop_cleanup(ndal);
         return NULL;
      }
      size_t datasize = dctxt->minfo.versz - sizeof(uint32_t);
      dctxt->full_crc = crc32_ieee_base(CRC_SEED, c_data, datasize);

      // calculate our blocksize
      size_t totalwritten = dctxt->minfo.totsz; // note the total volume of data to be contained in this object
      totalwritten += (dctxt->minfo.partsz * dctxt->minfo.N)
                        - (totalwritten % (dctxt->minfo.partsz * dctxt->minfo.N)); // account for erasure stripe alignment
      size_t iocnt = totalwritten / (datasize * dctxt->minfo.N); // calculate the number of buffers required to store this info
      dctxt->minfo.blocksz = iocnt * dctxt->minfo.versz; // record blocksz based on number of complete I/O buffers
      if ( totalwritten % (datasize * dctxt->minfo.N) ) { // account for misalignment
         // populate our 'tail' data buffer info
         size_t remainder = totalwritten % (datasize * dctxt->minfo.N);
         if ( remainder % dctxt->minfo.N ) { // sanity check
            LOG( LOG_ERR, "Remainder value of %zu is not cleanly divisible by N=%d ( tell 'gransom' that he doesn't understand math )\n",
                          remainder, dctxt->minfo.N );
            free( c_data );
            noop_cleanup(ndal);
            return NULL;
         }
         remainder /= dctxt->minfo.N;
         dctxt->tail_size = remainder + sizeof(uint32_t);
         dctxt->minfo.blocksz += dctxt->tail_size;
         dctxt->tail_crc = crc32_ieee_base(CRC_SEED, c_data, remainder);
      }
      free( c_data );

      // calculate our crcsum
      size_t ioindex = 0;
      for ( ; ioindex < iocnt; ioindex++ ) {
         dctxt->minfo.crcsum += dctxt->full_crc;
      }
      dctxt->minfo.crcsum += dctxt->tail_crc;
   }
   // NOTE -- no source cache defs is valid ( all reads will fail )
   //         only 'some', but not all source defs will result in init() error

   return ndal;
}




/* The following function was copied from Intel's ISA-L (https://github.com/intel/isa-l/blob/v2.30.0/crc/crc_base.c).
   The associated Copyright info has been reproduced below */

   /**********************************************************************
     Copyright(c) 2011-2015 Intel Corporation All rights reserved.

     Redistribution and use in source and binary forms, with or without
     modification, are permitted provided that the following conditions
     are met:
       * Redistributions of source code must retain the above copyright
         notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above copyright
         notice, this list of conditions and the following disclaimer in
         the documentation and/or other materials provided with the
         distribution.
       * Neither the name of Intel Corporation nor the names of its
         contributors may be used to endorse or promote products derived
         from this software without specific prior written permission.

     THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
     "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
     LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
     A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
     OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
     SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
     LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
     DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
     THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
     (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
     OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
   **********************************************************************/

static const uint32_t crc32_table_ieee_norm[256] = {
	0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9,
	0x130476dc, 0x17c56b6b, 0x1a864db2, 0x1e475005,
	0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61,
	0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd,
	0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9,
	0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75,
	0x6a1936c8, 0x6ed82b7f, 0x639b0da6, 0x675a1011,
	0x791d4014, 0x7ddc5da3, 0x709f7b7a, 0x745e66cd,
	0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
	0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5,
	0xbe2b5b58, 0xbaea46ef, 0xb7a96036, 0xb3687d81,
	0xad2f2d84, 0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d,
	0xd4326d90, 0xd0f37027, 0xddb056fe, 0xd9714b49,
	0xc7361b4c, 0xc3f706fb, 0xceb42022, 0xca753d95,
	0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
	0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d,
	0x34867077, 0x30476dc0, 0x3d044b19, 0x39c556ae,
	0x278206ab, 0x23431b1c, 0x2e003dc5, 0x2ac12072,
	0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16,
	0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca,
	0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde,
	0x6b93dddb, 0x6f52c06c, 0x6211e6b5, 0x66d0fb02,
	0x5e9f46bf, 0x5a5e5b08, 0x571d7dd1, 0x53dc6066,
	0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
	0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e,
	0xbfa1b04b, 0xbb60adfc, 0xb6238b25, 0xb2e29692,
	0x8aad2b2f, 0x8e6c3698, 0x832f1041, 0x87ee0df6,
	0x99a95df3, 0x9d684044, 0x902b669d, 0x94ea7b2a,
	0xe0b41de7, 0xe4750050, 0xe9362689, 0xedf73b3e,
	0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
	0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686,
	0xd5b88683, 0xd1799b34, 0xdc3abded, 0xd8fba05a,
	0x690ce0ee, 0x6dcdfd59, 0x608edb80, 0x644fc637,
	0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb,
	0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f,
	0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53,
	0x251d3b9e, 0x21dc2629, 0x2c9f00f0, 0x285e1d47,
	0x36194d42, 0x32d850f5, 0x3f9b762c, 0x3b5a6b9b,
	0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff,
	0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623,
	0xf12f560e, 0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7,
	0xe22b20d2, 0xe6ea3d65, 0xeba91bbc, 0xef68060b,
	0xd727bbb6, 0xd3e6a601, 0xdea580d8, 0xda649d6f,
	0xc423cd6a, 0xc0e2d0dd, 0xcda1f604, 0xc960ebb3,
	0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
	0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b,
	0x9b3660c6, 0x9ff77d71, 0x92b45ba8, 0x9675461f,
	0x8832161a, 0x8cf30bad, 0x81b02d74, 0x857130c3,
	0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640,
	0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c,
	0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8,
	0x68860bfd, 0x6c47164a, 0x61043093, 0x65c52d24,
	0x119b4be9, 0x155a565e, 0x18197087, 0x1cd86d30,
	0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
	0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088,
	0x2497d08d, 0x2056cd3a, 0x2d15ebe3, 0x29d4f654,
	0xc5a92679, 0xc1683bce, 0xcc2b1d17, 0xc8ea00a0,
	0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb, 0xdbee767c,
	0xe3a1cbc1, 0xe760d676, 0xea23f0af, 0xeee2ed18,
	0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
	0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0,
	0x9abc8bd5, 0x9e7d9662, 0x933eb0bb, 0x97ffad0c,
	0xafb010b1, 0xab710d06, 0xa6322bdf, 0xa2f33668,
	0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4
};

static uint32_t crc32_ieee_base(uint32_t seed, uint8_t * buf, uint64_t len)
{
	unsigned int crc = ~seed;

	while (len--) {
		crc = (crc << 8) ^ crc32_table_ieee_norm[((crc >> 24) ^ *buf) & 255];
		buf++;
	}

	return ~crc;
}

