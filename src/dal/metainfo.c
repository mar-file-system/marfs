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
#define LOG_PREFIX "metainfo"
#include "logging/logging.h"

#include "metainfo.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>


#define MINFO_VER 1


/* ------------------------------   INTERNAL HELPER FUNCTIONS   ------------------------------ */


// Internal helper function
// Estimate the space required for a meta_info string representation
size_t get_minfo_strlen( ) {
   // Note: Binary 3bits == max value of 7, so decimal representation requires at most 1byte per 3bits of the struct 
   //       plus an additional 13bytes for version tag, whitespace, and the terminating null character.
   return ( ( sizeof( struct meta_info_struct ) * 8 ) / 3 ) + 13;
}



/* ------------------------------   META INFO TRANSLATION   ------------------------------ */


/**
 * Perform a DAL get_meta call and parse the resulting string 
 * into the provided meta_info_struct reference.
 * @param ssize_t (*meta_filler)(BLOCK_CTXT handle, char *meta_buf, size_t size) :
 *                Function for retrieving meta info buffers
 *                Expected to fill 'meta_buf' with at most 'size' bytes of stored meta info
 *                Expected to return a total byte count of stored meta info ( even if 'size' < this value ), or -1 on failure
 * @param int block : Block on which this operation is being performed (for logging only)
 * @param meta_info* minfo : meta_info reference to populate with values 
 * @return int : Zero on success, a negative value if a failure occurred, or the number of 
 *               meta values successfully parsed if only portions of the meta info could 
 *               be recovered.
 */
int dal_get_meta_helper( ssize_t (*meta_filler)(BLOCK_CTXT handle, char *meta_buf, size_t size), BLOCK_CTXT handle, meta_info* minfo ) {
   // Allocate space for a string
   size_t strmax = get_minfo_strlen();
   char* str = (char*) malloc( strmax );
   if ( str == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for a meta_info string!\n" );
      return -1;
   }
   // set stand-ins for all values
   minfo->N = 0;
   minfo->E = -1;
   minfo->O = -1;
   minfo->partsz  = -1;
   minfo->versz   = -1;
   minfo->blocksz = -1;
   minfo->crcsum  = -1;
   minfo->totsz   = -1;
   // get the meta info for the given object
   ssize_t dstrbytes;
   if ( (dstrbytes = meta_filler( handle, str, strmax )) <= 0 ) {
      LOG( LOG_ERR, "failed to retrieve meta value!\n" );
      free( str );
      return -1;
   }
   if ( dstrbytes > strmax ) {
      LOG( LOG_ERR, "meta value of size %zd exceeds buffer bounds of %zu!\n", dstrbytes, strmax );
      free( str );
      return -1;
   }
   // make sure we have a trailing newline
   char valid_suffix = 0;
   if ( dstrbytes >= 2  &&  str[dstrbytes-2] == '\n' ) { valid_suffix = 1; }
   else { LOG( LOG_ERR, "meta string lacks trailing newline!\n" ); }
   // make SURE this string is null-terminated
   str[dstrbytes-1] = '\0';

   int status = 8; // initialize to the number of values we expect to parse
   // Parse the string into appropriate meta_info fields
   // declared here so that the compiler can hopefully free up this memory outside of the 'else' block
   char metaN[5];        /* char array to get n parts from the meta string */
   char metaE[5];        /* char array to get erasure parts from the meta string */
   char metaO[5];        /* char array to get erasure offset from the meta string */
   char metapartsz[20];  /* char array to get erasure partsz from the meta string */
   char metaversz[20];   /* char array to get compressed block size from the meta string */
   char metablocksz[20]; /* char array to get complete block size from the meta string */
   char metacrcsum[20];  /* char array to get crc sum from the meta string */
   char metatotsize[20]; /* char array to get object totsz from the meta string */

   LOG( LOG_INFO, "Parsing meta string: %s", str );

   // check the version number
   int vertag = 0; // assume no version tag
   char* parse = str;
   if ( *parse == 'v' ) {
      // now that we know this is tagged, assume it's our current structure until proven otherwise
      vertag = MINFO_VER;
      // read in the version tag value, if possible
      if ( sscanf( parse, "v%d ", &vertag ) ) {
         LOG( LOG_INFO, "Got minfo version tag = %d\n", vertag );
         // skip ahead to the next gap, while avoiding overruning the end of the string
         while( *parse != ' '  &&  *parse != '\0' ) {
            parse++;
         }
         if ( *parse == ' ' )
            parse++; // string ref should now be beyond the ver tag and whitespace
      }
   }
   
   int ret = 0;
   if ( vertag ) {
      // only process the meta string if we successfully retreived it
      ret = sscanf(parse,"%4s %4s %4s %19s %19s %19s %19s %19s",
                           metaN,
                           metaE,
                           metaO,
                           metapartsz,
                           metaversz,
                           metablocksz,
                           metacrcsum,
                           metatotsize);
   }
   else {
      // only process the meta string if we successfully retreived it
      ret = sscanf(parse,"%4s %4s %4s %19s %*19s %19s %19s %19s",
                           metaN,
                           metaE,
                           metaO,
                           metapartsz,
                           metablocksz,
                           metacrcsum,
                           metatotsize);
      strncpy( metaversz, metapartsz, 20 );
   }

   if ( ret < 1 ) {
      LOG( LOG_ERR, "sscanf failed to parse any values from meta info!\n" );
      free( str );
      return -1;
   }
   if (ret != 8) {
      LOG( LOG_WARNING, "sscanf parsed only %d values from meta info: \"%s\"\n", ret, str);
      status = ret;
   }  
   free( str );
   
   char* endptr;
   // simple macro to save some repeated lines of code
   // this is used to parse all meta values, check for errors, and assign them to their appropriate locations
#define PARSE_VALUE( VAL, STR, GT_VAL, PARSE_FUNC, TYPE ) \
   if ( ret > GT_VAL ) { \
      TYPE tmp_val = (TYPE) PARSE_FUNC ( STR, &(endptr), 10 ); \
      if ( *(endptr) == '\0'  &&  (tmp_val > VAL ) ) { \
         VAL = tmp_val; \
      } \
      else { \
         LOG( LOG_ERR, "failed to parse meta value at position %d: \"%s\"\n", GT_VAL, STR ); \
         status -= 1; \
      } \
   }
   // Parse all values into the meta_info struct
   PARSE_VALUE(       minfo->N,       metaN, 0,  strtol, int )
   PARSE_VALUE(       minfo->E,       metaE, 1,  strtol, int )
   PARSE_VALUE(       minfo->O,       metaO, 2,  strtol, int )
   PARSE_VALUE(  minfo->partsz,  metapartsz, 3,  strtol, ssize_t )
   PARSE_VALUE(   minfo->versz,   metaversz, 4,  strtol, ssize_t )
   PARSE_VALUE( minfo->blocksz, metablocksz, 5,  strtol, ssize_t )
   PARSE_VALUE(  minfo->crcsum,  metacrcsum, 6, strtoll, long long )
   PARSE_VALUE(   minfo->totsz, metatotsize, 7, strtoll, ssize_t )

   LOG( LOG_INFO, "Got values (N=%d,E=%d,O=%d,partsz=%zd,versz=%zd,blocksz=%zd,totsz=%zd)\n",
                  minfo->N, minfo->E, minfo->O, minfo->partsz, minfo->versz, minfo->blocksz, minfo->totsz );

   return ( valid_suffix  &&  status == 8 ) ? 0 : status;
}


/**
 * Convert a meta_info struct to string format and perform a DAL set_meta call
 * @param int (*meta_writer)(BLOCK_CTXT handle, const char *meta_buf, size_t size) :
 *            Function for storing meta info buffers to a block handle
 *            Expected to write 'size' bytes from 'meta_buf' as meta info of the given handle
 *            Expected to return zero on success, or -1 on failure
 * @param BLOCK_CTXT handle : Block on which this operation is being performed
 * @param meta_info* minfo : meta_info reference to populate with values 
 * @return int : Zero on success, or a negative value if an error occurred 
 */
int dal_set_meta_helper( int (*meta_writer)(BLOCK_CTXT handle, const char *meta_buf, size_t size), BLOCK_CTXT handle, const meta_info* minfo ) {
   // Allocate space for a string
   size_t strmax = get_minfo_strlen();
   char* str = (char*) malloc( strmax );
   if ( str == NULL ) {
      LOG( LOG_ERR, "failed to allocate space for a meta_info string!\n" );
      return -1;
   }

   LOG( LOG_INFO, "partsz %zd\n", minfo->partsz );
   LOG( LOG_INFO, "versz %zd\n", minfo->versz );
   LOG( LOG_INFO, "blocksz %zd\n", minfo->blocksz );
   LOG( LOG_INFO, "crcsum %zd\n", minfo->crcsum );

	// fill the string allocation with meta_info values
   if ( snprintf(str,strmax, "v%d %d %d %d %zd %zd %zd %llu %zd\n",
                  MINFO_VER, minfo->N, minfo->E, minfo->O,
                  minfo->partsz, minfo->versz,
                  minfo->blocksz, minfo->crcsum,
                  minfo->totsz) < 0 ) {
      LOG( LOG_ERR, "failed to convert meta_info to string format!\n" );
      free( str );
      return -1;
   }

	if ( meta_writer( handle, str, strlen( str ) + 1 ) ) {
		LOG( LOG_ERR, "failed to set meta value!\n" );
		free( str );
		return -1;
	}

   free( str );
	return 0;
}



/* ------------------------------   META INFO MANIPULATION   ------------------------------ */


/**
 * Duplicates info from one meta_info struct to another (excluding CRCSUM!)
 * @param meta_info* target : Target struct reference
 * @param meta_info* source : Source struct reference
 */
void cpy_minfo( meta_info* target, meta_info* source ) {
   target->N = source->N;
   target->E = source->E;
   target->O = source->O;
   target->partsz = source->partsz;
   target->versz = source->versz;
   target->blocksz = source->blocksz;
   target->totsz = source->totsz;
}

/**
 * Compares the values of two meta_info structs (excluding CRCSUM!)
 * @param meta_info* minfo1 : First struct reference
 * @param meta_info* minfo2 : Second struct reference
 * @return int : A zero value if the structures match, non-zero otherwise
 */
int cmp_minfo( meta_info* minfo1, meta_info* minfo2 ) {
   if ( minfo1->N != minfo2->N ) { return -1; }
   if ( minfo1->E != minfo2->E ) { return -1; }
   if ( minfo1->O != minfo2->O ) { return -1; }
   if ( minfo1->partsz != minfo2->partsz ) { return -1; }
   if ( minfo1->versz != minfo2->versz ) { return -1; }
   if ( minfo1->blocksz != minfo2->blocksz ) { return -1; }
   if ( minfo1->totsz != minfo2->totsz ) { return -1; }
   return 0;
}



