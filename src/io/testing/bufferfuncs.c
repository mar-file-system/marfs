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


#include "io/io.h"
#include "dal/dal.h"
#include <unistd.h>
#include <stdio.h>


// sentinel values to ensure good data transfer
char tail_sent = 'T';
unsigned int crc_sent = UINT_MAX;


size_t fill_buffer( size_t prev_data, size_t iosz, size_t partsz, void* buffer, DAL_MODE mode ) {
   // determine how much to write ( read == filling buffers from IO, write == filling from erasure/data parts )
   size_t data_to_write = ( mode == DAL_READ ) ? iosz : partsz;
   if ( mode == DAL_READ ) {
      // we always need to write a terminating CRC
      data_to_write -= CRC_BYTES;
   }
   size_t total_written = data_to_write; // don't count CRC bytes
   unsigned int parts = ( prev_data / partsz );
   printf( "Writing %zu bytes of part %u to buffer...\n", data_to_write, parts );
   // loop over parts, filling in data
   while ( data_to_write > 0 ) {
      size_t prev_pfill = prev_data % partsz;
      // check if we need to write out a head sentinel
      if ( prev_pfill < sizeof( unsigned int ) ) {
         size_t fill_size = sizeof( unsigned int ) - prev_pfill;
         if ( fill_size > data_to_write ) { fill_size = data_to_write; }
         printf( "   %zu bytes of header value %u at offset %zu\n", fill_size, parts, prev_pfill );
         memcpy( buffer, ((void*)(&parts)) + prev_pfill, fill_size );
         buffer += fill_size;
         prev_data += fill_size;
         prev_pfill += fill_size;
         data_to_write -= fill_size;
      }
      // check if we need to write out any filler data
      if ( data_to_write > 0 ) {
         // check if our data to be written is less than a complete filler
         size_t fill_size = ( data_to_write < ((partsz - prev_pfill) - sizeof(char)) ) ? 
                                 data_to_write : ((partsz - prev_pfill) - sizeof(char));
         printf( "   %zu bytes of zero-fill\n", fill_size );
         bzero( buffer, fill_size );
         buffer += fill_size;
         prev_data += fill_size;
         prev_pfill += fill_size;
         data_to_write -= fill_size;
      }
      // check if we need to write out a tail sentinel
      if ( data_to_write > 0 ) {
         memcpy( buffer, &tail_sent, sizeof( char ) );
         printf( "   1 byte tail\n" );
         buffer += sizeof( char );
         prev_data += sizeof( char );
         prev_pfill += sizeof( char );
         data_to_write -= sizeof( char );
      }
      printf( "Filled part %d, up to total data size %zu\n", parts, prev_data );
      // sanity check that we have properly filled a part
      if ( prev_pfill == partsz ) {
         parts++;
      }
      else if ( data_to_write != 0 ) {
         printf( "ERROR: data remains to write, but we haven't filled a part!\n" );
         return 0;
      }
   }

   if ( mode == DAL_READ ) {
      // finally, copy over a sentinel CRC value
      printf( "Copying CRC sentinel to end of target buffer after filling to part %u\n", parts );
      memcpy( buffer, &crc_sent, CRC_BYTES );
   }
   else {
      printf( "Filled up to part %u in buffer\n", parts );
   }

   return total_written;
}



size_t verify_data( size_t prev_ver, size_t partsz, size_t buffsz, void* buffer, DAL_MODE mode ) {
   // create dummy zero buffer for comparisons
   char* zerobuff = calloc( 1, partsz );
   if ( zerobuff == NULL ) {
      printf( "ERROR: failed to allocate space for dummy zero buffer!\n" );
      return 0;
   }
   // determine how much to write ( read == filling buffers from IO, write == filling from erasure/data parts )
   size_t data_to_chk = buffsz;
   if ( mode == DAL_WRITE ) {
      // we always need to write a terminating CRC
      data_to_chk -= CRC_BYTES;
   }
   size_t total_ver = data_to_chk; //don't count CRC bytes
   unsigned int parts = ( prev_ver / partsz );
   printf( "Verifying %zu bytes starting at part %u in buffer...", total_ver, parts );
   // loop over parts, filling in data
   while ( data_to_chk > 0 ) {
      size_t prev_pfill = prev_ver % partsz;
      // check if we need to write out a head sentinel
      if ( prev_pfill < sizeof( unsigned int ) ) {
         size_t fill_size = sizeof( unsigned int ) - prev_pfill;
         if ( fill_size > data_to_chk ) { fill_size = data_to_chk; }
         if ( memcmp( buffer, ((void*)(&parts)) + prev_pfill, fill_size ) ) {
            printf( "ERROR: Failed to verify %zu bytes of header for part %u (offset=%zu)!\n", fill_size, parts, prev_pfill );
            free( zerobuff );
            return 0;
         }
         buffer += fill_size;
         prev_ver += fill_size;
         prev_pfill += fill_size;
         data_to_chk -= fill_size;
      }
      // check if we need to write out any filler data
      if ( data_to_chk > 0 ) {
         // check if our data to be written is less than a complete filler
         size_t fill_size = ( data_to_chk < ((partsz - prev_pfill) - sizeof(char)) ) ? 
                                 data_to_chk : ((partsz - prev_pfill) - sizeof(char));
         if ( memcmp( buffer, zerobuff, fill_size ) ) {
            printf( "ERROR: Failed to verify filler of part %d!\n", parts );
            free( zerobuff );
            return 0;
         }
         buffer += fill_size;
         prev_ver += fill_size;
         prev_pfill += fill_size;
         data_to_chk -= fill_size;
      }
      // check if we need to write out a tail sentinel
      if ( data_to_chk > 0 ) {
         if ( memcmp( buffer, &tail_sent, sizeof( char ) ) ) {
            printf( "ERROR: failed to verify tail of part %d!\n", parts );
            free( zerobuff );
            return 0;
         }
         buffer += sizeof( char );
         prev_ver += sizeof( char );
         prev_pfill += sizeof( char );
         data_to_chk -= sizeof( char );
      }
      // sanity check that we have properly filled a part
      if ( prev_pfill == partsz ) {
         parts++;
      }
      else if ( data_to_chk != 0 ) {
         printf( "ERROR: data remains to verify, but we haven't completed a part!\n" );
         free( zerobuff );
         return 0;
      }
   }

   if ( mode == DAL_WRITE ) {
      // finally, copy over a sentinel CRC value
      printf( "Checking CRC sentinel at end of target buffer after checking to part %u\n", parts );
      if ( memcmp( buffer, &crc_sent, CRC_BYTES ) ) {
         printf( "ERROR: failed to verify CRC sentinel after part %u!\n", parts );
         free( zerobuff );
         return 0;
      }
   }
   else {
      printf( "checked up to part %u in buffer\n", parts );
   }

   free( zerobuff );
   return total_ver;
}


