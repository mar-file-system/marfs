/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */


#include "io/io.h"
#include "dal/dal.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>


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


