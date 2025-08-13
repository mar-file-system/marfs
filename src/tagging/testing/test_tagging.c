/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include <unistd.h>
#include <stdio.h>
// directly including the C file allows more flexibility for these tests
#include "tagging/tagging.c"

int main(int argc, char **argv)
{
   // NOTE -- I'm ignoring memory leaks for error conditions 
   //         which result in immediate termination

   // initialize an FTAG struct and output a string representation
   FTAG ftag;
   ftag.majorversion = FTAG_CURRENT_MAJORVERSION;
   ftag.minorversion = FTAG_CURRENT_MINORVERSION;
   ftag.ctag = "TEST_CLIENT";
   ftag.streamid = "teststreamidvalue";
   ftag.objfiles = 1024;
   ftag.objsize = 1073741824; // 1GiB
   ftag.refbreadth = 24;
   ftag.refdepth = 123;
   ftag.refdigits = 1;
   ftag.fileno = 0;
   ftag.objno = 0;
   ftag.offset = 234;
   ftag.endofstream = 0;
   ftag.protection.N = 10;
   ftag.protection.E = 2;
   ftag.protection.O = 1;
   ftag.protection.partsz = 1024;
   ftag.bytes = 1073741812; // 1GiB - 12 bytes
   ftag.availbytes = 1024; // 1KiB
   ftag.recoverybytes = 234;
   ftag.state = FTAG_SIZED | FTAG_WRITEABLE;
   size_t ftagstrlen = ftag_tostr( &(ftag), NULL, 0 );
   if ( ftagstrlen < 1 ) {
      printf( "failed to generate initial ftag string\n" );
      return -1;
   }
   char* ftagstr = malloc( sizeof(char) * (ftagstrlen + 1) );
   if ( ftagstr == NULL ) {
      printf( "failed to allocate space for initial ftag string\n" );
      return -1;
   }
   if ( ftag_tostr( &(ftag), ftagstr, ftagstrlen + 1 ) != ftagstrlen ) {
      printf( "inconsistent length of initial ftag string\n" );
      return -1;
   }
   // verify the ftag string
   FTAG oftag; // initialize to entirely different vals, ensuring all are changed
   oftag.majorversion = FTAG_CURRENT_MAJORVERSION + 12;
   oftag.minorversion = FTAG_CURRENT_MINORVERSION + 2;
   oftag.ctag = "TEST_OCLIENT";
   oftag.streamid = "testostreamid";
   oftag.objfiles = 10;
   oftag.objsize = 10737;
   oftag.refbreadth = 43;
   oftag.refdepth = 1;
   oftag.refdigits = 543;
   oftag.fileno = 1234;
   oftag.objno = 42;
   oftag.offset = 432;
   oftag.endofstream = 1;
   oftag.protection.N = 2;
   oftag.protection.E = 10;
   oftag.protection.O = 34;
   oftag.protection.partsz = 1;
   oftag.bytes = 1;
   oftag.availbytes = 1048576; // 1MiB
   oftag.recoverybytes = 5432;
   oftag.state = FTAG_INIT;
   if ( ftag_initstr( &(oftag), ftagstr ) ) {
      printf( "failed to init ftag from str: \"%s\"\n", ftagstr );
      return -1;
   }
   if ( ftag_cmp( &(ftag), &(oftag) ) ) {
      printf( "orig values differ from string vals: \"%s\"\n", ftagstr );
      return -1;
   }

   // output a meta tgt string
   char metatgtstr[1024] = {0};
   size_t metatgtstrlen = ftag_metatgt( &(ftag), metatgtstr, 1024 );
   if ( metatgtstrlen < 1  ||  metatgtstrlen >= 1024 ) {
      printf( "invalid length of metatgtstr: %zu\n", metatgtstrlen );
      return -1;
   }
   // identify the fileno value of the metatgtstr
   char enttype = -1;
   if ( ftag_metainfo( metatgtstr, &(enttype) ) ) {
      printf( "invalid fileno value from metatgtstr: \"%s\"\n", metatgtstr );
      return -1;
   }
   if ( enttype ) {
      printf( "unexpected type value for metatgtstr: %c\n", enttype );
      return -1;
   }

   // output a rebuild marker string
   metatgtstrlen = ftag_rebuildmarker( &(ftag), metatgtstr, 1024 );
   if ( metatgtstrlen < 1  ||  metatgtstrlen >= 1024 ) {
      printf( "invalid length of rebuild marker string: %zu\n", metatgtstrlen );
      return -1;
   }
   // identify the objno value of the rebuild marker str
   enttype = -1;
   if ( ftag_metainfo( metatgtstr, &(enttype) ) ) {
      printf( "invalid objno value from rebuild marker str: \"%s\"\n", metatgtstr );
      return -1;
   }
   if ( enttype != 1 ) {
      printf( "unexpected type value for rebuild marker str: %c\n", enttype );
      return -1;
   }

   // need to free strings allocated by us and the string initializer
   free( ftagstr );
   free( oftag.streamid );
   free( oftag.ctag );

   // test rebuild tag processing
   time_t tagtime = time(NULL);
   RTAG rtag = {
      .majorversion = RTAG_CURRENT_MAJORVERSION,
      .minorversion = RTAG_CURRENT_MINORVERSION,
      .createtime = tagtime,
      .stripewidth = 5,
      .stripestate.versz = 1234,
      .stripestate.blocksz = 19744,
      .stripestate.totsz = 59121,
      .stripestate.meta_status = NULL,
      .stripestate.data_status = NULL,
      .stripestate.csum = NULL
   };
   if ( rtag_alloc( &(rtag) ) ) {
      printf( "failed to allocate data/meta_status lists\n" );
      return -1;
   }
   rtag.stripestate.meta_status[4] = 1;
   rtag.stripestate.meta_status[1] = 1;
   rtag.stripestate.data_status[0] = 1;
   size_t rtaglen = rtag_tostr( &(rtag), NULL, 0 );
   if ( rtaglen < 1 ) {
      printf( "failed to indentify length of rebuild tag\n" );
      return -1;
   }
   char* rtagstr = malloc( sizeof(char) * (rtaglen + 1) );
   if ( rtagstr == NULL ) {
      printf( "failed to allocate rtag string\n" );
      return -1;
   }
   if ( rtag_tostr( &(rtag), rtagstr, rtaglen + 1 ) != rtaglen ) {
      printf( "inconsistent length of rebuild tag string\n" );
      return -1;
   }
   RTAG newrtag = { 0 };
   if ( rtag_initstr( &(newrtag), rtagstr ) ) {
      printf( "failed to parse rebuild tag string: \"%s\"\n", rtagstr );
      return -1;
   }
   if ( rtag.majorversion != newrtag.majorversion  ||  rtag.minorversion != newrtag.minorversion  ||
        rtag.createtime != newrtag.createtime  ||  rtag.stripewidth != newrtag.stripewidth  ||
        rtag.stripestate.versz != newrtag.stripestate.versz  ||  rtag.stripestate.blocksz != newrtag.stripestate.blocksz  ||
        rtag.stripestate.totsz != newrtag.stripestate.totsz ) {
      printf( "parsed rtag values do not match\n" );
      return -1;
   }
   int index = 0;
   for ( ; index < 5; index++ ) {
      if ( rtag.stripestate.meta_status[index] != newrtag.stripestate.meta_status[index] ) {
         printf( "meta status differs on index %d\n", index );
         return -1;
      }
      if ( rtag.stripestate.data_status[index] != newrtag.stripestate.data_status[index] ) {
         printf( "data status differs on index %d\n", index );
         return -1;
      }
   }
   free( rtagstr );
   rtag_free( &(rtag) );
   rtag_free( &(newrtag) );

   return 0;
}


