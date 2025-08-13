#ifndef _NUMDIGITS_H
#define _NUMDIGITS_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */


// NOTE: This file is intended for direct, inline inclusion in other SRC files

#include <limits.h>
#include <stdint.h>

static int numdigits_unsigned( unsigned long long val ) {
   // I have used hardcoded max values, to avoid underestimating digit counts if limits ever increase
   if( val < 10U ) { return 1; }
   if( val < 100U ) { return 2; }
   if( val < 1000U ) { return 3; }
   if( val < 10000U ) { return 4; }
   if( val < 100000U ) { return 5; }
   if( val < 1000000U ) { return 6; }
   if( val < 10000000U ) { return 7; }
   if( val < 100000000U ) { return 8; }
   if( val < 1000000000U ) { return 9; }
#if  __WORDSIZE < 64
   if ( val <= 4294967295U ) { return 10; } // hardcoded ULONG_MAX ( 32-bit )
#else
   // these values would exceed 32-bit compiler limits
   if( val < 10000000000U ) { return 10; }
   if( val < 100000000000U ) { return 11; }
   if( val < 1000000000000U ) { return 12; }
   if( val < 10000000000000U ) { return 13; }
   if( val < 100000000000000U ) { return 14; }
   if( val < 1000000000000000U ) { return 15; }
   if( val < 10000000000000000U ) { return 16; }
   if( val < 100000000000000000U ) { return 17; }
   if( val < 1000000000000000000U ) { return 18; }
   if( val < 10000000000000000000U ) { return 19; }
   if( val <= 18446744073709551615U ) { return 20; } // hardcoded ULONG_MAX ( 64-bit )
#endif
   return -1; // error case
}

// determining digits for size_t is very common, this is shorthand
#define SIZE_DIGITS numdigits_unsigned( (unsigned long long) SIZE_MAX )
// determining digits for unsigned int is very common, this is shorthand
#define UINT_DIGITS numdigits_unsigned( (unsigned long long) UINT_MAX )

#endif // _NUMDIGITS_H

