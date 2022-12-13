#ifndef _NUMDIGITS_H
#define _NUMDIGITS_H
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

