#ifndef _RESTRICTEDCHARS_H
#define _RESTRICTEDCHARS_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */


// NOTE: This file is intended for direct, inline inclusion in other SRC files


#define RESTRICTEDCHARS_STRING "/()|#^*"

// parse over a provided string to check for restricted chars
static int restrictedchars_check( const char* str ) {
   const char* parse = str;
   while ( *parse != '\0' ) {
      // compare to all restricted characters
      const char* cmpto = RESTRICTEDCHARS_STRING;
      while ( *cmpto != '\0' ) {
         if ( *parse == *cmpto ) {
            LOG( LOG_ERR, "Detected restricted character value: '%c'\n", *cmpto );
            return -1;
         }
         cmpto++;
      }
      parse++;
   }
   return 0;
}


#endif // _RESTRICTEDCHARS_H

