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


#include "marfs_auto_config.h"
#if defined(DEBUG_ALL)  ||  defined(DEBUG_TAGGING)
   #define DEBUG 1
#endif
#define LOG_PREFIX "tagging"

#include <logging.h>
#include "tagging.h"

#include <string.h>

//   -------------   INTERNAL DEFINITIONS    -------------

#define FTAG_VERSION_HEADER "VER"
#define FTAG_STREAMINFO_HEADER "STM"
#define FTAG_FILEPOSITION_HEADER "POS"
#define FTAG_DATACONTENT_HEADER "DAT"


//   -------------   INTERNAL FUNCTIONS    -------------




//   -------------   EXTERNAL FUNCTIONS    -------------

/**
 * Populate the given ftag struct based on the content of the given ftag string
 * @param FTAG* ftag : Reference to the ftag struct to be populated
 * @param char* ftagstr : String value to be parsed for structure values
 * @return int : Zero on success, or -1 if a failure occurred
 */
int ftag_initstr( FTAG* ftag, char* ftagstr ) {
   // check for NULL references
   if ( ftag == NULL ) {
      LOG( LOG_ERR, "Received a NULL FTAG reference\n" );
      return -1;
   }
   if ( ftagstr == NULL ) {
      LOG( LOG_ERR, "Received a NULL ftagstr reference\n" );
      return -1;
   }
   // parse in and verify version info
   if ( strcmp( ftagstr, FTAG_VERSION_HEADER"(" ) ) {
      LOG( LOG_ERR, "FTAG string does not begin with \"%s\" header\n", FTAG_VERSION_HEADER"(" );
      return -1;
   }
   char* parse = ftagstr + strlen(FTAG_VERSION_HEADER"(");
   char* endptr = NULL;
   unsigned long long parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != '.' ) {
      LOG( LOG_ERR, "Major version string has unexpected format\n" );
      return -1;
   }
   if ( parseval > UINT_MAX ) {
      LOG( LOG_ERR, "Major version value exceeds maximum allowable: %llu\n", parseval );
      return -1;
   }
   ftag->majorversion = parseval;
   parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != ')' ) {
      LOG( LOG_ERR, "Minor version string has unexpected format\n" );
      return -1;
   }
   if ( parseval > UINT_MAX ) {
      LOG( LOG_ERR, "Minor version value exceeds maximum allowable: %llu\n", parseval );
      return -1;
   }
   ftag->minorversion = parseval;
   parse = endptr + 1;
   if ( ftag->majorversion != FTAG_CURRENT_MAJORVERSION  ||
        ftag->minorversion != FTAG_CURRENT_MINORVERSION ) {
      LOG( LOG_ERR, "Unrecognized version number: %u.%.3u\n", ftag->majorversion, ftag->minorversion );
      return -1;
   }
   // parse stream identification info
   if ( strcmp( parse, FTAG_STREAMINFO_HEADER"(" ) ) {
      LOG( LOG_ERR, "Unrecognized stream info header\n" );
      return -1;
   }
   parse += strlen( FTAG_STREAMINFO_HEADER"(" );
   ftag->ctag = NULL;
   char* output = NULL;
   endptr = parse;
   size_t outputlen = 1;
   while ( *parse != '\0' ) { // don't allow extension beyond end of string
      if ( *parse == '|' ) {
         if ( ftag->ctag ) { break; }
         ftag->ctag = malloc( sizeof(char) * outputlen );
         if ( ftag->ctag == NULL ) {
            LOG( LOG_ERR, "Failed to allocate FTAG CTAG string\n" );
            return -1;
         }
         output = ftag->ctag;
         outputlen = 1;
         parse = endptr; // reset back to the beginning of the client tag string
         continue;
      }
      if ( ftag->ctag ) { *output = *parse; output++; }
      outputlen++;
      parse++;
   }
   if ( ftag->ctag == NULL ) {
      LOG( LOG_ERR, "Unrecognized CTAG format\n" );
      return -1;
   }
   parse++; // skip over '|' char
   ftag->streamid = NULL;
   output = NULL;
   endptr = parse;
   outputlen = 1;
   while ( *parse != '\0' ) { // don't allow extension beyond end of string
      if ( *parse == '|' ) {
         if ( ftag->streamid ) { break; }
         ftag->streamid = malloc( sizeof(char) * outputlen );
         if ( ftag->streamid == NULL ) {
            LOG( LOG_ERR, "Failed to allocate FTAG streamid string\n" );
            return -1;
         }
         output = ftag->streamid;
         outputlen = 1;
         parse = endptr; // reset back to the beginning of the streamid string
         continue;
      }
      if ( ftag->streamid ) { *output = *parse; output++; }
      outputlen++;
      parse++;
   }
   if ( ftag->streamid == NULL ) {
      LOG( LOG_ERR, "Unrecognized streamid format\n" );
      return -1;
   }
   parse++; // skip over the '|' char
   char foundvals = 0;
   while ( *parse != '\0' ) {
      size_t* tgtval = NULL;
      if ( *parse == 'f' ) { tgtval = &(ftag->objfiles); }
      else if ( *parse == 'd' ) { tgtval = &(ftag->objsize); }
      else { LOG( LOG_ERR, "Unrecognized value tag: \"%c\"\n", *parse ); return -1; }
      parseval = strtoull( parse + 1, &(endptr), 10 );
      if ( parseval > SIZE_MAX ) {
         LOG( LOG_ERR, "Parsed objfile value exceeds size limits: %llu\n", parseval );
         return -1;
      }
      *tgtval = parseval;
      foundvals++;
      if ( *endptr != '-'  &&  *endptr != ')' ) {
         LOG( LOG_ERR, "Unrecognized stream value format\n" );
         return -1;
      }
      parse = endptr + 1; // skip over the separator char
      if ( *endptr == ')' ) { break; }
   }
   if ( foundvals != 2 ) {
      LOG( LOG_ERR, "Expected two stream object values (objfiles/objsize), but found %d\n", (int)foundvals );
      return -1;
   }
   // parse file position info
   if ( strcmp( parse, FTAG_FILEPOSITION_HEADER"(" ) ) {
      LOG( LOG_ERR, "Failed to locate file position header\n" );
      return -1;
   }
   parse += strlen( FTAG_FILEPOSITION_HEADER"(" );
   foundvals = 0;
   while ( *parse != '\0' ) {
      // attempt to parse the numeric value
      parseval = strtoull( parse + 1, &(endptr), 10 );
      if ( parseval > SIZE_MAX ) {
         LOG( LOG_ERR, "File position value \'%c\' exceeds size limits\n", *parse );
         return -1;
      }
      if ( *endptr != '-'  &&  *endptr != ')' ) {
         LOG( LOG_ERR, "Unrecognized position value format\n" );
         return -1;
      }
      switch ( *parse ) {
         case 'F':
            ftag->fileno = parseval;
            break;
         case 'O':
            ftag->objno = parseval;
            break;
         case '@':
            ftag->offset = parseval;
            break;
         case 'E':
            if ( parseval == 1 ) {
               ftag->endofstream = 1;
            }
            else if ( parseval == 0 ) {
               ftag->endofstream = 0;
            }
            else {
               LOG( LOG_ERR, "Unexpected EndOfStream value: %llu\n", parseval );
               return -1;
            }
            break;
         default:
            LOG( LOG_ERR, "Unrecognized position value: \'%c\'\n", *parse );
            return -1;
      }
      foundvals++;
      parse = endptr + 1;
      if ( *endptr == ')' ) { break; }
   }
   if ( foundvals != 4 ) {
      LOG( LOG_ERR, "Failed to identify the expected number of position values\n" );
      return -1;
   }
   // parse data content info
   if ( strcmp( parse, FTAG_DATACONTENT_HEADER"(" ) ) {
      LOG( LOG_ERR, "Failed to locate data content header\n" );
      return -1;
   }
   parse += strlen( FTAG_DATACONTENT_HEADER"(" );
   foundvals = 0;
   while ( *parse != '\0' ) {
      // check for string values
      if ( strncmp( parse, "INIT", 4 ) ) {
         ftag->state = FTAG_INIT | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 4;
      }
      else if ( strncmp( parse, "SIZED", 5 ) ) {
         ftag->state = FTAG_SIZED | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 5;
      }
      else if ( strncmp( parse, "FIN", 3 ) ) {
         ftag->state = FTAG_FIN | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 3;
      }
      else if ( strncmp( parse, "COMP", 4 ) ) {
         ftag->state = FTAG_COMP | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 4;
      }
      else if ( strncmp( parse, "RO", 2 ) ) {
         ftag->state = FTAG_READABLE | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "WO", 2 ) ) {
         ftag->state = FTAG_WRITEABLE | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "RW", 2 ) ) {
         ftag->state = (FTAG_WRITEABLE & FTAG_READABLE) | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "NO", 2 ) ) {
         ftag->state = ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else {
         // attempt to parse the numeric value
         parseval = strtoull( parse + 1, &(endptr), 10 );
         if ( parseval > SIZE_MAX ) {
            LOG( LOG_ERR, "File position value \'%c\' exceeds size limits\n", *parse );
            return -1;
         }
         switch ( *parse ) {
            case 'N':
               ftag->protection.N = parseval;
               break;
            case 'E':
               ftag->protection.E = parseval;
               break;
            case 'O':
               ftag->protection.O = parseval;
               break;
            case 'S':
               ftag->protection.partsz = parseval;
               break;
            case 'B':
               ftag->bytes = parseval;
               break;
            case 'A':
               ftag->availbytes = parseval;
               break;
            case 'R':
               ftag->recoverybytes = parseval;
               break;
            case 'D':
               ftag->directbytes = parseval;
               break;
            default:
               LOG( LOG_ERR, "Unrecognized data content value: \'%c\'\n", *parse );
               return -1;
         }
      }
      if ( *endptr != '-'  &&  *endptr != ')' ) {
         LOG( LOG_ERR, "Unrecognized position value format\n" );
         return -1;
      }
      foundvals++;
      parse = endptr + 1;
      if ( *endptr == ')' ) { break; }
   }
   if ( foundvals != 10 ) {
      LOG( LOG_ERR, "Failed to identify the expected number of data content values\n" );
      return -1;
   }

   // finally done parsing
   if ( *parse != '\0' ) {
      LOG( LOG_ERR, "FTAG string has trailing characters: \"%s\"\n", parse );
      return -1;
   }
   return 0;
}

/**
 * Populate the given string buffer with the encoded values of the given ftag struct
 * @param const FTAG* ftag : Reference to the ftag struct to encode values from
 * @param char* tgtstr : String buffer to be populated with encoded info
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the encoded string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this 
 *                  indicates that insufficint buffer space was provided and the resulting 
 *                  output string was truncated.
 */
size_t ftag_tostr( const FTAG* ftag, char* tgtstr, size_t len ) {
   // check for NULL ftag
   if ( ftag == NULL ) {
      LOG( LOG_ERR, "Received a NULL FTAG reference\n" );
      return 0;
   }

   // only allow output of current version info
   if ( ftag->majorversion != FTAG_CURRENT_MAJORVERSION  ||
        ftag->minorversion != FTAG_CURRENT_MINORVERSION ) {
      LOG( LOG_ERR, "Cannot output strings for non-current FTAG versions\n" );
      return 0;
   }

   // keep track of total string length, even if we can't output that much
   size_t totsz = 0;

   // output version info first
   int prres = snprintf( tgtstr, len, "%s(%u.%.3u)", FTAG_VERSION_HEADER, ftag->majorversion, ftag->minorversion );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output version info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; }
   else { len = 0; }
   totsz += prres;

   // output stream identification info
   prres = snprintf( tgtstr, len, "%s(%s|%s|f%zu-d%zu)",
                     FTAG_STREAMINFO_HEADER,
                     ftag->ctag,
                     ftag->streamid,
                     ftag->objfiles,
                     ftag->objsize );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output stream info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; }
   else { len = 0; }
   totsz += prres;

   // output file position info
   prres = snprintf( tgtstr, len, "%s(F%zu-O%zu-@%zu-E%d)", FTAG_FILEPOSITION_HEADER, ftag->fileno, ftag->objno, ftag->offset, (int)(ftag->endofstream) );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output file position info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; }
   else { len = 0; }
   totsz += prres;

   // output data content info
   char* dstatestr = "INIT";
   FTAG_STATE dstate = ( ftag->state & FTAG_DATASTATE );
   if ( dstate == FTAG_SIZED ) {
      dstatestr = "SIZED";
   }
   else if ( dstate == FTAG_FIN ) {
      dstatestr = "FIN";
   }
   else if ( dstate == FTAG_COMP ) {
      dstatestr = "COMP";
   }
   char* daccstr = "NO"; // no access
   if ( ftag->state & FTAG_WRITEABLE ) {
      if ( ftag->state & FTAG_READABLE ) {
         daccstr = "RW"; // read write
      }
      else { daccstr = "WO"; } // write only
   }
   else if ( ftag->state & FTAG_READABLE ) {
      daccstr = "RO"; // read only
   }
   prres = snprintf( tgtstr, len, "%s(N%d-E%d-O%d-S%zu-B%zu-A%zu-R%zu-D%zu-%s-%s)",
                     FTAG_DATACONTENT_HEADER,
                     ftag->protection.N,
                     ftag->protection.E,
                     ftag->protection.O,
                     ftag->protection.partsz,
                     ftag->bytes,
                     ftag->availbytes,
                     ftag->recoverybytes,
                     ftag->directbytes,
                     dstatestr,
                     daccstr );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output data content info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; }
   else { len = 0; }
   totsz += prres;

   // all done!
   return totsz;
}

/**
 * Populate the given string buffer with the meta file ID string produced from the given ftag
 * @param const FTAG* ftag : Reference to the ftag struct to pull values from
 * @param char* tgtstr : String buffer to be populated with the meta file ID
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t ftag_metatgt( const FTAG* ftag, char* tgtstr, size_t len ) {
   // check for NULL ftag
   if ( ftag == NULL ) {
      LOG( LOG_ERR, "Received a NULL FTAG reference\n" );
      return 0;
   }
   // check for NULL string target
   if ( len  &&  tgtstr == NULL ) {
      LOG( LOG_ERR, "Receieved a NULL tgtstr value w/ non-zero len\n" );
      return 0;
   }
   return snprintf( tgtstr, len, "%s|%s.%zu", ftag->ctag, ftag->streamid, ftag->fileno );
}

/**
 * Populate the given string buffer with the object ID string produced from the given ftag
 * @param const FTAG* ftag : Reference to the ftag struct to pull values from
 * @param char* tgtstr : String buffer to be populated with the object ID
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t ftag_datatgt( const FTAG* ftag, char* tgtstr, size_t len ) {
   // check for NULL ftag
   if ( ftag == NULL ) {
      LOG( LOG_ERR, "Received a NULL FTAG reference\n" );
      return 0;
   }
   // check for NULL string target
   if ( len  &&  tgtstr == NULL ) {
      LOG( LOG_ERR, "Receieved a NULL tgtstr value w/ non-zero len\n" );
      return 0;
   }
   return snprintf( tgtstr, len, "%s|%s.%zu", ftag->ctag, ftag->streamid, ftag->objno );
}


