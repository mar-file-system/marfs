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
#ifdef DEBUG_TAGGING
#define DEBUG DEBUG_TAGGING
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "tagging"
#include <logging.h>
#include "tagging.h"

#include <string.h>
#include <errno.h>

//   -------------   INTERNAL DEFINITIONS    -------------

#define FTAG_VERSION_HEADER "VER"
#define FTAG_STREAMINFO_HEADER "STM"
#define FTAG_REFTREE_HEADER "REF"
#define FTAG_FILEPOSITION_HEADER "POS"
#define FTAG_DATACONTENT_HEADER "DAT"

#define RTAG_NAME "MARFS-REBUILD" // definied here, due to variability ( see rtag_getname() )
#define RTAG_VERSION_HEADER "VER"
#define RTAG_TIMESTAMP_HEADER "TIME"
#define RTAG_STRIPEINFO_HEADER "STP"
#define RTAG_DATAHEALTH_HEADER "DHLTH"
#define RTAG_METAHEALTH_HEADER "MHLTH"

#define GCTAG_VERSION_HEADER "VER"
#define GCTAG_SKIP_HEADER "SKIP"
#define GCTAG_PROGRESS_HEADER "PROG"


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
   if ( strncmp( ftagstr, FTAG_VERSION_HEADER"(", strlen(FTAG_VERSION_HEADER"(") ) ) {
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
   parse = endptr + 1; // skip over '.' separator
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
   parse = endptr + 1; // skip over ')' separator
   if ( ftag->majorversion != FTAG_CURRENT_MAJORVERSION  ||
        ftag->minorversion != FTAG_CURRENT_MINORVERSION ) {
      LOG( LOG_ERR, "Unrecognized version number: %u.%.3u\n", ftag->majorversion, ftag->minorversion );
      return -1;
   }
   // parse stream identification info
   if ( strncmp( parse, FTAG_STREAMINFO_HEADER"(", strlen(FTAG_STREAMINFO_HEADER"(") ) ) {
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
   *output = '\0'; // ensure we NULL-terminate the output string
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
   *output = '\0'; // ensure we NULL-terminate the output string
   if ( ftag->streamid == NULL ) {
      LOG( LOG_ERR, "Unrecognized streamid format\n" );
      return -1;
   }
   parse++; // skip over the '|' char
   char foundvals = 0;
   while ( *parse != '\0' ) {
      size_t* tgtval = NULL;
      if ( *parse == 'F' ) { tgtval = &(ftag->objfiles); }
      else if ( *parse == 'D' ) { tgtval = &(ftag->objsize); }
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
   // parse reference tree info
   if ( strncmp( parse, FTAG_REFTREE_HEADER"(", strlen(FTAG_REFTREE_HEADER"(") ) ) {
      LOG( LOG_ERR, "Failed to locate reference tree info header\n" );
      return -1;
   }
   parse += strlen( FTAG_REFTREE_HEADER"(" );
   foundvals = 0;
   while ( *parse != '\0' ) {
      int* tgtval = NULL;
      if ( *parse == 'B' ) { tgtval = &(ftag->refbreadth); }
      else if ( *parse == 'D' ) { tgtval = &(ftag->refdepth); }
      else if ( *parse == 'd' ) { tgtval = &(ftag->refdigits); }
      else { LOG( LOG_ERR, "Unrecognized ref tree value tag: \"%c\"\n", *parse ); return -1; }
      parseval = strtoull( parse + 1, &(endptr), 10 );
      if ( parseval > INT_MAX ) {
         LOG( LOG_ERR, "Parsed ref tree value exceeds size limits: %llu\n", parseval );
         return -1;
      }
      *tgtval = (int)parseval;
      LOG( LOG_INFO, "Parsed REFTREE '%c' value of %d\n", *parse, *tgtval );
      foundvals++;
      if ( *endptr != '-'  &&  *endptr != ')' ) {
         LOG( LOG_ERR, "Unrecognized stream value format\n" );
         return -1;
      }
      parse = endptr + 1; // skip over the separator char
      if ( *endptr == ')' ) { break; }
   }
   if ( foundvals != 3 ) {
      LOG( LOG_ERR, "Expected three reference tree values (breadth/depth/digits), but found %d\n", (int)foundvals );
      return -1;
   }
   // parse file position info
   if ( strncmp( parse, FTAG_FILEPOSITION_HEADER"(", strlen(FTAG_FILEPOSITION_HEADER"(") ) ) {
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
         case 'f':
            LOG( LOG_INFO, "Parsed FileNo value of %llu\n", parseval );
            ftag->fileno = parseval;
            break;
         case 'o':
            LOG( LOG_INFO, "Parsed ObjNo value of %llu\n", parseval );
            ftag->objno = parseval;
            break;
         case '@':
            LOG( LOG_INFO, "Parsed Offset value of %llu\n", parseval );
            ftag->offset = parseval;
            break;
         case 'e':
            LOG( LOG_INFO, "Parsed EOS value of %llu\n", parseval );
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
   if ( strncmp( parse, FTAG_DATACONTENT_HEADER"(", strlen(FTAG_DATACONTENT_HEADER"(") ) ) {
      LOG( LOG_ERR, "Failed to locate data content header\n" );
      return -1;
   }
   parse += strlen( FTAG_DATACONTENT_HEADER"(" );
   foundvals = 0;
   while ( *parse != '\0' ) {
      // check for string values
      if ( strncmp( parse, "INIT", 4 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'INIT' datastate\n" );
         ftag->state = FTAG_INIT | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 4;
      }
      else if ( strncmp( parse, "SIZED", 5 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'SIZED' datastate\n" );
         ftag->state = FTAG_SIZED | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 5;
      }
      else if ( strncmp( parse, "FIN", 3 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'FIN' datastate\n" );
         ftag->state = FTAG_FIN | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 3;
      }
      else if ( strncmp( parse, "COMP", 4 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'COMP' datastate\n" );
         ftag->state = FTAG_COMP | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 4;
      }
      else if ( strncmp( parse, "RO", 2 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'READ-ONLY' dataperms\n" );
         ftag->state = FTAG_READABLE | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "WO", 2 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'WRITE-ONLY' dataperms\n" );
         ftag->state = FTAG_WRITEABLE | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "RW", 2 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'READ-WRITE' dataperms\n" );
         ftag->state = (FTAG_WRITEABLE | FTAG_READABLE) | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "NO", 2 ) == 0 ) {
         LOG( LOG_INFO, "Parsed 'NO-ACCESS' dataperms\n" );
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
         LOG( LOG_INFO, "Parsed DATACON \'%c\' value of %llu\n", *parse, parseval );
         switch ( *parse ) {
            case 'n':
               ftag->protection.N = parseval;
               break;
            case 'e':
               ftag->protection.E = parseval;
               break;
            case 'o':
               ftag->protection.O = parseval;
               break;
            case 'p':
               ftag->protection.partsz = parseval;
               break;
            case 'b':
               ftag->bytes = parseval;
               break;
            case 'a':
               ftag->availbytes = parseval;
               break;
            case 'r':
               ftag->recoverybytes = parseval;
               break;
            default:
               LOG( LOG_ERR, "Unrecognized data content value: \'%c\'\n", *parse );
               return -1;
         }
      }
      if ( *endptr != '-'  &&  *endptr != ')' ) {
         LOG( LOG_ERR, "Unrecognized data value format: \"%s\" (%c)\n", parse, *endptr );
         return -1;
      }
      foundvals++;
      parse = endptr + 1;
      if ( *endptr == ')' ) { break; }
   }
   if ( foundvals != 9 ) {
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
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // output stream identification info
   prres = snprintf( tgtstr, len, "%s(%s|%s|F%zu-D%zu)",
                     FTAG_STREAMINFO_HEADER,
                     ftag->ctag,
                     ftag->streamid,
                     ftag->objfiles,
                     ftag->objsize );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output stream info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // output reference tree info
   prres = snprintf( tgtstr, len, "%s(B%d-D%d-d%d)",
                     FTAG_REFTREE_HEADER,
                     ftag->refbreadth,
                     ftag->refdepth,
                     ftag->refdigits );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output reference tree info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // output file position info
   prres = snprintf( tgtstr, len, "%s(f%zu-o%zu-@%zu-e%d)", FTAG_FILEPOSITION_HEADER, ftag->fileno, ftag->objno, ftag->offset, (int)(ftag->endofstream) );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output file position info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
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
   prres = snprintf( tgtstr, len, "%s(n%d-e%d-o%d-p%zu-b%zu-a%zu-r%zu-%s-%s)",
                     FTAG_DATACONTENT_HEADER,
                     ftag->protection.N,
                     ftag->protection.E,
                     ftag->protection.O,
                     ftag->protection.partsz,
                     ftag->bytes,
                     ftag->availbytes,
                     ftag->recoverybytes,
                     dstatestr,
                     daccstr );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output data content info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // all done!
   return totsz;
}

/**
 * Compare the content of the given FTAG references
 * @param const FTAG* ftag1 : First FTAG reference to compare
 * @param const FTAG* ftag2 : Second FTAG reference to compare
 * @return int : 0 if the two FTAGs match,
 *               1 if the two FTAGs differ,
 *               -1 if a failure occurred ( NULL ftag reference )
 */
int ftag_cmp( const FTAG* ftag1, const FTAG* ftag2 ) {
   // no NULL references allowed
   if ( ftag1 == NULL  ||  ftag2 == NULL ) {
      LOG( LOG_ERR, "Received a NULL FTAG reference\n" );
      return -1;
   }
   // compare all numeric componenets
   if ( ftag1->majorversion != ftag2->majorversion            ||
        ftag1->minorversion != ftag2->minorversion            ||
        ftag1->objfiles != ftag2->objfiles                    ||
        ftag1->objsize != ftag2->objsize                      ||
        ftag1->refbreadth != ftag2->refbreadth                ||
        ftag1->refdepth != ftag2->refdepth                    ||
        ftag1->refdigits != ftag2->refdigits                  ||
        ftag1->fileno != ftag2->fileno                        ||
        ftag1->objno != ftag2->objno                          ||
        ftag1->offset != ftag2->offset                        ||
        ftag1->endofstream != ftag2->endofstream              ||
        ftag1->protection.N != ftag2->protection.N            ||
        ftag1->protection.E != ftag2->protection.E            ||
        ftag1->protection.O != ftag2->protection.O            ||
        ftag1->protection.partsz != ftag2->protection.partsz  ||
        ftag1->bytes != ftag2->bytes                          ||
        ftag1->availbytes != ftag2->availbytes                ||
        ftag1->recoverybytes != ftag2->recoverybytes          ||
        ftag1->state != ftag2->state ) {
      LOG( LOG_INFO, "Detected numeric FTAG difference\n" );
      return 1;
   }
   // compare string elements
   if ( strcmp( ftag1->ctag, ftag2->ctag )  ||
        strcmp( ftag1->streamid, ftag2->streamid ) ) {
      LOG( LOG_INFO, "Detected string FTAG difference\n" );
      return 1;
   }
   return 0;
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
   // sanitize the streamID, removing any '|' chars
   char* sanstream = strdup( ftag->streamid );
   if ( sanstream == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate streamID: \"%s\"\n", ftag->streamid );
      return 0;
   }
   char* parse = sanstream;
   while ( *parse != '\0' ) {
      if ( *parse == '|' ) { *parse = '#'; }
      parse++;
   }
   size_t retval = snprintf( tgtstr, len, "%s|%s|%zu", ftag->ctag, sanstream, ftag->fileno );
   free( sanstream );
   return retval;
}

/**
 * Populate the given string buffer with the rebuild marker produced from the given ftag
 * @param const FTAG* ftag : Reference to the ftag struct to pull values from
 * @param char* tgtstr : String buffer to be populated with the rebuild marker name
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t ftag_rebuildmarker( const FTAG* ftag, char* tgtstr, size_t len ) {
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
   // sanitize the streamID, removing any '|' chars
   char* sanstream = strdup( ftag->streamid );
   if ( sanstream == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate streamID: \"%s\"\n", ftag->streamid );
      return 0;
   }
   char* parse = sanstream;
   while ( *parse != '\0' ) {
      if ( *parse == '|' ) { *parse = '#'; }
      parse++;
   }
   size_t retval = snprintf( tgtstr, len, "%s|%s|%zurebuild", ftag->ctag, sanstream, ftag->objno );
   free( sanstream );
   return retval;
}

/**
 * Populate the given string buffer with the repack marker produced from the given ftag
 * NOTE -- repack markers should NOT be randomly hashed to a reference location, they should 
 *         instead be placed directly alongside their corresponding original metatgt
 * @param const FTAG* ftag : Reference to the ftag struct to pull values from
 * @param char* tgtstr : String buffer to be populated with the repack marker name
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficient buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t ftag_repackmarker( const FTAG* ftag, char* tgtstr, size_t len ) {
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
   // sanitize the streamID, removing any '|' chars
   char* sanstream = strdup( ftag->streamid );
   if ( sanstream == NULL ) {
      LOG( LOG_ERR, "Failed to duplicate streamID: \"%s\"\n", ftag->streamid );
      return 0;
   }
   char* parse = sanstream;
   while ( *parse != '\0' ) {
      if ( *parse == '|' ) { *parse = '#'; }
      parse++;
   }
   size_t retval = snprintf( tgtstr, len, "%s|%s|%zuREPACK", ftag->ctag, sanstream, ftag->fileno );
   free( sanstream );
   return retval;
}

/**
 * Identify whether the given pathname refers to a rebuild marker, repack marker, or a meta file ID and 
 * which object or file number it is associated with
 * @param const char* metapath : String containing the meta pathname
 * @param char* entrytype : Reference to a char value to be populated by this function
 *                          If set to zero, the pathname is a meta file ID
 *                           ( return value is a file number )
 *                          If set to one, the pathname is a rebuild marker
 *                           ( return value is an object number )
 *                          If set to two, the pathname is a repack marker
 *                           ( return value is a file number )
 * @return ssize_t : File/Object number value, or -1 if a failure occurred
 */
ssize_t ftag_metainfo( const char* metapath, char* entrytype ) {
   // check for valid args
   if ( metapath == NULL ) {
      LOG( LOG_ERR, "Received a NULL metapath\n" );
      errno = EINVAL;
      return -1;
   }
   if ( entrytype == NULL ) {
      LOG( LOG_ERR, "Received a NULL entrytype pointer\n" );
      errno = EINVAL;
      return -1;
   }
   // parse over the metapath str, waiting for the end of the string
   const char* parse = metapath;
   const char* finfield = NULL;
   while ( *parse != '\0' ) {
      // keep track of the final '|' char before EOS
      if ( *parse == '|' ) { finfield = parse; }
      parse++;
   }
   // verify that we located the expected tail string
   if ( finfield == NULL  ||  *(finfield + 1) == '\0' ) {
      LOG( LOG_ERR, "Provided string has no '|<file/objno>' tail: \"%s\"\n", metapath );
      errno = EINVAL;
      return -1;
   }
   // parse the fileno value
   char* endptr = NULL;
   unsigned long long parseval = strtoull( finfield + 1, &(endptr), 10 );
   if ( endptr == NULL ) {
      LOG( LOG_ERR, "Failed to parse file/objno tail: \"%s\"\n", metapath );
      errno = EINVAL;
      return -1;
   }
   if ( *endptr == 'r'  &&  strncmp( endptr, "rebuild", 7 ) == 0 ) {
      LOG( LOG_INFO, "Marking pathname as a rebuild marker\n" );
      endptr += 7; // skip over rebuild marker
      *entrytype = 1;
   }
   else if ( *endptr == 'R'  &&  strncmp( endptr, "REPACK", 6 ) == 0 ) {
      LOG( LOG_INFO, "Marking pathname as a repack marker\n" );
      endptr += 6; // skip over repack marker
      *entrytype = 2;
   }
   else {
      LOG( LOG_INFO, "Marking pathname as a meta file ID\n" );
      *entrytype = 0;
   }
   if ( *endptr != '\0' ) {
      LOG( LOG_ERR, "Encountered trailing chars in file/objno string field: \"%s\"\n", endptr );
      errno = EINVAL;
      return -1;
   }
   if ( parseval > SSIZE_MAX ) {
      LOG( LOG_ERR, "Parsed fileno value exceeds return value bounds: %llu\n", parseval );
      errno = ERANGE;
      return -1;
   }
   return (ssize_t)parseval;
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
   size_t retval = snprintf( tgtstr, len, "%s|%s|%zu", ftag->ctag, ftag->streamid, ftag->objno );
   return retval;
}

// MARFS REBUILD TAG  --  attached to damaged marfs files, providing rebuild info

/**
 * Generate the appropraite RTAG name value for a specific data object
 * @param size_t objno : Object number associated with the RTAG
 * @return char* : String name of the RTAG value, or NULL on failure
 *                 NOTE -- it is the caller's responsibility to free this
 */
char* rtag_getname( size_t objno ) {
   // identify the rebuild tag name
   ssize_t rtagnamelen = snprintf( NULL, 0, "%s-%zu", RTAG_NAME, objno );
   if ( rtagnamelen < 1 ) {
      LOG( LOG_ERR, "Failed to identify the length of rebuild tag for object %zu\n", objno );
      return NULL;
   }
   char* rtagname = malloc( sizeof(char) * (rtagnamelen + 1) );
   if ( rtagname == NULL ) {
      LOG( LOG_ERR, "Failed to allocate space for rebuild tag of length %zd\n", rtagnamelen );
      return NULL;
   }
   if ( snprintf( rtagname, rtagnamelen + 1, "%s-%zu", RTAG_NAME, objno ) != rtagnamelen ) {
      LOG( LOG_ERR, "Rebuild tag name has inconsistent length\n" );
      free(rtagname);
      return NULL;
   }
   return rtagname;
}

/**
 * Initialize an RTAG based on the provided string value
 * @param ne_state* rtag : Reference to the ne_state structure to be populated
 *                         NOTE -- If this RTAG has allocated (non-NULL) ne_state.meta/data_status arrays,
 *                                 they are assumed to be of rtag.stripewidth length.  If this length matches
 *                                 that of the parsed RTAG, the arrays will be reused.  Otherwise, the arrays
 *                                 will be freed and recreated with an appropriate length.
 * @param size_t stripewidth : Expected N+E stripe width
 * @param const char* rtagstr : Reference to the string to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int rtag_initstr( RTAG* rtag, const char* rtagstr ) {
   // check for NULL rtag
   if ( rtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_state reference\n" );
      errno = EINVAL;
      return -1;
   }
   // parse version info first
   const char* parse = rtagstr;
   if ( strncmp( parse, RTAG_VERSION_HEADER "(", strlen(RTAG_VERSION_HEADER) + 1 ) ) {
      LOG( LOG_ERR, "Unexpected version header for RTAG string\n" );
      errno = EINVAL;
      return -1;
   }
   parse += strlen(RTAG_VERSION_HEADER) + 1;
   char* endptr = NULL;
   unsigned long long parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != '.' ) {
      LOG( LOG_ERR, "RTAG version string has unexpected format\n" );
      errno = EINVAL;
      return -1;
   }
   if ( parseval != RTAG_CURRENT_MAJORVERSION ) {
      LOG( LOG_ERR, "Unexpected RTAG major version: %llu\n", parseval );
      errno = EINVAL;
      return -1;
   }
   unsigned int parsedmajorversion = (unsigned int)parseval;
   parse = endptr + 1;
   parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != ')' ) {
      LOG( LOG_ERR, "RTAG version string has unexpected format\n" );
      errno = EINVAL;
      return -1;
   }
   if ( parseval != RTAG_CURRENT_MINORVERSION ) {
      LOG( LOG_ERR, "Unexpected RTAG minor version: %llu\n", parseval );
      errno = EINVAL;
      return -1;
   }
   unsigned int parsedminorversion = (unsigned int)parseval;
   parse = endptr + 1;
   // parse the tag timestamp value
   if ( strncmp( parse, RTAG_TIMESTAMP_HEADER "(", strlen(RTAG_TIMESTAMP_HEADER) + 1 ) ) {
      LOG( LOG_ERR, "Invalid stripe info header in RTAG string\n" );
      errno = EINVAL;
      return -1;
   }
   parse += strlen(RTAG_TIMESTAMP_HEADER) + 1;
   parseval = strtoull( parse, &(endptr), 10 );
   if ( parseval > SIZE_MAX ) {
      LOG( LOG_ERR, "Parsed timestamp value exceeds type bounds: \"%llu\"\n", parseval );
      errno = ERANGE;
      return -1;
   }
   time_t parsedcreatetime = (time_t)parseval;
   if ( *endptr != ')' ) {
      LOG( LOG_ERR, "Unexpected terminating char on timestamp value: '%c'\n", *endptr );
      errno = EINVAL;
      return -1;
   }
   parse = endptr + 1; // progress to the next element
   // check for premature end of tag ( stripe info / health is not 'explicitly' required )
   if ( *parse == '\0' ) {
      rtag->majorversion = parsedmajorversion;
      rtag->minorversion = parsedminorversion;
      rtag->createtime = parsedcreatetime;
      rtag->stripewidth = 0;
      rtag->stripestate.versz = 0;
      rtag->stripestate.blocksz = 0;
      rtag->stripestate.totsz = 0;
      if ( rtag->stripestate.meta_status ) { free( rtag->stripestate.meta_status ); }
      if ( rtag->stripestate.data_status ) { free( rtag->stripestate.data_status ); }
      rtag->stripestate.meta_status = NULL;
      rtag->stripestate.data_status = NULL;
      return 0;
   }
   // parse stripe info values
   if ( strncmp( parse, RTAG_STRIPEINFO_HEADER "(", strlen(RTAG_STRIPEINFO_HEADER) + 1 ) ) {
      LOG( LOG_ERR, "Invalid stripe info header in RTAG string\n" );
      errno = EINVAL;
      return -1;
   }
   parse += strlen(RTAG_STRIPEINFO_HEADER) + 1;
   size_t parsedstripewidth = 0;
   size_t parsedversz = 0;
   size_t parsedblocksz = 0;
   size_t parsedtotsz = 0;
   while ( *parse != '\0' ) {
      size_t* tgtval = NULL;
      switch ( *parse ) {
         case 'w':
            tgtval = &(parsedstripewidth);
            break;
         case 'v':
            tgtval = &(parsedversz);
            break;
         case 'b':
            tgtval = &(parsedblocksz);
            break;
         case 't':
            tgtval = &(parsedtotsz);
            break;
         default:
            LOG( LOG_ERR, "Unrecognized stripe info value tag: '%c'\n", *parse );
            errno = EINVAL;
            return -1;
      }
      parseval = strtoull( parse+1, &(endptr), 10 );
      if ( parseval > SIZE_MAX  ||  parseval == 0 ) {
         LOG( LOG_ERR, "Parsed '%c' value exceeds type bounds: \"%llu\"\n", *parse, parseval );
         errno = ERANGE;
         return -1;
      }
      *tgtval = (size_t)parseval;
      if ( *endptr != '|' ) {
         if ( *endptr == ')' ) { break; } // end of stripe info
         LOG( LOG_ERR, "Unexpected terminating char on '%c' value: '%c'\n", *parse, *endptr );
         errno = EINVAL;
         return -1;
      }
      parse = endptr + 1; // progress to the next element
   }
   if ( !(parsedstripewidth) ||  !(parsedversz) || !(parsedblocksz) || !(parsedtotsz) ) {
      LOG( LOG_ERR, "Missing some required stripe info values\n" );
      errno = EINVAL;
      return -1;
   }
   parse = endptr + 1;
   // parse health strings
   char datahval = 0;
   char metahval = 0;
   char* parseddata_status = rtag->stripestate.data_status;
   if ( rtag->stripewidth != parsedstripewidth  ||  rtag->stripestate.data_status == NULL ) {
      parseddata_status = calloc( sizeof(char), parsedstripewidth );
      if ( parseddata_status == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a new data_status array\n" );
         return -1;
      }
   }
   char* parsedmeta_status = rtag->stripestate.meta_status;
   if ( rtag->stripewidth != parsedstripewidth  ||  rtag->stripestate.meta_status == NULL ) {
      parsedmeta_status = calloc( sizeof(char), parsedstripewidth );
      if ( parsedmeta_status == NULL ) {
         LOG( LOG_ERR, "Failed to allocate a new meta_status array\n" );
         return -1;
      }
   }
   while ( *parse != '\0' ) {
      // check for data vs meta health
      char* healthlist = NULL;
      if ( strncmp( parse, RTAG_DATAHEALTH_HEADER "(", strlen(RTAG_DATAHEALTH_HEADER) + 1 ) == 0 ) {
         if ( datahval ) {
            LOG( LOG_ERR, "Detected duplicate data health stanza\n" );
            break;
         }
         datahval = 1;
         healthlist = parseddata_status;
         parse += strlen(RTAG_DATAHEALTH_HEADER) + 1;
      }
      else if ( strncmp( parse, RTAG_METAHEALTH_HEADER "(", strlen(RTAG_METAHEALTH_HEADER) + 1 ) == 0 ) {
         if ( metahval ) {
            LOG( LOG_ERR, "Detected duplicate meta health stanza\n" );
            break;
         }
         metahval = 1;
         healthlist = parsedmeta_status;
         parse += strlen(RTAG_METAHEALTH_HEADER) + 1;
      }
      else {
         LOG( LOG_ERR, "Unrecognized RTAG health header\n" );
         break;
      }
      // parse over health values
      size_t hindex = 0;
      while ( *parse != ')'  &&  hindex < parsedstripewidth ) {
         switch ( *parse ) {
            case '1':
               *(healthlist + hindex) = 1;
               break;
            case '0':
               *(healthlist + hindex) = 0;
               break;
            default:
               LOG( LOG_ERR, "Health value is neither '0' nor '1': '%c'\n", *parse );
               if ( rtag->stripestate.meta_status != parsedmeta_status ) { free( parsedmeta_status ); }
               if ( rtag->stripestate.data_status != parseddata_status ) { free( parseddata_status ); }
               errno = EINVAL;
               return -1;
         }
         parse++;
         if ( *parse == '-' ) { parse++; hindex++; } // progress to the next health value
      }
      if ( *parse != ')' ) {
         if ( hindex == parsedstripewidth ) {
            LOG( LOG_ERR, "ne_state struct has insufficient stripewidth value\n" );
            if ( rtag->stripestate.meta_status != parsedmeta_status ) { free( parsedmeta_status ); }
            if ( rtag->stripestate.data_status != parseddata_status ) { free( parseddata_status ); }
            errno = EFBIG;
            return -1;
         }
         LOG( LOG_ERR, "Unexpected char in health info string: '%c'\n", *parse );
         if ( rtag->stripestate.meta_status != parsedmeta_status ) { free( parsedmeta_status ); }
         if ( rtag->stripestate.data_status != parseddata_status ) { free( parseddata_status ); }
         errno = EINVAL;
         return -1;
      }
      parse++; // progress beyond the end of the health stanza
   }
   if ( *parse != '\0' ) { // catch previous error conditions
      if ( rtag->stripestate.meta_status != parsedmeta_status ) { free( parsedmeta_status ); }
      if ( rtag->stripestate.data_status != parseddata_status ) { free( parseddata_status ); }
      errno = EINVAL;
      return -1;
   }
   if ( metahval != 1  ||  datahval != 1 ) {
      LOG( LOG_ERR, "Failed to locate all expected health stanzas\n" );
      if ( rtag->stripestate.meta_status != parsedmeta_status ) { free( parsedmeta_status ); }
      if ( rtag->stripestate.data_status != parseddata_status ) { free( parseddata_status ); }
      errno = EINVAL;
      return -1;
   }
   // finally populate any health values we didn't previously
   rtag->majorversion = parsedmajorversion;
   rtag->minorversion = parsedminorversion;
   rtag->createtime = parsedcreatetime;
   rtag->stripewidth = parsedstripewidth;
   rtag->stripestate.versz = parsedversz;
   rtag->stripestate.blocksz = parsedblocksz;
   rtag->stripestate.totsz = parsedtotsz;
   if ( rtag->stripestate.meta_status  &&
        rtag->stripestate.meta_status != parsedmeta_status ) { free( rtag->stripestate.meta_status ); }
   if ( rtag->stripestate.data_status  &&
        rtag->stripestate.data_status != parseddata_status ) { free( rtag->stripestate.data_status ); }
   rtag->stripestate.meta_status = parsedmeta_status;
   rtag->stripestate.data_status = parseddata_status;
   return 0;
}

/**
 * Populate a string based on the provided RTAG
 * @param const RTAG* rtag : Reference to the RTAG structure to pull values from
 * @param char* tgtstr : Reference to the string to be populated
 * @param size_t len : Allocated length of the target length
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t rtag_tostr( const RTAG* rtag, char* tgtstr, size_t len ) {
   // check for NULL rtag
   if ( rtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_state reference\n" );
      errno = EINVAL;
      return 0;
   }
   if ( rtag->stripewidth  &&  (rtag->stripestate.meta_status == NULL  ||  rtag->stripestate.data_status == NULL) ) {
      LOG( LOG_ERR, "Received RTAG has undefined meta/data_status\n" );
      errno = EINVAL;
      return 0;
   }

   // keep track of total string length, even if we can't output that much
   size_t totsz = 0;

   // output version info first
   int prres = snprintf( tgtstr, len, "%s(%u.%.3u)", RTAG_VERSION_HEADER, rtag->majorversion, rtag->minorversion );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output version info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // output timestamp value
   prres = snprintf( tgtstr, len, "%s(%zu)", RTAG_TIMESTAMP_HEADER, (size_t)rtag->createtime );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output timestamp info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // potentially stop here
   if ( rtag->stripewidth == 0 ) {
      return totsz;
   }

   // output stripe info
   prres = snprintf( tgtstr, len, "%s(w%zu|v%zu|b%zu|t%zu)",
                     RTAG_STRIPEINFO_HEADER,
                     rtag->stripewidth,
                     rtag->stripestate.versz,
                     rtag->stripestate.blocksz,
                     rtag->stripestate.totsz );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output stripe info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // output data health header
   prres = snprintf( tgtstr, len, "%s(", RTAG_DATAHEALTH_HEADER );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output data health header string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;
   // output per-block data health
   size_t curblock = 0;
   for ( ; curblock < rtag->stripewidth; curblock++ ) {
      char* blkhealth = rtag->stripestate.data_status + curblock;
      if ( curblock ) {
         // print flag with seperator
         prres = snprintf( tgtstr, len, "-%c", (*blkhealth) ? '1' : '0' );
      }
      else {
         // print flag without seperator
         prres = snprintf( tgtstr, len, "%c", (*blkhealth) ? '1' : '0' );
      }
      if ( prres < 1 ) {
         LOG( LOG_ERR, "Failed to output data health string for block %zu\n", curblock );
         return 0;
      }
      if ( len > prres ) { len -= prres; tgtstr += prres; }
      else { len = 0; }
      totsz += prres;
   }
   // output per-block data health tail AND meta health header
   prres = snprintf( tgtstr, len, ")%s(", RTAG_METAHEALTH_HEADER );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output meta health header string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;
   // output per-block meta health info
   for ( curblock = 0; curblock < rtag->stripewidth; curblock++ ) {
      char* blkhealth = rtag->stripestate.meta_status + curblock;
      if ( curblock ) {
         // print flag with seperator
         prres = snprintf( tgtstr, len, "-%c", (*blkhealth) ? '1' : '0' );
      }
      else {
         // print flag without seperator
         prres = snprintf( tgtstr, len, "%c", (*blkhealth) ? '1' : '0' );
      }
      if ( prres < 1 ) {
         LOG( LOG_ERR, "Failed to output meta health string for block %zu\n", curblock );
         return 0;
      }
      if ( len > prres ) { len -= prres; tgtstr += prres; }
      else { len = 0; }
      totsz += prres;
   }
   // output per-block meta health tail
   prres = snprintf( tgtstr, len, ")" );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output meta health tail string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   return totsz;
}

/**
 * Allocates internal memory for the given RTAG ( based on rtag->stripewidth )
 * @param RTAG* rtag : Reference to the RTAG to be allocated
 * @return int : Zero on success, or -1 on failure
 */
int rtag_alloc( RTAG* rtag ) {
   // check for NULL rtag
   if ( rtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_state reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( rtag->stripewidth  &&  rtag->stripestate.meta_status != NULL  &&  rtag->stripestate.data_status != NULL ) {
      LOG( LOG_INFO, "RTAG appears to already have internal allocations created\n" );
      return 0;
   }
   // free any existing arrays, if necessary
   size_t stripewidth = rtag->stripewidth;
   rtag_free( rtag );
   rtag->stripestate.meta_status = calloc( sizeof(char), stripewidth );
   rtag->stripestate.data_status = calloc( sizeof(char), stripewidth );
   if ( rtag->stripestate.meta_status == NULL  ||  rtag->stripestate.data_status == NULL ) {
      LOG( LOG_ERR, "Failed to allocate RTAG stipe state arrays\n" );
      if ( rtag->stripestate.meta_status ) { free( rtag->stripestate.meta_status ); }
      if ( rtag->stripestate.data_status ) { free( rtag->stripestate.data_status ); }
      rtag->stripewidth = 0;
      rtag->stripestate.meta_status = NULL;
      rtag->stripestate.data_status = NULL;
      return -1;
   }
   rtag->stripewidth = stripewidth;
   return 0;
}

/**
 * Frees internal memory allocations of the given RTAG
 */
void rtag_free( RTAG* rtag ) {
   if ( rtag ) {
      if ( rtag->stripestate.meta_status ) { free( rtag->stripestate.meta_status ); }
      if ( rtag->stripestate.data_status ) { free( rtag->stripestate.data_status ); }
      rtag->stripestate.meta_status = NULL;
      rtag->stripestate.data_status = NULL;
      rtag->stripewidth = 0;
   }
}

/**
 * Produce a duplicate of the given RTAG
 * @param const RTAG* srcrtag : Reference to the RTAG to duplicate
 * @param RTAG* destrtag : Reference to the RTAG to be copied into
 *                         NOTE -- This func will call rtag_free() on this reference
 * @return int : Zero on success, or -1 on failure
 */
int rtag_dup( const RTAG* srcrtag, RTAG* destrtag ) {
   // check for NULL rtags
   if ( srcrtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL src RTAG reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( destrtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL dest RTAG reference\n" );
      errno = EINVAL;
      return -1;
   }
   // produce duplicates of the src rtag's stripestate arrays, if necessary
   char* new_meta_status = NULL;
   char* new_data_status = NULL;
   if ( srcrtag->stripewidth ) {
      new_meta_status = calloc( srcrtag->stripewidth, sizeof(char) );
      new_data_status = calloc( srcrtag->stripewidth, sizeof(char) );
      if ( new_meta_status == NULL  ||  new_data_status == NULL ) {
         LOG( LOG_ERR, "Failed to allocate duplicate meta/data status arrays\n" );
         if ( new_meta_status ) { free( new_meta_status ); }
         if ( new_data_status ) { free( new_data_status ); }
         return -1;
      }
      memcpy( new_meta_status, srcrtag->stripestate.meta_status, srcrtag->stripewidth * sizeof(char) );
      memcpy( new_data_status, srcrtag->stripestate.data_status, srcrtag->stripewidth * sizeof(char) );
   }
   // call free on the dest rtag, to ensure we aren't leaking memory
   rtag_free( destrtag );
   // directly copy all src rtag values
   *destrtag = *srcrtag;
   // then overwrite pointer values to new allocations
   destrtag->stripestate.meta_status = new_meta_status;
   destrtag->stripestate.data_status = new_data_status;
   return 0;
}


// MARFS Garbage Collection TAG  -- attached to files when subsequent datastream references have been deleted

/**
 * Initialize a GCTAG based on the provided string value
 * @param GCTAG* gctag : Reference to the GCTAG structure to be populated
 * @param const char* gctagstr : Reference to the string to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int gctag_initstr( GCTAG* gctag, char* gctagstr ) {
   // check args
   if ( gctag == NULL ) {
      LOG( LOG_ERR, "Received a NULL gctag arg\n" );
      errno = EINVAL;
      return -1;
   }
   if ( gctagstr == NULL ) {
      LOG( LOG_ERR, "Received a NULL gctagstr arg\n" );
      errno = EINVAL;
      return -1;
   }
   // parse version info first
   const char* parse = gctagstr;
   if ( strncmp( parse, GCTAG_VERSION_HEADER "(", strlen(GCTAG_VERSION_HEADER) + 1 ) ) {
      LOG( LOG_ERR, "Unexpected version header for GCTAG string\n" );
      errno = EINVAL;
      return -1;
   }
   parse += strlen(GCTAG_VERSION_HEADER) + 1;
   char* endptr = NULL;
   unsigned long long parseval = strtoull( parse, &(endptr), 10 );
   if ( endptr == NULL  ||  *endptr != '.'  ||  parseval == ULONG_MAX ) {
      LOG( LOG_ERR, "Failed to parse major version value of GCTAG\n" );
      errno = EINVAL;
      return -1;
   }
   if ( parseval != GCTAG_CURRENT_MAJORVERSION ) {
      LOG( LOG_ERR, "Unexpected GCTAG major version: %llu\n", parseval );
      errno = EINVAL;
      return -1;
   }
   parse = endptr + 1;
   parseval = strtoull( parse, &(endptr), 10 );
   if ( *endptr != ')' ) {
      LOG( LOG_ERR, "GCTAG version string has unexpected format\n" );
      errno = EINVAL;
      return -1;
   }
   if ( parseval != GCTAG_CURRENT_MINORVERSION ) {
      LOG( LOG_ERR, "Unexpected GCTAG minor version: %llu\n", parseval );
      errno = EINVAL;
      return -1;
   }
   parse = endptr + 1;
   // parse skip info
   if ( strncmp( parse, GCTAG_SKIP_HEADER "(", strlen(GCTAG_SKIP_HEADER) + 1 ) ) {
      LOG( LOG_ERR, "Unexpected SKIP header for GCTAG string\n" );
      errno = EINVAL;
      return -1;
   }
   parse += strlen(GCTAG_SKIP_HEADER) + 1;
   endptr = NULL;
   parseval = strtoull( parse, &(endptr), 10 );
   if ( endptr == NULL  ||  *endptr != '|'  ||  parseval == ULONG_MAX ) {
      LOG( LOG_ERR, "Failed to parse refcnt value of GCTAG\n" );
      errno = EINVAL;
      return -1;
   } // tag populated later
   parse = endptr + 1;
   char eos = 0;
   if ( *parse == 'E' ) {
      eos = 1;
   }
   else if ( *parse != '-' ) {
      LOG( LOG_ERR, "GCTAG has inappriate EOS value\n" );
      errno = EINVAL;
      return -1;
   }
   parse++;
   if ( *parse != ')' ) {
      LOG( LOG_ERR, "Unexpected tail string of SKIP stanza\n" );
      errno = EINVAL;
      return -1;
   }
   parse++;
   // potentially parse PROGRESS stanza
   char delzero = 0;
   char inprog = 0;
   if ( *parse != '\0' ) {
      // parse stanza header
      if ( strncmp( parse, GCTAG_PROGRESS_HEADER "(", strlen(GCTAG_PROGRESS_HEADER) + 1 ) ) {
         LOG( LOG_ERR, "Unexpected PROGRESS header for GCTAG string\n" );
         errno = EINVAL;
         return -1;
      }
      parse += strlen(GCTAG_PROGRESS_HEADER) + 1;
      // parse delzero value
      if ( *parse == 'D' ) {
         delzero = 1;
      }
      else if ( *parse != '-' ) {
         LOG( LOG_ERR, "GCTAG has inappriate DEL-ZERO value\n" );
         errno = EINVAL;
         return -1;
      }
      parse++;
      if ( *parse != '|' ) {
         LOG( LOG_ERR, "GCTAG has inappriate PROGRESS stanza format\n" );
         errno = EINVAL;
         return -1;
      }
      parse++;
      // parse inprog value
      if ( *parse == 'I' ) {
         inprog = 1;
      }
      else if ( *parse != '-' ) {
         LOG( LOG_ERR, "GCTAG has inappriate INPROG value\n" );
         errno = EINVAL;
         return -1;
      }
      parse++;
      if ( *parse != ')' ) {
         LOG( LOG_ERR, "GCTAG has inappriate PROGRESS stanza tail\n" );
         errno = EINVAL;
         return -1;
      }
      parse++;
   }
   gctag->refcnt = (size_t) parseval;
   gctag->eos = eos;
   gctag->delzero = delzero;
   gctag->inprog = inprog;
   return 0;
}

/**
 * Populate a string based on the provided GCTAG
 * @param const GCTAG* gctag : Reference to the GCTAG structure to pull values from
 * @param char* tgtstr : Reference to the string to be populated
 * @param size_t len : Allocated length of the target length
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t gctag_tostr( GCTAG* gctag, char*tgtstr, size_t len ) {
   // check for NULL args
   if ( gctag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_state reference\n" );
      errno = EINVAL;
      return 0;
   }

   // keep track of total string length, even if we can't output that much
   size_t totsz = 0;

   // output version info first
   int prres = snprintf( tgtstr, len, "%s(%u.%.3u)", GCTAG_VERSION_HEADER, GCTAG_CURRENT_MAJORVERSION, GCTAG_CURRENT_MINORVERSION );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output version info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;
   // output skip info
   prres = snprintf( tgtstr, len, "%s(%zu|%c)", GCTAG_SKIP_HEADER, gctag->refcnt, (gctag->eos) ? 'E' : '-' );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output refcnt value %zu\n", gctag->refcnt );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;
   // potentially output inprog and delzero flag
   if ( gctag->inprog  ||  gctag->delzero ) {
      prres = snprintf( tgtstr, len, "%s(%c|%c)", GCTAG_PROGRESS_HEADER,
                                                   (gctag->delzero) ? 'D' : '-',
                                                   (gctag->inprog) ? 'I' : '-' );
      if ( prres < 1 ) {
         LOG( LOG_ERR, "Failed to output refcnt value %zu\n", gctag->refcnt );
         return 0;
      }
      if ( len > prres ) { len -= prres; tgtstr += prres; }
      else { len = 0; }
      totsz += prres;
   }

   return totsz;
}


