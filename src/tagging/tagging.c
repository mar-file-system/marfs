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
#define FTAG_FILEPOSITION_HEADER "POS"
#define FTAG_DATACONTENT_HEADER "DAT"

#define RTAG_VERSION_HEADER "VER"
#define RTAG_STRIPEINFO_HEADER "STP"
#define RTAG_DATAHEALTH_HEADER "DHLTH"
#define RTAG_METAHEALTH_HEADER "MHLTH"


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
            ftag->fileno = parseval;
            break;
         case 'o':
            ftag->objno = parseval;
            break;
         case '@':
            ftag->offset = parseval;
            break;
         case 'e':
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
         ftag->state = FTAG_INIT | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 4;
      }
      else if ( strncmp( parse, "SIZED", 5 ) == 0 ) {
         ftag->state = FTAG_SIZED | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 5;
      }
      else if ( strncmp( parse, "FIN", 3 ) == 0 ) {
         ftag->state = FTAG_FIN | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 3;
      }
      else if ( strncmp( parse, "COMP", 4 ) == 0 ) {
         ftag->state = FTAG_COMP | ( ftag->state & ~(FTAG_DATASTATE) );
         endptr = parse + 4;
      }
      else if ( strncmp( parse, "RO", 2 ) == 0 ) {
         ftag->state = FTAG_READABLE | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "WO", 2 ) == 0 ) {
         ftag->state = FTAG_WRITEABLE | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "RW", 2 ) == 0 ) {
         ftag->state = (FTAG_WRITEABLE | FTAG_READABLE) | ( ftag->state & FTAG_DATASTATE );
         endptr = parse + 2;
      }
      else if ( strncmp( parse, "NO", 2 ) == 0 ) {
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
         LOG( LOG_INFO, "Parsed \'%c\' value of %llu\n", *parse, parseval );
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
   size_t retval = snprintf( tgtstr, len, "%s|%s|%zuREPACK", ftag->ctag, sanstream, ftag->objno );
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
 * Initialize a ne_state value based on the provided string value
 * @param ne_state* rtag : Reference to the ne_state structure to be populated
 * @param size_t stripewidth : Expected N+E stripe width
 * @param const char* rtagstr : Reference to the string to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int rtag_initstr( ne_state* rtag, size_t stripewidth, const char* rtagstr ) {
   // check for NULL rtag
   if ( rtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_state reference\n" );
      errno = EINVAL;
      return -1;
   }
   if ( rtag->meta_status == NULL  ||  rtag->data_status == NULL ) {
      LOG( LOG_ERR, "Received ne_state struct has undefined meta/data_status\n" );
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
   parse = endptr + 1;
   // parse stripe info values
   if ( strncmp( parse, RTAG_STRIPEINFO_HEADER "(", strlen(RTAG_STRIPEINFO_HEADER) + 1 ) ) {
      LOG( LOG_ERR, "Invalid stripe info header in RTAG string\n" );
      errno = EINVAL;
      return -1;
   }
   parse += strlen(RTAG_STRIPEINFO_HEADER) + 1;
   char verszval = 0;
   char blockszval = 0;
   char totszval = 0;
   while ( *parse != '\0' ) {
      size_t* tgtval = NULL;
      switch ( *parse ) {
         case 'v':
            tgtval = &(rtag->versz);
            verszval = 1;
            break;
         case 'b':
            tgtval = &(rtag->blocksz);
            blockszval = 1;
            break;
         case 't':
            tgtval = &(rtag->totsz);
            totszval = 1;
            break;
         default:
            LOG( LOG_ERR, "Unrecognized stripe info value tag: '%c'\n", *parse );
            errno = EINVAL;
            return -1;
      }
      parseval = strtoull( parse+1, &(endptr), 10 );
      if ( parseval > SIZE_MAX ) {
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
   if ( !(verszval) || !(blockszval) || !(totszval) ) {
      LOG( LOG_ERR, "Missing some required stripe info values\n" );
      errno = EINVAL;
      return -1;
   }
   parse = endptr + 1;
   // parse health strings
   char datahval = 0;
   char metahval = 0;
   while ( *parse != '\0' ) {
      // check for data vs meta health
      char* healthlist = NULL;
      if ( strncmp( parse, RTAG_DATAHEALTH_HEADER "(", strlen(RTAG_DATAHEALTH_HEADER) + 1 ) == 0 ) {
         if ( datahval ) {
            LOG( LOG_ERR, "Detected duplicate data health stanza\n" );
            break;
         }
         datahval = 1;
         healthlist = rtag->data_status;
         parse += strlen(RTAG_DATAHEALTH_HEADER) + 1;
      }
      else if ( strncmp( parse, RTAG_METAHEALTH_HEADER "(", strlen(RTAG_METAHEALTH_HEADER) + 1 ) == 0 ) {
         if ( metahval ) {
            LOG( LOG_ERR, "Detected duplicate meta health stanza\n" );
            break;
         }
         metahval = 1;
         healthlist = rtag->meta_status;
         parse += strlen(RTAG_METAHEALTH_HEADER) + 1;
      }
      else {
         LOG( LOG_ERR, "Unrecognized RTAG health header\n" );
         break;
      }
      // parse over health values
      size_t hindex = 0;
      while ( *parse != ')'  &&  hindex < stripewidth ) {
         switch ( *parse ) {
            case '1':
               *(healthlist + hindex) = 1;
               break;
            case '0':
               *(healthlist + hindex) = 0;
               break;
            default:
               LOG( LOG_ERR, "Health value is neither '0' nor '1': '%c'\n", *parse );
               errno = EINVAL;
               return -1;
         }
         parse++;
         if ( *parse == '-' ) { parse++; hindex++; } // progress to the next health value
      }
      if ( *parse != ')' ) {
         LOG( LOG_ERR, "Unexpected char in health info string: '%c'\n", *parse );
         errno = EINVAL;
         return -1;
      }
      parse++; // progress beyond the end of the health stanza
   }
   if ( *parse != '\0' ) { errno = EINVAL; return -1; } // catch previous error conditions
   if ( metahval != 1  ||  datahval != 1 ) {
      LOG( LOG_ERR, "Failed to locate all expected health stanzas\n" );
      errno = EINVAL;
      return -1;
   }
   return 0;
}

/**
 * Populate a string based on the provided ne_state value
 * @param const ne_state* rtag : Reference to the ne_state structure to pull values from
 * @param size_t stripewidth : Current N+E stripe width ( length of allocated health lists )
 * @param char* tgtstr : Reference to the string to be populated
 * @param size_t len : Allocated length of the target length
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t rtag_tostr( const ne_state* rtag, size_t stripewidth, char* tgtstr, size_t len ) {
   // check for NULL rtag
   if ( rtag == NULL ) {
      LOG( LOG_ERR, "Received a NULL ne_state reference\n" );
      errno = EINVAL;
      return 0;
   }
   if ( rtag->meta_status == NULL  ||  rtag->data_status == NULL ) {
      LOG( LOG_ERR, "Received ne_state struct has undefined meta/data_status\n" );
      errno = EINVAL;
      return 0;
   }

   // keep track of total string length, even if we can't output that much
   size_t totsz = 0;

   // output version info first
   int prres = snprintf( tgtstr, len, "%s(%u.%.3u)", RTAG_VERSION_HEADER, RTAG_CURRENT_MAJORVERSION, RTAG_CURRENT_MINORVERSION );
   if ( prres < 1 ) {
      LOG( LOG_ERR, "Failed to output version info string\n" );
      return 0;
   }
   if ( len > prres ) { len -= prres; tgtstr += prres; }
   else { len = 0; }
   totsz += prres;

   // output stripe info
   prres = snprintf( tgtstr, len, "%s(v%zu|b%zu|t%zu)",
                     RTAG_STRIPEINFO_HEADER,
                     rtag->versz,
                     rtag->blocksz,
                     rtag->totsz );
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
   for ( ; curblock < stripewidth; curblock++ ) {
      char* blkhealth = rtag->data_status + curblock;
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
   for ( curblock = 0; curblock < stripewidth; curblock++ ) {
      char* blkhealth = rtag->meta_status + curblock;
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


