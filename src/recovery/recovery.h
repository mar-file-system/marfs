#ifndef _RECOVERY_H
#define _RECOVERY_H
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

MarFS uses libaws4c for Amazon S3 object communication. The original version
is at https://aws.amazon.com/code/Amazon-S3/2601 and under the LGPL license.
LANL added functionality to the original work. The original work plus
LANL contributions is found at https://github.com/jti-lanl/aws4c.

GNU licenses can be found at http://www.gnu.org/licenses/.
*/


#define RECOVERY_CURRENT_MAJORVERSION 0
#define RECOVERY_CURRENT_MINORVERSION 1
#define RECOVERY_MINORVERSION_PADDING 3

#include "generic/numdigits.h"

#include <sys/types.h>

// ALTERING HEADER OR MSG STRUCTURE IS DANGEROUS, 
// AS IT MAY HORRIBLY BREAK PREVIOUS RECOVERY INFO AND STREAM LOGIC
#define RECOVERY_MSGHEAD "\nRECOV("
#define RECOVERY_MSGTAIL ")\n"
typedef struct recovery_header_struct {
   unsigned int majorversion;
   unsigned int minorversion;
   char* ctag;
   char* streamid;
} RECOVERY_HEADER;
#define RECOVERY_HEADER_TYPE "HEADER||"


// ALTERING PER-FILE INFO IS SAFE-ISH, 
// SO LONG AS YOU INCREMENT HEADER VERSIONS AND ADJUST PARSING
typedef struct recovery_finfo_struct {
   ino_t  inode;
   mode_t mode;
   uid_t  owner;
   gid_t  group;
   size_t size;            // altering this will NOT alter representative string len
   struct timespec mtime;
   char   eof;
   char*  path;
} RECOVERY_FINFO;
#define RECOVERY_FINFO_TYPE "FINFO||"

// forward decl, for type safety
typedef struct recovery_struct* RECOVERY;

/**
 * Produce a string representation of the given recovery header
 * @param const RECOVERY_HEADER* header : Reference to the header to be encoded
 * @param char* tgtstr : Reference to the string buffer to be populated
 * @param size_t len : Size of the provided buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t recovery_headertostr( const RECOVERY_HEADER* header, char* tgtstr, size_t size );

/**
 * Produce a string representation of the given recovery FINFO
 * @param const RECOVERY_FINFO* finfo : Reference to the FINFO to be encoded
 * @param char* tgtstr : Reference to the string buffer to be populated
 * @param size_t len : Size of the provided buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficint buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t recovery_finfotostr( const RECOVERY_FINFO* finfo, char* tgtstr, size_t size );

/**
 * Initialize a RECOVERY reference for a data stream, based on the given object data, 
 * and populate a RECOVERY_HEADER reference with the stream info
 * @param void* objbuffer : Reference to the data content of an object to produce a 
 *                          recovery reference for
 * @param size_t objsize : Size of the previous data buffer argument
 * @param RECOVERY_HEADER* header : Reference to a RECOVERY_HEADER struct to be populated, 
 *                                  ignored if NULL
 * @return RECOVERY : Newly created RECOVERY reference, or NULL if a failure occurred
 */
RECOVERY recovery_init( void* objbuffer, size_t objsize, RECOVERY_HEADER* header );

/**
 * Shift a given RECOVERY reference to the content of a new object
 * @param RECOVERY recovery : RECOVERY reference to be updated
 * @param void* objbuffer : Reference to the data content of the new object
 * @param size_t objsize : Size of the previous data buffer argument
 * @return int : Zero on success, or -1 if a failure occurred
 *               NOTE -- an error condition will be produced if the given object buffer 
 *               includes differing RECOVERY_HEADER info
 */
int recovery_cont( RECOVERY recovery, void* objbuffer, size_t objsize );

/**
 * Iterate over file info and content included in the current object data buffer
 * @param RECOVERY recovery : RECOVERY reference to iterate over
 * @param RECOVERY_FINFO* : Reference to the RECOVERY_FINFO struct to be populated with
 *                          info for the next file in the stream; ignored if NULL
 * @param void** databuf : Reference to a void*, to be updated with a reference to the data
 *                         content of the next recovery file; ignored if NULL
 * @param size_t* bufsize : Size of the data content buffer of the next recovery file;
 *                          ignored if NULL
 * @return int : One, if another set of file info was produced;
 *               Zero, if no files remain in the current recovery object;
 *               -1, if a failure occurred.
 */
int recovery_nextfile( RECOVERY recovery, RECOVERY_FINFO* finfo, void** databuf, size_t* bufsize );

/**
 * Close the given RECOVERY reference
 * @param RECOVERY recovery : RECOVERY reference to be closed
 * @param int : Zero on success, or -1 if a failure occurred
 */
int recovery_close( RECOVERY recovery );

#endif // _RECOVERY_H

