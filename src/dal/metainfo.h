#ifndef __METAINFO_H__
#define __METAINFO_H__

#ifdef __cplusplus
extern "C"
{
#endif

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

// THIS INTERFACE RELIES ON THE DAL INTERFACE!
#include "dal/dal.h"
#include <stdint.h>


/**
 * Perform a DAL get_meta call and parse the resulting string 
 * into the provided meta_info_struct reference.
 * @param ssize_t (*meta_filler)(BLOCK_CTXT handle, char *meta_buf, size_t size) :
 *                Function for retrieving meta info buffers
 *                Expected to fill 'meta_buf' with at most 'size' bytes of stored meta info
 *                Expected to return a total byte count of stored meta info ( even if 'size' < this value ), or -1 on failure
 * @param int block : Block on which this operation is being performed (for logging only)
 * @param meta_info* minfo : meta_info reference to populate with values 
 * @return int : Zero on success, a negative value if a failure occurred, or the number of 
 *               meta values successfully parsed if only portions of the meta info could 
 *               be recovered.
 */
int dal_get_meta_helper( ssize_t (*meta_filler)(BLOCK_CTXT handle, char *meta_buf, size_t size), BLOCK_CTXT handle, meta_info* minfo );

/**
 * Convert a meta_info struct to string format and perform a DAL set_meta call
 * @param int (*meta_writer)(BLOCK_CTXT handle, const char *meta_buf, size_t size) :
 *            Function for storing meta info buffers to a block handle
 *            Expected to write 'size' bytes from 'meta_buf' as meta info of the given handle
 *            Expected to return zero on success, or -1 on failure
 * @param BLOCK_CTXT handle : Block on which this operation is being performed
 * @param meta_info* minfo : meta_info reference to populate with values 
 * @return int : Zero on success, or a negative value if an error occurred 
 */
int dal_set_meta_helper( int (*meta_writer)(BLOCK_CTXT handle, const char *meta_buf, size_t size), BLOCK_CTXT handle, const meta_info* minfo );


#ifdef __cplusplus
}
#endif

#endif

