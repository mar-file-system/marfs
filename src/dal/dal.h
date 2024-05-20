
#ifndef __DAL_H_INCLUDE__
#define __DAL_H_INCLUDE__

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

#include <libxml/tree.h>
#include <strings.h>
#include <string.h>
#include <errno.h>

#ifndef LIBXML_TREE_ENABLED
#error "Included Libxml2 does not support tree functionality!"
#endif

// just to provide some type safety (don't want to pass the wrong void*)
typedef void *DAL_CTXT;
typedef void *BLOCK_CTXT;

// location struct
typedef struct DAL_location_struct
{
   int pod; //
   int block;
   int cap;
   int scatter;
} DAL_location;

// open mode
typedef enum DAL_MODE_enum
{
   DAL_READ = 0,    // retrieve the data and/or meta info of an object
   DAL_WRITE = 1,   // store data and/or meta info to an object
   DAL_REBUILD = 2, // same as WRITE, but with a distinct temporary location (if applicable)
   DAL_METAREAD = 4 // retrieve the meta info of an object
} DAL_MODE;

// meta information which can be attached to DAL objects
typedef struct meta_info_struct
{
   int N;
   int E;
   int O;
   ssize_t partsz;
   ssize_t versz;
   ssize_t blocksz;
   long long crcsum;
   ssize_t totsz;
} meta_info;

/**
 * Duplicates info from one meta_info struct to another (excluding CRCSUM!)
 * @param meta_info* target : Target struct reference
 * @param meta_info* source : Source struct reference
 */
void cpy_minfo( meta_info* target, meta_info* source );

/**
 * Compares the values of two meta_info structs (excluding CRCSUM!)
 * @param meta_info* minfo1 : First struct reference
 * @param meta_info* minfo2 : Second struct reference
 * @return int : A zero value if the structures match, non-zero otherwise
 */
int cmp_minfo( meta_info* minfo1, meta_info* minfo2 );

// flags for DAL verify
//  these flags should stay in sync with the flags in marfs/src/config/config.h
//  so that the same flags value can be passed from the MarFS code to the
//  libNE verify functions.
enum {
   CFG_FIX         = 0x1, // fix any problems found during verification
   CFG_OWNERCHECK  = 0x2  // check that the owner of the security directory matches current user
   // 0x4 used
   // 0x8 used
   // 0x10 used
};

typedef struct DAL_struct
{
   // Name -- Used to identify and configure the DAL
   const char *name;

   // DAL Internal Context -- passed to each DAL function
   DAL_CTXT ctxt;

   // Preferred I/O Size
   size_t io_size;

   // DAL Functions --
   int (*verify)(DAL_CTXT ctxt, int flags);
   // Description:
   //  Ensure that the DAL is properly configured, functional, and secure.  Log any problems encountered.
   //  If the 'flags' argument includes CFG_FIX, attempt to correct such problems.
   //  Note - the specifics of this implementaiton will depend GREATLY on the nature of the DAL.
   // Return Values:
   //  Zero on success, Non-zero if unresolved problems were found
   int (*migrate)(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline);
   // Description:
   //  Relocate an object referenced by 'objID' & 'src' to new location 'dest'.
   //  If the 'offline' argument is zero, this relocation will be performed such that the object can
   //  still be referenced at the original 'src' location during and after completion of this process.
   //  If the 'offline' argument is non-zero, this relocation will be performed such that the original
   //  'src' location is invalidated and any additial resources associated with that reference are
   //  recovered.
   //  Note - this function should always fail if asked to alter only the 'block' value of an object location.
   //  (Intended to avoid overwriting existing object parts)
   // Return Values:
   //  Positive on success, negative if the operation could not be completed. 0 if complete success. 1 if the
   //  object is relocated, but the 'src' location is not successfully invalidated. -2 if the operation could
   //  not be completed but there may be some duplicate (data and/or meta) data at the 'dest' location.
   int (*del)(DAL_CTXT ctxt, DAL_location location, const char *objID);
   // Description:
   //  Delete the DAL object identified by the given ID, at the given location.
   // Return Values:
   //  Zero on success, Non-zero if the operation could not be completed
   int (*stat)(DAL_CTXT ctxt, DAL_location location, const char *objID);
   // Description:
   //  Verify the existence of the given object.
   // Retrun Values:
   //  Zero on success (object exists), Non-zero if the object was not found
   int (*cleanup)(struct DAL_struct *dal);
   // Description:
   //  Destroy and clean up all resources associated with the given DAL reference.
   //  Note - this will NOT clean up any associated BLOCK_CTXT references.  Those must be handled
   //  individually.
   // Return Values:
   //  Zero on success, Non-zero if the operation could not be completed
   BLOCK_CTXT(*open)(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID);
   // Description:
   //  Open a READ/WRITE/REBUILD/META_READ handle for accessing the specified object.
   // Return Values:
   //  Non-NULL on success, NULL if the operation could not be completed
   int (*set_meta)(BLOCK_CTXT ctxt, const meta_info* source);
   // Description:
   //  Attach the provided meta information to the object associated with the given WRITE/REBUILD BLOCK_CTXT.
   // Return Values:
   //  Zero on success, Non-zero if the operation could not be completed
   int (*get_meta)(BLOCK_CTXT ctxt, meta_info* target);
   // Description:
   //  Retrieve the meta information of the object associated with the given READ/META_READ BLOCK_CTXT.
   // Return Values:
   //  Meta byte count on success, negative if the operation could not be completed
   int (*put)(BLOCK_CTXT ctxt, const void *buf, size_t size);
   // Description:
   //  Store data to the object associated with the given WRITE/REBUILD BLOCK_CTXT.
   // Return Values:
   //  Zero on success, Non-zero if the operation could not be completed
   ssize_t (*get)(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset);
   // Description:
   //  Retrieve data from the object associated with the given READ BLOCK_CTXT.
   // Return Values:
   //  Byte count on success, Non-zero if the operation could not be completed
   int (*abort)(BLOCK_CTXT ctxt);
   // Description:
   //  Abandon a given WRITE/REBUILD BLOCK_CTXT.  This is roughly equivalent to calling close() on the
   //  BLOCK_CTXT; however, NO data changes should be applied to the underlying object (same state as before
   //  the BLOCK_CTXT was opened).
   // Return Values:
   //  Zero on success, Non-zero if the operation could not be completed
   int (*close)(BLOCK_CTXT ctxt);
   // Description:
   //  Close a given BLOCK_CTXT reference, freeing any associated resources and finalizing any data changes.
   // Return Values:
   //  Zero on success, Non-zero if the operation could not be completed
} * DAL;

// Forward decls of specific DAL initializations
DAL posix_dal_init(xmlNode *posix_dal_conf_root, DAL_location max_loc);
DAL fuzzing_dal_init(xmlNode *fuzzing_dal_conf_root, DAL_location max_loc);
DAL s3_dal_init(xmlNode *s3_dal_conf_root, DAL_location max_loc);
DAL timer_dal_init(xmlNode *timer_dal_conf_root, DAL_location max_loc);
DAL noop_dal_init(xmlNode *noop_dal_conf_root, DAL_location max_loc);
#ifdef RECURSION
DAL rec_dal_init(xmlNode *rec_dal_conf_root, DAL_location max_loc);
#endif
// Function to provide specific DAL initialization calls based on name
DAL init_dal(xmlNode *dal_conf_root, DAL_location max_loc);

#endif
