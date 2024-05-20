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

#define _GNU_SOURCE // for O_DIRECT

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_DAL
#define DEBUG DEBUG_DAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "posix_dal"
#include "logging/logging.h"

#include "dal.h"
#include "metainfo.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <pwd.h>
#include <unistd.h>
#include <stdint.h>

//   -------------    POSIX DEFINITIONS    -------------

// NOTE -- make sure to adjust SFX_PADDING if changing any SFX strings!
#define SFX_PADDING 14         // number of extra chars required to fit any suffix combo
#define WRITE_SFX ".partial"   // 8 characters
#define REBUILD_SFX ".rebuild" // 8 characters
#define META_SFX ".meta"       // 5 characters (in ADDITION to other suffixes!)

#define IO_SIZE 1048576 // Preferred I/O Size

#define MAX_LOC_BUF 1048576 // Default Location Buffer Size

#define REB_DIR "rebuild-" // For emergency rebuild
#define RBLOCK_DIR "/p%d-b%d-c%d-s%d" // For emergency rebuild

//   -------------    POSIX CONTEXT    -------------

typedef struct posix_block_context_struct
{
   int fd;         // File Descriptor (if open)
   int mfd;        // Meta File Descriptor (if open)
   int sfd;        // Secure Root File Descriptor (if open)
   char *filepath; // File Path (if open)
   int filelen;    // Length of filepath string
   DAL_MODE mode;  // Mode in which this block was opened
} * POSIX_BLOCK_CTXT;

typedef struct posix_dal_context_struct
{
   char *dirtmp;         // Template string for generating directory paths
   int tmplen;           // Length of the dirtmp string
   DAL_location max_loc; // Maximum pod/cap/block/scatter values
   int dirpad;           // Number of chars by which dirtmp may expand via substitutions
   int sec_root;         // Handle of secure root directory
   int dataflags;        // Any additional flag values to be passed to open() of data files
   int metaflags;        // Any additional flag values to be passed to open() of meta files
} * POSIX_DAL_CTXT;

/* For emergency rebuild. This indicates all the combinations of locations that either need to be rebuilt,
 * or locations that the rebuild data can be distributed to.
 */
typedef struct posix_rebuild_location_struct
{
   int pod[MAX_LOC_BUF];     // List of pod numbers for rebuild
   int pod_size;             // Size of pod
   int block[MAX_LOC_BUF];   // List of block numbers for rebuild
   int block_size;           // Size of block
   int cap[MAX_LOC_BUF];     // List of cap numbers for rebuild
   int cap_size;             // Size of block
   int scatter[MAX_LOC_BUF]; // List of scatter numbers for rebuild
   int scatter_size;         // Size of scatter
   char* ts;                 // Time the rebuild was initiated
} * POSIX_REBUILD_LOC;

//   -------------    POSIX INTERNAL FUNCTIONS    -------------

/** (INTERNAL HELPER FUNCTION)
 * Simple check of limits to ensure we don't overrun allocated strings
 * @param DAL_location loc : Location to be checked
 * @param DAL_location* max_loc : Reference to the maximum acceptable location value
 * @return int : Zero if the location is acceptable, -1 otherwise
 */
static int check_loc_limits(DAL_location loc, const DAL_location *max_loc)
{
   //
   if (loc.pod > max_loc->pod)
   {
      LOG(LOG_ERR, "pod value of %d exceeds limit of %d\n", loc.pod, max_loc->pod);
      return -1;
   }
   if (loc.cap > max_loc->cap)
   {
      LOG(LOG_ERR, "cap value of %d exceeds limit of %d\n", loc.cap, max_loc->cap);
      return -1;
   }
   if (loc.block > max_loc->block)
   {
      LOG(LOG_ERR, "block value of %d exceeds limit of %d\n", loc.block, max_loc->block);
      return -1;
   }
   if (loc.scatter > max_loc->scatter)
   {
      LOG(LOG_ERR, "scatter value of %d exceeds limit of %d\n", loc.scatter, max_loc->scatter);
      return -1;
   }
   return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Calculate the number of decimal digits required to represent a given value
 * @param int value : Integer value to be represented in decimal
 * @return int : Number of decimal digits required, or -1 on a bounds error
 */
static int num_digits(int value)
{
   if (value < 0)
   {
      return -1;
   } // negative values not permitted
   if (value < 10)
   {
      return 1;
   }
   if (value < 100)
   {
      return 2;
   }
   if (value < 1000)
   {
      return 3;
   }
   if (value < 10000)
   {
      return 4;
   }
   if (value < 100000)
   {
      return 5;
   }
   // only support values up to 5 digits long
   return -1;
}

/** (INTERNAL HELPER FUNCTION)
 * Parse a comma-separated string of open() flag values and add them to the referenced flag set
 * @param const char* flagstr : String to be parsed
 * @param int* oflags : Reference to the flag values to be updated
 * @return int : Zero on success, or -1 on failure
 */
static int parse_open_flags( const char* flagstr, int* oflags ) {
   if ( flagstr == NULL ) {
      LOG( LOG_ERR, "Received a NULL open flag value string\n" );
      return -1;
   }
   int tmpflags = 0;
   while ( *flagstr != '\0' ) {
      // before doing anything else, find the end of the current flag element
      const char* readahead = flagstr;
      while ( *readahead != '\0'  &&  *readahead != ',' ) { readahead++; }
      // note that these string comparisions DO NOT include the NULL-terminator, as the parsed string may not have one yet
      if ( (readahead - flagstr) == 9  &&  strncasecmp( flagstr, "O_NOATIME", 9 ) == 0 ) {
         tmpflags |= O_NOATIME;
      }
      else if ( (readahead - flagstr) == 8  &&  strncasecmp( flagstr, "O_DIRECT", 8 ) == 0 ) {
         tmpflags |= O_DIRECT;
      }
      else if ( (readahead - flagstr) == 7  &&  strncasecmp( flagstr, "O_DSYNC", 7 ) == 0 ) {
         tmpflags |= O_DSYNC;
      }
      else if ( (readahead - flagstr) == 6  &&  strncasecmp( flagstr, "O_SYNC", 6 ) == 0 ) {
         tmpflags |= O_SYNC;
      }
      else if ( readahead - flagstr ) { // only complain if this element was populated at all
         LOG( LOG_ERR, "POSIX DAL def contains unrecognized open() flag value: \"%.*s\"\n", (int)(readahead - flagstr), flagstr );
         return -1;
      }
      // update to the next flag element
      flagstr = readahead;
      while ( *flagstr == ',' ) { flagstr++; } // skip over any number of repeated commas
   }
   // update our *actual* flags
   *oflags |= tmpflags;
   return 0;
}

static char *expand_path(const char *parse, char *fill, DAL_location loc, DAL_location *loc_flags, int dir)
{
   char escp = 0;
   char *end = fill;

   while (*parse != '\0')
   {

      switch (*parse)
      {

      case '\\': // check for escape character '\'
         if (escp)
         { // only add literal '\' if already escaped
            *fill = *parse;
            fill++;
            escp = 0;
         }
         else
         {
            escp = 1;
         } // escape the next character
         break;

      case '{': // check for start of a substitution
         if (escp)
         { // only add literal '{' if escaped
            *fill = '{';
            fill++;
            escp = 0;
         }
         else
         {
            parse++;
            int fillval = 0;
            if (*parse == 'p')
            {
               fillval = loc.pod;
               if (loc_flags)
               {
                  loc_flags->pod = 1;
               }
            }
            else if (*parse == 'b')
            {
               fillval = loc.block;
               if (loc_flags)
               {
                  loc_flags->block = 1;
               }
            }
            else if (*parse == 'c')
            {
               fillval = loc.cap;
               if (loc_flags)
               {
                  loc_flags->cap = 1;
               }
            }
            else if (*parse == 's')
            {
               fillval = loc.scatter;
               if (loc_flags)
               {
                  loc_flags->scatter = 1;
               }
            }
            else
            {
               LOG(LOG_WARNING, "dir_template contains an unescaped '{' followed by '%c', rather than an expected 'p'/'b'/'c'/'s'\n", *parse);
               *fill = '{';
               fill++;
               continue;
            }
            // ensure the '}' (end of substitution character) follows
            if (*(parse + 1) != '}')
            {
               LOG(LOG_WARNING, "dir_template contains an '{%c' substitution sequence with no closing '}' character\n", *parse);
               *fill = '{';
               fill++;
               continue;
            }
            // print the numeric value into the fill string
            fillval = snprintf(fill, 5, "%d", fillval);
            if (fillval <= 0)
            {
               // if snprintf failed for some reason, we can't recover
               LOG(LOG_ERR, "snprintf failed when attempting dir_template substitution!\n");
               return NULL;
            }
            fill += fillval; // update fill pointer to refernce the new end of the string
            parse++;         // skip over the '}' character that we have already verified
         }
         break;

      case '/': // check for the end of a directory
         *fill = *parse;
         fill++;
         end = fill;
         break;

      default:
         if (escp)
         {
            LOG(LOG_WARNING, "invalid '\\%c' escape encountered in the dir_template string\n", *parse);
            escp = 0;
         }
         *fill = *parse;
         fill++;
         break;
      }

      parse++;
   }
   if (dir)
   {
      *end = '\0';
   }
   return fill;
}

/** (INTERNAL HELPER FUNCTION)
 * Perform necessary string substitutions/calculations to populate all values in a new POSIX_BLOCK_CTXT
 * @param POSIX_DAL_CTXT dctxt : Context reference of the current POSIX DAL
 * @param POSIX_BLOCK_CTXT bctxt : Block context to be populated
 * @param DAL_location loc : Location of the object to be referenced by bctxt
 * @param const char* objID : Object ID to be referenced by bctxt
 * @return int : Zero on success, -1 on failure
 */
static int expand_dir_template(POSIX_DAL_CTXT dctxt, POSIX_BLOCK_CTXT bctxt, DAL_location loc, const char *objID)
{
   // check that our DAL_location is within bounds
   if (check_loc_limits(loc, &(dctxt->max_loc)) != 0)
   {
      errno = EDOM;
      return -1;
   }

   //
   bctxt->sfd = dctxt->sec_root;

   // allocate string to hold the dirpath
   // NOTE -- allocation size is an estimate, based on the above pod/block/cap/scat limits
   bctxt->filepath = malloc(sizeof(char) * (dctxt->tmplen + dctxt->dirpad + strlen(objID) + SFX_PADDING + 1));
   if ( bctxt->filepath == NULL ) {
      LOG( LOG_ERR, "Failed to allocate filepath string of length %d\n", (dctxt->tmplen + dctxt->dirpad + strlen(objID) + SFX_PADDING + 1) );
      return -1;
   } // malloc will set errno
   // parse through the directory template string, populating filepath as we go
   const char *parse = dctxt->dirtmp;
   char *fill = bctxt->filepath;
   if (!(fill = expand_path(parse, fill, loc, NULL, 0)))
   {
      free(bctxt->filepath);
      bctxt->filepath = NULL;
      return -1;
   }

   // parse through the given objID, populating filepath as we go
   parse = objID;
   while (*parse != '\0')
   {

      switch (*parse)
      {

      // posix won't allow '/' in filenames; replace with '#'
      case '/':
         *fill = '#';
         fill++;
         break;

      default:
         *fill = *parse;
         fill++;
         break;
      }

      parse++;
   }
   // ensure we null terminate the string
   *fill = '\0';
   // use pointer arithmetic to determine length of path
   bctxt->filelen = fill - bctxt->filepath;
   return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Delete various components of a given object, identified by it's block context.
 * @param POSIX_BLOCK_CTXT bctxt : Context of the object to be deleted
 * @param char components : Identifies which components of the object to delete
 *                          0 - working data/meta files only
 *                          1 - ALL data/meta files
 * @return int : Zero on success, -1 on failure
 */
static int block_delete(POSIX_BLOCK_CTXT bctxt, char components)
{
   char *working_suffix = WRITE_SFX;
   if (bctxt->mode == DAL_REBUILD)
   {
      working_suffix = REBUILD_SFX;
   }
   // append the meta suffix and check for success
   char *res = strncat(bctxt->filepath + bctxt->filelen, META_SFX, SFX_PADDING);
   if (res != (bctxt->filepath + bctxt->filelen))
   {
      LOG(LOG_ERR, "failed to append meta suffix \"%s\" to file path \"%s\"!\n", META_SFX, bctxt->filepath);
      errno = EBADF;
      return -1;
   }

   int metalen = strlen(META_SFX);

   // append the working suffix and check for success
   res = strncat(bctxt->filepath + bctxt->filelen + metalen, working_suffix, SFX_PADDING - metalen);
   if (res != (bctxt->filepath + bctxt->filelen + metalen))
   {
      LOG(LOG_ERR, "failed to append working suffix \"%s\" to file path \"%s\"!\n", working_suffix, bctxt->filepath);
      errno = EBADF;
      *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
      return -1;
   }

   // unlink any in-progress meta file (only failure with ENOENT is acceptable)
   if (unlinkat(bctxt->sfd, bctxt->filepath, 0) != 0 && errno != ENOENT)
   {
      LOG(LOG_ERR, "failed to unlink \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
      *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
      return -1;
   }

   // trim the working suffix off
   *(bctxt->filepath + bctxt->filelen + metalen) = '\0';

   if (components)
   {
      // unlink any meta file (only failure with ENOENT is acceptable)
      if (unlinkat(bctxt->sfd, bctxt->filepath, 0) != 0 && errno != ENOENT)
      {
         LOG(LOG_ERR, "failed to unlink \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
         *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
         return -1;
      }
   }

   // trim the meta suffix off and attach a working suffix in its place
   *(bctxt->filepath + bctxt->filelen) = '\0';
   res = strncat(bctxt->filepath + bctxt->filelen, working_suffix, SFX_PADDING - metalen);
   if (res != (bctxt->filepath + bctxt->filelen))
   {
      LOG(LOG_ERR, "failed to append working suffix \"%s\" to file path \"%s\"!\n", working_suffix, bctxt->filepath);
      errno = EBADF;
      *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
      return -1;
   }

   // unlink any in-progress data file (only failure with ENOENT is acceptable)
   if (unlinkat(bctxt->sfd, bctxt->filepath, 0) != 0 && errno != ENOENT)
   {
      LOG(LOG_ERR, "failed to unlink \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
      *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
      return -1;
   }

   *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains

   if (components)
   {
      // unlink any data file (only failure with ENOENT is acceptable, as this implies a non-existent file)
      if (unlinkat(bctxt->sfd, bctxt->filepath, 0) != 0 && errno != ENOENT)
      {
         LOG(LOG_ERR, "failed to unlink \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
         *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
         return -1;
      }
   }

   return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Forms a path to a source location relative to a destination location.
 * @param char* oldpath : Path to our source location (relative to a source root)
 * @param char* newpath : Path to our destination location (relative to the same source root as oldpath)
 * @return char* : Path to oldpath's location relative to newpath's location. NULL on failure
 */
static char *convert_relative(char *oldpath, char *newpath)
{
   // check that both paths exist
   if (oldpath == NULL || newpath == NULL)
   {
      return NULL;
   }

   // parse through our destination path, counting the number of directories
   int nBack = 0;
   if (*newpath == '.' && *(newpath + 1) == '/')
   {
      newpath += 2;
   }
   while (*newpath != '\0')
   {
      if (*newpath == '/' && *(newpath + 1) != '/')
      {
         nBack++;
      }
      newpath++;
   }

   // allocate space for our return string
   char *result = malloc(sizeof(char) * (3 * nBack + strlen(oldpath) + 1));
   if (result == NULL)
   {
      return NULL;
   }
   *result = '\0';

   // form path that traverses from destination location to secure root
   int i;
   for (i = 0; i < nBack; i++)
   {
      if (strcat(result, "../") != result)
      {
         LOG(LOG_ERR, "failed to append \"../\" to source path!\n");
         errno = EBADF;
         free(result);
         return NULL;
      }
      result += 3;
   }

   // append source location to path
   if (strcat(result, oldpath) != result)
   {
      LOG(LOG_ERR, "failed to append \"../\" to source path!\n");
      errno = EBADF;
      free(result);
      return NULL;
   }

   return result - 3 * nBack;
}

/** (INTERNAL HELPER FUNCTION)
 * Checks if a value matches any elements of an integer array.
 * @param int val : Value to check
 * @param int* arr : The array to check against
 * @param int size: Size of arr
 * @return int : 0 in case of a match, 1 otherwise
 */
int checkMatch(int val, int* arr, int size) {
   int i;
   for (i = 0; i < size; i++) {
      if (val == arr[i]) {
         return 0;
      }
   }
   return 1;
}

/** (INTERNAL HELPER FUNCTION)
 * Checks if a DAL_location matches any possible rebuild location combinations.
 * @param DAL_location* loc : Location to check
 * @param POSIX_REBUILD_LOCATION tgt : Rebuild locations to check against
 * @return int : 0 in case of a match, 1 otherwise
 */
int tgtMatch(DAL_location* loc, POSIX_REBUILD_LOC tgt) {
   if (tgt == NULL) {
      return 1;
   }
   return checkMatch(loc->pod, tgt->pod, tgt->pod_size) ||
         checkMatch(loc->block, tgt->block, tgt->block_size) ||
         checkMatch(loc->cap, tgt->cap, tgt->cap_size) ||
         checkMatch(loc->scatter, tgt->scatter, tgt->scatter_size);
}

// Next two fn's taken from marfs/src/hash/hash.c

/** (INTERNAL HELPER FUNCTION)
 * Taken from https://github.com/mar-file-system/marfs/fuse/src/common.c
 * POLYHASH implementation
 *
 * Computes a good, uniform, hash of the string.
 *
 * Treats each character in the length n string as a coefficient of a
 * degree n polynomial.
 *
 * f(x) = string[n -1] + string[n - 2] * x + ... + string[0] * x^(n-1)
 *
 * The hash is computed by evaluating the polynomial for x=33 using
 * Horner's rule.
 *
 * Reference: http://cseweb.ucsd.edu/~kube/cls/100/Lectures/lec16/lec16-14.html
 * @param const char* string : String to hash
 * @return uint64_t : hash of the string
 */
uint64_t polyhash(const char* string) {
   // According to http://www.cse.yorku.ca/~oz/hash.html
   // 33 is a magical number that inexplicably works the best.
   const int salt = 33;
   char c;
   uint64_t h = *string++;
   while((c = *string++))
      h = salt * h + c;
   return h;
}

/** (INTERNAL HELPER FUNCTION)
 * Taken from https://github.com/mar-file-system/marfs/fuse/src/common.c
 * compute the hash function h(x) = (a*x) >> 32
 * @param const uint64_t key : first value to hash
 * @param uint64_t a : second value to hash
 * @return uint64_t : hash of the two values
 */
uint64_t h_a(const uint64_t key, uint64_t a) {
   return ((a * key) >> 32);
}

/** (INTERNAL HELPER FUNCTION)
 * Adapted from get_path_template() in https://github.com/mar-file-system/marfs/fuse/src/common.c
 * Hashes a path name to a location from the provided distribution
 * @param char* pathname : Name of the path to hash
 * @param POSIX_REBUILD_LOC dist : Possible locations to distribute to
 * @return DAL_location : Hashed location
 */
DAL_location hash_loc(char* pathname, POSIX_REBUILD_LOC dist) {
   DAL_location ret_loc;
   uint64_t hash = polyhash(pathname);
   unsigned int seed = hash;
   uint64_t a[4];
   int i;
   for (i = 0; i < 4; i++) {
   a[i] = rand_r(&seed) * 2 + 1;
   }
   ret_loc.pod = dist->pod[h_a(hash, a[0]) % dist->pod_size];
   ret_loc.block = dist->block[h_a(hash, a[1]) % dist->block_size];
   ret_loc.cap = dist->cap[h_a(hash, a[2]) % dist->cap_size];
   ret_loc.scatter = dist->scatter[h_a(hash, a[3]) % dist->scatter_size];

   return ret_loc;
}

/** (INTERNAL HELPER FUNCTION)
 * Recursively makes missing directories in file path relative to directory file
 * descriptor. If rebuild target and distribution locations are given, also
 * creates subdirs within the distribution that are pointed to by symlinks from
 * target locations.
 * @param int dirfd : Directory file descriptor all paths are relative to
 * @param char *pathname : math of directory to create
 * @param mode_t mode : permissions to use
 * @param char* dirtmp : Template string for generating directory paths
 * @param int dirpad : Number of chars by which dirtmp may expand via substitutions
 * @param DAL_location* loc : DAL location that pathname corresponds with. Only
 * valid if tgt != NULL
 * @param POSIX_REBUILD_LOC tgt : List of rebuild location targets that should
 * be directed to a distribution location, or NULL
 * @param POSIX_REBUILD_LOC dist : List of distribution locations that target
 * locations could point to. Only valid if tgt != NULL
 * @return int : Zero on success, Non-zero if one or more directories could not
 * be created
 */
static int r_mkdirat(int dirfd, char *pathname, mode_t mode, char *dirtmp, int dirpad,
   DAL_location* loc, POSIX_REBUILD_LOC tgt, POSIX_REBUILD_LOC dist)
{
   char *parse = pathname;
   int num_err = 0;
   struct stat info;
   // Iterate through the dirs in pathname, creating them (if needed) as we go
   while (*parse != '\0')
   {
      if (*parse == '/')
      {
         *parse = '\0';
         // check if we have any more dirs remaining
         char* lookahead = parse + 1;
         while ( *lookahead != '\0'  &&  *lookahead != '/' ) { lookahead++; }
         // If this is the last dir, and it is a rebuild target, create a symlink to a distribution location
         if ( *lookahead == '\0'  && !tgtMatch(loc, tgt)) {
               // Generate our write target from the distribution locations
               DAL_location wtgt_loc = hash_loc(pathname, dist);

               // If our write target is also a rebuild target, create a dir like normal
               if (!tgtMatch(&wtgt_loc, tgt)) {
                  goto normal;
               }

               // Form the base path for our write target
               int rblock_len = strlen(RBLOCK_DIR) + num_digits(loc->pod) + num_digits(loc->block) + num_digits(loc->cap) + num_digits(loc->scatter) - 8;
               char wtgt_path[256] = {0};
               expand_path(dirtmp, wtgt_path, wtgt_loc, NULL, 1);
               char* wtgt_end = wtgt_path + strlen(wtgt_path);

               // If they don't already exist, create the normal dirs above the write target
               // We need to call this before r_mkdirat() on the full write target to ensure that the last
               // normal dir above the write target subdirs has the correct permissions
               if (r_mkdirat(dirfd, wtgt_path, mode, dirtmp, dirpad, NULL, NULL, NULL)) {
                  LOG(LOG_ERR, "failed to create directory \"%s\" (%s)\n", wtgt_path, strerror(errno));
                  num_err++;
                  *parse = '/';
                  return num_err;
               }

               // Form the rest of the path for our write target
               char *res = strncat(wtgt_end, REB_DIR, 255 - strlen(wtgt_path));\
               if (res != wtgt_end) {
                  num_err++;
                  *parse = '/';
                  return num_err;
               }
               wtgt_end += strlen(REB_DIR);

               res = strncat(wtgt_end, tgt->ts, 255 - strlen(wtgt_path));\
               if (res != wtgt_end) {
                  num_err++;
                  *parse = '/';
                  return num_err;
               }
               wtgt_end += strlen(tgt->ts);

               sprintf(wtgt_end, RBLOCK_DIR, loc->pod, loc->block, loc->cap, loc->scatter);

               wtgt_end += rblock_len;

               res = strncat(wtgt_end, "/\0", 255 - strlen(wtgt_path));\
               if (res != wtgt_end) {
                  num_err++;
                  *parse = '/';
                  return num_err;
               }

               // Create our write target
               if (r_mkdirat(dirfd, wtgt_path, mode, dirtmp, dirpad, NULL, NULL, NULL)) {
                  LOG(LOG_ERR, "failed to create rebuild directory \"%s\" (%s)\n", wtgt_path, strerror(errno));
                  num_err++;
                  *parse = '/';
                  return num_err;
               }

               // Remove any existing file/dir at the rebuild target to replace with a symlink to the write target
               if (!fstatat(dirfd, pathname, &info, 0) && unlinkat(dirfd, pathname, AT_REMOVEDIR)) {
                  LOG(LOG_ERR, "failed to remove existing file/dir \"%s\" to replace with symlink for rebuild (%s)\n", pathname, strerror(errno));
                  num_err++;
                  *parse = '/';
                  return num_err;
               }

               // attempt to symlink rebuild target to write target
               printf("creating symlink \"%s\" that points to write target \"%s\"\n", pathname, wtgt_path);
               LOG(LOG_INFO, "creating symlink \"%s\" that points to write target \"%s\"\n", pathname, wtgt_path);
               char *oldpath = convert_relative(wtgt_path, pathname);
               if (oldpath == NULL)
               {
                  LOG(LOG_ERR, "failed to create relative data path for symlink\n");
                  num_err++;
                  *parse = '/';
                  return num_err;
               }

               if (symlinkat(oldpath, dirfd, pathname))
               {
                  LOG(LOG_ERR, "failed to create data symlink from \"%s\" to \"%s\"\n", pathname, oldpath);
                  num_err++;
                  *parse = '/';
                  return num_err;
               }
         }
         else {
normal:
            // If a directory does not exist at the current path, create it
            if (fstatat(dirfd, pathname, &info, 0) || !S_ISDIR(info.st_mode))
            {
               // The last dir needs special permissions
               if (*lookahead == '\0') {
                  mode |= S_IWOTH | S_IXOTH;
               }
               if (mkdirat(dirfd, pathname, mode))
               {
                  LOG(LOG_ERR, "failed to create directory \"%s\" (%s)\n", pathname, strerror(errno));
                  num_err++;
                  *parse = '/';
                  return num_err;
               }
               else
               {
                  LOG(LOG_INFO, "successfully created directory \"%s\"\n", pathname);
               }
            }
         }
         *parse = '/';
      }
      parse++;
   }
   return num_err;
}

/** (INTERNAL HELPER FUNCTION)
 * Set meta info as an xattr of the given block
 * @param BLOCK_CTXT ctxt : Reference to the target block
 * @param const char* meta_buf : Reference to the source buffer
 * @param size_t size : Size of the source buffer
 * @return int : Zero on success, or -1 on failure
 */
int posix_set_meta_internal(BLOCK_CTXT ctxt, const char *meta_buf, size_t size)
{

   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   POSIX_BLOCK_CTXT bctxt = (POSIX_BLOCK_CTXT)ctxt; // should have been passed a posix context

   // reseek to the start of the sidecar file
   if ( lseek( bctxt->mfd, 0, SEEK_SET ) ) {
      LOG( LOG_ERR, "failed to reseek to start of meta file: \"%s\" (%s)\n", bctxt->filepath, strerror(errno) );
      return -1;
   }
   // write the provided buffer out to the sidecar file
   if (write(bctxt->mfd, meta_buf, size) != size)
   {
      LOG(LOG_ERR, "failed to write buffer to meta file: \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
      return -1;
   }

   return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Get meta info from the xattr of the given block
 * @param BLOCK_CTXT ctxt : Reference to the target block
 * @param const char* meta_buf : Reference to the buffer to be populated
 * @param size_t size : Size of the dest buffer
 * @return ssize_t : Total meta info size for the block, or -1 on failure
 */
ssize_t posix_get_meta_internal(BLOCK_CTXT ctxt, char *meta_buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   POSIX_BLOCK_CTXT bctxt = (POSIX_BLOCK_CTXT)ctxt; // should have been passed a posix context

   // reseek to the start of the sidecar file
   if ( lseek( bctxt->mfd, 0, SEEK_SET ) ) {
      LOG( LOG_ERR, "failed to reseek to start of meta file: \"%s\" (%s)\n", bctxt->filepath, strerror(errno) );
      return -1;
   }
   ssize_t result = read(bctxt->mfd, meta_buf, size);
   // potentially indicate excess meta information
   if ( result == size ) {
      // we need to stat this sidecar file to get the total meta info length
      struct stat stval;
      if ( fstat(bctxt->mfd, &stval) ) {
         LOG( LOG_ERR, "failed to stat meta file \"%s\" to check for possible excess meta length (%s)\n", bctxt->filepath, strerror(errno) );
         return -1;
      }
      // increase result to match the total meta info size, if appropriate
      if ( result < stval.st_size ) { result = stval.st_size; }
   }
   return result;
}


// forward-declarations to allow these functions to be used in manual_migrate
int posix_del(DAL_CTXT ctxt, DAL_location location, const char *objID);

BLOCK_CTXT posix_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID);

int posix_put(BLOCK_CTXT ctxt, const void *buf, size_t size);

ssize_t posix_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset);

int posix_abort(BLOCK_CTXT ctxt);

int posix_close(BLOCK_CTXT ctxt);

/** (INTERNAL HELPER FUNCTION)
 * Attempt to manually migrate an object from one location to another using put/get/set_meta/get_meta dal functions..
 * @param POSIX_DAL_CTXT dctxt : Context reference of the current POSIX DAL
 * @param const char* objID : Object ID reference of object to be migraded
 * @param DAL_location src : Source location of the object to be migrated
 * @param DAL_location dest : Destination location of the object to be migrated
 * @param const char* objID : Object ID to be referenced by bctxt
 * @return int : Zero on success, -1 on failure
 */
int manual_migrate(POSIX_DAL_CTXT dctxt, const char *objID, DAL_location src, DAL_location dest)
{
   // allocate buffers to transfer between locations
   void *data_buf = malloc(IO_SIZE);
   if (data_buf == NULL)
   {
      return -1;
   }
   char *meta_buf = malloc(IO_SIZE);
   if (meta_buf == NULL)
   {
      free(data_buf);
      return -1;
   }

   // open both locations
   POSIX_BLOCK_CTXT src_ctxt = (POSIX_BLOCK_CTXT)posix_open((DAL_CTXT)dctxt, DAL_READ, src, objID);
   if (src_ctxt == NULL)
   {
      free(data_buf);
      free(meta_buf);
      return -1;
   }
   POSIX_BLOCK_CTXT dest_ctxt = (POSIX_BLOCK_CTXT)posix_open((DAL_CTXT)dctxt, DAL_WRITE, dest, objID);
   if (dest_ctxt == NULL)
   {
      posix_abort((BLOCK_CTXT)src_ctxt);
      free(data_buf);
      free(meta_buf);
      return -1;
   }

   // move data file from source location to destination location
   ssize_t res;
   off_t off = 0;
   do
   {
      res = posix_get((BLOCK_CTXT)src_ctxt, data_buf, IO_SIZE, off);
      if (res < 0)
      {
         posix_abort((BLOCK_CTXT)src_ctxt);
         block_delete(dest_ctxt, 0);  // delete any in-progress output
         posix_abort((BLOCK_CTXT)dest_ctxt);
         free(data_buf);
         free(meta_buf);
         return -1;
      }
      off += res;
      if (posix_put((BLOCK_CTXT)dest_ctxt, data_buf, res))
      {
         posix_abort((BLOCK_CTXT)src_ctxt);
         block_delete(dest_ctxt, 0);  // delete any in-progress output
         posix_abort((BLOCK_CTXT)dest_ctxt);
         free(data_buf);
         free(meta_buf);
         return -1;
      }
   } while (res > 0);

   // move meta file from source location to destination location
   res = posix_get_meta_internal((BLOCK_CTXT)src_ctxt, meta_buf, IO_SIZE);
   if (res < 0)
   {
      posix_abort((BLOCK_CTXT)src_ctxt);
      block_delete(dest_ctxt, 0);  // delete any in-progress output
      posix_abort((BLOCK_CTXT)dest_ctxt);
      free(data_buf);
      free(meta_buf);
      return -1;
   }
   if (posix_set_meta_internal((BLOCK_CTXT)dest_ctxt, meta_buf, res))
   {
      posix_abort((BLOCK_CTXT)src_ctxt);
      block_delete(dest_ctxt, 0);  // delete any in-progress output
      posix_abort((BLOCK_CTXT)dest_ctxt);
      free(data_buf);
      free(meta_buf);
      return -1;
   }

   free(data_buf);
   free(meta_buf);

   // close both locations
   if (posix_close((BLOCK_CTXT)src_ctxt))
   {
      block_delete(dest_ctxt, 0);  // delete any in-progress output
      posix_abort((BLOCK_CTXT)dest_ctxt);
      return -1;
   }
   if (posix_close((BLOCK_CTXT)dest_ctxt))
   {
      return -1;
   }

   // delete old file
   if (posix_del((DAL_CTXT)dctxt, src, objID))
   {
      return 1;
   }

   return 0;
}

/** (INTERNAL HELPER FUNCTION)
 * Ensure that the DAL is properly configured, functional, and secure. Log any problems encountered
 * @param DAL_CTXT ctxt : Context reference of the current POSIX DAL
 * @param int flags : flags for verify - if CFG_FIX is true, then attempt to fix problems
 *                                       if CFG_OWNERCHECK is true, then check UID/GID of parent dir(s)
 * @param POSIX_REBUILD_LOC tgt : List of rebuild location targets that should
 * be directed to a distribution location, or NULL
 * @param POSIX_REBUILD_LOC dist : List of distribution locations that target
 * locations could point to. Only valid if tgt != NULL
 * @return int : Zero on success, Non-zero if unresolved problems were found
 */
int _posix_verify(DAL_CTXT ctxt, int flags, POSIX_REBUILD_LOC tgt, POSIX_REBUILD_LOC dist) {
   int fix = flags & CFG_FIX;
   int check_owner = flags & CFG_OWNERCHECK;

   // NOTE: fix arg is forced high when rebuilding
   if (!tgt != !dist) {
      LOG(LOG_ERR, "received incomplete rebuild locations!\n");
      return -1;
   }

   if (tgt && dist) {
      fix = 1;
   }

   // try to limit access explicitly to the running user/group
   uid_t uid = geteuid();
   gid_t gid = getegid();
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   POSIX_DAL_CTXT dctxt = (POSIX_DAL_CTXT)ctxt; // should have been passed a POSIX context

   // zero out umask
   mode_t mask = umask(0);

   int num_err = 0;
   struct stat st;

   if ( dctxt->sec_root != AT_FDCWD ) {
      // verify secure root
      if (fstat(dctxt->sec_root, &st) || !S_ISDIR(st.st_mode))
      {
         LOG(LOG_ERR, "failed to verify secure root exists (%s)\n", strerror(errno));
         umask(mask); // restore umask
         return -1;
      }

      if ( check_owner && (st.st_uid != uid || st.st_gid != gid) )
      {
         LOG(LOG_ERR, "secure root does not have ownership matching this process\n");
         if (fix)
         {
            if (fchown(dctxt->sec_root, uid, gid))
            {
               LOG(LOG_ERR, "failed to set owner and group of secure root (%s)\n", strerror(errno));
               num_err++;
            }
            else
            {
               LOG(LOG_INFO, "successfully set owner and group of secure root\n");
            }
         }
         else
         {
            num_err++;
         }
      }
      if ( (st.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) != (S_IRWXU | S_IXGRP | S_IXOTH) )
      {
         LOG(LOG_ERR, "failed to verify secure root permissions\n");
         if (fix)
         {
            if (fchmod(dctxt->sec_root, S_IRWXU | S_IXGRP | S_IXOTH))
            {
               LOG(LOG_ERR, "failed to set secure root permissions (%s)\n", strerror(errno));
               num_err++;
            }
            else
            {
               LOG(LOG_INFO, "successfully set secure root permissions\n");
            }
         }
         else
         {
            num_err++;
         }
      }

      int w_fd = -1;
      int o_fd = dctxt->sec_root;
      ino_t o_ino = -1;
      while (st.st_ino != o_ino)
      {
         o_ino = st.st_ino;
         if ((w_fd = openat(o_fd, "..", O_RDONLY)) < 0)
         {
            LOG(LOG_ERR, "failed to open parent dir\n");
            num_err++;
            o_ino = -1;
            break;
         }

         if (fstat(w_fd, &st))
         {
            LOG(LOG_ERR, "failed to stat parent dir\n");
            num_err++;
            close(w_fd);
            o_ino = -1;
            break;
         }

         if ((!check_owner || (st.st_uid == uid && st.st_gid == gid)) &&
             (st.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)) == S_IRWXU)
         {
            close(w_fd);
            o_ino = -1;
            break;
         }

         if (o_fd != dctxt->sec_root)
         {
            close(o_fd);
         }
         o_fd = w_fd;
      }
      if (o_ino != -1)
      {
         close(w_fd);
         LOG(LOG_ERR, "no parent directory of the secure root has fully restrictive permissions\n");
         if (fix)
         {
            if (fchownat(dctxt->sec_root, "..", uid, gid, 0))
            {
               LOG(LOG_ERR, "failed to set owner and group of parent of secure root (%s)\n", strerror(errno));
               num_err++;
            }
            else
            {
               LOG(LOG_INFO, "successfully set owner and group of parent of secure root\n");
            }

            if (fchmodat(dctxt->sec_root, "..", S_IRWXU, 0))
            {
               LOG(LOG_ERR, "failed to set permissions of parent of secure root (%s)\n", strerror(errno));
               num_err++;
            }
            else
            {
               LOG(LOG_INFO, "successfully set permissions of parent of secure root\n");
            }
         }
         else
         {
            num_err++;
         }
      }
      else if (o_fd != dctxt->sec_root)
      {
         close(o_fd);
      }
   }
   char *path = malloc(sizeof(char) * (dctxt->tmplen + dctxt->dirpad + SFX_PADDING + 1));
   DAL_location loc = {.pod = 0, .block = 0, .cap = 0, .scatter = 0};
   DAL_location loc_flags = {.pod = 0, .block = 0, .cap = 0, .scatter = 0};
   expand_path(dctxt->dirtmp, path, dctxt->max_loc, &loc_flags, 1);
   // return if there are not any directories to check
   if (!strlen(path))
   {
      free(path);
      umask(mask); // restore umask
      return num_err;
   }
   // check every valid combination of pod/block/cap/scatter
   int p;
   for (p = 0; p <= (loc_flags.pod ? dctxt->max_loc.pod : 0); p++)
   {
      loc.pod = p;
      int b;
      for (b = 0; b <= (loc_flags.block ? dctxt->max_loc.block : 0); b++)
      {
         loc.block = b;
         int c;
         for (c = 0; c <= (loc_flags.cap ? dctxt->max_loc.cap : 0); c++)
         {
            loc.cap = c;
            int s;
            for (s = 0; s <= (loc_flags.scatter ? dctxt->max_loc.scatter : 0); s++)
            {
               loc.scatter = s;
               expand_path(dctxt->dirtmp, path, loc, NULL, 1);
               LOG(LOG_INFO, "checking path %s\n", path);
               // If we are fixing and the dir does not exist, or corresponds to a rebuild target and is not a symlink, create it
               if (fstatat(dctxt->sec_root, path, &st, 0) || !S_ISDIR(st.st_mode) || (!tgtMatch(&loc, tgt) && !S_ISLNK(st.st_mode)))
               {
                  LOG(LOG_ERR, "failed to verify directory \"%s\" exists (%s)\n", path, strerror(errno));
                  if (!fix || r_mkdirat(dctxt->sec_root, path, S_IRWXU | S_IXGRP | S_IXOTH, dctxt->dirtmp, dctxt->dirpad, &loc, tgt, dist) || fchmodat(dctxt->sec_root, path, S_IRWXU | S_IWGRP | S_IXGRP | S_IWOTH | S_IXOTH, 0))
                  {
                     num_err++;
                  }
               }
            }
         }
      }
   }
   free(path);
   umask(mask); // restore umask
   return num_err;
}


//   -------------    POSIX IMPLEMENTATION    -------------

int posix_verify(DAL_CTXT ctxt, int flags) {
   return _posix_verify(ctxt, flags, NULL, NULL);
}

int posix_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
{
   // fail if only the block is different
   if (src.pod == dest.pod && src.cap == dest.cap && src.scatter == dest.scatter)
   {
      LOG(LOG_ERR, "received identical locations!\n");
      return -1;
   }

   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   POSIX_DAL_CTXT dctxt = (POSIX_DAL_CTXT)ctxt; // should have been passed a posix context

   POSIX_BLOCK_CTXT srcctxt = malloc(sizeof(struct posix_block_context_struct));
   if (srcctxt == NULL)
   {
      return -1; // malloc will set errno
   }
   POSIX_BLOCK_CTXT destctxt = malloc(sizeof(struct posix_block_context_struct));
   if (destctxt == NULL)
   {
      free(srcctxt);
      return -1; // malloc will set errno
   }

   // popultate the full file path for this object
   if (expand_dir_template(dctxt, srcctxt, src, objID))
   {
      free(srcctxt->filepath);
      free(srcctxt);
      free(destctxt);
      return -1;
   }
   if (expand_dir_template(dctxt, destctxt, dest, objID))
   {
      free(srcctxt->filepath);
      free(srcctxt);
      free(destctxt->filepath);
      free(destctxt);
      return -1;
   }

   // permanently move object from old location to new location (link to dest loc and unlink src loc)
   if (offline)
   {
      // append the meta suffix to the source and check for success
      char *res = strncat(srcctxt->filepath + srcctxt->filelen, META_SFX, SFX_PADDING);
      if (res != (srcctxt->filepath + srcctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append meta suffix \"%s\" to source file path!\n", META_SFX);
         errno = EBADF;
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         return -1;
      }

      // duplicate the source meta path and check for success
      char *src_meta_path = strdup(srcctxt->filepath);
      if (src_meta_path == NULL)
      {
         LOG(LOG_ERR, "failed to allocate space for a new source meta string! (%s)\n", strerror(errno));
         *(srcctxt->filepath + srcctxt->filelen) = '\0'; // make sure no suffix remains
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         return -1;
      }
      *(srcctxt->filepath + srcctxt->filelen) = '\0'; // make sure no suffix remains

      // append the meta suffix to the destination and check for success
      res = strncat(destctxt->filepath + destctxt->filelen, META_SFX, SFX_PADDING);
      if (res != (destctxt->filepath + destctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append meta suffix \"%s\" to destination file path!\n", META_SFX);
         errno = EBADF;
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(src_meta_path);
         return -1;
      }

      // duplicate the destination meta path and check for success
      char *dest_meta_path = strdup(destctxt->filepath);
      if (dest_meta_path == NULL)
      {
         LOG(LOG_ERR, "failed to allocate space for a new destination meta string! (%s)\n", strerror(errno));
         *(destctxt->filepath + destctxt->filelen) = '\0'; // make sure no suffix remains
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(src_meta_path);
         return -1;
      }
      *(destctxt->filepath + destctxt->filelen) = '\0'; // make sure no suffix remains

      // attempt to link data and check for success
      if (linkat(dctxt->sec_root, srcctxt->filepath, dctxt->sec_root, destctxt->filepath, 0))
      {
         LOG(LOG_ERR, "failed to link data file \"%s\" to \"%s\" (%s)\n", srcctxt->filepath, destctxt->filepath, strerror(errno));
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(src_meta_path);
         free(dest_meta_path);
         return manual_migrate(dctxt, objID, src, dest);
      }

      int ret = 0;
      // attempt to link meta and check for success
      if (linkat(dctxt->sec_root, src_meta_path, dctxt->sec_root, dest_meta_path, 0))
      {
         LOG(LOG_ERR, "failed to link meta file \"%s\" to \"%s\" (%s)\n", src_meta_path, dest_meta_path, strerror(errno));
         if (unlinkat(dctxt->sec_root, destctxt->filepath, 0))
         {
            ret = -2;
         }
         else
         {
            ret = manual_migrate(dctxt, objID, src, dest);
         }
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(src_meta_path);
         free(dest_meta_path);
         return ret;
      }

      // attempt to unlink data and check for success
      if (unlinkat(dctxt->sec_root, srcctxt->filepath, 0))
      {
         LOG(LOG_ERR, "failed to unlink source data file \"%s\" (%s)\n", srcctxt->filepath, strerror(errno));
         ret = 1;
      }

      // attempt to unlink meta and check for success
      if (unlinkat(dctxt->sec_root, src_meta_path, 0))
      {
         LOG(LOG_ERR, "failed to unlink source meta file \"%s\" to (%s)\n", src_meta_path, strerror(errno));
         ret = 1;
      }

      free(src_meta_path);
      free(dest_meta_path);
      free(srcctxt->filepath);
      free(srcctxt);
      free(destctxt->filepath);
      free(destctxt);
      return ret;
   }
   // allow object to be accessed from both locations (symlink dest loc to src loc)
   else
   {
      char *oldpath = convert_relative(srcctxt->filepath, destctxt->filepath);
      if (oldpath == NULL)
      {
         LOG(LOG_ERR, "failed to create relative data path for symlink\n");
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         return -1;
      }

      // attempt to symlink data and check for success
      if (symlinkat(oldpath, dctxt->sec_root, destctxt->filepath))
      {
         LOG(LOG_ERR, "failed to create data symlink\n");
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(oldpath);
         return -1;
      }

      // append the meta suffix and check for success
      char *res = strncat(srcctxt->filepath + srcctxt->filelen, META_SFX, SFX_PADDING);
      if (res != (srcctxt->filepath + srcctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append meta suffix \"%s\" to source file path!\n", META_SFX);
         errno = EBADF;
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(oldpath);
         return -1;
      }
      res = strncat(destctxt->filepath + destctxt->filelen, META_SFX, SFX_PADDING);
      if (res != (destctxt->filepath + destctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append meta suffix \"%s\" to destination file path!\n", META_SFX);
         errno = EBADF;
         *(srcctxt->filepath + srcctxt->filelen) = '\0'; // make sure no suffix remains
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(oldpath);
         return -1;
      }

      free(oldpath);
      oldpath = convert_relative(srcctxt->filepath, destctxt->filepath);
      if (oldpath == NULL)
      {
         LOG(LOG_ERR, "failed to create relative meta path for symlink\n");
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         return -1;
      }

      // attempt to symlink meta and check for success
      if (symlinkat(oldpath, dctxt->sec_root, destctxt->filepath))
      {
         LOG(LOG_ERR, "failed to create meta symlink\n");
         *(srcctxt->filepath + srcctxt->filelen) = '\0';   // make sure no suffix remains
         *(destctxt->filepath + destctxt->filelen) = '\0'; // make sure no suffix remains
         free(srcctxt->filepath);
         free(srcctxt);
         free(destctxt->filepath);
         free(destctxt);
         free(oldpath);
         return -1;
      }

      *(srcctxt->filepath + srcctxt->filelen) = '\0';   // make sure no suffix remains
      *(destctxt->filepath + destctxt->filelen) = '\0'; // make sure no suffix remains
      free(oldpath);
   }

   free(srcctxt->filepath);
   free(srcctxt);
   free(destctxt->filepath);
   free(destctxt);
   return 0;
}

int posix_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   POSIX_DAL_CTXT dctxt = (POSIX_DAL_CTXT)ctxt; // should have been passed a posix context

   // allocate space for a new BLOCK context
   POSIX_BLOCK_CTXT bctxt = malloc(sizeof(struct posix_block_context_struct));
   if (bctxt == NULL)
   {
      return -1;
   } // malloc will set errno
   bctxt->mode = DAL_WRITE; // assume write mode

   // popultate the full file path for this object
   if (expand_dir_template(dctxt, bctxt, location, objID) != 0)
   {
      free(bctxt);
      return -1;
   }

   int res = block_delete(bctxt, 1);

   free(bctxt->filepath);
   free(bctxt);
   return res;
}

int posix_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   POSIX_DAL_CTXT dctxt = (POSIX_DAL_CTXT)ctxt; // should have been passed a posix context

   // allocate space for a new BLOCK context
   POSIX_BLOCK_CTXT bctxt = malloc(sizeof(struct posix_block_context_struct));
   if (bctxt == NULL)
   {
      return -1;
   } // malloc will set errno

   // popultate the full file path for this object
   if (expand_dir_template(dctxt, bctxt, location, objID) != 0)
   {
      free(bctxt);
      return -1;
   }

   // perform a stat() call, and just check the return code
   struct stat sstr;
   int res = fstatat(dctxt->sec_root, bctxt->filepath, &sstr, 0);

   free(bctxt->filepath);
   free(bctxt);
   return res;
}

int posix_cleanup(DAL dal)
{
   if (dal == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal!\n");
      return -1;
   }
   POSIX_DAL_CTXT dctxt = (POSIX_DAL_CTXT)dal->ctxt; // should have been passed a posix context

   // free DAL context state
   if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
   free(dctxt->dirtmp);
   free(dctxt);
   // free the DAL struct and its associated state
   free(dal);
   return 0;
}

BLOCK_CTXT posix_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return NULL;
   }
   POSIX_DAL_CTXT dctxt = (POSIX_DAL_CTXT)ctxt; // should have been passed a posix context

   // allocate space for a new BLOCK context
   POSIX_BLOCK_CTXT bctxt = calloc( 1, sizeof(struct posix_block_context_struct));
   if (bctxt == NULL)
   {
      LOG( LOG_ERR, "Failed to allocate a new block ctxt struct\n" );
      return NULL;
   } // calloc will set errno

   // popultate the full file path for this object
   if (expand_dir_template(dctxt, bctxt, location, objID) != 0)
   {
      free(bctxt);
      return NULL;
   }

   // populate other BLOCK context fields
   bctxt->mode = mode;

   char *res = NULL;

   // append the meta suffix and check for success
   res = strncat(bctxt->filepath + bctxt->filelen, META_SFX, SFX_PADDING);
   if (res != (bctxt->filepath + bctxt->filelen))
   {
      LOG(LOG_ERR, "failed to append meta suffix \"%s\" to file path!\n", META_SFX);
      errno = EBADF;
      free(bctxt->filepath);
      free(bctxt);
      return NULL;
   }

   int metalen = strlen(META_SFX);

   int oflags = O_WRONLY | O_CREAT | O_EXCL;
   if (mode == DAL_READ)
   {
      LOG(LOG_INFO, "Open for READ\n");
      oflags = O_RDONLY;
   }
   else if (mode == DAL_METAREAD)
   {
      LOG(LOG_INFO, "Open for METAREAD\n");
      oflags = O_RDONLY;
      bctxt->fd = -1;
   }
   else
   {
      // append the proper suffix and check for success
      if (mode == DAL_WRITE)
      {
         res = strncat(bctxt->filepath + bctxt->filelen + metalen, WRITE_SFX, SFX_PADDING - metalen);
      }
      else if (mode == DAL_REBUILD)
      {
         res = strncat(bctxt->filepath + bctxt->filelen + metalen, REBUILD_SFX, SFX_PADDING - metalen);
      }
      if (res != (bctxt->filepath + bctxt->filelen + metalen))
      {
         LOG(LOG_ERR, "failed to append suffix to meta path!\n");
         errno = EBADF;
         free(bctxt->filepath);
         free(bctxt);
         return NULL;
      }
   }

   // open the meta file and check for success
   mode_t mask = umask(0);
   bctxt->mfd = openat(dctxt->sec_root, bctxt->filepath, oflags | dctxt->metaflags, S_IRWXU | S_IRWXG | S_IRWXO); // mode arg should be harmlessly ignored if reading
   if (bctxt->mfd < 0  &&  errno == EEXIST  &&  mode != DAL_READ  &&  mode != DAL_METAREAD ) {
      // specifically for a write EEXIST error, unlink the dest path and retry once
      unlinkat( dctxt->sec_root, bctxt->filepath, 0 ); // don't bother checking for this failure, only the open result matters
      bctxt->mfd = openat(dctxt->sec_root, bctxt->filepath, oflags | dctxt->metaflags, S_IRWXU | S_IRWXG | S_IRWXO);
   }
   if (bctxt->mfd < 0)
   {
      LOG(LOG_ERR, "failed to open meta file: \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
      if (mode == DAL_METAREAD)
      {
         umask(mask);
         free(bctxt->filepath);
         free(bctxt);
         return NULL;
      }
   }
   // remove any suffix in the simplest possible manner
   *(bctxt->filepath + bctxt->filelen) = '\0';

   if (mode == DAL_WRITE || mode == DAL_REBUILD)
   {
      // append the proper suffix
      if (mode == DAL_WRITE)
      {
         LOG(LOG_INFO, "Open for WRITE\n");
         res = strncat(bctxt->filepath + bctxt->filelen, WRITE_SFX, SFX_PADDING);
      }
      else if (mode == DAL_REBUILD)
      {
         LOG(LOG_INFO, "Open for REBUILD\n");
         res = strncat(bctxt->filepath + bctxt->filelen, REBUILD_SFX, SFX_PADDING);
      } // NOTE -- invalid mode will leave res == NULL
      // check for success appending the suffix
      if (res != (bctxt->filepath + bctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append suffix to file path!\n");
         errno = EBADF;
         close(bctxt->mfd);
         umask(mask);
         free(bctxt->filepath);
         free(bctxt);
         return NULL;
      }
   }

   if (mode != DAL_METAREAD)
   {
      // open the file and check for success
      bctxt->fd = openat(dctxt->sec_root, bctxt->filepath, oflags | dctxt->dataflags, S_IRWXU | S_IRWXG | S_IRWXO); // mode arg should be harmlessly ignored if reading
      if (bctxt->fd < 0  &&  errno == EEXIST  &&  mode != DAL_READ ) {
         // specifically for a write EEXIST error, unlink the dest path and retry once
         unlinkat( dctxt->sec_root, bctxt->filepath, 0 ); // don't bother checking for this failure, only the open result matters
         bctxt->fd = openat(dctxt->sec_root, bctxt->filepath, oflags | dctxt->metaflags, S_IRWXU | S_IRWXG | S_IRWXO);
      }
      if (bctxt->fd < 0)
      {
         LOG(LOG_ERR, "failed to open file: \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
         close(bctxt->mfd);
         umask(mask);
         free(bctxt->filepath);
         free(bctxt);
         return NULL;
      }
   }
   // remove any suffix in the simplest possible manner
   *(bctxt->filepath + bctxt->filelen) = '\0';

   // restore the previous umask
   umask(mask);

   // finally, return a reference to our BLOCK context
   return bctxt;
}

int posix_set_meta(BLOCK_CTXT ctxt, const meta_info* source )
{
   return dal_set_meta_helper( posix_set_meta_internal, ctxt, source );
}

int posix_get_meta(BLOCK_CTXT ctxt, meta_info* target )
{
   return dal_get_meta_helper( posix_get_meta_internal, ctxt, target );
}

int posix_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   POSIX_BLOCK_CTXT bctxt = (POSIX_BLOCK_CTXT)ctxt; // should have been passed a posix context

   // just a write to our pre-opened FD
   if (write(bctxt->fd, buf, size) != size)
   {
      LOG(LOG_ERR, "write to \"%s\" failed (%s)\n", bctxt->filepath, strerror(errno));
      return -1;
   }

   return 0;
}

ssize_t posix_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   POSIX_BLOCK_CTXT bctxt = (POSIX_BLOCK_CTXT)ctxt; // should have been passed a posix context

   // abort, unless we're reading
   if (bctxt->mode != DAL_READ)
   {
      LOG(LOG_ERR, "Can only perform get ops on a DAL_READ block handle!\n");
      return -1;
   }

   // always reseek ( minimal performance impact and a bit more explicitly safe )
   LOG(LOG_INFO, "Performing seek to offset of %zd\n", offset);
   off_t newoff = lseek(bctxt->fd, offset, SEEK_SET);
   // make sure our new offset makes sense
   if (newoff != offset)
   {
      LOG(LOG_ERR, "failed to seek to offset %zd of file \"%s\" (%s)\n", offset, bctxt->filepath, strerror(errno));
      return -1;
   }

   // just a read from our pre-opened FD
   ssize_t res = read(bctxt->fd, buf, size);

   return res;
}

int posix_abort(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   POSIX_BLOCK_CTXT bctxt = (POSIX_BLOCK_CTXT)ctxt; // should have been passed a posix context

   int retval = 0;
   // close the file descriptor, note but bypass failure
   if (close(bctxt->fd) != 0)
   {
      LOG(LOG_WARNING, "failed to close data file \"%s\" during abort (%s)\n", bctxt->filepath, strerror(errno));
   }
   if (close(bctxt->mfd) != 0)
   {
      LOG(LOG_WARNING, "failed to close meta file \"%s%s\" during abort (%s)\n", bctxt->filepath, META_SFX, strerror(errno));
   }

   // free state
   free(bctxt->filepath);
   free(bctxt);
   return retval;
}

int posix_close(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   POSIX_BLOCK_CTXT bctxt = (POSIX_BLOCK_CTXT)ctxt; // should have been passed a posix context

   // if this is not a meta-only reference, attempt to close our FD and check for success
   if ((bctxt->mode != DAL_METAREAD) && (close(bctxt->fd) != 0))
   {
      LOG(LOG_ERR, "failed to close data file \"%s\" (%s)\n", bctxt->filepath, strerror(errno));
      return -1;
   }

   // attempt to close our meta FD and check for success
   if (close(bctxt->mfd))
   {
      LOG(LOG_ERR, "failed to close meta file \"%s%s\" (%s)\n", bctxt->filepath, META_SFX, strerror(errno));
      return -1;
   }

   char *res = NULL;
   if (bctxt->mode == DAL_WRITE || bctxt->mode == DAL_REBUILD)
   {
      if (bctxt->mode == DAL_WRITE)
      {
         // append the write suffix
         res = strncat(bctxt->filepath + bctxt->filelen, WRITE_SFX, SFX_PADDING);
      }
      else
      {
         // append the rebuild suffix
         res = strncat(bctxt->filepath + bctxt->filelen, REBUILD_SFX, SFX_PADDING);
      }
      // check for success
      if (res != (bctxt->filepath + bctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append write suffix \"%s\" to file path!\n", WRITE_SFX);
         errno = EBADF;
         *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
         return -1;
      }

      // duplicate the path and check for success
      char *write_path = strdup(bctxt->filepath);
      *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains

      // attempt to rename and check for success
      if (renameat(bctxt->sfd, write_path, bctxt->sfd, bctxt->filepath) != 0)
      {
         LOG(LOG_ERR, "failed to rename data file \"%s\" to \"%s\" (%s)\n", write_path, bctxt->filepath, strerror(errno));
         free(write_path);
         return -1;
      }
      free(write_path);

      // append the meta suffix and check for success
      res = strncat(bctxt->filepath + bctxt->filelen, META_SFX, SFX_PADDING);
      if (res != (bctxt->filepath + bctxt->filelen))
      {
         LOG(LOG_ERR, "failed to append meta suffix \"%s\" to file path!\n", META_SFX);
         errno = EBADF;
         return -1;
      }

      int metalen = strlen(META_SFX);

      // append the proper suffix and check for success
      if (bctxt->mode == DAL_WRITE)
      {
         res = strncat(bctxt->filepath + bctxt->filelen + metalen, WRITE_SFX, SFX_PADDING - metalen);
      }
      if (bctxt->mode == DAL_REBUILD)
      {
         res = strncat(bctxt->filepath + bctxt->filelen + metalen, REBUILD_SFX, SFX_PADDING - metalen);
      }
      if (res != (bctxt->filepath + bctxt->filelen + metalen))
      {
         LOG(LOG_ERR, "failed to append write suffix \"%s\" to file path!\n", WRITE_SFX);
         errno = EBADF;
         *(bctxt->filepath + bctxt->filelen) = '\0'; // make sure no suffix remains
         return -1;
      }

      // duplicate the path and check for success
      char *meta_path = strdup(bctxt->filepath);
      *(bctxt->filepath + bctxt->filelen + metalen) = '\0'; // make sure no suffix remains

      // attempt to rename and check for success
      if (renameat(bctxt->sfd, meta_path, bctxt->sfd, bctxt->filepath) != 0)
      {
         LOG(LOG_ERR, "failed to rename meta file \"%s\" to \"%s\" (%s)\n", meta_path, bctxt->filepath, strerror(errno));
         free(meta_path);
         return -1;
      }
      free(meta_path);
   }

   // free state
   free(bctxt->filepath);
   free(bctxt);
   return 0;
}

//   -------------    POSIX INITIALIZATION    -------------

DAL posix_dal_init(xmlNode *root, DAL_location max_loc)
{
   // first, calculate the number of digits required for pod/cap/block/scatter
   int d_pod = num_digits(max_loc.pod);
   if (d_pod < 1)
   {
      errno = EDOM;
      LOG(LOG_ERR, "detected an inappropriate value for maximum pod: %d\n", max_loc.pod);
      return NULL;
   }
   int d_cap = num_digits(max_loc.cap);
   if (d_cap < 1)
   {
      errno = EDOM;
      LOG(LOG_ERR, "detected an inappropriate value for maximum cap: %d\n", max_loc.cap);
      return NULL;
   }
   int d_block = num_digits(max_loc.block);
   if (d_block < 1)
   {
      errno = EDOM;
      LOG(LOG_ERR, "detected an inappropriate value for maximum block: %d\n", max_loc.block);
      return NULL;
   }
   int d_scatter = num_digits(max_loc.scatter);
   if (d_scatter < 1)
   {
      errno = EDOM;
      LOG(LOG_ERR, "detected an inappropriate value for maximum scatter: %d\n", max_loc.scatter);
      return NULL;
   }

   // allocate space for our context struct
   POSIX_DAL_CTXT dctxt = calloc(1,sizeof(struct posix_dal_context_struct));
   if (dctxt == NULL)
   {
      return NULL;
   } // calloc will set errno

   // initialize some vals
   dctxt->dirtmp = NULL;
   dctxt->sec_root = AT_FDCWD;
   dctxt->dataflags = 0;
   dctxt->metaflags = 0;
   size_t io_size = IO_SIZE;

   int origerrno = errno;
   errno = EINVAL; // assume EINVAL

   // find the secure root node and io size
   while (root != NULL)
   {
      if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "dir_template", 13) == 0)
      {

         // check for duplicate
         if ( dctxt->dirtmp != NULL ) {
            LOG( LOG_ERR, "Detected duplicate 'dir_template' definition\n" );
            if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
            free( dctxt->dirtmp );
            free( dctxt );
            return NULL;
         }

         // make sure that node contains a text element within it
         if (root->children == NULL || root->children->type != XML_TEXT_NODE)
         {
            LOG( LOG_ERR, "'dir_template' node has invalid content\n" );
            if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
            free(dctxt);
            return NULL;
         }

         // copy the dir template into the context struct
         dctxt->dirtmp = strdup((char *)root->children->content);
         if (dctxt->dirtmp == NULL)
         {
            if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
            free(dctxt);
            return NULL;
         } // strdup will set errno

         // initialize all other context fields
         dctxt->tmplen = strlen(dctxt->dirtmp);
         dctxt->max_loc = max_loc;
         dctxt->dirpad = 0;

         // calculate a real value for dirpad based on number of p/c/b/s substitutions
         char *parse = dctxt->dirtmp;
         while (*parse != '\0')
         {
            if (*parse == '{')
            {
               // possible substituion, but of what type?
               int increase = 0;
               switch (*(parse + 1))
               {
               case 'p':
                  increase = d_pod;
                  break;

               case 'c':
                  increase = d_cap;
                  break;

               case 'b':
                  increase = d_block;
                  break;

               case 's':
                  increase = d_scatter;
                  break;
               }
               // if this looks like a valid substitution, check for a final '}'
               if (increase > 0 && *(parse + 2) == '}')
               {                                 // NOTE -- we know *(parse+1) != '\0'
                  dctxt->dirpad += increase - 3; // add increase, adjusting for chars used in substitution
               }
            }
            parse++; // next char
         }
      }
      else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "sec_root", 9) == 0)
      {
         if (root->children != NULL  &&  root->children->type == XML_TEXT_NODE)
         {
            dctxt->sec_root = open((char *)root->children->content, O_DIRECTORY | O_RDONLY );
         }
      }
      else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "io", 3) == 0)
      {
         // loop through and parse all 'io' attribute values
         xmlAttr* attr = root->properties;
         for ( ; attr; attr = attr->next ) {
            if ( attr->type != XML_ATTRIBUTE_NODE ) {
               LOG( LOG_ERR, "Encountered unrecognized property type of POSIX DAL 'io' definition\n" );
               break;
            }
            if ( attr->children == NULL  ||  attr->children->type != XML_TEXT_NODE  ||  attr->children->content == NULL ) {
               LOG( LOG_ERR, "Encountered a \"%s\" property of POSIX DAL 'io' definition with no associated value\n", (char*)attr->name );
               break;
            }
            if ( strncasecmp( (char*)attr->name, "size", 5 ) == 0 ) {
               if (atol((char *)attr->children->content))
               {
                  io_size = atol((char *)attr->children->content);
               }
               else {
                  LOG( LOG_ERR, "Failed to parse POSIX DAL 'io' size value: \"%s\"\n", (char *)attr->children->content );
                  break;
               }
            }
            else if ( strncasecmp( (char*)attr->name, "dataflags", 10 ) == 0 ) {
               if ( parse_open_flags( (const char*)attr->children->content, &(dctxt->dataflags) ) ) { break; }
            }
            else if ( strncasecmp( (char*)attr->name, "metaflags", 10 ) == 0 ) {
               if ( parse_open_flags( (const char*)attr->children->content, &(dctxt->metaflags) ) ) { break; }
            }
            else {
               LOG( LOG_ERR, "Encountered an unrecognized \"%s\" property of POSIX DAL 'io' definition\n", (char*)attr->name );
               break;
            }
         }
         if ( attr ) { // indicates a 'break' from the above loop
            if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
            if ( dctxt->dirtmp ) { free( dctxt->dirtmp ); }
            free( dctxt );
            return NULL;
         }
      }
      else {
         LOG( LOG_ERR, "Encountered unrecognized config element: \"%s\"\n", (char *)root->name );
         if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
         if ( dctxt->dirtmp ) { free( dctxt->dirtmp ); }
         free( dctxt );
         return NULL;
      }
      root = root->next;
   }

   // make sure a dir template was provided
   if ( dctxt->dirtmp == NULL ) {
      LOG( LOG_ERR, "No 'dir_template' specification was provided\n" );
      if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
      free( dctxt );
      return NULL;
   }

   // make sure the secure root handle was properly opened (if specified)
   if (dctxt->sec_root == -1)
   {
      LOG(LOG_ERR, "failed to find or open secure root handle\n");
      free( dctxt->dirtmp );
      free(dctxt);
      return NULL;
   }

   // allocate and populate a new DAL structure
   DAL pdal = calloc( 1, sizeof(struct DAL_struct) );
   if (pdal == NULL)
   {
      LOG(LOG_ERR, "failed to allocate space for a DAL_struct\n");
      if ( dctxt->sec_root > 0 ) { close( dctxt->sec_root ); }
      free( dctxt->dirtmp );
      free(dctxt);
      return NULL;
   } // calloc will set errno
   pdal->name = "posix";
   pdal->ctxt = (DAL_CTXT)dctxt;
   pdal->io_size = io_size;
   pdal->verify = posix_verify;
   pdal->migrate = posix_migrate;
   pdal->open = posix_open;
   pdal->set_meta = posix_set_meta;
   pdal->get_meta = posix_get_meta;
   pdal->put = posix_put;
   pdal->get = posix_get;
   pdal->abort = posix_abort;
   pdal->close = posix_close;
   pdal->del = posix_del;
   pdal->stat = posix_stat;
   pdal->cleanup = posix_cleanup;
   errno = origerrno; // cleanup errno
   return pdal;


}
