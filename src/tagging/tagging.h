#ifndef _TAGGING_H
#define _TAGGING_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#include "ne/ne.h"


// MARFS FILE TAG  --  attached to every marfs file, providing stream/data info

#define FTAG_CURRENT_MAJORVERSION 0
#define FTAG_CURRENT_MINORVERSION 1

#define FTAG_RESERVED_CHARS "()|"

#define FTAG_NAME "MARFS-FILE"

typedef enum 
{
   // Data Object State Indicators ( Every file will be in one of the following states )
   FTAG_INIT = 0,  // initial state   -- no file data exists
   FTAG_SIZED = 1, // sized state     -- known lower bound on file size (may be up to objsize bytes larger)
   FTAG_FIN = 2,   // finalized state -- known total file size
   FTAG_COMP = 3,  // completed state -- all data written
   FTAG_DATASTATE = FTAG_COMP, // mask value for retrieving data state indicator

   // State Flag values ( These may or may not be set )
   FTAG_WRITEABLE = 4,  // Writable flag -- file's data is writable by arbitrary procs
   FTAG_READABLE = 8,  // Readable flag -- file's data is readable by arbitrary procs
} FTAG_STATE;


typedef struct ftag_struct {
   // version info
   unsigned int majorversion;
   unsigned int minorversion;
   // stream identification info
   char* ctag;
   char* streamid;
   // stream structure info
   size_t objfiles;
   size_t objsize;
   // reference tree info
   int    refbreadth;
   int    refdepth;
   int    refdigits;
   // file position info
   size_t fileno;
   size_t objno;
   size_t offset;
   char   endofstream;
   // data content info
   ne_erasure protection;
   size_t bytes;
   size_t availbytes;
   size_t recoverybytes;
   FTAG_STATE state;
} FTAG;

/**
 * Populate the given ftag struct based on the content of the given ftag string
 * @param FTAG* ftag : Reference to the ftag struct to be populated
 * @param char* ftagstr : String value to be parsed for structure values
 * @return int : Zero on success, or -1 if a failure occurred
 */
int ftag_initstr( FTAG* ftag, char* ftagstr );

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
size_t ftag_tostr( const FTAG* ftag, char* tgtstr, size_t len );

/**
 * Compare the content of the given FTAG references
 * @param const FTAG* ftag1 : First FTAG reference to compare
 * @param const FTAG* ftag2 : Second FTAG reference to compare
 * @return int : 0 if the two FTAGs match,
 *               1 if the two FTAGs differ,
 *               -1 if a failure occurred ( NULL ftag reference )
 */
int ftag_cmp( const FTAG* ftag1, const FTAG* ftag2 );

/**
 * Populate the given string buffer with the meta file ID string produced from the given ftag
 * @param const FTAG* ftag : Reference to the ftag struct to pull values from
 * @param char* tgtstr : String buffer to be populated with the meta file ID
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficient buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t ftag_metatgt( const FTAG* ftag, char* tgtstr, size_t len );

/**
 * Populate the given string buffer with the rebuild marker produced from the given ftag
 * @param const FTAG* ftag : Reference to the ftag struct to pull values from
 * @param char* tgtstr : String buffer to be populated with the rebuild marker name
 * @param size_t len : Byte length of the target buffer
 * @return size_t : Length of the produced string ( excluding NULL-terminator ), or zero if
 *                  an error occurred.
 *                  NOTE -- if this value is >= the length of the provided buffer, this
 *                  indicates that insufficient buffer space was provided and the resulting
 *                  output string was truncated.
 */
size_t ftag_rebuildmarker( const FTAG* ftag, char* tgtstr, size_t len );

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
size_t ftag_repackmarker( const FTAG* ftag, char* tgtstr, size_t len );

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
ssize_t ftag_metainfo( const char* fileid, char* entrytype );

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
size_t ftag_datatgt( const FTAG* ftag, char* tgtstr, size_t len );


// MARFS REBUILD TAG  --  attached to rebuild marker files, providing rebuild info

#define RTAG_CURRENT_MAJORVERSION 0
#define RTAG_CURRENT_MINORVERSION 1

// NOTE -- RTAG names depend upon the object number they are associated with
//         Use the rtag_getname() func, instead of a static _NAME definition

typedef struct rtag_struct {
   // version info
   unsigned int majorversion;
   unsigned int minorversion;
   // marker info
   time_t   createtime;
   // erasure state info
   size_t   stripewidth;
   ne_state stripestate;
} RTAG;

/**
 * Generate the appropraite RTAG name value for a specific data object
 * @param size_t objno : Object number associated with the RTAG
 * @return char* : String name of the RTAG value, or NULL on failure
 *                 NOTE -- it is the caller's responsibility to free this
 */
char* rtag_getname( size_t objno );

/**
 * Initialize an RTAG based on the provided string value
 * @param ne_state* rtag : Reference to the RTAG to be populated
 *                         NOTE -- If this RTAG has allocated (non-NULL) ne_state.meta/data_status arrays,
 *                                 they are assumed to be of rtag.stripewidth length.  If this length matches
 *                                 that of the parsed RTAG, the arrays will be reused.  Otherwise, the arrays
 *                                 will be freed and recreated with an appropriate length.
 * @param size_t stripewidth : Expected N+E stripe width
 * @param const char* rtagstr : Reference to the string to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int rtag_initstr( RTAG* rtag, const char* rtagstr );

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
size_t rtag_tostr( const RTAG* rtag, char* tgtstr, size_t len );

/**
 * Allocates internal memory for the given RTAG ( based on rtag->stripewidth )
 * @param RTAG* rtag : Reference to the RTAG to be allocated
 * @return int : Zero on success, or -1 on failure
 */
int rtag_alloc( RTAG* rtag );

/**
 * Frees internal memory allocations of the given RTAG
 */
void rtag_free( RTAG* rtag );

/**
 * Produce a duplicate of the given RTAG
 * @param const RTAG* srcrtag : Reference to the RTAG to duplicate
 * @param RTAG* destrtag : Reference to the RTAG to be copied into
 *                         NOTE -- This func will call rtag_free() on this reference
 * @return int : Zero on success, or -1 on failure
 */
int rtag_dup( const RTAG* srcrtag, RTAG* destrtag );


// MARFS ORIGINAL REPACK TAG  --  attached to repacked files, storing original FTAG info

#define OREPACK_TAG_NAME "ORIG-MARFS-FILE"

// MARFS TARGET REPACK TAG  --  attached to files during a repack, storing target FTAG info

#define TREPACK_TAG_NAME "TGT-MARFS-FILE"


// MARFS Garbage Collection TAG  -- attached to files when subsequent datastream references have been deleted

#define GCTAG_CURRENT_MAJORVERSION 0
#define GCTAG_CURRENT_MINORVERSION 1

#define GCTAG_NAME "MARFS-GC"

typedef struct gctag_struct {
   size_t refcnt;
   char   eos;
   char   delzero;
   char   inprog;
} GCTAG;

/**
 * Initialize a GCTAG based on the provided string value
 * @param GCTAG* gctag : Reference to the GCTAG structure to be populated
 * @param const char* gctagstr : Reference to the string to be parsed
 * @return int : Zero on success, or -1 on failure
 */
int gctag_initstr( GCTAG* gctag, char* gctagstr );

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
size_t gctag_tostr( GCTAG* gctag, char*tgtstr, size_t len );


#endif // _TAGGING_H

