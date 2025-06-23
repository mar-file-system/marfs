#ifndef __METAINFO_H__
#define __METAINFO_H__

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
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

