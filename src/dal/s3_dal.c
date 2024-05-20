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

#include "erasureUtils_auto_config.h"
#ifdef DEBUG_DAL
#define DEBUG DEBUG_DAL
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "s3_dal"
#include "logging/logging.h"

#include "dal.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <libs3.h>

//   -------------    S3 DEFINITIONS    -------------

#define TIMEOUT 0            // S3 request timeout
#define TRIES 5              // Number of times to retry a request
#define IO_SIZE (5 << 20)    // Preferred I/O Size: 5M
#define NO_OBJID "noneGiven" // Substitute ID when one is provided

//   -------------    S3 CONTEXT    -------------

// Growbuffer from https://github.com/bji/libs3/blob/master/src/s3.c
typedef struct growbuffer
{
   // The total number of bytes, and the start byte
   int size;
   // The start byte
   int start;
   // The blocks
   char data[64 * 1024];
   struct growbuffer *prev, *next;
} growbuffer;

typedef struct s3_block_context_struct
{
   char *bucket;                   // Bucket name
   S3BucketContext *bucketContext; // Context for object's bucket
   char *key;                      // Object key
   DAL_MODE mode;                  // Mode in which this block was opened

   growbuffer *data_gb; // Buffer for data
   int data_size;       // Size of data buffer

   char *meta; // Metadata buffer to be written on close (if any)

   char *upload_id;     // Upload ID for multipart upload (if write enabled)
   int seq;             // Part number for multipart upload (if write enabled)
   growbuffer *part_gb; // Buffer to hold list of parts (if write enable)
   int part_size;       // Size of part buffer (if write enable)
} * S3_BLOCK_CTXT;

typedef struct s3_dal_context_struct
{
   DAL_location max_loc; // Maximum pod/cap/block/scatter values
   char *accessKey;      // AWS Access Key ID
   char *secretKey;      // AWS Secret Access Key
   char *region;         // AWS Region Name
} * S3_DAL_CTXT;

static S3Status statusG;

//   -------------    S3 INTERNAL FUNCTIONS    -------------

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
 * Add data to the given growbuffer. From
 * https://github.com/bji/libs3/blob/master/src/s3.c
 * @param growbuffer** gb : Buffer to add data to
 * @param const char* data : Data to be added
 * @param int dataLen : Number of chars to be added
 * @return int : Number of chars added, or 0 on an error
 */
static int growbuffer_append(growbuffer **gb, const char *data, int dataLen)
{
   int origDataLen = dataLen;
   while (dataLen)
   {
      growbuffer *buf = *gb ? (*gb)->prev : 0;
      if (!buf || (buf->size == sizeof(buf->data)))
      {
         buf = (growbuffer *)malloc(sizeof(growbuffer));
         if (!buf)
         {
            return 0;
         }
         buf->size = 0;
         buf->start = 0;
         if (*gb && (*gb)->prev)
         {
            buf->prev = (*gb)->prev;
            buf->next = *gb;
            (*gb)->prev->next = buf;
            (*gb)->prev = buf;
         }
         else
         {
            buf->prev = buf->next = buf;
            *gb = buf;
         }
      }

      int toCopy = (sizeof(buf->data) - buf->size);
      if (toCopy > dataLen)
      {
         toCopy = dataLen;
      }

      memcpy(&(buf->data[buf->size]), data, toCopy);

      buf->size += toCopy, data += toCopy, dataLen -= toCopy;
   }

   return origDataLen;
}

/** (INTERNAL HELPER FUNCTION)
 * Read data from the given growbuffer. From
 * https://github.com/bji/libs3/blob/master/src/s3.c
 * @param growbuffer** gb : Buffer to add data to
 * @param int amt : Number of chars requested to be read
 * @param int* amtReturn : Number of chars actually read
 * @param char* buffer : Buffer to read data into
 */
static void growbuffer_read(growbuffer **gb, int amt, int *amtReturn,
                            char *buffer)
{
   *amtReturn = 0;

   growbuffer *buf = *gb;

   if (!buf)
   {
      return;
   }

   *amtReturn = (buf->size > amt) ? amt : buf->size;

   memcpy(buffer, &(buf->data[buf->start]), *amtReturn);

   buf->start += *amtReturn, buf->size -= *amtReturn;

   if (buf->size == 0)
   {
      if (buf->next == buf)
      {
         *gb = 0;
      }
      else
      {
         *gb = buf->next;
         buf->prev->next = buf->next;
         buf->next->prev = buf->prev;
      }
      free(buf);
      buf = NULL;
   }
}

/** (INTERNAL HELPER FUNCTION)
 * Free all data in the given growbuffer. From
 * https://github.com/bji/libs3/blob/master/src/s3.c
 * @param growbuffer** gb : Buffer to add data to
 */
static void growbuffer_destroy(growbuffer *gb)
{
   growbuffer *start = gb;

   while (gb)
   {
      growbuffer *next = gb->next;
      free(gb);
      gb = (next == start) ? 0 : next;
   }
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made after initiation of a multipart upload operation.  It
 * indicates that the multi part upload has been created and provides the
 * id that can be used to associate multi upload parts with the multi upload
 * operation.
 * @param upload_id is the unique identifier if this multi part upload
 *        operation, to be used in calls to S3_upload_part and
 *        S3_complete_multipart_upload
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 */
static S3Status initialMultipartCallback(const char *upload_id, void *callbackData)
{
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)callbackData;
   bctxt->upload_id = strdup(upload_id);
   return S3StatusOK;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made during a put object operation, to obtain the next
 * chunk of data to put to the S3 service as the contents of the object.  This
 * callback is made repeatedly, each time acquiring the next chunk of data to
 * write to the service, until a negative or 0 value is returned.
 * @param bufferSize gives the maximum number of bytes that may be written
 *        into the buffer parameter by this callback
 * @param buffer gives the buffer to fill with at most bufferSize bytes of
 *        data as the next chunk of data to send to S3 as the contents of this
 *        object
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return < 0 to abort the request with the S3StatusAbortedByCallback, which
 *        will be pased to the response complete callback for this request, or
 *        0 to indicate the end of data, or > 0 to identify the number of
 *        bytes that were written into the buffer by this callback
 **/
static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData)
{
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)callbackData;

   int ret = 0;

   if (bctxt->data_size)
   {
      int toRead = ((bctxt->data_size > (unsigned)bufferSize) ? (unsigned)bufferSize : bctxt->data_size);
      if (bctxt->data_gb)
      {
         growbuffer_read(&(bctxt->data_gb), toRead, &ret, buffer);
      }
      else
      {
         LOG(LOG_ERR, "missing data growbuffer!\n");
         return -1;
      }
   }

   bctxt->data_size -= ret;

   return ret;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made during a get object operation, to provide the next
 * chunk of data available from the S3 service constituting the contents of
 * the object being fetched.  This callback is made repeatedly, each time
 * providing the next chunk of data read, until the complete object contents
 * have been passed through the callback in this way, or the callback
 * returns an error status.
 * @param bufferSize gives the number of bytes in buffer
 * @param buffer is the data being passed into the callback
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 **/
static S3Status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData)
{
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)callbackData;

   if (!growbuffer_append(&(bctxt->data_gb), buffer, bufferSize))
   {
      LOG(LOG_ERR, "data not appended to growbuffer\n");
      return S3StatusAbortedByCallback;
   }
   bctxt->data_size += bufferSize;

   return S3StatusOK;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made after commit of a multipart upload operation.  It
 * indicates that the data uploaded via the multipart upload operation has
 * been committed.
 * @param location is ??? someone please document this
 * @param etag is the S3 etag of the complete object after the multipart
 *        upload
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 **/
static int commitObjectCallback(int bufferSize, char *buffer,
                                void *callbackData)
{
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)callbackData;
   int ret = 0;
   if (bctxt->part_size)
   {
      int toRead = ((bctxt->part_size > bufferSize) ? bufferSize : bctxt->part_size);
      growbuffer_read(&(bctxt->part_gb), toRead, &ret, buffer);
   }
   bctxt->part_size -= ret;
   return ret;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made whenever the response properties become available for
 * any request.
 * @param properties are the properties that are available from the response
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 **/
static S3Status responsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData)
{
   return S3StatusOK;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made whenever the response properties become available for
 * a get_meta() operation.
 * @param properties are the properties that are available from the response
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 **/
static S3Status getMetaResponsePropertiesCallback(const S3ResponseProperties *properties, void *callbackData)
{
   responsePropertiesCallback(properties, callbackData);
   char *buf = (char *)callbackData;
   strcpy(buf, properties->metaData->value);
   return S3StatusOK;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made whenever the response properties become available for
 * a put() operation.
 * @param properties are the properties that are available from the response
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 **/
static S3Status putResponseProperiesCallback(const S3ResponseProperties *properties, void *callbackData)
{
   responsePropertiesCallback(properties, callbackData);
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)callbackData;
   char buf[256];
   int n = snprintf(buf, sizeof(buf), "<Part><ETag>%s</ETag><PartNumber>%d</PartNumber></Part>", properties->eTag, bctxt->seq);
   bctxt->part_size += growbuffer_append(&(bctxt->part_gb), buf, n);
   return S3StatusOK;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made for each bucket resulting from a list service
 * operation.
 *
 * @param ownerId is the ID of the owner of the bucket
 * @param ownerDisplayName is the owner display name of the owner of the bucket
 * @param bucketName is the name of the bucket
 * @param creationDateSeconds if < 0 indicates that no creation date was
 *        supplied for the bucket; if >= 0 indicates the number of seconds
 *        since UNIX Epoch of the creation date of the bucket
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 * @return S3StatusOK to continue processing the request, anything else to
 *         immediately abort the request with a status which will be
 *         passed to the S3ResponseCompleteCallback for this request.
 *         Typically, this will return either S3StatusOK or
 *         S3StatusAbortedByCallback.
 **/
static S3Status listCallback(const char *ownerId, const char *ownerDisplayName, const char *bucketName, int64_t creationDateSeconds, void *callbackData)
{
   return S3StatusOK;
}

/** (INTERNAL HELPER FUNCTION)
 * This callback is made when the response has been completely received, or an
 * error has occurred which has prematurely aborted the request, or one of the
 * other user-supplied callbacks returned a value intended to abort the
 * request.  This callback is always made for every request, as the very last
 * callback made for that request.
 * @param status gives the overall status of the response, indicating success
 *        or failure; use S3_status_is_retryable() as a simple way to detect
 *        whether or not the status indicates that the request failed but may
 *        be retried.
 * @param errorDetails if non-NULL, gives details as returned by the S3
 *        service, describing the error
 * @param callbackData is the callback data as specified when the request
 *        was issued.
 **/
static void responseCompleteCallback(S3Status status, const S3ErrorDetails *error, void *callbackData)
{
   statusG = status;

   if (error && error->message)
   {
      LOG(LOG_ERR, "  Message: %s\n", error->message);
   }
   if (error && error->resource)
   {
      LOG(LOG_ERR, "  Resource: %s\n", error->resource);
   }
   if (error && error->furtherDetails)
   {
      LOG(LOG_ERR, "  Further Details: %s\n", error->furtherDetails);
   }
   if (error && error->extraDetailsCount)
   {
      LOG(LOG_ERR, "%s", "  Extra Details:\n");
      int i;
      for (i = 0; i < error->extraDetailsCount; i++)
      {
         LOG(LOG_ERR, "    %s: %s\n", error->extraDetails[i].name, error->extraDetails[i].value);
      }
   }
}

//   -------------    S3 HANDLERS    -------------

// Callbacks for verify() operations
static S3ResponseHandler verifyHandler = {
    &responsePropertiesCallback,
    &responseCompleteCallback

};

// Callbacks for migrate() operations
static S3ResponseHandler migrateHandler = {
    &responsePropertiesCallback,
    &responseCompleteCallback

};

// Callbacks for delete_object operations
static S3ResponseHandler delHandler = {
    &responsePropertiesCallback,
    &responseCompleteCallback

};

// Callbacks for stat() operations
static S3ResponseHandler statHandler = {
    &responsePropertiesCallback,
    &responseCompleteCallback

};

// Callbacks for multipart initialization operations
static S3MultipartInitialHandler initHandler = {
    {&responsePropertiesCallback,
     &responseCompleteCallback},
    &initialMultipartCallback

};

// Callbacks for set_meta() operations
static S3ResponseHandler setMetaHandler = {
    &responsePropertiesCallback,
    &responseCompleteCallback

};

// Callbacks for get_meta() operations
static S3ResponseHandler getMetaHandler = {
    &getMetaResponsePropertiesCallback,
    &responseCompleteCallback

};

// Callbacks for put_object operations
static S3PutObjectHandler putHandler = {
    {&putResponseProperiesCallback,
     &responseCompleteCallback},
    &putObjectDataCallback

};

// Callbacks for get_object operations
static S3GetObjectHandler getHandler = {
    {&responsePropertiesCallback,
     &responseCompleteCallback},
    &getObjectDataCallback

};

// Callbacks for multipart abort operations
static S3AbortMultipartUploadHandler abortHandler = {
    {&responsePropertiesCallback,
     &responseCompleteCallback},

};

// Callbacks for multipart commit operations
static S3MultipartCommitHandler commitHandler = {
    {&responsePropertiesCallback,
     &responseCompleteCallback},
    &commitObjectCallback,
    NULL

};

// Callbacks for list service operations
static S3ListServiceHandler listHandler = {
    {&responsePropertiesCallback,
     &responseCompleteCallback},
    &listCallback

};


int s3_set_meta_internal(BLOCK_CTXT ctxt, const char *meta_buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)ctxt; // should have been passed a s3 context

   // abort, unless we're writing or rebuliding
   if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
   {
      LOG(LOG_ERR, "Can only perform set_meta ops on a DAL_WRITE or DAL_REBUILD block handle!\n");
      return -1;
   }

   // Save metadata to be written back later
   bctxt->meta = strdup(meta_buf);

   // Strip any newlines from the end of the buffer
   while (bctxt->meta[strlen(bctxt->meta) - 1] == '\n')
   {
      bctxt->meta[strlen(bctxt->meta) - 1] = '\0';
   }

   return 0;
}

ssize_t s3_get_meta_internal(BLOCK_CTXT ctxt, char *meta_buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)ctxt; // should have been passed a s3 context

   // abort, unless we're reading
   if (bctxt->mode != DAL_READ && bctxt->mode != DAL_METAREAD)
   {
      LOG(LOG_ERR, "Can only perform get_meta ops on a DAL_READ or DAL_METAREAD block handle!\n");
      return -1;
   }

   // Give several tries to retrieve metadata
   int i = TRIES;
   do
   {
      S3_head_object(bctxt->bucketContext, bctxt->key, NULL, TIMEOUT, &getMetaHandler, meta_buf);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to retrieve metadata from \"%s/%s\" (%s)\n", bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
      errno = EIO;
      return -1;
   }

   // Add a newline to the end of the metadata if we have room
   if (strlen(meta_buf) < size)
   {
      strcat(meta_buf, "\n\0");
   }

   return strlen(meta_buf) + 1;
}




//   -------------    S3 IMPLEMENTATION    -------------

int s3_verify(DAL_CTXT ctxt, int flags)
{
   int fix = flags & CFG_FIX;

   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   S3_DAL_CTXT dctxt = (S3_DAL_CTXT)ctxt; // should have been passed a s3 context

   int size = sizeof(char) * (4 + num_digits(dctxt->max_loc.block) + num_digits(dctxt->max_loc.cap) + num_digits(dctxt->max_loc.scatter));
   char *bucket = malloc(size);
   int num_err = 0;
   int b;
   for (b = 0; b <= dctxt->max_loc.block; b++)
   {
      int c;
      for (c = 0; c <= dctxt->max_loc.cap; c++)
      {
         int s;
         for (s = 0; s <= dctxt->max_loc.scatter; s++)
         {
            sprintf(bucket, "b%d.%d.%d", b, c, s);
            int i = TRIES;
            do
            {
               S3_test_bucket(S3ProtocolHTTP, S3UriStylePath, dctxt->accessKey, dctxt->secretKey, NULL, NULL, bucket, dctxt->region, 0, NULL, NULL, TIMEOUT, &verifyHandler, NULL);
               i--;
            } while (S3_status_is_retryable(statusG) && i >= 0);

            if (statusG != S3StatusOK)
            {
               LOG(LOG_ERR, "failed to verify bucket \"%s\" (%s)\n", bucket, S3_get_status_name(statusG));
               if (fix)
               {
                  int i = TRIES;
                  do
                  {
                     S3_create_bucket(S3ProtocolHTTP, dctxt->accessKey, dctxt->secretKey, NULL, NULL, bucket, dctxt->region, S3CannedAclPrivate, NULL, NULL, TIMEOUT, &verifyHandler, NULL);
                     i--;
                  } while (S3_status_is_retryable(statusG) && i >= 0);

                  if (statusG != S3StatusOK)
                  {
                     LOG(LOG_ERR, "failed to create bucket \"%s\" (%s)\n", bucket, S3_get_status_name(statusG));
                     num_err++;
                  }
                  else
                  {
                     LOG(LOG_INFO, "successfully created bucket \"%s\"\n", bucket);
                  }
               }
               else
               {
                  num_err++;
               }
            }
         }
      }
   }
   free(bucket);
   return num_err;
}

/** NOTE: "online" migrations are not true migrations, they just copy an object from one location
 * to another, with no guarantee that both objects will remain synchronized after the migrate
 * is completed. This is due to AWS S3's inability for two keys to map to one value.
*/
int s3_migrate(DAL_CTXT ctxt, const char *objID, DAL_location src, DAL_location dest, char offline)
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
   S3_DAL_CTXT dctxt = (S3_DAL_CTXT)ctxt; // should have been passed a s3 context

   if (strlen(objID) == 0)
   {
      objID = NO_OBJID;
   }

   // Form source bucket from location
   int srcSize = sizeof(char) * (4 + num_digits(src.block) + num_digits(src.cap) + num_digits(src.scatter));
   char *srcBucket = malloc(srcSize);
   snprintf(srcBucket, srcSize, "b%d.%d.%d", src.block, src.cap, src.scatter);

   S3BucketContext srcBucketContext = {
       NULL,
       srcBucket,
       S3ProtocolHTTP,
       S3UriStylePath,
       dctxt->accessKey,
       dctxt->secretKey,
       NULL,
       dctxt->region

   };

   // Form destination bucket from location
   int destSize = sizeof(char) * (4 + num_digits(dest.block) + num_digits(dest.cap) + num_digits(dest.scatter));
   char *destBucket = malloc(destSize);
   snprintf(destBucket, destSize, "b%d.%d.%d", dest.block, dest.cap, dest.scatter);

   // Give several tries to copy object
   int i = TRIES;
   do
   {
      S3_copy_object(&srcBucketContext, objID, destBucket, NULL, NULL, NULL, 0, NULL, NULL, TIMEOUT, &migrateHandler, NULL);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to migrate %s from bucket %s to bucket %s (%s)\n", objID, srcBucketContext.bucketName, destBucket, S3_get_status_name(statusG));
      free(srcBucket);
      free(destBucket);
      errno = EIO;
      return -1;
   }

   // Delete source for offline migrations
   if (offline)
   {
      // Give several tries to delete object
      i = TRIES;
      do
      {
         S3_delete_object(&srcBucketContext, objID, NULL, TIMEOUT, &delHandler, NULL);
         i--;
      } while (S3_status_is_retryable(statusG) && i >= 0);

      if (statusG != S3StatusOK)
      {
         LOG(LOG_ERR, "failed to delete \"%s/%s\" (%s)\n", srcBucketContext.bucketName, objID, S3_get_status_name(statusG));
         free(srcBucket);
         free(destBucket);
         errno = EIO;
         return 1;
      }
   }

   free(srcBucket);
   free(destBucket);
   return 0;
}

int s3_del(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   S3_DAL_CTXT dctxt = (S3_DAL_CTXT)ctxt; // should have been passed a s3 context

   if (strlen(objID) == 0)
   {
      objID = NO_OBJID;
   }

   // Form bucket from location
   int size = sizeof(char) * (4 + num_digits(location.block) + num_digits(location.cap) + num_digits(location.scatter));
   char *bucket = malloc(size);
   snprintf(bucket, size, "b%d.%d.%d", location.block, location.cap, location.scatter);

   S3BucketContext bucketContext = {
       NULL,
       bucket,
       S3ProtocolHTTP,
       S3UriStylePath,
       dctxt->accessKey,
       dctxt->secretKey,
       NULL,
       dctxt->region

   };

   // Give several tries to delete object
   int i = TRIES;
   do
   {
      S3_delete_object(&bucketContext, objID, NULL, TIMEOUT, &delHandler, NULL);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to delete \"%s/%s\" (%s)\n", bucketContext.bucketName, objID, S3_get_status_name(statusG));
      free(bucket);
      errno = EIO;
      return -1;
   }

   free(bucket);
   return 0;
}

int s3_stat(DAL_CTXT ctxt, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return -1;
   }
   S3_DAL_CTXT dctxt = (S3_DAL_CTXT)ctxt; // should have been passed a s3 context

   if (strlen(objID) == 0)
   {
      objID = NO_OBJID;
   }

   // Form bucket from location
   int size = sizeof(char) * (4 + num_digits(location.block) + num_digits(location.cap) + num_digits(location.scatter));
   char *bucket = malloc(size);
   snprintf(bucket, size, "b%d.%d.%d", location.block, location.cap, location.scatter);

   S3BucketContext bucketContext = {
       NULL,
       bucket,
       S3ProtocolHTTP,
       S3UriStylePath,
       dctxt->accessKey,
       dctxt->secretKey,
       NULL,
       dctxt->region

   };

   // Give several tries to detect object
   int i = TRIES;
   do
   {
      S3_head_object(&bucketContext, objID, NULL, TIMEOUT, &statHandler, NULL);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to stat \"%s/%s\" (%s)\n", bucketContext.bucketName, objID, S3_get_status_name(statusG));
      free(bucket);
      errno = EIO;
      return -1;
   }

   free(bucket);
   return 0;
}

int s3_cleanup(DAL dal)
{
   if (dal == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal!\n");
      return -1;
   }
   S3_DAL_CTXT dctxt = (S3_DAL_CTXT)dal->ctxt; // should have been passed a s3 context

   // shut down libs3
   S3_deinitialize();

   // free the DAL struct and its associated state
   free(dctxt->accessKey);
   free(dctxt->secretKey);
   free(dctxt->region);
   free(dctxt);
   free(dal);
   return 0;
}

BLOCK_CTXT s3_open(DAL_CTXT ctxt, DAL_MODE mode, DAL_location location, const char *objID)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL dal context!\n");
      return NULL;
   }
   S3_DAL_CTXT dctxt = (S3_DAL_CTXT)ctxt; // should have been passed a s3 context

   // allocate space for a new DAL context
   S3_BLOCK_CTXT bctxt = malloc(sizeof(struct s3_block_context_struct));
   if (bctxt == NULL)
   {
      return NULL;
   } // malloc will set errno

   bctxt->mode = mode;
   bctxt->seq = 1;

   if (strlen(objID) == 0)
   {
      objID = NO_OBJID;
   }
   bctxt->key = strdup(objID);
   bctxt->meta = NULL;

   bctxt->data_gb = 0;
   bctxt->data_size = 0;

   // Form bucket from location
   int size = sizeof(char) * (4 + num_digits(location.block) + num_digits(location.cap) + num_digits(location.scatter));
   bctxt->bucket = malloc(size);
   snprintf(bctxt->bucket, size, "b%d.%d.%d", location.block, location.cap, location.scatter);

   bctxt->bucketContext = malloc(sizeof(S3BucketContext));
   bctxt->bucketContext->hostName = NULL;
   bctxt->bucketContext->bucketName = bctxt->bucket;
   bctxt->bucketContext->protocol = S3ProtocolHTTP;
   bctxt->bucketContext->uriStyle = S3UriStylePath;
   bctxt->bucketContext->accessKeyId = dctxt->accessKey;
   bctxt->bucketContext->secretAccessKey = dctxt->secretKey;
   bctxt->bucketContext->securityToken = NULL;
   bctxt->bucketContext->authRegion = dctxt->region;

   if (mode == DAL_READ)
   {
      LOG(LOG_INFO, "Open for READ\n");
   }
   else if (mode == DAL_METAREAD)
   {
      LOG(LOG_INFO, "Open for META_READ\n");
   }
   else // DAL_WRITE or DAL_REBUILD (no difference in this implementation)
   {
      if (mode == DAL_WRITE)
      {
         LOG(LOG_INFO, "Open for WRITE\n");
      }
      else if (mode == DAL_REBUILD)
      {
         LOG(LOG_INFO, "Open for REBUILD\n");
      }

      // Give several tries to initiate a multipart upload
      int i = TRIES;
      do
      {
         S3_initiate_multipart(bctxt->bucketContext, bctxt->key, NULL, &initHandler, NULL, TIMEOUT, bctxt);
         i--;
      } while (S3_status_is_retryable(statusG) && i >= 0);

      if (statusG != S3StatusOK)
      {
         LOG(LOG_ERR, "failed to initiate multipart upload for \"%s/%s\" (%s)\n", bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
         free(bctxt->bucket);
         free(bctxt->bucketContext);
         free(bctxt->key);
         free(bctxt->upload_id);
         free(bctxt);
         errno = EIO;
         return NULL;
      }

      bctxt->part_gb = 0;
      bctxt->part_size = growbuffer_append(&(bctxt->part_gb), "<CompleteMultipartUpload>", strlen("<CompleteMultipartUpload>"));
   }

   return bctxt;
}

int s3_set_meta(BLOCK_CTXT ctxt, const meta_info* source)
{
   return dal_set_meta_helper( s3_set_meta_internal, ctxt, source );
}

int s3_get_meta(BLOCK_CTXT ctxt, meta_info* target)
{
   return dal_get_meta_helper( s3_get_meta_internal, ctxt, target );
}

int s3_put(BLOCK_CTXT ctxt, const void *buf, size_t size)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)ctxt; // should have been passed a s3 context

   // abort, unless we're writing or rebuliding
   if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
   {
      LOG(LOG_ERR, "Can only perform put ops on a DAL_WRITE or DAL_REBUILD block handle!\n");
      return -1;
   }

   // Add data to growbuffer to be handed to libs3
   bctxt->data_size = size;
   if (!growbuffer_append(&(bctxt->data_gb), buf, size))
   {
      LOG(LOG_ERR, "data not appended to growbuffer\n");
      growbuffer_destroy(bctxt->data_gb);
      bctxt->data_size = 0;
      return -1;
   }

   // Give several tries to add data to the object's multipart upload
   int i = TRIES;
   do
   {
      S3_upload_part(bctxt->bucketContext, bctxt->key, NULL, &putHandler, bctxt->seq, bctxt->upload_id, size, NULL, TIMEOUT, bctxt);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to upload part %d of \"%s/%s\" (%s)\n", bctxt->seq, bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
      errno = EIO;
      growbuffer_destroy(bctxt->data_gb);
      bctxt->data_size = 0;
      return -1;
   }
   bctxt->seq++;
   growbuffer_destroy(bctxt->data_gb);
   bctxt->data_size = 0;
   return 0;
}

ssize_t s3_get(BLOCK_CTXT ctxt, void *buf, size_t size, off_t offset)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)ctxt; // should have been passed a s3 context

   // abort, unless we're reading
   if (bctxt->mode != DAL_READ)
   {
      LOG(LOG_ERR, "Can only perform get ops on a DAL_READ block handle!\n");
      return -1;
   }

   bctxt->data_size = 0;

   // Give several tries to retrieve data from specified location
   int i = TRIES;
   do
   {
      S3_get_object(bctxt->bucketContext, bctxt->key, NULL, offset, size, NULL, TIMEOUT, &getHandler, bctxt);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to read from \"%s/%s\" (%s)\n", bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
      errno = EIO;
      growbuffer_destroy(bctxt->data_gb);
      return -1;
   }

   // Add data in growbuffer to buf
   int ret = 0;
   size = 0;
   do
   {
      growbuffer_read(&(bctxt->data_gb), bctxt->data_size, &ret, buf);
      buf += ret;
      size += ret;
   } while (ret > 0);
   growbuffer_destroy(bctxt->data_gb);

   return size;
}

int s3_abort(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)ctxt; // should have been passed a s3 block context

   // abort, unless we wrote data
   if (bctxt->mode != DAL_WRITE && bctxt->mode != DAL_REBUILD)
   {
      LOG(LOG_ERR, "Can only perform abort ops on a DAL_WRITE or DAL_REBUILD block handle!\n");
      return -1;
   }

   int retval = 0;

   // abort the multipart upload
   int i = TRIES;
   do
   {
      S3_abort_multipart_upload(bctxt->bucketContext, bctxt->key, bctxt->upload_id, TIMEOUT, &abortHandler);
      i--;
   } while (S3_status_is_retryable(statusG) && i >= 0);

   if (statusG != S3StatusOK)
   {
      LOG(LOG_ERR, "failed to abort multipart upload for \"%s/%s\" (%s)\n", bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
      errno = EIO;
      retval = -1;
   }

   // free state
   if (bctxt->meta)
   {
      free(bctxt->meta);
   }
   if (bctxt->part_gb)
   {
      growbuffer_destroy(bctxt->part_gb);
   }
   free(bctxt->upload_id);
   free(bctxt->bucket);
   free(bctxt->bucketContext);
   free(bctxt->key);
   free(bctxt);
   return retval;
}

int s3_close(BLOCK_CTXT ctxt)
{
   if (ctxt == NULL)
   {
      LOG(LOG_ERR, "received a NULL block context!\n");
      return -1;
   }
   S3_BLOCK_CTXT bctxt = (S3_BLOCK_CTXT)ctxt; // should have been passed a s3 block context

   // Commit any data written
   if (bctxt->mode == DAL_WRITE || bctxt->mode == DAL_REBUILD)
   {

      bctxt->part_size += growbuffer_append(&(bctxt->part_gb), "</CompleteMultipartUpload>", strlen("</CompleteMultipartUpload>"));

      // Give several tries to complete the multipart upload
      int i = TRIES;
      do
      {
         S3_complete_multipart_upload(bctxt->bucketContext, bctxt->key, &commitHandler, bctxt->upload_id, bctxt->part_size, NULL, TIMEOUT, bctxt);
         i--;
      } while (S3_status_is_retryable(statusG) && i >= 0);

      if (statusG != S3StatusOK)
      {
         LOG(LOG_ERR, "failed to complete multipart upload for \"%s/%s\" (%s)\n", bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
         errno = EIO;
         return -1;
      }

      // Write any metadata that was set
      if (bctxt->meta)
      {
         S3NameValue meta = {
             "meta",
             bctxt->meta

         };

         S3PutProperties setMetaProperties = {
             NULL,
             NULL,
             NULL,
             NULL,
             NULL,
             -1,
             0,
             1,
             &meta,
             0

         };

         // Give several tries to write metadata
         i = TRIES;
         do
         {
            S3_copy_object(bctxt->bucketContext, bctxt->key, NULL, NULL, &setMetaProperties, NULL, 0, NULL, NULL, TIMEOUT, &setMetaHandler, NULL);
            i--;
         } while (S3_status_is_retryable(statusG) && i >= 0);

         if (statusG != S3StatusOK)
         {
            LOG(LOG_ERR, "failed to upload metadata for \"%s/%s\" (%s)\n", bctxt->bucketContext->bucketName, bctxt->key, S3_get_status_name(statusG));
            errno = EIO;
            return -1;
         }
         free(bctxt->meta);
      }

      if (bctxt->part_gb)
      {
         growbuffer_destroy(bctxt->part_gb);
      }
      free(bctxt->upload_id);
   }

   // free state
   free(bctxt->bucket);
   free(bctxt->bucketContext);
   free(bctxt->key);
   free(bctxt);
   return 0;
}

//   -------------    S3 INITIALIZATION    -------------

DAL s3_dal_init(xmlNode *root, DAL_location max_loc)
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

   // make sure we start on a 'hostname' node
   if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "hostname", 13) == 0)
   {

      // make sure that node contains a text element within it
      if (root->children != NULL && root->children->type == XML_TEXT_NODE)
      {

         char *hostname = (char *)root->children->content;

         // allocate space for our context struct
         S3_DAL_CTXT dctxt = malloc(sizeof(struct s3_dal_context_struct));
         if (dctxt == NULL)
         {
            return NULL;
         } // malloc will set errno

         dctxt->max_loc = max_loc;

         size_t io_size = IO_SIZE;

         // find the access key, secret key, and region. Fail if any are missing
         while (root != NULL)
         {
            if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "access_key", 11) == 0)
            {
               dctxt->accessKey = strdup((char *)root->children->content);
            }
            else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "secret_key", 11) == 0)
            {
               dctxt->secretKey = strdup((char *)root->children->content);
            }
            else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "region", 7) == 0)
            {
               dctxt->region = strdup((char *)root->children->content);
            }
            else if (root->type == XML_ELEMENT_NODE && strncmp((char *)root->name, "io_size", 8) == 0)
            {
               if (atol((char *)root->children->content) > io_size)
               {
                  io_size = atol((char *)root->children->content);
               }
            }
            root = root->next;
         }

         if (dctxt->accessKey == NULL || dctxt->secretKey == NULL || dctxt->region == NULL)
         {
            if (dctxt->accessKey != NULL)
            {
               free(dctxt->accessKey);
            }
            if (dctxt->secretKey != NULL)
            {
               free(dctxt->secretKey);
            }
            if (dctxt->region != NULL)
            {
               free(dctxt->region);
            }
            free(dctxt);
            errno = EINVAL;
            return NULL;
         }

         // Initialize libs3
         S3Status status;
         if ((status = S3_initialize("s3_dal", S3_INIT_ALL, hostname)) != S3StatusOK)
         {
            LOG(LOG_ERR, "failed to initialize libs3: %s\n", S3_get_status_name(status));
            free(dctxt->accessKey);
            free(dctxt->secretKey);
            free(dctxt->region);
            free(dctxt);
            return NULL;
         }

         // test for a connection to the S3 server
         int i = TRIES;
         do
         {
            S3_list_service(S3ProtocolHTTP, dctxt->accessKey, dctxt->secretKey, NULL, hostname, dctxt->region, NULL, TIMEOUT, &listHandler, NULL);
            i--;
         } while (S3_status_is_retryable(statusG) && i >= 0);

         if (statusG != S3StatusOK)
         {
            LOG(LOG_ERR, "failed to verify connection to S3 server\n");
            free(dctxt->accessKey);
            free(dctxt->secretKey);
            free(dctxt->region);
            free(dctxt);
            errno = ENONET;
            return NULL;
         }

         // allocate and populate a new DAL structure
         DAL s3dal = malloc(sizeof(struct DAL_struct));
         if (s3dal == NULL)
         {
            LOG(LOG_ERR, "failed to allocate space for a DAL_struct\n");
            free(dctxt->accessKey);
            free(dctxt->secretKey);
            free(dctxt->region);
            free(dctxt);
            return NULL;
         } // malloc will set errno
         s3dal->name = "s3";
         s3dal->ctxt = (DAL_CTXT)dctxt;
         s3dal->io_size = io_size;
         s3dal->verify = s3_verify;
         s3dal->migrate = s3_migrate;
         s3dal->open = s3_open;
         s3dal->set_meta = s3_set_meta;
         s3dal->get_meta = s3_get_meta;
         s3dal->put = s3_put;
         s3dal->get = s3_get;
         s3dal->abort = s3_abort;
         s3dal->close = s3_close;
         s3dal->del = s3_del;
         s3dal->stat = s3_stat;
         s3dal->cleanup = s3_cleanup;
         return s3dal;
      }
      else
      {
         LOG(LOG_ERR, "the \"hostname\" node is expected to contain a template string\n");
      }
   }
   else
   {
      LOG(LOG_ERR, "root node of config is expected to be \"hostname\"\n");
   }
   errno = EINVAL;
   return NULL; // failure of any condition check fails the function
}
