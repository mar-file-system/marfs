#include "logging/logging.h"

#include <stdlib.h>             // malloc()
#include <string.h>
#include <assert.h>

// only defined/used when not USE_SYSLOG
//
// QUESTION: Do we really want fuse to abort routines with error-codes
//    if the logging function fails?  I suspect not.  If you wanted to,
//    you could define LOG(...) to be TRY_GE0(printf_log(...))

ssize_t printf_log(size_t prio, const char* format, ...) {
   va_list list;
   va_start(list, format);

#if 0
   // COMMENTED OUT.  This is now handled in the LOG() macro
   ssize_t written;
   if (prio <= LOG_ERR) {
      const char*  stand_out = "*** ERROR ";
      const size_t stand_out_len = strlen(stand_out);
      const size_t format_len = strlen(format);

      char* tmp_buf = malloc(format_len + stand_out_len +1);
      assert(tmp_buf);

      memcpy(tmp_buf, stand_out, stand_out_len);
      memcpy(tmp_buf + stand_out_len, format, format_len);
      tmp_buf[stand_out_len + format_len] = 0;

      written = vfprintf(stderr, tmp_buf, list);
      free(tmp_buf);
   }
   else {
      written = vfprintf(stderr, format, list);
   }
#else
   ssize_t written = vfprintf(stderr, format, list);
#endif

   fflush(stderr);
   return written;
}
