#ifndef _MARFS_LOGGING_H
#define _MARFS_LOGGING_H


#include <syslog.h>             // we always need priority-names
#include <unistd.h>             // ssize_t
#include <stdarg.h>             // va_arg
#include <pthread.h>            // pthread_self()
#include <string.h>             // strlen()

#include <stdio.h>


#  ifdef __cplusplus
extern "C" {
#  endif



// ---------------------------------------------------------------------------
// logging macros use syslog for root, printf for user.
// See syslog(3) for priority constants.
//
// NOTE: To see output to stderr/stdout, you must start fuse with '-f'
// ---------------------------------------------------------------------------


// LOG_PREFIX is prepended to all output.  Allows grepping in syslogs.
//
// NOTE: Changing this requires corresponding changes to /etc/rsyslog.conf
//       on the destination log-server (e.g. on stb-dsu-master), so marfs
//       output will continue to be routed to its own destination log-file

#ifndef LOG_PREFIX
#  define LOG_PREFIX  "libne_logging"
#endif

/// // without pthread_self()
/// #define xFMT  " [%s:%4d]%*s %-21s | %s"
#define xFMT  "%-15s  %08x  %s:%-4d%*s %-20.20s | %s"

// size of longest file-name string, plus some
#define LOG_FNAME_SIZE 20



#if (DEBUG) && (defined USE_SYSLOG)
// calling syslog() as a regular user on rrz seems to be an expensive no-op
// #  define INIT_LOG()  openlog(LOG_PREFIX, LOG_CONS|LOG_PERROR, LOG_USER)
#  define INIT_LOG()  openlog(LOG_PREFIX, LOG_CONS|LOG_PID, LOG_USER)

#  define LOG(PRIO, FMT, ...)                                              \
   if ( DEBUG == 1  ||                                                     \
        ( DEBUG == 2  &&  PRIO <= LOG_WARNING )  ||                        \
        ( PRIO <= LOG_ERR ) ) {                                            \
      syslog((PRIO), xFMT FMT,                                             \
             LOG_PREFIX,                                                   \
             (unsigned int)pthread_self(),                                 \
             __FILE__, __LINE__,                                           \
             LOG_FNAME_SIZE-(int)strlen(__FILE__), "",                     \
             __FUNCTION__,                                                 \
             (((PRIO)<=LOG_ERR) ? "#ERR " : ""), ## __VA_ARGS__);          \
   }

#elif (DEBUG)
// must start fuse with '-f' in order to allow stdout/stderr to work
// NOTE: print_log call merges LOG_PREFIX w/ user format at compile-time
#  define INIT_LOG()

#  define LOG(PRIO, FMT, ...)                                              \
   if ( DEBUG == 1  ||                                                     \
        ( DEBUG == 2  &&  PRIO <= LOG_WARNING )  ||                        \
        ( PRIO <= LOG_ERR ) ) {                                            \
      printf_log((PRIO), xFMT FMT,                                         \
                 LOG_PREFIX,                                               \
                 (unsigned int)pthread_self(),                             \
                 __FILE__, __LINE__,                                       \
                 LOG_FNAME_SIZE-(int)strlen(__FILE__), "",                 \
                 __FUNCTION__,                                             \
                 (((PRIO)<=LOG_ERR) ? "#ERR " : ""), ## __VA_ARGS__);      \
   }

   ssize_t printf_log(size_t prio, const char* format, ...);

#else
// Without DEBUG, these become no-ops
#  define INIT_LOG()
#  define LOG(PRIO, FMT, ...)



#endif


#  ifdef __cplusplus
}
#  endif





#endif // _MARFS_LOGGING_H
