#ifndef _MARFS_LOGGING_H
#define _MARFS_LOGGING_H


#include <syslog.h>             // we always need priority-names
#include <unistd.h>             // ssize_t
#include <stdarg.h>             // va_arg

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
#ifndef LOG_PREFIX
#  define LOG_PREFIX  "marfs_fuse"
#endif

#define xFMT  " [%s:%4d]%*s %-21s | "


#ifdef USE_SYSLOG
// calling syslog() as a regular user on rrz seems to be an expensive no-op
// #  define INIT_LOG()  openlog(LOG_PREFIX, LOG_CONS|LOG_PERROR, LOG_USER)
#  define INIT_LOG()  openlog(LOG_PREFIX, LOG_CONS|LOG_PID, LOG_USER)

#  define LOG(PRIO, FMT, ...)                                           \
   syslog((PRIO), xFMT FMT, __FILE__, __LINE__,                         \
          17-(int)strlen(__FILE__), "", __FUNCTION__, ## __VA_ARGS__)

#elif USE_STDOUT
// must start fuse with '-f' in order to allow stdout/stderr to work
// NOTE: print_log call merges LOG_PREFIX w/ user format at compile-time
#  define INIT_LOG()

#  define LOG(PRIO, FMT, ...)                                           \
   printf_log((PRIO), LOG_PREFIX xFMT FMT, __FILE__, __LINE__,          \
              17-(int)strlen(__FILE__), "", __FUNCTION__, ## __VA_ARGS__)

   ssize_t printf_log(size_t prio, const char* format, ...);

#else
// Without USE_STDOUT or USE_SYSLOG, these become no-ops
#  define INIT_LOG()
#  define LOG(PRIO, FMT, ...)



#endif


#  ifdef __cplusplus
}
#  endif





#endif // _MARFS_LOGGING_H
