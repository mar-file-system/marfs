
#include <stdint.h>

/* ---------- GUFI Trace Defines & structures --------- */
#define MAXPATH     4096   // Maximum path length for a value in a trace entry
#define MAXXATTR    1024   // Maximum length for an xattr name/value pair

#define STAT_ino    "lu"
#define STAT_mode   "u"
#define STAT_nlink  "lu"
#define STAT_uid    "u"
#define STAT_gid    "u"
#define STAT_size   "ld"
#define STAT_bsize  "ld"
#define STAT_blocks "ld"

/**
 * Structures to hold the data from the trace
 */

/* single xattr name-value pair */
struct xattr {
    char   name[MAXXATTR];
    size_t name_len;
    char   value[MAXXATTR];
    size_t value_len;
};

/* list of xattr pairs */
struct xattrs {
    struct xattr *pairs;
    size_t        name_len; /* name lengths only - no separators */
    size_t        len;      /* name + value lengths only - no separators */
    size_t        count;
};

/* trace entry data structure... */
struct entry_data {
   int           parent_fd;    /* holds an FD that can be used for fstatat(2), etc. */
   char          type;
   char          linkname[MAXPATH];
   uint8_t       lstat_called;
   struct stat   statuso;
   long long int offset;
   struct xattrs xattrs;
   int           crtime;
   int           ossint1;
   int           ossint2;
   int           ossint3;
   int           ossint4;
   char          osstext1[MAXXATTR];
   char          osstext2[MAXXATTR];
   char          pinodec[128];
   int           suspect;  // added for bfwreaddirplus2db for suspect
};

// External global parser variables
extern const char TRACEDELIM;     // Trace entry delimiter
extern const char XATTRDELIM;     // xattr delimiter

