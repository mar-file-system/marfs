#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>

#include "mimosa_logging.h"
#ifdef DEBUG_MIMOSA
#define DEBUG DEBUG_MIMOSA
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "mimosa_parser"
#include <logging/logging.h>

#include "mimosa.h"                  // contains global variable declaration for config


const char TRACEDELIM = '\x1E';     // ASCII record Separator
const char XATTRDELIM = '\x1F';     // ASCII unit Separator
static pthread_mutex_t MarFS_mutex; // Needed for MarFS calls...

int SNPRINTF(char *str, size_t size, const char *format, ...) {
    va_list args;
    va_start(args, format);
    const int n = vsnprintf(str, size, format, args);
    va_end(args);
    if ((size_t) n >= size) {
        fprintf(stderr, "%s:%d Warning: Message "
                "was truncated to %d characters: %s\n",
                __FILE__, __LINE__, n, str);
    }
    return n;
}

/* strstr/strtok replacement */
/* does not terminate on NULL character */
/* does not skip to the next non-empty column */
char *split(char *src, const char *delim, const size_t delim_len, const char *end) {
    if (!src || !delim || !delim_len || !end || (src > end)) {
        return NULL;
    }

    while (src < end) {
        for(size_t i = 0; i < delim_len; i++) {
            if (*src == delim[i]) {
                *src = '\x00';
                return src + 1;
            }
        }

        src++;
    }

    return NULL;
}

int xattrs_setup(struct xattrs *xattrs) {
    /* Not checking argument */

    memset(xattrs, 0, sizeof(*xattrs));
    return 0;
}

/* here to be the opposite function of xattrs_cleanup */
int xattrs_alloc(struct xattrs *xattrs) {
    if (!xattrs || xattrs->pairs) {
        return 1;
    }

    xattrs->pairs = calloc(xattrs->count, sizeof(struct xattr));
    return !xattrs->pairs;
}

void xattrs_cleanup(struct xattrs *xattrs) {
    /* Not checking argument */

    free(xattrs->pairs);
    xattrs->pairs = NULL;
}

/* parse serialized xattrs */
int xattrs_from_line(char *start, const char *end, struct xattrs *xattrs, const char delim) {
    /* Not checking arguments */

    xattrs_setup(xattrs);

    /* count NULL terminators */
    for(char *curr = start; curr != end; curr++) {
        if (*curr == delim) {
            xattrs->count++;
        }
    }
    xattrs->count >>= 1;

    xattrs_alloc(xattrs);

    /* extract pairs */
    char *next = split(start, &delim, 1, end);
    for(size_t i = 0; i < xattrs->count; i++) {
        struct xattr *xattr = &xattrs->pairs[i];

        xattr->name_len = SNPRINTF(xattr->name, sizeof(xattr->name), "%s", start);
        start = next; next = split(start, &delim, 1, end);

        xattr->value_len = SNPRINTF(xattr->value, sizeof(xattr->value), "%s", start);
        start = next; next = split(start, &delim, 1, end);

        xattrs->name_len += xattr->name_len;
        xattrs->len += xattr->name_len + xattr->value_len;
    }

    return xattrs->count;
}

/* Based on GUFI's linetowork() */
int linetodata(char *line, const size_t len, const char delim,
               char **entry_name, struct entry_data *ed) {
    if (!line || !entry_name || !ed) {
        return -1;
    }

    const char *end = line + len;
    char *p, *q;

    p=line; q = split(p, &delim, 1, end);
    *entry_name = strdup(p);

    p = q;  q = split(p, &delim, 1, end); ed->type = *p;

    if (ed->type == 'e') {
        return 0;
    }

    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_ino, &ed->statuso.st_ino);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_mode, &ed->statuso.st_mode);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_nlink, &ed->statuso.st_nlink);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_uid, &ed->statuso.st_uid);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_gid, &ed->statuso.st_gid);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_size, &ed->statuso.st_size);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_bsize, &ed->statuso.st_blksize);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_blocks, &ed->statuso.st_blocks);
    p = q; q = split(p, &delim, 1, end); ed->statuso.st_atime = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->statuso.st_mtime = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->statuso.st_ctime = atol(p);
    p = q; q = split(p, &delim, 1, end); SNPRINTF(ed->linkname,MAXPATH, "%s", p);
    p = q; q = split(p, &delim, 1, end); xattrs_from_line(p, q - 1, &ed->xattrs, XATTRDELIM);
    p = q; q = split(p, &delim, 1, end); ed->crtime = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint1 = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint2 = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint3 = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint4 = atol(p);
    p = q; q = split(p, &delim, 1, end); SNPRINTF(ed->osstext1, MAXXATTR, "%s", p);
    p = q; q = split(p, &delim, 1, end); SNPRINTF(ed->osstext2, MAXXATTR, "%s", p);
//    p = q;     split(p, &delim, 1, end); new_work->pinode = atol(p);

    return 0;
}

// Argument definition string for getopt(). The command line arguments are as 
// follows:
//    -d <MarFS Dest path/root> Location of file metadata in the MarFs filesystem.
//    -l <log file>		Log file for this process. Should be a full path.
//    -s <Trace root/prefix>    The prefix or root that will be stripped from the 
//                              trace files, as they are created in MarFS.
#define GETOPT_STR ":d:hl:s:"
#define USAGE "Usage: mimosa [-l <log file>] -d <MarFS Destination path/root> -s <Trace root/prefix> <GUFI Trace file>\n"

/**
 * Mimosa main
 */
int main(int argc, char *argv[]) {
    int opt;                         // holds current command line arg from getopt()
    int patharg_idx;                 // holds the arument index of the first path argument
    char *marfs_dpath = NULL;        // the MarFS destination path
    char *trace_prefix = NULL;       // the root, or prefix of the files in the trace
    char *trace_path = NULL;         // the path to the trace file to parse
    int prefixlen = 0;               // length of the trace prefix arg

    char *path;
    struct entry_data path_data;
    char *line = NULL;
    size_t len = 0;

    FILE *trace_ptr;
    ssize_t nread;

    errno = 0; // to guarantee an initially successful context and avoid "false positive" errno settings (errno not guaranteed to be initialized)

	// put ':' in the starting of the 
	// string so that program can 
	//distinguish between '?' and ':'     
    while((opt = getopt(argc, argv, GETOPT_STR)) > 0) { 
          switch(opt) { 
             case 'd':            // assign the MarFS path/root
		       marfs_dpath = strdup(optarg);
		       if (strrchr(marfs_dpath,'/') == (marfs_dpath+strlen(marfs_dpath)-1)) 
		          marfs_dpath[strlen(marfs_dpath)-1] = '\0';
                       break; 
             case 's':            // assign the Trace prefix/root
		       trace_prefix = strdup(optarg);
                       prefixlen = strlen(trace_prefix);
		       if (strrchr(trace_prefix,'/') == (trace_prefix+prefixlen-1)) {
			  prefixlen--;
		          trace_prefix[prefixlen] = '\0';
		       }	  
                       break; 
             case 'l':            // If stderr not being used for logging, redirect stdout and stderr to specified file (redirection is default behavior)
                       if (strncmp(optarg, "stderr", strlen("stderr")) != 0) {
                           int log_fd = open(optarg, O_WRONLY | O_CREAT | O_APPEND, 0644);

                           if (dup2(log_fd, STDERR_FILENO) == -1) 
                               fprintf(stderr, "Failed to redirect stderr to file \"%s\"! (%s)\n", optarg, strerror(errno));
                           close(log_fd);
                       }
                       break;
             case 'h':
                       fprintf(stderr,USAGE);
                       return 0; 
             case ':': 
                       fprintf(stderr, "Command option %c needs a value\n",optopt); 
                       fprintf(stderr,USAGE);
		       return 1;
             case '?': 
                       fprintf(stderr, "Unknown option: %c\n", optopt); 
                       fprintf(stderr,USAGE);
		       return 1;
          } 
    } 
    patharg_idx = optind;            // value of optind can get funky later in the program. Caputure it now.
	
    // Verify commandline values

    // Make sure we have a MarFs destination path specified
    if (!marfs_dpath) {
       LOG(LOG_ERR, "No MarFS destination (-d <MarFS Destination path/root>) was provided. Exiting ...\n");
       return 1;
    }	    

    // Make sure we have a Trace prefix specified
    if (!trace_prefix) {
       LOG(LOG_ERR, "No Trace prefix or root (-s <Trace root/prefix>) was provided. Exiting ...\n");
       return 1;
    }	    

    // Make sure we have a trace file specified
    if (patharg_idx <= 0 || patharg_idx >= argc) {
       LOG(LOG_ERR, "No trace file was specified. Exiting ...\n");
       return 1;
    }
    trace_path = strdup(argv[patharg_idx]);
    trace_ptr = fopen(trace_path, "r");
    if (trace_ptr == NULL) {
       LOG(LOG_ERR, "Failed to open file \"%s\" for writing to output (%s)\n", trace_path, strerror(errno)); 
       return 1;
    }

    // Initialize MarFS and Mimosa variables
    if (pthread_mutex_init(&MarFS_mutex, NULL)) {
       LOG(LOG_ERR, "Failed to initialize the MarFS mutex\n");
       return 1;
    }
    mimosa_init(&config, &MarFS_mutex, trace_prefix, marfs_dpath);

    // Main Parsing loop
    while ((nread = getline(&line, &len, trace_ptr)) != -1) {
	int rc;                                          // general return code variable

        linetodata(line,nread,TRACEDELIM,&path,&path_data);
	LOG(LOG_INFO, "Trace Entry (%zu bytes) - [%c] Path: %s (%lu bytes)\n", nread, path_data.type, path, path_data.statuso.st_size);

	if (strncmp(path,trace_prefix,prefixlen)) {     // If specified source prefix is not found, skip entry
	   LOG(LOG_INFO, "   SKIPPING ENTRY %s  - directory prefix does not match.\n", path);
	} else if ((rc=mimosa_convert(path,&path_data)) < 0) {
	   if (rc !=-EEXIST) {	
	      LOG(LOG_WARNING, "Failed move metadata for %s to %s\n", path, marfs_dpath);
	   }   
	}   
	else if (path_data.type == 'd')               // if entry was a directory, update the times in MarFS
	   mimosa_update_times(path,&path_data.statuso);	

	xattrs_cleanup(&path_data.xattrs);           // clean up parsing structures (for xattrs) 
	free(path); free(line);                      // we're done with these values ...
	line=NULL; len=0;
    }

    mimosa_cleanup();              // Get rid of data structures used in processing trace entries
    free(trace_prefix);            // mimisa_init() makes a copy. We are done with this arg.
    free(marfs_dpath);             // mimisa_init() makes a copy. We are done with this arg.
    free(line);
    fclose(trace_ptr);
    exit(EXIT_SUCCESS);
}

