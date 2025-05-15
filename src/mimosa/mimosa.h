#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "config/config.h"
#include "tagging/tagging.h"

#include "hardlink.h"
#include "mimosa_gufi.h"

// External global variables
extern marfs_config* config;
extern marfs_position* root_ns_pos; // not currently used
extern marfs_position* dest_pos;

// Name of the xattr holding the cache/source path
#define MARFS_CACHE_XATTR "MARFS-CACHE"

/* ---------- External Functions to be Called by the GUFI Trace parser --------- */

/**
 * This function initializes the global config and sets up initial positions.
 * @param config: reference of global config struct to be initialized. This config is extern and declared in mimosa.h.
 * @param mutex: MarFS internal stuff
 * @param dest_arg: path to the destination MarFS user path. Received from the command line arguments.
 * @return none
 */
void mimosa_init(marfs_config** config, pthread_mutex_t* mutex, char* source_arg, char* dest_arg);

/**
 * Free global data structures and strings
 */
void mimosa_cleanup();

/**
 * This function contains the entire process of mapping a file/directory from a GUFI trace entry to MarFS. It is intended to be the single function called by the parser to handle all the conversion needs. Decides the type of file from path and calls a separate function to handle each case.
 * @param tentry_path: path of the file/dir read by the parser. The destination MarFS path will be determined from this path.
 * @param tentry_struct: the entry data for the file tentry_path. This is passed as an argument to reduce I/O load. 
 * @return 0 on success, -1 on failure
 */
int mimosa_convert(char* tentry_path, struct entry_data* tentry_struct);

/**
 * External wrapper to update_times that is intended to be called on the second pass over the tree to update the atimes and mtimes of directories. This function uses the global position for this thread and cannot take a position argument because it is called in pwalk.
 * @param entry_path: path of directory received from tree walk util
 * @param stat_struct: stat struct generated in tree walk util, no need to call stat twice
 */
void mimosa_update_times(char* entry_path, struct stat* stat_struct);


/* ---------- Internal Conversion Functions ---------- */

/**
 * Function to map a file from source to destination. Takes the source file (dest_rel_path) and creates a new reference file and user file  in the destination location. Executes the process openref -> fsetxattr -> linkref. 
 * @param pos: MarFS position to create the file relative to
 * @param dest_rel_path: MarFS user tree path for the file, in which the user will be able to find it.
 * @param ftag: ftag struct containing metadata for the reference tree filei
 * @param stat_struct: stat struct generated in tree walk util, no need to call stat twice
 * @return -1 if the file already exists, or 0 on success
 */
int mimosa_create_file(marfs_position* pos, char* dest_rel_path, struct stat* stat_struct);

/**
 * Create a MarFS directory at a specific position.
 * @param pos: MarFS position to create the directory relative to
 * @param dest_rel_path: path to create the directory at relative to the position
 * @param stat_struct: for permission info 
 * @return -1 on error, 0 on success
 */
int mimosa_create_dir(marfs_position* pos, char* dest_rel_path, struct stat* stat_struct);

/*
 * Create a symlink at a specific position. Will link to whatever is referenced by source_abs_path. Works with relative links within the source root but not absolute. 
 * @param pos: marfs position to create the link relative to
 * @param link: the path of the link to create
 * @param source_abs_path: absolute path of source link to read to determine dest
 * @return -1 on fail, 0 on success
 */
int mimosa_create_symlink(marfs_position* pos, char* link, char* source_abs_path);

/* ---------- Internal Helper Functions ---------- */

/*
 * When an inode with hard links is passed in, need to see if at least one instance has been mapped to the destination before linking.
 * For this function, locking should be done before and after call if being used in multithreaded context.
 * @param dest_abs_path: for dest_filename in hardlink_node
 * @param stat_struct: to get inode num
 * @param node: node to initialize if not found
 * @return: 0 if a corresponding inode exists in destination, 1 if a file needs to be created to represent it 
 */
int check_new_linked_inode(char* dest_abs_path, struct stat* stat_struct, hardlink_node* node);

/**
 * Create the parents of a file or directory in MarFS. This function is intended to fix the issue of the parser returning entries before their parents are created. 
 * @param pos
 * @param dest_rel_path_copy: copy of the path of the entiry who's parents need to be created
 * @return -1 on failure, 0 on success
 */
int mkdir_p(marfs_position* pos, char* dest_rel_path_copy);

/**
 * After a MarFS file is created, update its atime and mtime with the ones from the source file.
 * @param pos
 * @param dest_rel_path
 * @param stat_struct: for original times
 * @param symlink: if the entry is a symlink and AT_SYMLINK_NOFOLLOW should be used
 * @return -1 on failure, 0 on success
 */
int update_times(marfs_position* pos, char* dest_rel_path, struct stat* stat_struct, int symlink);

/**
 * After a MarFS file is created, create the MARFS-CACHE xattr and store the given path
 * into it
 * @param pos
 * @param dest_rel_path: the MarFS user path, relative to the Namespace
 * @param cache_path: the value of the MARFS-CACHE xattr
 * @return -1 on failure, 0 on success
 */
int set_cachepath(marfs_position* pos, char* dest_rel_path, char* cache_path);

/**
 * After a MarFS file is created, create any user-specifed xattrs that may have
 * been included with the file.
 * @param pos
 * @param dest_rel_path: the MarFS user path, relative to the Namespace
 * @param xattr_list: list of name/value pairs representing user xattrs
 * @return -1 on failure, 0 on success
 */
int set_userxattrs(marfs_position* pos, char* dest_rel_path, struct xattrs* xattr_list);

/* ---------- Path String Helper Functions ---------- */

// translate entry_path into a relative path from the root namespace
char* dot_to_source(char* entry_path, char* source_arg);

// concat prefix and entry_path to make absolute path
char* gen_abs_path(char* entry_path, char* prefix);

// generate a relative path by removing a prefix from the
// given absolute path
char* gen_rel_path(char* abs_path, char* prefix);

// return the relative path of the root namespace: /usr/bin -> bin
char* marfs_root_rel_path();

// helper for mkdir_p; make the relative curr_path absolute based on SOURCE_ARG
char* rel_to_abs_src_path(char* curr_path);
