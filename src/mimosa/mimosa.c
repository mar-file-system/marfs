#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h> 
#include <string.h>
#include <utime.h>
#include <assert.h>

#include "mimosa.h"

#include "mimosa_logging.h"
#ifdef DEBUG_MIMOSA
#define DEBUG DEBUG_MIMOSA
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "mimosa"
#include <logging/logging.h>

//MarFS Includes

#include "tagging/tagging.h"
#include "config/config.h"
#include "datastream/datastream.h"

#define STR_BUF_SIZE 4096

// MarFS Globals (initialize with mimosa_init())
marfs_config* config = NULL;
marfs_position* root_ns_pos = NULL;
marfs_position* dest_pos = NULL;

// global references to source and dest arguments
static char *SOURCE_ARG = NULL;  		// The source  (or trace) root
static char *DEST_ARG = NULL;			// Root of MIMOSA output tree
static DATASTREAM MIMOSA_STREAM = NULL;         // The datastream to use when creating files from the parser
char* MDAL_REF_PATH = "/var/marfs/mdal-root/MDAL_subspaces/full-access-subspace/MDAL_reference/"; // there is probably a better way to do this
char* MARFS_ROOT_REL_PATH;

// hardlink globals
list_meta* hardlink_list;
pthread_mutex_t hardlink_mutex;

/**
 * This function initializes the global config and sets up initial positions.
 * @param config: reference of global config struct to be initialized. This config is extern and declared in mimosa.h.
 * @param mutex: MarFS internal stuff
 * @param dest_arg: path to the destination MarFS user path for this pwalk iteration. Received from the command line arguments of mimosa.py.
 * @return none
 */ 
void mimosa_init(marfs_config** config, pthread_mutex_t* mutex, char* source_arg, char* dest_arg)
{
	// arg sanitation and glob prefix setup
	SOURCE_ARG = strdup(source_arg);
	DEST_ARG = strdup(dest_arg);
	MARFS_ROOT_REL_PATH = marfs_root_rel_path();
	
	// setup hardlink management data structure and mutex	
	hardlink_init();

	// populate global config struct
	*config = config_init(getenv("MARFS_CONFIG_PATH"), mutex);
	if (*config == NULL)
		LOG(LOG_ERR, "config_init: failed to Initialize MarFS context. Is MARFS_CONFIG_PATH set? \n"); 
	
	// establish position based on config
	root_ns_pos = (marfs_position*) calloc(1, sizeof(marfs_position));
	if (config_establishposition(root_ns_pos, *config) == -1)
		LOG(LOG_ERR, "config_establishposition: failed to establish position of root namespace\n");
	
	// set position of destination
	dest_pos = calloc(1, sizeof(marfs_position));
	config_duplicateposition(root_ns_pos, dest_pos);

	if (config_traverse(*config, dest_pos, &dest_arg, 1) < 0)
		LOG(LOG_ERR, "config_traverse: failed to traverse to destination position\n");
	
	// create destination root directory
	if (dest_pos->ns->prepo->metascheme.mdal->access(dest_pos->ctxt, dest_arg, F_OK, 0) == 0) {
                LOG(LOG_WARNING, "root directory %s exists: continuing to map unmapped files\n", dest_arg);
	}	
	else
		mimosa_create_dir(dest_pos, MARFS_ROOT_REL_PATH, NULL);

	if (config_fortifyposition(dest_pos) == -1)
		LOG(LOG_ERR, "config_fortifyposition: failed to create MDAL_CTXT for dest_pos\n");
	
}

/*
 * Free global data structures and strings
 */
void mimosa_cleanup()
{
	free(SOURCE_ARG);
	free(DEST_ARG);
	free(MARFS_ROOT_REL_PATH);
	hardlink_destruct(hardlink_list);
	// config_abandonposition led to free() error, removed completely due to end of summer time restrictions
	config_term(config);
}

// EXTERNAL FUNCTIONS

/**
 * This function contains the entire process of mapping a file/directory from POSIX to MarFS. It is intended to be the single function called in a parallel treewalk utility to handle all the conversion needs. Decides the type of file from path and calls a separate function to handle each case. Contains lots of boilerplate code to deal with pwalk issues. 
 * @param tentry_path: path read from a GUFI trace file. This path is already an absolute path. The destination MarFS path will be determined from this path.
 * @param tentry_struct: the entry data for the file tentry_path. This is passed as an argument to reduce I/O load.
 * @return 0 on success, -1 on failure
 */
int mimosa_convert(char* tentry_path, struct entry_data* tentry_struct)
{
	// important paths
	char* ent_rel_path = gen_rel_path(tentry_path, SOURCE_ARG);
	char* dest_rel_path = dot_to_source(ent_rel_path, MARFS_ROOT_REL_PATH);
	char* dest_abs_path = gen_abs_path(ent_rel_path, DEST_ARG);
        struct stat* stat_struct = &tentry_struct->statuso;       // the stat structure for the entry
	
	// do not reprocess files that exist in destination
        if (dest_pos->ns->prepo->metascheme.mdal->access(dest_pos->ctxt, dest_abs_path, F_OK, AT_SYMLINK_NOFOLLOW) == 0)
        {
		#if defined(ALL) || defined(CONVERSION)
                LOG(LOG_INFO, "SKIPPING\t%s exists\n", dest_abs_path);
		#endif
		free(ent_rel_path);
		free(dest_rel_path);
		free(dest_abs_path);
		return -EEXIST;
        }
	
	if (S_ISDIR(stat_struct->st_mode))
	{
		#if defined(ALL) || defined(CONVERSION)
		LOG(LOG_INFO, "CONVERSION\tDIRECTORY\tTARGET:  %s\tDEST:  %s\n", tentry_path, dest_abs_path);
		#endif
		mimosa_create_dir(dest_pos, dest_rel_path, stat_struct);
		// update utime and atime of all dirs after conversion
	}
	if (S_ISLNK(stat_struct->st_mode))
	{
		#if defined(ALL) || defined(CONVERSION)
		LOG(LOG_INFO, "CONVERSION\tSYMLINK\tTARGET:  %s\tDEST:  %s\n", tentry_path, dest_abs_path);
		#endif
		mimosa_create_symlink( dest_pos, dest_rel_path, tentry_path );
		update_times(dest_pos, dest_rel_path, stat_struct, 1); // 1 to indicate symlink
	}
	if (S_ISREG(stat_struct->st_mode))
	{
		#if defined(ALL) || defined(CONVERSION)
		LOG(LOG_INFO, "CONVERSION\tFILE\tTARGET:  %s\tDEST:  %s\n", tentry_path, dest_abs_path);
	 	#endif
		
		// for detection of hardlinked inode with no MarFS instances yet
		hardlink_node* node = NULL;
		int hardlink_create_first_file = 0;

		// if link count > 1, search hardlink list for inode to see if a regular file needs to be created
		if (stat_struct->st_nlink > 1)
		{
			pthread_mutex_lock(&hardlink_mutex);
			node = hardlink_search_src_inode(hardlink_list, stat_struct->st_ino);
			hardlink_create_first_file = check_new_linked_inode(dest_abs_path, stat_struct, node); 		
			pthread_mutex_unlock(&hardlink_mutex);
		}
		
		// creating a new MarFS file
		if (stat_struct->st_nlink <= 1 || hardlink_create_first_file)
		{

			// loop to catch exception of duplicate reference path
			for(int i = 0; i <= 1; i++)
			{
				int ret = mimosa_create_file( dest_pos, dest_rel_path, stat_struct);

				if (ret == 0) //success
					break;

				if (i == 1) // failed twice, give up
					LOG(LOG_ERR, "fsetxattr\tgenerating duplicate reference path failed\n");
			}
		
			// If update_times fails, something went wrong in the creation process, so try again to create. Best approach to navigate around pwalk issues is to catch errors, resolve the issue an try again.
			// This control structure can likely be removed after pwalk is scrapped because the majority of issues originate from it.
			if (update_times(dest_pos, dest_abs_path, stat_struct, 0) == -1)
			{
				#if defined(ALL) || defined(WARNING)
				LOG(LOG_WARNING, "update_times()\t%s\t%s\tcalling mkdir_p and retry\n", dest_abs_path, strerror(errno));
				#endif
				#if defined(ALL) || defined(NOTICE)
				LOG(LOG_INFO, " ...Attempting to recreate %s and parent dirs\n", dest_abs_path);
				#endif 

				char* dest_rel_path_copy =  strdup(dest_rel_path);             // strtok will mangle path
				
				mkdir_p(dest_pos, dest_rel_path_copy); // in case files are created before parent directories
				mimosa_create_file( dest_pos, dest_rel_path, stat_struct);
				
				if ( update_times(dest_pos, dest_abs_path, stat_struct, 0) == -1)
			        {		
					LOG(LOG_ERR, "update_times()\t%s failed to set time after recreate\n", dest_abs_path);
			        }
				else
				{
					#if defined(ALL) || defined(NOTICE)
					LOG(LOG_INFO, "Successfully recreated %s and updated times\n", dest_abs_path);
					#endif
				}

				free(dest_rel_path_copy);
			} 

			// Now attach the source path to the MarFS file via the MARFS-CACHE xattr
			if (set_cachepath(dest_pos, dest_rel_path, tentry_path) == -1)
			{
				LOG(LOG_ERR, "set_cachepath()\t%s failed to set MARFS-CACHE xattr\n", dest_abs_path);
			}
		}
	        else // create a hardlink to an existing inode	
		{
			pthread_mutex_lock(&hardlink_mutex);

			// link
			#if defined(ALL) || defined(NOTICE) || defined(HARDLINK)
			LOG(LOG_INFO, "HARDLINK\tlinking %s to %s\n", dest_rel_path, node->dest_filename); 
			#endif
			if (dest_pos->ns->prepo->metascheme.mdal->link(dest_pos->ctxt, node->dest_filename, dest_pos->ctxt, dest_rel_path, 0) == -1)
			{
				// does not warn when encounters hardlink outside source root
				LOG(LOG_ERR, "HARDLINK\toriginal: %s\tnew: %s\t%s\n", node->dest_filename, dest_rel_path, strerror(errno));
			}

			node->dest_link_count++; // account for newly created link in data structure
			
			// check if this file this was the last link to inode 
			if ( node->source_link_count == node->dest_link_count )
			{
				// all hard links to this node mapped, can free hardlink_node
				if ( hardlink_delete_entry(hardlink_list, node->source_inode_num) == -1)
				{
					LOG(LOG_ERR, "hardlink_delete_entry()\t%s\t%ld\n", dest_abs_path, stat_struct->st_ino);
			        }
				else	
				{
					#if defined(ALL) || defined(NOTICE) || defined(HARDLINK)
					int debug_src_inode = node->source_inode_num;
					LOG(LOG_INFO, "HARDLINK\tFinished mapping files for inode\tsource: %d\n", debug_src_inode);
					#endif
				}
			}
			pthread_mutex_unlock(&hardlink_mutex);
		}

		
	}
	
	free(ent_rel_path);
	free(dest_rel_path);
	free(dest_abs_path);
	return 0;
}

/**
 * External wrapper to update_times that is intended to be called on the second pass over the tree to update the atimes and mtimes of directories. This function uses the global position for this thread and cannot take a position argument because it is called in pwalk.
 * @param tentry_path: path of directory received from the parser
 * @param stat_struct: stat struct read from the parser, no need to call stat twice
 */ 
void mimosa_update_times(char* tentry_path, struct stat* stat_struct)
{
	char* ent_rel_path = gen_rel_path(tentry_path, SOURCE_ARG);
	char* dest_abs_path = gen_abs_path(ent_rel_path, DEST_ARG);

	update_times(dest_pos, dest_abs_path, stat_struct, 0); // regular because only dirs should be passed into this function, no symlinks
	free(ent_rel_path);
	free(dest_abs_path);

	return;
}

// INTERNAL FUNCTIONS

/**
 * Function to map a file from source to destination. Takes the source file (dest_rel_path) and creates a new reference file and user file  in the destination location, via a datastream.
 * @param pos: MarFS position to create the file relative to
 * @param dest_rel_path: MarFS user tree path for the file, in which the user will be able to find it.
 * @param stat_struct: stat struct generated in tree walk util, no need to call stat twice
 * @return < 0 if there are problems creating the file, or 0 on success
 */	
int mimosa_create_file(marfs_position* pos, char* dest_rel_path, struct stat* stat_struct)
{
	mode_t mode = (stat_struct)?stat_struct->st_mode:S_IRWXU;	// create mode for file
	size_t size = (stat_struct)?stat_struct->st_size:0;             // size of file in bytes
	int err_rc = 0;                                                 // returned error
	
	if (!datastream_create(&MIMOSA_STREAM, dest_rel_path, pos, mode, "mimosa-created"))
	{
	   errno = 0;                                                  // make sure errno is clear before calling next function
	   if (datastream_extend(&MIMOSA_STREAM, size))                // extend the file to make sure the size is recorded in the ftag
           {
	      err_rc = errno;	   
              LOG(LOG_ERR, "Failed to extend %s by %ld bytes (error %d: %s)\n", dest_rel_path, size, err_rc, strerror(err_rc));		   
	      return -(err_rc);
	   }
	}
        else
	{
	   err_rc = errno;
           LOG(LOG_ERR, "Failed to create %s (error %d: %s)\n", dest_rel_path, err_rc, strerror(err_rc));		   
	   return -(err_rc);
        }	   

	return 0;
}

/**
 * Create a MarFS directory at a specific position.
 * @param pos: MarFS position to create the directory relative to
 * @param dest_rel_path: path to create the directory at relative to the position
 * @param stat_struct: for permission info 
 * @return -1 on error, 0 on success
 */	
int mimosa_create_dir(marfs_position* pos, char* dest_rel_path, struct stat* stat_struct)
{
	mode_t mode = (stat_struct)?stat_struct->st_mode:S_IRWXU;	// create mode for directory

	if (pos->ns->prepo->metascheme.mdal->mkdir(pos->ctxt, dest_rel_path, mode) == -1)
	{
		#if defined(ALL) || defined(WARNING)
		printf("WARNING\tmdal->mkdir\t%s\t%s\tcalling mkdir_p and retry\n", dest_rel_path, strerror(errno));
		#endif
		#if defined(ALL) || defined(NOTICE)
		printf("NOTICE\tBuilding parent directories of %s\n", dest_rel_path);
		#endif

		char* dest_rel_path_copy = strdup(dest_rel_path);

		mkdir_p(pos, dest_rel_path_copy);
		
		if (pos->ns->prepo->metascheme.mdal->mkdir(pos->ctxt, dest_rel_path, mode) == -1)
			printf("ERROR\tmdal->mkdir\t%s\tfailed to create directory after mkdir_p: %s\n", dest_rel_path, strerror(errno));
	
		free(dest_rel_path_copy);
	}

	return 0;
}

/*
 * Create a symlink at a specific position. Will link to whatever is referenced by source_abs_path. Works with relative links within the source root but not absolute. 
 * @param pos: marfs position to create the link relative to
 * @param link: the path of the link to create
 * @param source_abs_path: absolute path of source link to read to determine dest
 * @return -1 on fail, 0 on success
 */
int mimosa_create_symlink(marfs_position* pos, char* link, char* source_abs_path)
{
	char* dest = calloc(1, STR_BUF_SIZE);

	readlink(source_abs_path, dest, STR_BUF_SIZE); // reading POSIX source so no mdal

	// add later: modify absolute dest path to be absolute MarFS path
	//  - this should not take too much work but is an edge case I just noticed I did not test when documenting
	
	if (pos->ns->prepo->metascheme.mdal->symlink(pos->ctxt, dest, link) == -1)
	{
		#if defined(ALL) || defined(WARNING)
		printf("WARNING\tmdal->symlink\t%s to %s\t%s\tcalling mkdir_p and retry\n", dest, link, strerror(errno));
		#endif
		mkdir_p(pos, link);
		
		if (pos->ns->prepo->metascheme.mdal->symlink(pos->ctxt, dest, link) == -1)   
			printf("ERROR\tmdal->symlink\t%s to %s\t%s\n", link, dest, strerror(errno));
		else
		{
			#if defined(ALL) || defined(NOTICE)
			printf("NOTICE\tSuccessfully build parents and linked %s to %s\n", link, dest);
			#endif
		}

		free(dest);
		return -1;
	}

	free(dest);	
	return 0;
}

// HELPER FUNCTIONS

/*
 * When an inode with hard links is passed in, need to see if at least one instance has been mapped to the destination before linking.
 * For this function, locking should be done before and after call if being used in multithreaded context.
 * @param dest_abs_path: for dest_filename in hardlink_node
 * @param stat_struct: to get inode num
 * @param node: node to initialize if not found
 * @return: 0 if a corresponding inode exists in destination, 1 if a file needs to be created to represent it 
 */
int check_new_linked_inode(char* dest_abs_path, struct stat* stat_struct, hardlink_node* node)
{
	int hardlink_create_first_file = 0;

	if (node != NULL)
		return hardlink_create_first_file;
	else
	{
		hardlink_create_first_file = 1;
	
		#if defined(ALL) || defined(NOTICE) || defined(HARDLINK) 
		printf("NOTICE\tHARDLINK\t%s\tinode_num: %ld\tentry does not exist\tcreating\n", dest_abs_path, stat_struct->st_ino); 
		#endif

		node = hardlink_add_entry(hardlink_list, stat_struct->st_ino, stat_struct->st_nlink, 1, dest_abs_path);
	}

	return hardlink_create_first_file;

}

/**
 * Create the parents of a file or directory in MarFS. This function is intended to fix the issue of pwalk returning entries before their parents are created. 
 * @param pos
 * @param dest_rel_path_copy: copy of the path of the entiry who's parents need to be created
 * @return -1 on failure, 0 on success
 */
int mkdir_p(marfs_position* pos, char* dest_rel_path_copy)
{	
	// path string set up
	char* curr_path = calloc(1, strlen(dest_rel_path_copy) + 1);
	char dest_rel_path[4096]; // big size for static array ( strtok breaking with char* )
	memset(dest_rel_path, 0, 4096);
	memcpy(dest_rel_path, dest_rel_path_copy, strlen(dest_rel_path_copy));

	char* token = strtok(dest_rel_path, "/");

	while ( token != NULL )
	{
		// extend path
		strcat(curr_path, token);
		strcat(curr_path, "/");

		token = strtok(NULL, "/");

		if ( token == NULL )
                    break;
		else // create MarFS parent
		{
			struct stat* curr_dir_stat = calloc(1, sizeof(struct stat));

			if ( pos->ns->prepo->metascheme.mdal->mkdir(pos->ctxt, curr_path, S_IRWXU) == -1)
			{	
				if ( errno == EEXIST )
				{
					#if defined(ALL) || defined(NOTICE)
					printf("NOTICE\tmkdir_p\t%s\tparent directory exists\n", curr_path);
					#endif
				}
				else
				{
					printf("ERROR\tmkdir_p\t%s\t%s\n", curr_path, strerror(errno));
				}
			}
			free(curr_dir_stat);
		}
	}

	free(curr_path);
	return 0;
}

/**
 * After a MarFS file is created, update its atime and mtime with the ones from the source file. Owner and Group are also updated.
 * @param pos
 * @param dest_rel_path: the MarFS user path, relative to the Namespace
 * @param stat_struct: for original times
 * @param symlink: if the entry is a symlink and AT_SYMLINK_NOFOLLOW should be used
 * @return -1 on failure, 0 on success
 */
int update_times(marfs_position* pos, char* dest_rel_path, struct stat* stat_struct, int symlink)
{
	uid_t uid = (stat_struct)?stat_struct->st_uid:0;	// get UID for file
	gid_t gid = (stat_struct)?stat_struct->st_gid:0;	// get GID for file
	struct timespec times[2];

	// set up utimens arg
	times[0].tv_sec = stat_struct->st_atime;
	times[0].tv_nsec = 0;
	times[1].tv_sec = stat_struct->st_mtime;
	times[1].tv_nsec = 0;
	
	#if defined(ALL) || defined(CONVERSION)
	LOG(LOG_INFO, "CONVERSION\tUPDATE TIMES/OWNER\t%s\n", dest_rel_path);
	#endif
	
	if (symlink)
	{
		if(pos->ns->prepo->metascheme.mdal->utimens(pos->ctxt, dest_rel_path, times, AT_SYMLINK_NOFOLLOW) == -1)
		{
			LOG(LOG_ERR, "utimens()\tSYMLINK\t%s\tfailed to update atime and mtime\t%s\n", dest_rel_path, strerror(errno));
			return -1;
		}
		if(pos->ns->prepo->metascheme.mdal->chown(pos->ctxt, dest_rel_path, uid, gid, AT_SYMLINK_NOFOLLOW) == -1)
		{
			LOG(LOG_ERR, "chown()\tSYMLINK\t%s\tfailed to update uid and/or gid\t%s\n", dest_rel_path, strerror(errno));
			return -1;
		}
	}
	else // not a symlink
	{
		if(pos->ns->prepo->metascheme.mdal->utimens(pos->ctxt, dest_rel_path, times, 0) == -1)
		{
			LOG(LOG_ERR, "utimens()\tREGULAR\t%s\tfailed to update atime and mtime\t%s\n", dest_rel_path, strerror(errno));
			return -1;
		}
		if(pos->ns->prepo->metascheme.mdal->chown(pos->ctxt, dest_rel_path, uid, gid, 0) == -1)
		{
			LOG(LOG_ERR, "chown()\tREGULAR\t%s\tfailed to update uid and/or gid\t%s\n", dest_rel_path, strerror(errno));
			return -1;
		}
	}

	return 0;
}

/**
 * After a MarFS file is created, create the MARFS-CACHE xattr and store the given path
 * into it
 * @param pos
 * @param dest_rel_path: the MarFS user path, relative to the Namespace
 * @param cache_path: the value of the MARFS-CACHE xattr
 * @return -1 on failure, 0 on success
 */
int set_cachepath(marfs_position* pos, char* dest_rel_path, char* cache_path)
{
        MDAL_FHANDLE marfs_fh = pos->ns->prepo->metascheme.mdal->open(pos->ctxt, dest_rel_path, O_WRONLY);

	#if defined(ALL) || defined(CONVERSION)
	LOG(LOG_INFO, "CONVERSION\tXATTR\t%s\n", dest_rel_path);
	#endif
	
	if (!marfs_fh)
	{
                LOG(LOG_ERR, "open()\tXATTR\t%s\tcould not populate MARFS-CACHE\t%s\n", dest_rel_path, strerror(errno));
		return -1;
	}

	if (pos->ns->prepo->metascheme.mdal->fsetxattr(marfs_fh, 1, "MARFS-CACHE", cache_path, strlen(cache_path), XATTR_CREATE) == -1)
	{
		LOG(LOG_ERR, "fsetxattr()\tXATTR\t%s\tfailed to set MARFS-CACHE\t%s\n", dest_rel_path, strerror(errno));
                pos->ns->prepo->metascheme.mdal->close(marfs_fh);
		return -1;
	}

        pos->ns->prepo->metascheme.mdal->close(marfs_fh);
	return 0;
}

/**
 * Generates a relative path to the last directory of the prefix
 * (i.e. source_arg). Basically it tacks on the last directory as a
 * parent directory to rel_path
 * @param rel_path    a path that does not start with /
 * @param source_arg  the last directory of a prefix
 * @return a relative path
 */
char* dot_to_source(char* rel_path, char* source_arg)
{
	return gen_abs_path(rel_path,source_arg);	

}

/**
 * Concatonates a prefix to a relative path. This 
 * function assumes that the prefix does NOT contain
 * a trailing slash (/), and that the relative
 * path does NOT start with a slash (/).
 * @param rel_path    a path that does not start with /
 * @param prefix      a path that starts with /, but does not
 *                    end with /
 * @return the absolute path                   
 */
char* gen_abs_path(char* rel_path, char* prefix)
{
	int abslen = strlen(rel_path) + strlen(prefix) + 2;     // 2 because we need one for the extra slash
	char* abs_path = calloc(1, abslen);

	snprintf(abs_path, abslen, "%s/%s", prefix, rel_path);
	return abs_path; 
}

/**
 * Generates a relative path, which, by definition does not
 * start with a slash (/). This is done by removing the given
 * prefix. If the prefix is not found, then a copy of the 
 * absolute path minus the leading slash (/) is returned
 * @param abs_path     an absolute path used to generate the
 *                     realive path
 * @param prefix       a string of charater to remove from the 
 *                     start of the absolute path. By definition
 *                     this string does not have a trailing /
 * @return a path that does not start with /
 */
char* gen_rel_path(char* abs_path, char* prefix)
{
        size_t plen = strlen(prefix);

	if (!plen || strlen(abs_path) < plen)  // if absolute path cannot contain prefix, return a copy of absolute path
	   return strdup(abs_path+1);	       // +1 removes the leading slash

                    // if prefix is found -> make relative path. Otherwise return copy of absolute path
	return (!strncmp(abs_path,prefix,plen))?strdup(abs_path+plen+1):strdup(abs_path+1);
}

// return the relative path of the root namespace: /usr/bin -> bin
char* marfs_root_rel_path()
{
	int pos = 0;
	int slash_index = -1;

	while(*(DEST_ARG + pos) != '\0')
	{
		if ( *(DEST_ARG + pos) == '/')
			slash_index = pos;
		
		pos++;
	}

	// no / detected
	if ( slash_index == -1)
	{
		char* ret = strdup(DEST_ARG);
		return ret;
	}
		
	char* root_rel_path = calloc(1, strlen(DEST_ARG) + 1); // indeterminant amount of space needed, but guaranteed to be <= len source_arg
	strncpy(root_rel_path, DEST_ARG + slash_index + 1, strlen(DEST_ARG) - slash_index - 1);

	return root_rel_path;
}
