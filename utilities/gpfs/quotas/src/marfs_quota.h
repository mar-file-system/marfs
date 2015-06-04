#ifndef MARFS_QUOTA_H
#define MARFS_QUOTA_H

#include <stdint.h>
#include <stddef.h>             // size_t
#include <sys/types.h>          // ino_t
#include <sys/stat.h>
#include <math.h>               // floorf
#include <gpfs_fcntl.h>

//#define MAX_XATTR_VAL_LEN 64
//#define MAX_XATTR_NAME_LEN 32
#define MAX_XATTR_VAL_LEN 1024 
#define MAX_XATTR_NAME_LEN 1024 

#define MAX_FILESET_NAME_LEN 256

#define SMALL_FILE_MAX 4096
#define MEDIUM_FILE_MAX 1048576


// Xattr info
#define MAX_MARFS_XATTR 3


extern float MarFS_config_vers;
// Eventually would like to link fuse marfs so that these are defined in only one location
// and also call fuse xattr parsing functions
#define MARFS_POST_FORMAT       "ver.%03d_%03d/%c/off.%ld/objs.%ld/bytes.%ld/corr.%016lx/crypt.%016lx/gc.%s"
// how objects are used to store "files"
// NOTE: co-maintain encode/decode_obj_type()
typedef enum {
   OBJ_NONE = 0,
   OBJ_UNI,            // one object per file
   OBJ_MULTI,          // file spans multiple objs (list of objs as chunks)
   OBJ_PACKED,         // multiple files per objects
   OBJ_STRIPED,        // (like Lustre does it)
   OBJ_FUSE,           // written by FUSE.  (i.e. not packed, maybe uni/multi.
                       // Only used in object-ID, not in Post xattr)
} MarFS_ObjType;

#define MARFS_MAX_MD_PATH 1024 

typedef uint64_t CorrectInfo;   // e.g. checksum
typedef uint64_t EncryptInfo;  // e.g. encryption key (doesn't go into object-ID)

// "Post" has info that is not known until storage into the object(s)
// behind a file is completed.  For example, in the case of an object being
// written via fuse, we have no knowledge of the total size until the
// file-descriptor is closed.
typedef struct MarFS_XattrPost {
   float              config_vers;  // redundant w/ config_vers in Pre?
   MarFS_ObjType      obj_type;
   size_t             obj_offset;    // offset of file in the obj (for packed)
   CorrectInfo        correct_info;  // correctness info  (e.g. the computed checksum)
   EncryptInfo        encrypt_info;  // any info reqd to decrypt the data
   size_t             num_objects;   // number ChunkInfos written in MDFS file
   size_t             chunk_info_bytes; // total size of chunk-info in MDFS file
   char               gc_path[MARFS_MAX_MD_PATH];
} MarFS_XattrPost;




struct histogram {
  size_t small_count;
  size_t medium_count;
  size_t large_count;
};

struct marfs_xattr {
//  char xattr_name[MAX_XATTR_NAME_LEN];
//  char xattr_value[MAX_XATTR_VAL_LEN];
   
// Getting the following array sizez from gpfs_fcntl.h
  char xattr_name[GPFS_FCNTL_XATTR_MAX_NAMELEN];
  char xattr_value[GPFS_FCNTL_XATTR_MAX_VALUELEN];

};

typedef struct fileset_stats {
      int fileset_id;
      char fileset_name[MAX_FILESET_NAME_LEN];
      //unsigned long long int sum_size;
      size_t sum_size;
      int sum_blocks;
      size_t sum_filespace_used;
      size_t sum_file_count;
      size_t sum_trash;
      size_t adjusted_size;
      size_t small_count;
      size_t medium_count;
      size_t large_count;
} fileset_stat;


int read_inodes(const char *fnameP, FILE *outfd, int fileset_id, fileset_stat *fileset_stat_ptr, size_t rec_count, size_t offset_start);
int clean_exit(FILE *fd, gpfs_iscan_t *iscanP, gpfs_fssnap_handle_t *fsP, int terminate);
int get_xattr_value(struct marfs_xattr *xattr_ptr, const char *desired_xattr, int cnt);
int get_xattrs(gpfs_iscan_t *iscanP,
                 const char *xattrP,
                 unsigned int xattrLen,
                 const char * desired_xattr,
                 struct marfs_xattr *xattr_ptr);
void print_usage();
void init_records(fileset_stat *fileset_stat_buf, unsigned int record_count);
int lookup_fileset(fileset_stat *fileset_stat_ptr, size_t rec_count, size_t offset_start, char *inode_fileset);
static void fill_size_histo(const gpfs_iattr_t *iattrP, fileset_stat *fileset_buffer, int index);
int str_2_post(MarFS_XattrPost* post, struct marfs_xattr* post_str);
void write_fsinfo(FILE* outfd, fileset_stat* fileset_stat_ptr, size_t rec_count, size_t index_start);

#endif

