#include <aws4c.h>
//#include <object_stream.h>
#include <marfs_base.h>
       char* MARFS_POST_FORMAT2 = "ver.%03hu_%03hu/%c/off.%d/objs.%d/bytes.%d/corr.%016ld/crypt.%016ld/flags.%02hhX/mdfs.%c";
       //works char* MARFS_POST_FORMAT2 = "ver.%03hu_%03hu/%c/off.%d/objs.%d/bytes.%d/corr.%016ld/crypt.%016ld/flags.%02d/mdfs.%c";
       //char* MARFS_POST_FORMAT2 = "ver.%03hu_%03hu/%c/off.%d/objs.%d/bytes.%d/corr.%016d/crypt.%016d/flags.%02hhX/mdfs.%c";


typedef struct MarFS_XattrPost2 {
   uint16_t           config_vers_maj; // redundant w/ config_vers in Pre?
   uint16_t           config_vers_min; // redundant w/ config_vers in Pre?
   MarFS_ObjType      obj_type;      // type of storage
   int            obj_offset;    // offset of file in the obj (Packed)
   int            chunks;        // (context-dependent.  See NOTE)
   int             chunk_info_bytes; // total size of chunk-info in MDFS file (Multi)
   CorrectInfo        correct_info;  // correctness info  (e.g. the computed checksum)
   EncryptInfo        encrypt_info;  // any info reqd to decrypt the data
   PostFlagsType	flags;
   char               md_path[MARFS_MAX_MD_PATH]; // full path to MDFS file
} MarFS_XattrPost2;
struct walk_path{
	int inode;
	char path[1024];
	char parent[1024];
};

struct marfs_inode {
	time_t atime;
	time_t ctime;
	time_t mtime;
        int inode;
        int size;
        int offset;
	char path[1024];
        //char url[MARFS_MAX_URL_SIZE];
//      struct MarFS_XattrPost mpost;   
        MarFS_XattrPre pre;
//        char pre[1215];
//      char post[1215];
        MarFS_XattrPost2 post;
};
//typedef struct list_el item;
//typedef struct list_olist list;
//struct list_el {
typedef struct inode_lnklist {
	struct marfs_inode val;
	int count;
	//struct list_el * next;
	struct inode_lnklist *next;
} inode_lnklist;

//struct list_olist {
typedef struct obj_lnklist {
   //struct list_olist * next;
   struct obj_lnklist *next;
   int count;
   //struct item * val;
   struct inode_lnklist *val;
} obj_lnklist;

