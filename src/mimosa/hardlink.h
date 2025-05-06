#include "list.h"

typedef struct hardlink_node_ {
	int source_inode_num;
	int source_link_count;
	int dest_link_count;
	char* dest_filename;
} hardlink_node;

// External functions to be called to manage the hardlink tracking data structure

int hardlink_init();
hardlink_node* hardlink_add_entry(list_meta* hardlink_list, int src_inum, int src_lc, int dest_lc, char* dest_fn);
int hardlink_delete_entry(list_meta* hardlink_list, int source_inode_num);
hardlink_node* hardlink_search_src_inode(list_meta* hardlink_list, int source_inode_num);
int hardlink_destruct();
