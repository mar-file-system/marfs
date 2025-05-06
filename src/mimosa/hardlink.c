#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>

#include "hardlink.h"

list_meta* hardlink_list;
pthread_mutex_t hardlink_mutex;

/*
 * Initialize the data structure and pthreads to track hardlink conversion info.
 */	
int hardlink_init()
{
	hardlink_list = list_init();
	pthread_mutex_init(&hardlink_mutex, NULL);
	
	return 0;
}


/*
 * When there is no existing entry for a specific inode, add a new node to represent it
 */
hardlink_node* hardlink_add_entry(list_meta* hardlink_list, int src_inum, int src_lc, int dest_lc, char* dest_fn) 
{
	hardlink_node* node = calloc(1, sizeof(hardlink_node));
	
	node->source_inode_num = src_inum;
	node->source_link_count = src_lc;
	node->dest_link_count = dest_lc;
	node->dest_filename = strdup(dest_fn);
	
	list_add_node(hardlink_list, (void *) node);
	
	return node;
}

/*
 * When all links to an inode have been processed, delete it.
 */
int hardlink_delete_entry(list_meta* hardlink_list, int source_inode_num)
{
	list_node* node = list_search_inode(hardlink_list, source_inode_num);
	if (node == NULL)
		return -1;
	
	// free hardlink node data outside of list interface	
	hardlink_node* hnode = (hardlink_node*) node->data;
	free(hnode->dest_filename);
	
	list_del_node(hardlink_list, node);

	return 0;
}

/*
 * Search for an entry with specific inode and return its location. 
 */
hardlink_node* hardlink_search_src_inode(list_meta* hardlink_list, int source_inode_num)
{
	list_node* lnode = list_search_inode(hardlink_list, source_inode_num);
	
	if (lnode == NULL)
		return NULL;
	else
		return (hardlink_node*) lnode->data;
}

/*
 * Destroy data structure and free all associated memory when finished. 
 */
int hardlink_destruct(list_meta* hardlink_list)
{
	list_destruct(hardlink_list);
	return 0;
}
