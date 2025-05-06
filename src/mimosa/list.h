
#include <stdlib.h>
// Interface Functions

typedef struct list_node_ {
	struct list_node_* next;
	struct list_node_* prev;
	void* data;
} list_node;

typedef struct list_meta_ {
	list_node* head;
	list_node* tail;
	int length;
} list_meta;

list_meta* list_init();
int list_destruct(list_meta* list);
int list_add_node(list_meta* list, void* data);
int list_del_node(list_meta* list, list_node* node);
list_node* list_search_inode(list_meta* list, int inode_num);   
list_node* list_head(list_meta* list);
list_node* list_tail(list_meta* list);
int list_length(list_meta* list);
list_node* list_to_index(list_meta* list, int index);
