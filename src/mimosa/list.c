#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "hardlink.h"

list_meta* list_init()
{
	list_meta* list = calloc(1, sizeof(list_meta));
	
	list->head = NULL;
	list->tail = NULL;
	list->length = 0;

	return list;
}


int list_destruct(list_meta* list)
{
	if (list == NULL)
	{
		printf("list_destruct: passed NULL list\n");
		return -1;
	}

	list_node* rover = list->head;
	
	if(list->length == 0) // empty list
	{
		free(list);
	}
	else if ( list->length == 1 ) // single node list
	{
		free(list->head->data);
		free(list->head);
		free(list);
	}
	else // length 2 or more
	{

		do
		{
			free(rover->data);
			rover = rover->next;
			free(rover->prev);

		} while ( rover != list->tail);
	
		free(rover->data);
		free(rover);
		free(list);
	}

	return 0;
}

/*
 * Add a new list node to the end of the list. 
 */
int list_add_node(list_meta* list, void* data)
{
	if (list == NULL) 
	{
		printf("list_add_node: received NULL list_meta\n");
		return -1;
	}
	
	// allocate new node
	list_node* node = calloc(1, sizeof(list_node));
	node->data = data;

	// only node, list empty
	if ( list->head == NULL && list->tail == NULL)
	{
		node->next = NULL;
		node->prev = NULL;
		
		list->head = node;
		list->tail = node;
	}
	else // new tail
	{
		node->prev = list->tail;
		node->next = NULL;
		
		list->tail->next = node;
		list->tail = node;
	}

	list->length++;
	return 0;
}

/*
 * Frees node and associated data. If user wants data after deletion, must save a copy before this call. 
 */
int list_del_node(list_meta* list, list_node* node)
{
	
	if (list == NULL)
	{
		printf("list_del_node: received NULL list_meta\n");	
		return -1;
	}

	if ( node == NULL)
	{
		printf("list_del_node: received NULL list_node\n");
		return -1;
	}
	
	// only node
	if ( list->head == node && list->tail == node )
	{
		list->head = NULL;
		list->tail = NULL;
	}
	else if (list->head == node) // deleting head
	{
		list->head = list->head->next;
		list->head->prev = NULL;
	}
	else if (list->tail == node) // deleting tail
	{
		list->tail = list->tail->prev;
		list->tail->next = NULL;
	}
	else // deleting middle node
	{
		// don't touch head or tail

		node->prev->next = node->next;
		node->next->prev = node->prev;
	}

	free(node->data);
	free(node);
	list->length--;
	return 0;	
}

/*
 * Feel free to add extra search functions that look for specific things based on data being stored
 */
list_node* list_search_inode(list_meta* list, int inode_num)
{
	if (list == NULL)
	{
		printf("list_search_node: received NULL list_meta\n");
		return NULL;
	}

	// if empty list, return not found
	if (list_length(list) == 0)
		return NULL;
	
	list_node* rover = list->head;
	
	do
	{
		hardlink_node* data = (hardlink_node*) rover->data;
		if ( data->source_inode_num == inode_num )
			return rover;
		else
			rover = rover->next;
	}
	while ( rover != NULL );
	
	return NULL; // not found
}

list_node* list_to_index(list_meta* list, int index)
{
	if (list == NULL)
	{
		printf("list_to_index: received NULL list\n");
		return NULL;
	}

	if (index >= list->length)
	{
		printf("list_to_index: index greater than length of list\n");
		return NULL;
	}

	list_node* rover = calloc(1, sizeof(list_node));
	rover = list->head;

	for ( int i = 0; i < index; i++) // loop until count == index
	{
		rover = rover->next;
	}	

	return rover;
}

list_node* list_head(list_meta* list)
{
	return list->head;
}

list_node* list_tail(list_meta* list)
{
	return list->tail;
}

int list_length(list_meta* list)
{
	return list->length;
}

