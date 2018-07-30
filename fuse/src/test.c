#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <common.h>
#include <marfs_ops.h>

size_t get_tens(size_t idx, size_t len)
{
	if (idx == (len - 1))
		return 1;

	size_t ret = 1;
	size_t i;
	
	for(i = 0; i < len - 1 - idx; i++)
	{
		ret = ret * 10;
	}
	return ret;
}

size_t str_to_size_t(const char* input)
{
	size_t ret = 0;
	size_t i;
	size_t len = (size_t)strlen(input);

	for(i = 0; i < len; i++)
	{
		size_t tens = get_tens(i, len);
		ret = ret + ((size_t)((input[i] - '0'))) * tens;
	}

	return ret;
}

void show_usage(char* prog_name)
{
   fprintf(stderr, "Usage: %s [option]\n", prog_name);
   fprintf(stderr, "\n");
   fprintf(stderr, "\toptions:\n");
   fprintf(stderr, "\t\t-h                     help\n");
   fprintf(stderr, "\t\t-p [ <path> ]          file path to be converted to object path\n");
   fprintf(stderr, "\t\t-n [ <chunk number> ]  chunk number of the file\n");
   fprintf(stderr, "\n");
}

int main(int argc, char* argv[])
{
	int c;
	int usage = 0;
	char* path = NULL;
	size_t chunk_no = 0;
	path = argv[1];

	char path_template[MC_MAX_PATH_LEN];
	struct statvfs st;
	//first need to read config
	if (read_configuration())
	{
		printf("ERROR: Reading Marfs configuration failed\n");
		return -1;
	}

	if (init_xattr_specs())
	{
		printf("ERROR: init_xattr_specs failed\n");
		return -1;
	}

	int rc = marfs_statvfs(marfs_sub_path(path), &st);
	return 0;
}
