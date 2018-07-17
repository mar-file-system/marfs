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
	int    c;
	int    usage = 0;
	char*  path = NULL;
	size_t chunk_no = 0;
   int    got_an_option = 0;
	while((c = getopt(argc, argv, "hp:c:")) != -1)
	{
		switch(c)
		{
			case 'h':
				usage = 1;
				break;

			case 'p':
				path = optarg;
            got_an_option = 1;
				break;
			
			case 'c':
				chunk_no = str_to_size_t(optarg);
            got_an_option = 1;
				break;

			default:
				usage = 1;
				break;
		}
	}
	
	if (usage || !got_an_option)
	{
		show_usage(argv[0]);
		return 0;
	}

	char path_template[MC_MAX_PATH_LEN];
	struct stat st;
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

	//first we check if the file exists or not
	int mode = marfs_getattr(marfs_sub_path(path), &st);

	MarFS_FileHandle fh;
	memset(&fh, 0, sizeof(fh));
	
	const char* marfs_path = marfs_sub_path(path);
	marfs_path_convert(mode, marfs_path, &fh, chunk_no, path_template);
	//int rc = marfs_open(marfs_path, &fh, O_RDONLY, 0);
	//get_path_template(path_template, &fh);

	//printf("host from pre %s; repo form pre %s; objid from pre %s\n", fh.info.pre.host, fh.info.pre.repo, fh.info.pre.objid);
	//rc = marfs_release(marfs_path, &fh);
	printf("%s\n", path_template);

	return 0;
}
