#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <common.h>
#include <marfs_ops.h>

int show_usage(char* prog_name)
{
   fprintf(stderr, "Usage: %s [option]\n", prog_name);
   fprintf(stderr, "\n");
   fprintf(stderr, "\toptions:\n");
   fprintf(stderr, "\t\t-h                     help\n");
   fprintf(stderr, "\t\t-u [ <user> ]          user name\n");
   fprintf(stderr, "\t\t-g [ <group> ]         group name\n");
   fprintf(stderr, "\n");
   fprintf(stderr, "\tCan't use more than one of -u, -g\n");
   return 0;
}

//START OF A REALLY SHORT PROGRAM
int main(int argc, char* argv[])
{
	int c;
	int usage = 0;
	int uflag = 0;
	int gflag = 0;
	char* user = NULL;
	char* group = NULL;
	int ulen = 0;
	int glen = 0;
	if (argc == 1 || argc > 5)
	{
		show_usage(argv[0]);
		printf("Invalid arguments\n");
		return -1;
	}
	while((c = getopt(argc, argv, "hu:g:")) != -1)
	{
		switch(c)
		{
			case 'h':
				usage = 1;
				break;

			case 'u':
				uflag = 1;
				user = optarg;
				ulen = strlen(user);
				break;

			case 'g':
				gflag = 1;
				group = optarg;
				glen = strlen(group);
				break;
			case '?':
				printf("Unrecognized option '%s'\n", argv[optind-1]);
				usage = 1;
				break;

			default:
				usage = 1;
				printf("Getopt charactor code 0%o\n", c);
		}
	}

	if (uflag + gflag != 2)
	{
		show_usage(argv[0]);
		printf("Need user and group\n");
		return -1;
	}

	if (ulen == 0 || user == NULL)
	{
		show_usage(argv[0]);
		printf("No user\n");
		return -1;
	}

	if (glen == 0 || group == NULL)
	{
		show_usage(argv[0]);
		printf("No group\n");
		return -1;
	}
	printf("Start updating marfs namespace!!!\n");
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

	build_namespace_md(user, group);
	return 0;
}

