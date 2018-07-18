#include <stdio.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <common.h>
#include <marfs_ops.h>


//START OF A REALLY SHORT PROGRAM
int main()
{
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

	if (init_mdfs())
	{
		printf("ERROR: failed to update marfs namespace\n");
		return -1;
	}

	return 0;
}

