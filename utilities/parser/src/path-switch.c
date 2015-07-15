#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef DATAPARSE
#include "parse-inc/config-structs.h"
#endif

#include "confpars-structs.h"
#include "confpars.h"
#include "parse-types.h"
#include "path-switch.h"

int countSwitchPaths(struct line *plist)
{
struct line *c_ptr;
int cnt = 0;

if (plist == (struct line *)NULL)					// we're looking at the 'base' next one is 1st one
   return 0;

c_ptr = plist;

if (c_ptr->next == (struct line *)NULL)
   printf("it is pointing to NULL\n");
while (c_ptr->next != (struct line *)NULL) {
   c_ptr = c_ptr->next;
   cnt++;								// found another one.
   }

return cnt;
}



int createSwitchPaths(struct line *paths, int cnt)
{
FILE *fs_ptr, *fn_ptr;
struct line *c_ptr;
char str_line[32];
int lcnt = 0;

if (paths == (struct line *)NULL)
   return 0;

c_ptr = paths;

   fs_ptr = fopen("./parse-inc/path-switch.inc", "w");								// this is where the path <--> struct switch goes
   fn_ptr = fopen("./parse-inc/path-names.inc", "w");								// this is where the 'struct path' names go

if (fs_ptr != (FILE *)NULL && fn_ptr != (FILE *)NULL) {								// we need these files for the 2nd stage compile !!
   sprintf(str_line, "char *configFields[%d] = {", cnt-1);							//  index starts at 0
   fprintf(fn_ptr, "%s\n", str_line);
   while (c_ptr->next != (struct line *)NULL) {									// as long as there's a valid next
      c_ptr = c_ptr->next;											// go to the next one
      if (lcnt != 0) {												// we don't put the 'config super struct' in this list
         fprintf(fs_ptr, "case % 3d: { \n", lcnt-1);								// open the case
         if (c_ptr->type == TYPE_CHAR) {									// we treat dynamic (char *) different than static structs
            fprintf(fs_ptr, "   return (void *)&(%s);\n", c_ptr->ln);						// for (char *) just return the pointer
            }
         if (c_ptr->type == TYPE_STRUCT) {									// structs are static for now, we don't free/alloc them
            fprintf(fs_ptr, "   if (sw_task == GET_PTR)\n");
            fprintf(fs_ptr, "      return (void *)&(%s);\n", c_ptr->ln);					// return pointer for 'regular' addressing
            fprintf(fs_ptr, "   if (sw_task == GET_F_PTR)\n");
            fprintf(fs_ptr, "      return (void *)NULL;\n");							// return NULL if pointer is needed for free/malloc
            }
         fprintf(fs_ptr, "   break;\n   };\n");									// case closed

         fprintf(fn_ptr, "\"%s\"", c_ptr->ln);									// list of paths in the structure
         if (++lcnt == cnt)
            fprintf(fn_ptr, "\n");										// last entry does not end with a comma
         else
            fprintf(fn_ptr, ",\n");										// all others do
         }
      else
         lcnt++;
      }

   fprintf(fn_ptr, "};\n");
   fclose(fs_ptr);
   fclose(fn_ptr);
   }
else
   printf("Error opening include files\n");

return lcnt;
}
