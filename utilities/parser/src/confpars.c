/*
 * 
 *   confpars.c
 *
 *   xml config file parser 
 *
 *   Ron Croonenberg rocr@lanl.gov
 *   High Performance Computing (HPC-3)
 *   Los Alamos National Laboratory
 *
 *
 *   06-08-2015:        initial start rocr@lanl.gov
 *   06-08-2015:        redesigned collate functions
 *
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef DATAPARSE
#include "parse-inc/config-structs.h"
#include "parsedata.h"
#endif

#include "confpars-structs.h"
#include "confpars.h"
#include "parse-types.h"
#include "path-switch.h"




char *stripLeadingWhiteSpace(char *line)
{
char *ln, *str_ptr;
int i = 0;

str_ptr = line;
while (isspace(*str_ptr))										// leading whitespace  .....
   str_ptr++;

ln = (char *)malloc(strlen(line)+1);

strcpy(ln, str_ptr);
strcpy(line, ln);

free(ln);

return line;
}



char *stripTrailingWhiteSpace(char *line)
{
int i;

i=strlen(line);												// start at the end
while (isspace(line[i-- - 1]))										// trailing whitespace  .....

line[i] = 0;												// truncate line

return line;
}



char *stripWhiteSpace(char *line)
{
stripLeadingWhiteSpace(line);										// strip leading white space
stripTrailingWhiteSpace(line);										// strip trailing white space

return line;
}



struct line *findClosingStructBracket(struct line *hdr)
{
struct line *c_ptr;
int cnt = 1;

if (hdr == (struct line *)NULL)
   return (struct line *)NULL;										// there is nothing to find

while (c_ptr->next != (struct line *)NULL && cnt >0) {							// as long as there are lines and we didn't find it yet
   if (strstr(c_ptr->ln, "};") != (char *)NULL)
      cnt--;												// found a struct closing bracket
   if (strstr(c_ptr->ln, "{")  != (char *)NULL)
      cnt++;												// found a struct openening bracket
   }
return c_ptr;												// report what we found
}



int checkElementName(char *str, char *buff)
{
int i;
char *reserved[33] = { "auto", "break", "case", "char", "const", "continue", "default",			// list of reserved keywords, probably not complete
                       "do", "double", "else", "entry", "enum", "extern", "float",
                       "for", "goto", "if", "int", "long", "register", "return", "short",
                       "signed", "sizeof", "static", "struct", "switch", "typedef",
                       "union", "unsigned", "void", "volatile", "while" };


for (i=0; i<RSRV_LST; i++) {
   if (strcmp(str, reserved[i]) == 0) {									// check element with reserved words
      printf("Error: An XML element can not be a C reserved word in this configuration. (%s)\n", str);
      exit(-1);											// fatal error
      }
   }


for (i=0; i<strlen(str)-1; i++) {
   if (!(isalnum(str[i]) ||  str[i] == '_')) {								// check if element name is a valid variable name
      printf("Error: non C type variable/element name detected (%s).\n", str);
      exit(-1);
      }
   }

return 0;												// everything fine
}




char *readConfigFile(char *fn)
{
FILE *f_ptr;
long int f_size;
char *conf;

f_ptr = fopen(fn, "r");											// open config file
if (f_ptr != (FILE *)NULL) {										// on succesful open
   fseek(f_ptr, 0, SEEK_END);										// go to last position
   f_size = ftell(f_ptr);										// get position
   conf = (char *)malloc((size_t)f_size + 1);								// alloc 1 byte more for termination
   if (conf != (char *)NULL) {
      fseek(f_ptr, 0, SEEK_SET);									// go back to start of file
      fread(conf, (size_t)f_size, 1, f_ptr);								// read in the file
      conf[f_size] = 0;											// terminate
      }
   else {
      printf("Could not allocate memory. Aborting.\n");
      return (char *)NULL;
      }
   fclose(f_ptr);											// close config file, we opened it.
   }
else {
   printf("***Could not open configuration file (%s). Aborting.\n", fn);
   return (char *)NULL;
   }

return conf;
}



int findLineNumber(char *pos_ptr, char *buffstart)
{
int nl_cnt = 1;
char *p_ptr;

p_ptr = pos_ptr;											// where we are in the configuration 'file'
while (p_ptr > buffstart) {										// as long as we're in the configuration file
   if (p_ptr[0] == '\n')										// and we see a newline
      nl_cnt++;												// count it
   p_ptr--;												// move one character back
   }

return nl_cnt;												// return how many lines we counted
}



int findElement(char *content_start, char *content_end, char *element, char *buffstart)
{
char *el_ptr;
int i, cnt_len, ln_nr, ret=0;



element[0] = 0;												// start with empty element name
el_ptr = strstr(content_start, "<");									// find a new element start tag
if (el_ptr != (char *)NULL) {										// check to see if there is an element tag
   if (el_ptr <= content_end && el_ptr[1] != '/') {							// check it is not an end tag
      el_ptr++;												// point at token after the '<' where the name starts
      cnt_len = content_end - el_ptr;									// counter for searching only withing this content
      i=0;
      while (el_ptr[i] != '>' && i < cnt_len)								// end of element tag or end of content?
         element[i++] = el_ptr[i];									// copy  the element name from the tag
      element[i] = 0;											// terminate it
      ret = 1;
      }
   else {
      if (el_ptr <= content_end) {									// we either ran out of content (weird) OR found an illegal end tag
         strPrintTag(element, el_ptr);
         ln_nr = findLineNumber(el_ptr, buffstart);
         printf("ERROR: Found a bad tag: %s on line number: %d. Aborting parse process.\n", element, ln_nr);
         ret = -1;
         }
      element[0] = 0;											// there is no element on this level
      }
   }

if (strlen(element) > 0) {
   if (checkElementName(element, buffstart) == 1) {
      }
   }

return ret;
}



int strPrintTag(char *str_tag, char *tag_ptr)								// print element tag name to a separate string
{
char *t_ptr;
int i=0;

t_ptr = tag_ptr;											// point at the tag
while (t_ptr[0] != '>') {										// as long as we didn't find the '>' end of tag token
   str_tag[i++] = t_ptr[0];										// copy token and advance index
   t_ptr++;												// advance the ptr
   }
str_tag[i++] = t_ptr[0];										// include the '>' token
str_tag[i]   = 0;											// terminate

return i;
}



int printContent(char *content_start, char *content_end)						// for debugging purposes
{
char *str_ptr;
int i;

str_ptr = content_start;
printf("****************************\n");
printf("%p - %p\n", content_start, content_end);							// the area we'll print
printf("***** Start of Content *****\n|");
while (str_ptr <= content_end) {
   printf("%c", str_ptr[0]);										// print character at a time, because of no 0 termination.
   str_ptr++;
   i++;
   }
printf("|\n***** End of Content *****\n");

return i;
}



char *processElementContent(char *elem_tag, char *content_start, char *content_end)			// example for processing data, fill structure for example
{
char *str_ptr, *buff;
int i;

buff = (char *)malloc(strlen(elem_tag) +1 + (content_end - content_start)+1 + 1);			// buffer for the result/value, seperating tag from value with a 0 (yes you read that right)

if (buff != (char *)NULL) {
   strcpy(buff, elem_tag);										// elem_tag header
   i = strlen(elem_tag) + 1;										// move one spot after elem_tag's terminating 0
   str_ptr = content_start;										// set the content pointer to beginning
   while (str_ptr <= content_end) {									// walk through the elements content
      buff[i] = str_ptr[0];										// copy it to the buffer
      str_ptr++;											// advance content pointer
      i++;												// advance buffer index
      }
   buff[i] = 0;												// terminate buffer
   }

return buff;												// return pointer to buffer
}



char *getElemVal(char *val_buff)
{
char *val_ptr, *nv_ptr;

val_ptr = val_buff;                                                                                     // point to start of buffer
while (val_ptr[0] != 0)                                                                                 // as long as we don't see terminating 0
    val_ptr++;                                                                                          // advance the pointer

val_ptr++;                                                                                              // point at position after the 0

nv_ptr = (char *)malloc(strlen(val_ptr) +1);								// create a new buffer
strcpy(nv_ptr, val_ptr);										// copy the data in there

return nv_ptr;												// return ptr to buffer
}



char *elemValPtr(char *val_buff)
{
char *val_ptr;

if (val_buff == (char *)NULL)
   return (char *)NULL;

val_ptr = val_buff;											// point to start of buffer
while (val_ptr[0] != 0 && val_ptr+1 != (char *)NULL)											// as long as we don't see terminating 0
    val_ptr++;												// advance the pointer to point at the field data

val_ptr++;												// point at position after the 0

return val_ptr;
}


// for format reasons we keep track of recursion level		(move from global to local in a newer version);
char indent[65] = "                                                                ";
int r_lvl = 0;

void setIndent(int offset)
{
if (3*offset > 0)
   indent[3*offset] = 0;										// truncate the indent string
else
   indent[0] = 0;											// if zero or negative, no ident
}



void resetIndent(int offset)
{
if (3*offset > 0)											// fix the indent string ...
   indent[3*offset] = ' ';
else
   indent[0] = ' ';											//                       ... where we broke it before
}



char *isVarIn(char *haystack, char *needle)
{
char *ndl_ptr, *e_ptr, *ret_ptr;
int sccs = 0;


ndl_ptr = (char *)malloc(strlen(needle) + 3);

strcpy(ndl_ptr, needle);

e_ptr  = haystack;
e_ptr += strlen(haystack);
if (strlen(haystack) >= strlen(needle))
   if (strcmp(e_ptr, ndl_ptr) == 0) {
      free(ndl_ptr);
      return e_ptr;
      }

strcat(ndl_ptr, " ");
ret_ptr = strstr(haystack, ndl_ptr);
if (ret_ptr != (char *)NULL) {
   free(ndl_ptr);
   return ret_ptr;												// trying 'needle ' success
   }

ndl_ptr[strlen(ndl_ptr)-1] = '.';
ret_ptr = strstr(haystack, ndl_ptr);
if (ret_ptr != (char *)NULL){
   free(ndl_ptr);
   return ret_ptr;
   }														// trying 'needle.' success

ndl_ptr[strlen(ndl_ptr)-1] = '[';
ret_ptr = strstr(haystack, ndl_ptr);
if (ret_ptr != (char *)NULL) {
   free(ndl_ptr);
   return ret_ptr;												// trying 'needle[' success
   }

ndl_ptr[strlen(ndl_ptr)-1] = '-';
strcat(ndl_ptr, ">");
ret_ptr = strstr(haystack, ndl_ptr);
if (ret_ptr != (char *)NULL) {
   free(ndl_ptr);
   return ret_ptr;												// trying 'needle->'  succes.
   }

free(ndl_ptr);

return (char *)NULL;
}


/*
struct line *findNextLine(struct line *head, char *srch1, char *srch2)
{
struct line *ptr;
char *s_ptr1, *s_ptr2, *st_ptr;

if (head == (struct line *)NULL)
   return (struct line *)NULL;										// there's nothing to find here
   
ptr = head;												// start at the base
while (ptr->next != (struct line *)NULL) {
   ptr = ptr->next;											// go to next entry
   if (ptr->ln != (char *)NULL) {									// if we have a line
      s_ptr1 = strstr(ptr->ln, srch1);									// check if first search item is there
      if (s_ptr1 != (char *)NULL) {
         st_ptr = s_ptr1 + strlen(srch1);
         if (st_ptr[0] == ' ' ||
             st_ptr[0] == 0   ||
             st_ptr[0] == '.' ||
            (st_ptr[0] == '-'  && st_ptr[1] == '>') ||
             str_ptr == '[') {					// check it is REALLY nothing but the search string??
            s_ptr2 = strstr(st_ptr, srch2);								// check if second search item comes after first
            if (s_ptr2 != (char *)NULL) {
               st_ptr = s_ptr2+strlen(srch2);
               if (st_ptr[0] == ' ' || 
                   st_ptr[0] == 0   || 
                   st_ptr[0] == '.' || 
                  (st_ptr[0] == '-'  && st_ptr[1] == '>') ||
                   str_ptr == '[')
                  return ptr;
               }											// it does, it might be what we're looking for
            }
         }
      }
   }
return (struct line *)NULL;										// nothing found
}*/



struct line *findNextLine(struct line *head, char *srch1, char *srch2)
{
struct line *ptr = (struct line *)NULL;
char *s_ptr1, *s_ptr2;

if (head == (struct line *)NULL)
   return ptr;                                                                                          // there's nothing to find here

ptr = head;                                                                                             // start at the base
while (ptr->next != (struct line *)NULL) {
   ptr = ptr->next;                                                                                     // go to next entry
   if (ptr->ln != (char *)NULL) {                                                                       // if we have a line
      s_ptr1 = strstr(ptr->ln, srch1);                                                                  // check if first search item is there
      if (s_ptr1 != (char *)NULL) {
         s_ptr2 = strstr(s_ptr1, srch2);                                                                // check if second search item comes after first
         if (s_ptr2 != (char *)NULL)
            return ptr;                                                                                 // it does, it might be what we're looking for
         }
      }
   }
return (struct line *)NULL;                                                                             // nothing found
}



/*
struct line *findNextLine(struct line *head, char *srch1, char *srch2)
{
struct line *ptr = (struct line *)NULL;
char *s_ptr1, *s_ptr2;

if (head == (struct line *)NULL)
   return ptr;                                                                                          // there's nothing to find here

ptr = head;                                                                                             // start at the base
while (ptr->next != (struct line *)NULL) {
   ptr = ptr->next;                                                                                     // go to next entry
   if (ptr->ln != (char *)NULL) {                                                                       // if we have a line
      s_ptr1 = isVarIn(ptr->ln, srch1);                                                                  // check if first search item is there
      if (s_ptr1 != (char *)NULL) {
         s_ptr2 = isVarIn(s_ptr1, srch2);                                                                // check if second search item comes after first
         if (s_ptr2 != (char *)NULL)
            return ptr;                                                                                 // it does, it might be what we're looking for
         }
      }
   }
return (struct line *)NULL;                                                                             // nothing found
}
*/



struct line *findNextOccurence(struct line *header, char *srch_str)
{
struct line *srch_ptr;

if (header == (struct line *)NULL)
   return (struct line *)NULL;										// there's nothing to find here

srch_ptr = header;
if (srch_ptr != (struct line *)NULL && strlen(srch_str) > 0) {
   srch_ptr = srch_ptr->next;										// move to next line else we find ourself again.
   srch_ptr = findNextLine(srch_ptr, srch_str, "");							// find next header line
   if (srch_ptr != (struct line *)NULL)
      if (srch_ptr->ln != (char *)NULL)
         return srch_ptr;										// found one! return it
   }
return (struct line *)NULL;										// nothing found
}



struct line *findNextStruct(struct line *header)
{
struct line *srch_ptr;
char *srch_str;

if (header == (struct line *)NULL)
   return (struct line *)NULL;										// there's nothing to find here

srch_ptr = header;
srch_str = (char *)malloc(strlen(header->ln)+1);
strcpy(srch_str, header->ln);										// create a search string
stripWhiteSpace(srch_str);										// without trailing nor leading whitespace
srch_ptr = findNextOccurence(srch_ptr, srch_str);							// get next one if there is one
free(srch_str);												// free the search string

return srch_ptr;											// return whatever result
}



struct line *findEmptyStruct(struct line *header)
{
struct line *srch_ptr, *p_ptr;

if (header == (struct line *)NULL)
   return (struct line *)NULL;										// there's nothing to find here

srch_ptr = header;
while (srch_ptr != (struct line *)NULL) {
   srch_ptr = findNextLine(srch_ptr, "struct", "{");							// get next struct, if there is one
   if (srch_ptr != (struct line *)NULL) {
      p_ptr = srch_ptr;											// remember where structure started
      srch_ptr = srch_ptr->next;									// check the next line
      if (strstr(srch_ptr->ln, "};") != (char *)NULL) {							// is it the closing line?
#ifdef DEBUG
printf("Found empty:\n%s\n%s\n", p_ptr->ln, srch_ptr->ln);
#endif
         return p_ptr;											// return the location of the empty struct
         }
      }
   }

return (struct line *)NULL;										// we didn't find an empty struct
}



int removeEmptyStruct(struct line *base, struct line *h_struct)
{
struct line *p_ptr, *c_ptr, *n_ptr; 

if (h_struct == (struct line *)NULL)
   return -1;											// there's nothing to remove

c_ptr = base;											// point at the start of the list
while (c_ptr != h_struct) {									// go to the line just before where the struct starts
   p_ptr = c_ptr;										// previous become current
   c_ptr = c_ptr->next;										// advance current pointer one position
   }
n_ptr = c_ptr->next;										// next pointer pointing at line after the struct definition

if (strstr(n_ptr->ln, "};") != (char *)NULL) {							//n_ptr should contain '};' and maybe some whitespace
   p_ptr->next = n_ptr->next;									// p_ptr->next is pointing at the struct after the one we're deleting
   free(c_ptr);											// free the definition
   free(n_ptr);											// free the last line of this struct
   n_ptr = c_ptr->next;
   }
else {
#ifdef DEBUG
   printf("Not empty, found: '%s'", n_ptr->ln);
#endif
   return -1;
   }

return 0;
}



void removeStruct(struct line *base, struct line *h_struct)
{
struct line *c_ptr, *n_ptr;

if (h_struct == (struct line *)NULL)									// there's nothing to remove
   return;

c_ptr = base;												// point at the start of the list
while (c_ptr->next != h_struct)										// go to the line just before where the struct starts
   c_ptr = c_ptr->next;											// current pointer

n_ptr = c_ptr->next;											// next pointer pointing to the struct definition
while (strstr(n_ptr->ln, "};") == (char *)NULL) {							// as long as we're not at the end of the struct, keep deleting
   c_ptr->next = n_ptr->next;										// point at the struct after the one we're deleting
   free(n_ptr);												// free, delete
   n_ptr = c_ptr->next;
   }
c_ptr->next = n_ptr->next;										// n_ptr is pointing a the closing line
free(n_ptr);												// so get rid of it.
}



struct line *findStructMember(struct line *header, char *member)
{
struct line *c_ptr, *e_ptr;
int in_struct = 1;

if (header == (struct line *)NULL)
   return (struct line *)NULL;										// there's nothing to find here

c_ptr = header;												// this is the 'current line' pointer
//e_ptr = findClosingStructBracket(header);

#ifdef DEBUG
printf("%s - %s\n", c_ptr->ln, c_ptr->dbg);
#endif

while (c_ptr->next != (struct line *)NULL && in_struct) {
//while (c_ptr->next != (struct line *)NULL && in_struct && c_ptr < e_ptr) {

   c_ptr = c_ptr->next;											// walk down the list
#ifdef DEBUG
printf("%s - %s\n", c_ptr->ln, c_ptr->dbg);								// move to the next line
#endif
   if (strstr(c_ptr->ln, "};") == (char *)NULL) {							// stop at the end of this structure
      if (strstr(c_ptr->ln, member) != (char *)NULL) {
#ifdef DEBUG
printf("returning\n");
#endif
         return c_ptr;											// found it;
         }
      }
   else
      in_struct = 0;											// we're at the end of this struct
   }
return (struct line *)NULL;
}



int removeStructField(struct line *h_struct, char *srch_str)
{
struct line *p_ptr, *c_ptr, *n_ptr;
int in_struct = 1;

#ifdef DEBUG
printf("Entering: 'removeStructField'\n");
#endif

if (h_struct == (struct line *)NULL)									// there's nothing to remove
   return -1;

c_ptr = h_struct;											// this is the 'current line' pointer
while (c_ptr->next != (struct line *)NULL && in_struct) {						// as long as there are members in this struct
   p_ptr = c_ptr;
   c_ptr = c_ptr->next;											// move to the next line
   n_ptr = c_ptr->next;										// point to the line after the current line (could be NULL)
   if (strstr(c_ptr->ln, "};") == (char *)NULL) {							// stop at the end of this structure
      if (strstr(c_ptr->ln, srch_str) != (char *)NULL) {						// if this is the field/member we're looking for
#ifdef DEBUG
         printf("Removing: '%s - %s' member\n", srch_str, c_ptr->ln);
#endif
	 p_ptr->next = n_ptr;										// drop current and connect 'previous line' to 'next line'
	 free(c_ptr);		
#ifdef DEBUG
printf("Removed\n");
#endif
	 in_struct = 0;											// we delete only one field at a time
	 }
      }
   else
      in_struct = 0;											// we're at the end of this struct
   }

#ifdef DEBUG
printf("Leaving: 'removeStructField'\n");
#endif
}



int countStructMembers(struct line *h_struct)
{
struct line *c_ptr;
int field_cnt = 0, in_struct = 1;;

if (h_struct == (struct line *)NULL)									// there is nothing to count here
   return -1;

c_ptr = h_struct;
while (c_ptr->next != (struct line *)NULL && in_struct) {						// as long as we're in this struct
   c_ptr = c_ptr->next;											// go to the next line
   if (strstr(c_ptr->ln, "};") == (char *)NULL)								// if we're not at the end of the struct
	 field_cnt++;											// there's another field/member
   else
      in_struct = 0;											// end of the struct
   }

return field_cnt;
}



int countFieldOccurences(struct line *h_struct, char *srch_str)
{
struct line *c_ptr;
int in_struct = 1, field_cnt = 0;
char *st_ptr, *lst_ptr;

if (h_struct == (struct line *)NULL)									// there is nothing to count here
   return -1;

c_ptr = h_struct;
while (c_ptr->next != (struct line *)NULL && in_struct) {						// as long as we're in this struct
   c_ptr = c_ptr->next;											// go to the next line
   if (strstr(c_ptr->ln, "};") == (char *)NULL) {							// not the end of the struct yet
      st_ptr = strstr(c_ptr->ln, srch_str);
      if (st_ptr != (char *)NULL) {									// could it be the field/member we're looking for?
         st_ptr += strlen(srch_str);									// move to the character after srch_str 
         if (st_ptr[0] == ' ' ||									// a struct name or so?
             st_ptr[0] == 0   ||									// a variable name?
             st_ptr[0] == '.' ||                                                                        // a struct member?
            (st_ptr[0] == '-' && st_ptr[1] == '>') ||                                                   // a struct member?
             st_ptr[0] == '[') {									// a list?
            if (st_ptr[0] == '[') { 									// is it a list?
               field_cnt += atoi(st_ptr+1);								// yup, find out how long
#ifdef DEBUG
printf("List Counting:   %s\n", c_ptr->ln);
#endif
               }
            else {
	       field_cnt++;										// just one, add one
#ifdef DEBUG
printf("Single Counting: %s\n", c_ptr->ln);
#endif
               }
            }
         }
      }
   else
      in_struct = 0;											// end of struct
   }

#ifdef DEBUG
printf("I counted '%s' %d times,\n", srch_str, field_cnt);
#endif

return field_cnt;
}



void printStruct(struct line *hdr)
{
struct line *c_ptr;
int in_struct = 1;

if (hdr == (struct line *)NULL)
   return;

c_ptr = hdr;
if (c_ptr != (struct line *) NULL)
   if (c_ptr->ln != (char *)NULL)
      printf("%s\n", c_ptr->ln);
while (c_ptr->next != (struct line *)NULL && in_struct == 1) {
   c_ptr = c_ptr->next;
   printf("%s\n", c_ptr->ln);
   if (strstr(c_ptr->ln, "};") != (char *)NULL)
      in_struct = 0;
   }
}



struct line *collateFields(struct line *l_struct, struct line *r_struct)
{
struct line *r_ptr, *l_ptr;
char lst_cnt[6], *srch_str, *tr_ptr;
int field_cnt = 0, old_fld_cnt, in_struct = 1, i;

#ifdef DEBUG
printf("Entering: collateFields.\n");
#endif

if (l_struct == (struct line *) NULL || r_struct == (struct line *) NULL)
   return (struct line *)NULL;

srch_str = (char *)malloc(TMP_BUFF_SZ);
if (srch_str == (char *)NULL)
   printf("Memory allocation error\n");						// we should do a meaningfull return here

l_ptr = l_struct;								// the first occurence of it this structure definition
r_ptr = r_struct;								// the other occurence of it this structure definition (starts with l_struct)

#ifdef DEBUG
printf("l_struct:\n");
printStruct(l_struct);
#endif

while (l_ptr->next != (struct line *)NULL && in_struct) {			// as long as we have lines in this struct
   l_ptr = l_ptr->next;								// walk through the fields of the structure occurence we keep
   strcpy(srch_str, l_ptr->ln);							// make the field a search string ...
   stripWhiteSpace(srch_str);							//                                ... without trailing/leading white space
   tr_ptr = strstr(srch_str, "[");						// is this an array already?
   if (tr_ptr != (char *)NULL)							// does this need to be truncated"
      tr_ptr[0] = 0;								// yup, truncate it
   if (strstr(l_ptr->ln, "};") == (char *)NULL) {				// is this the last line of this structure?
      field_cnt   = countFieldOccurences(r_struct, srch_str);			// count how often the field appears in the right side occurence
      old_fld_cnt = countFieldOccurences(l_struct, srch_str);			// count how long a list we have ourself
      r_ptr = r_struct;
      for (i=0; i < field_cnt; i++) {						// IF field_cnt is 1 or larger I found one or more extra somewhere somewhere
         if (l_struct == r_struct) {						// we're checking ourself ...
            if (i != 0)								//                        ... but we need at least one occurence of the field
               removeStructField(r_ptr, srch_str);				// delete rest
            }
         else
            removeStructField(r_ptr, srch_str);					// delete all of them if we're not checking ourself
         r_ptr = findStructMember(r_struct, srch_str);				// find the left over entry
         }									// there will be at least one field left in my own (l_ptr) struct
      strcpy(lst_cnt, "");							// set to nothing, change to [n] if needed
      if (l_struct == r_struct) {						// IF we're looking at ourself, skip the first one (we're working with that
         l_ptr = findStructMember(l_struct, srch_str);				// find our only one entry again, just in case
         if (old_fld_cnt > 1)
            sprintf(lst_cnt, "[%d];", old_fld_cnt);				//                            ... old_fld_cnt is the correct one, use it
         }
      else {									// we're NOT counting ourself
         if (field_cnt > old_fld_cnt) {
            if (field_cnt > 1)
               sprintf(lst_cnt, "[%d];", field_cnt);				// we found a larger list, use the larger list
            }
         else {
            if (old_fld_cnt > 1)
              sprintf(lst_cnt, "[%d];", old_fld_cnt);				// our own list is long enough
            }
         }

      tr_ptr = strstr(l_ptr->ln, "[");                                              // is this an array already?
      if (tr_ptr != (char *)NULL)                                                  // does this need to be truncated"
         tr_ptr[0] = 0;
      stripTrailingWhiteSpace(l_ptr->ln);

      strcat(l_ptr->ln, lst_cnt);                                             // add it after the field name

#ifdef DEBUG
      if (l_struct != r_struct) {
         printf("r_struct:\n");
         printStruct(r_struct);
         }
#endif
      }
   else
      in_struct = 0;                                                            // done with this struct
   }
free(srch_str);                                                                 // cleanup

#ifdef DEBUG
printf("Leaving: collateFields\n");
#endif

return l_struct;
}




struct line *collateStructures(struct line *header)
{
struct line *l_ptr, *r_ptr;
char *definition;
int empty, rmv_str;


definition = (char *)malloc(TMP_BUFF_SZ);

#ifdef DEBUG
printf("Entering: collateStructures\n");
#endif

l_ptr = header;												// we start at the base
while (l_ptr != (struct line *)NULL) {
   empty = 0;												// reset it every iteration
   l_ptr = findNextLine(l_ptr, "struct", "{");								// we go through the list finding all and collate all structures
   if (l_ptr != (struct line *)NULL) {
      r_ptr = l_ptr;											// we start with ourself
      strcpy(definition, l_ptr->ln);									// copy the definition of the struct
      stripWhiteSpace(definition);									// strip all trailing and leading white space
      while (r_ptr != (struct line *)NULL) {								// as lon as we find structs to collate
         collateFields(l_ptr, r_ptr);									// compare and collate the fields in these two occurences
         r_ptr = findNextOccurence(r_ptr, definition);							// find another occurence of this structure
         }
      while (!empty) {
         r_ptr = findEmptyStruct(header);								// find an empty struct
         if (r_ptr != (struct line *)NULL) {
#ifndef DEBUG
            removeEmptyStruct(header, r_ptr);								// and remove it if tehre is one
#else
            if (removeEmptyStruct(header, r_ptr) == 0)
               printf("Removed empty '%s' structure.\n", definition);
            else
               printf("could not remove %s, not empty.\n", definition);
#endif
            }
         else {
            empty = 1;											// there are no empty structs anymore
            }
         }
      }													// we should be done here
   }


free(definition);											// free the buffer again

#ifdef DEBUG
printf("Leaving: collateStructures\n");
#endif
}



char *getStructName(char *str)
{
char *s_ptr, *e_ptr, *sn_ptr;

s_ptr=strstr(str, "struct");
if (s_ptr != (char *)NULL) {
   s_ptr += 6;
   e_ptr = &(s_ptr[strlen(s_ptr) - 1]);

   sn_ptr = (char *)malloc((e_ptr - s_ptr) + 1 + 1);							// 1 for the difference between front and end, 1 for the terminator

   strcpy(sn_ptr, s_ptr);
   sn_ptr[strlen(sn_ptr)-1] = 0;
   stripWhiteSpace(sn_ptr);
   }
else
   sn_ptr = (char *)NULL;

return sn_ptr;
}



struct line *listHeaderFile(struct line *base, int l_order)
{
FILE *f_ptr;
struct line *c_ptr, *p_ptr, *n_ptr;
struct line header, *new, *cn_ptr;
char *structName, *tmp_line;
int order = 0, i, cnt;


#ifdef DEBUG
printf("Entering listHeader: ");
if (l_order == GEN_PARSE_STRUCTS)
   printf("GEN_PARSE_STRUCTS\n");
if (l_order == GEN_STRUCT_SWITCH)
   printf("GEN_STRUCT_SWITCH\n");
#endif

if (base == (struct line *)NULL)									// nothing here
   return (struct line *)NULL;

tmp_line = (char *)malloc(TMP_BUFF_SZ);

memset(&header, 0x00, sizeof(struct line));
header.next = (struct line *)NULL;
cn_ptr = &header;

if (l_order == DECONSTRUCT) {										// DECONSTRUCT get the structs out of the nested XML from highest recursion to lowest
   c_ptr = base;											// point to the base of the list
   while (c_ptr->next != (struct line *) NULL) {
      c_ptr   = c_ptr->next;										// base is never used except for attaching things to, go to next
#ifdef DEBUG
printf("===%s\n", c_ptr->ln);
#endif
      if (c_ptr->lvl > order)										// if this recursion level was higher ...
         order = c_ptr->lvl;										//                                    ... take it
      }
#ifdef DEBUG
   printf("Highest r-lvl: %d\n", order);
#endif
   for (i = order; i > 0; i--) {									// were going down, pairwise   n - n-1 ... 3-2, 2-1, 1-0
      c_ptr = base;											// current pointer pointing to the list
      while (c_ptr->next != (struct line *) NULL) {							// only end of the list should be NULL
         p_ptr = c_ptr;											// we're pointing to the previous line, if there is one
         c_ptr = c_ptr->next;										// this is our 'current' line
         if (c_ptr->next != (struct line *)NULL)
            n_ptr = c_ptr->next;									// possible next line ...
         else
            n_ptr->next = (struct line *)NULL;								//                    ... or not
         if (c_ptr->tag == 0) {										// if the line is not tagged, it's up for grabs
            if (c_ptr->lvl == i && c_ptr->ln != (char *)NULL) {						// it is the level we're looking for and there is a line
               if (c_ptr->lvl - p_ptr->lvl == 1) {							// level of previous line is one less, so it is our 'head'
                  p_ptr->tag = 0;									// but we don't tag it because the next struct needs it also

#ifdef DEBUG
                  printf("[%d] [%d] [%d] +1+ %s\n", p_ptr->lvl, p_ptr->tag, i, p_ptr->ln);		// print previous header line
#endif
                  new = (struct line *)malloc(sizeof(struct line));					// create new line for the header line
                  memset(new, 0x00, sizeof(struct line));
                  new->ln = (char *)malloc(strlen(p_ptr->ln) + 1);
                  strcpy(new->ln, p_ptr->ln);								// copy the string
                  new->next = (struct line *)NULL;							// we don't have a next line yet
                  cn_ptr->next = new;									// attach to previous line
                  cn_ptr = cn_ptr->next;								// point at new our addition
                  p_ptr->ln[strlen(p_ptr->ln)-1] = 0;							// this gets rid of the '{' bracket
                  }
               c_ptr->tag = 1;										// this is our line, tag and take it
#ifdef DEBUG
               printf("[%d] [%d] [%d] +2+ %s\n", c_ptr->lvl, c_ptr->tag, i, c_ptr->ln);			// print current header line
#endif
               new = (struct line *)malloc(sizeof(struct line));					// create new line for the header line
               memset(new, 0x00, sizeof(struct line));
               structName = getStructName(c_ptr->ln);
               if (structName != (char *)NULL)
                  new->ln = (char *)malloc(strlen(c_ptr->ln) + 1 + strlen(structName) + 2);		// allocate for line   ln + " " + structName + 0
               else
                  new->ln = (char *)malloc(strlen(c_ptr->ln) + 1);					// just the original length
               strcpy(new->ln, c_ptr->ln);								// copy the string
               stripTrailingWhiteSpace(new->ln);
               if (structName != (char *)NULL) {
                  strcat(new->ln, " ");
                  strcat(new->ln, structName);
                  free(structName);
                  }
               new->next = (struct line *)NULL;								// we don't have a next line yet
               cn_ptr->next = new;									// attach to previous line
               cn_ptr = cn_ptr->next;									// point at new our addition
               }
            }
         }												// end while.
      }													// for every recursion level higher than 0
   collateStructures(&header);										// delete all double fieds in all structs
#ifdef DEBUG
printf("##############  AND WE'RE DONE ##########\n");
#endif
   }													// end of deconstruct

if (l_order == NO_ORDER) {
   c_ptr = base;
   while (c_ptr->next != (struct line *) NULL) {
      c_ptr = c_ptr->next;
      if (c_ptr->ln != (char *)NULL) {
         strcpy(tmp_line, c_ptr->ln);
         stripWhiteSpace(tmp_line);
         if (strstr(tmp_line, "struct") != (char *)NULL && strstr(tmp_line, "{") != (char *)NULL)
            printf("%s\n", tmp_line);
         else
            printf("   %s\n", tmp_line);
         if (strstr(tmp_line, "};") != (char *)NULL)
            printf("\n");
         }
      }
   }

if (l_order == GEN_PARSE_STRUCTS) {
   f_ptr = fopen("./parse-inc/config-structs.h", "w");
   if (f_ptr != (FILE *)NULL) {
      c_ptr = base;
      while (c_ptr->next != (struct line *) NULL) {
         c_ptr = c_ptr->next;
         if (c_ptr->ln != (char *)NULL) {
            strcpy(tmp_line, c_ptr->ln);
            stripWhiteSpace(tmp_line);
            if (strstr(tmp_line, "struct") != (char *)NULL && strstr(tmp_line, "{") != (char *)NULL)
               fprintf(f_ptr, "%s\n", tmp_line);
            else {
               if (tmp_line[strlen(tmp_line)-1] == ';')
                  fprintf(f_ptr, "   %s\n", tmp_line);
               else
                  fprintf(f_ptr, "   %s;\n", tmp_line);
               }
            if (strstr(tmp_line, "};") != (char *)NULL)
               fprintf(f_ptr, "\n");
            }
         }
      fclose(f_ptr);
      }
   else
      printf("Could not open/create confpars-structs.h for output.\n");
   }

if (l_order == NO_ORDER_DEBUG) {
   c_ptr = base;
   while (c_ptr->next != (struct line *) NULL) {
      c_ptr = c_ptr->next;
      if (c_ptr->ln != (char *)NULL)
         printf("[%d] [%d] %s %s\n", c_ptr->lvl, c_ptr->tag, c_ptr->dbg, c_ptr->ln);
      }
   }


if (l_order == GEN_STRUCT_SWITCH) {
   c_ptr = base;
   cnt = countSwitchPaths(c_ptr);
   createSwitchPaths(c_ptr, cnt);
   }

free(tmp_line);

#ifdef DEBUG
printf("Leaving listHeader\n");
#endif

return header.next;
}



void freeHeaderFile(struct line *base)
{
struct line *ptr;

if (base == (struct line *)NULL)
   return;

ptr = base;                                                                             		// base of the list
if (ptr->next != (struct line *)NULL)                                                     		// walk to the ...
   ptr = ptr->next;
if (ptr->ln != (char *) NULL)
   free(ptr->ln);

free(ptr);
}



struct line *addToHeaderFile(int lvl, char *ln, char *dbg, struct line *base)
{
struct line *ptr, *new;

new = (struct line *)malloc(sizeof(struct line));							// create a new entry for the list
if (new != (struct line *) NULL) {
   memset(new, 0x00, sizeof(struct line));								// reset everything to 0
   new->lvl = lvl;											// what was the rec level

   new->ln = (char *)malloc(strlen(ln) + 1);								// create space for the header line
   strcpy(new->ln, ln);											// copy header line
   strcpy(new->dbg, dbg);										// debug info
   new->next = (struct line *)NULL;									// terminate list

   ptr = base;												// base of the list
   while (ptr->next != (struct line *)NULL)								// walk to the ...
      ptr = ptr->next;											//             ... end of the list

   ptr->next = new;											// attach new line
   }

return new;
}



int checkForList(char *elem_tag, char *element, char *struct_h)
{
char *e_tag, *struct_ptr, *field_ptr, *is_list;
char *struct_end_ptr, *field_end_ptr, *list_ptr;

if (struct_h == (char *)NULL)
   printf("we have a NULL problem\n");

e_tag = (char *)malloc(TMP_BUFF_SZ);

strcpy(e_tag, "struct ");
strcat(e_tag, elem_tag);
strcat(e_tag, " {");

struct_ptr = strstr(struct_h, e_tag);									// find the structure the field is a member of

if (struct_ptr != (char *)NULL) {
   struct_end_ptr = strstr(struct_ptr, "};");								// find the end of that structure
   if (struct_end_ptr != (char *)NULL) {
      field_ptr = strstr(struct_ptr, element);								// find the field/member
      if (field_ptr != (char *)NULL) {
         if (field_ptr < struct_end_ptr) {								// needs to be within the same structure
            field_end_ptr = strstr(field_ptr, ";");							// find the end of the field
            list_ptr = strstr(field_ptr, "[");								// find the square bracket
            if (list_ptr != (char *)NULL) {
               if (list_ptr < field_end_ptr) {								// if it comes before the end of the field it is always a list
#ifdef DEBUG
                  printf("0: member %s of struct %s is a list\n", element, e_tag);
#endif
                  return 1;
                  }
               else {
#ifdef DEBUG
                  printf("1: member %s of struct %s is NOT a list\n", element, e_tag);
#endif
                  }
               }
            else {
#ifdef DEBUG
               printf("2: %s %s is not a list\n", e_tag, element);
#endif
               }
            }
         else {
#ifdef DEBUG
            printf("3: Field %s not found in struct %s !!\n", element, e_tag);
#endif
            }
         }
      else {
#ifdef DEBUG
         printf("4: Field %s not found in struct %s !!\n", element, e_tag);
#endif
         }
      }
   else {
#ifdef DEBUG
      printf("5: Struct %s format error!!\n", element, e_tag);
#endif
      }
   }
else {
#ifdef DEBUG
   printf("6: Field %s not found in struct %s !!\n", element, e_tag);
#endif
   }

free(e_tag);

return 0;
}



char *findLastMember(char *str)
{
char *ptr;

ptr = &(str[strlen(str)-1]);										// point at the last character

while (ptr >= str) {
   if (ptr[0] == '.' || ptr[0] == '>')
      return ++ptr;
   else
      ptr--;
      }
return (char *)NULL;											// no . found
}



int checkForStruct(char *struct_path, char *struct_h)
{
char *mbr_ptr, *br_ptr, member[64], str_name[64], *struct_ptr;

if (struct_h == (char *)NULL)
   printf("we have a NULL problem\n");

mbr_ptr = findLastMember(struct_path);

strcpy(member, mbr_ptr);
br_ptr = strstr(member, "[");
if (br_ptr != (char *)NULL)
   *br_ptr = 0;

strcpy(str_name, "struct ");
strcat(str_name, member);
strcat(str_name, " ");

struct_ptr = strstr(struct_h, str_name);                                                                   // find the structure the field is a member of

if (struct_ptr != (char *)NULL)
   return 1;

return 0;
}



int checkElemCnt(char *element, struct elmPathCnt *base)
{
struct elmPathCnt *c_ptr, *new;

c_ptr = base;
if (c_ptr == (struct elmPathCnt *)NULL)
   return -1;

while (c_ptr->next != (struct elmPathCnt *)NULL) {
   c_ptr = c_ptr->next;
   if (strcasecmp(c_ptr->elem, element) == 0) {
      c_ptr->cnt++;
      return c_ptr->cnt;
      }
   }

new = (struct elmPathCnt *)malloc(sizeof(struct elmPathCnt));
memset(new, 0x00, sizeof(struct elmPathCnt));
new->next = (struct elmPathCnt *)NULL;
strcpy(new->elem, element);
new->cnt = 0;
c_ptr->next = new;

return c_ptr->cnt;
}



int countPaths(struct line *paths)
{
struct line *c_ptr;
int cnt = 0;

if (paths == (struct line *)NULL)                                       // we're looking at the 'base' next one is 1st one
   return 0;

c_ptr = paths;

while (c_ptr->next != (struct line *)NULL) {
   c_ptr = c_ptr->next;
   cnt++;
   }

return cnt;
}



void *addToNamesList(char *ln, struct line *fld_nm_lst, int type)
{
struct line *ptr, *new;

new = (struct line *)malloc(sizeof(struct line));							// create a new entry for the list
if (new != (struct line *) NULL) {
   memset(new, 0x00, sizeof(struct line));								// reset everything to 0
   new->lvl = 0;											// what was the rec level

   new->ln = (char *)malloc(strlen(ln) + 1);								// create space for the header line
   strcpy(new->ln, ln);											// copy header line
   strcpy(new->dbg, "");										// debug info
   new->type = type;
   new->next = (struct line *)NULL;									// terminate list

   ptr = fld_nm_lst;											// base of the list
   while (ptr->next != (struct line *)NULL) 								// walk to the ...
      ptr = ptr->next;											//             ... end of the list
 
   ptr->next = new;											// attach new line
   }

return new;
}


#ifdef DATAPARSE
char *parseElementContent(char *element_start, char *element_end, char *elem_tag,			// definition for parser in data parse 'mode'
                          char *buffstart, char **par_elem_data, int *prcss_cntnt,
                          int task, struct line *base, char *struct_h, 
                          struct line *fld_nm_lst, char *c_struct_path, struct config *config, int verbose)
#else
char *parseElementContent(char *element_start, char *element_end, char *elem_tag,			// definition of parser not in data parse "mode'
                          char *buffstart, char **par_elem_data, int *prcss_cntnt,
                          int task, struct line *base, char *struct_h,
                          struct line *fld_nm_lst, char *c_struct_path, int verbose)
#endif
{
char start_tag[EL_NAME_SIZE+4], end_tag[EL_NAME_SIZE+4], element[EL_NAME_SIZE],
     elementData[1024], hf_line[128], *content_start, *content_end, *next_content,
     *elem_data, *elem_val_ptr, new_cs_path[256], *ncp_ptr, **field;
int  parsing=1, nest_elem=0, processed=0, process_content=0, fld_idx;
struct elmPathCnt elemCntBase;


memset(&elemCntBase, 0x00, sizeof(struct elmPathCnt));

// increase recursionlevel
r_lvl++;												// we should make that local instead of global ....


elem_data = (char *) NULL;
element[0] = 0;												// make sure element is initially an empty string
new_cs_path[0] = 0;

sprintf(start_tag, "<%s>", elem_tag);									// create element start tag
sprintf(end_tag, "</%s>", elem_tag);									// create element end tag

if (element_start != (char *)NULL) {
   content_start = strcasestr(element_start, start_tag);						// point at start of current content
   if (content_start != (char *)NULL) {
      content_start += strlen(start_tag);								// content starts after the tag
      content_end   = strcasestr(content_start, end_tag);						// point at end of current content
      if (content_end != (char *)NULL) {
         content_end--;											// content ends before the end tag
         next_content = strstr(content_end, ">");							// find next chunk of content
         if (next_content != (char *)NULL) {
            next_content++;
            }            										// content start after a tag if there is any
         }
      else {
         printf("Missing %s element\n", end_tag);
         }
      }
   else {
      content_end = (char *)NULL;
      printf("Missing %s element\n", start_tag);
      }
   }
else
   {
   content_start = (char *)NULL;									// element start was NULL
   content_end   = (char *)NULL;									// didn't see an end tag
   }

if (content_start != (char *)NULL && content_end != (char *)NULL) {
   while (parsing) {
      parsing = findElement(content_start, content_end, element, buffstart);	 			// as long as we keep finding a next element in this element/content
      if (parsing == 1) {										// IF there is an element in this one we won't have element data
         nest_elem = 1;											//  ... on this level when done
         if (processed == 0 && process_content == 0) {
            switch (task) {
               case DISPLAY: {
                  printf("Populating new structure.\n");
                  break;
                  }
               case CREATE_STRUCT_PATHS:
               case CREATE_STRUCT: {
                  setIndent(r_lvl-1);
                  sprintf(hf_line, "%sstruct %s {\n", indent, elem_tag);
#ifdef DEBUG
printf("ATH-1: %s\n", hf_line);
#endif
                  stripTrailingWhiteSpace(hf_line);
                  addToHeaderFile(r_lvl-1, hf_line, "-1-", base);
                  resetIndent(r_lvl-1);
                  break;
                  }
               default: {
                  break;
                  }
               } 
            }

         if (task == CREATE_STRUCT_PATHS) {
            strcpy(new_cs_path, c_struct_path);									// copy the path
            if (strcmp(new_cs_path, "config") == 0)
               strcat(new_cs_path, "->");
            else
               strcat(new_cs_path, ".");
            strcat(new_cs_path, element);									// new field/element
            ncp_ptr = &(new_cs_path[strlen(new_cs_path)]);							// point at string terminator
            if (checkForList(elem_tag, element, struct_h) == 1) {						// check if 'struct field' is a list'
               fld_idx = checkElemCnt(element, &elemCntBase);							// check to see how often we saw it on this level
               sprintf(ncp_ptr, "[%d]", fld_idx);								// add index/occurenceA
               }
            if (checkForStruct(new_cs_path, struct_h) == 1)
               addToNamesList(new_cs_path, fld_nm_lst, TYPE_STRUCT);						// this pretty crude
            else												//
               addToNamesList(new_cs_path, fld_nm_lst, TYPE_CHAR);						// and when types are implemented we probably have to do better.
            }

#ifdef DATAPARSE
         content_start = parseElementContent(content_start, content_end, element, buffstart,		// call in data parse 'mode'
                                             &elem_data, &process_content, task, base, struct_h,	// recursively find elements/objects, these are structures part of a 'super structure'
                                             fld_nm_lst, new_cs_path, config, verbose);			// part of a 'super structure'
#else
         content_start = parseElementContent(content_start, content_end, element, buffstart,		// call when not in data parse 'mode'
                                             &elem_data, &process_content, task, base, struct_h,
                                             fld_nm_lst, new_cs_path, verbose);					// recursively find elements/objects, these are structures part of a 'super structure'
#endif
#ifdef DEBUG
         if (elem_data != (char *) NULL) {
            if (processed == 0 || process_content == 1) {
               if (strlen(new_cs_path) > 0) {
                  elem_val_ptr = elemValPtr(elem_data);
                  printf("Path: %s -- [%s]\n", new_cs_path, elem_val_ptr);
                  }
               }
            }
#endif

         if (processed == 0 || process_content == 1) {
            elem_val_ptr = elemValPtr(elem_data);
#ifdef DEBUG_PARSE
printf("elem_data - elem_val_ptr: %s - %s\n", elem_data, elem_val_ptr);
#endif
            switch (task) {
               case DISPLAY: {
                  if (elem_val_ptr != (char *)NULL)
                     printf("Parent name is %s, the element name is %s (%s) and the content is %s\n", elem_tag, elem_data, element, elem_val_ptr);
                  else
                     printf("Parent name is %s, the element name is %s (%s) and there is no content.", elem_tag, elem_data, element);
                  break;
                  }
               case CREATE_STRUCT: {
                  if (elem_data != (char *)NULL) {
                     setIndent(r_lvl);
                     sprintf(hf_line, "%schar *%s\n", indent, elem_data);
                     stripTrailingWhiteSpace(hf_line);
                     strcat(hf_line, ";");
#ifdef DEBUG
                     printf("ATH-2: %s\n", hf_line);
#endif
                     addToHeaderFile(r_lvl, hf_line, "-2-", base);
                     resetIndent(r_lvl);
                     }
#ifdef DEBUG
                  else
                     printf("elem_data is NULL\n");
#endif
                  break;
                  }
#ifdef DATAPARSE
               case POPULATE_STRUCT: {

                  if (elem_data != (char *) NULL) {
                     if (processed == 0 || process_content == 1) {
                        if (strlen(new_cs_path) > 0) {
                           if (verbose == VERBOSE)
                              printf("Populating:  %s: %s\n", new_cs_path, elem_val_ptr);
                           elem_val_ptr = getElemVal(elem_data);
                           field  = (char **)getMemberPtr(new_cs_path, config);				// get the pointer to char * in a structure
                           *field = elem_val_ptr;							// change it
                           }
                        }
                     }
                  break;
                  }
#endif
               default: {
                  break;
                  }
               }
            processed = 1;										// to prevent multiple evaluations of same data from happening
            process_content = 0;									// force evaluation from data we know for sure doesn't have nested elements
            }
         }
      else {
         if (parsing == -1) {										// found a bad tag
            free(buffstart);										// be nice and release memory
            exit(-1);											// just bail out
            }
         if (nest_elem == 0) {										// no nested elements, it's data (fill struct fields here).
            *par_elem_data = processElementContent(elem_tag, content_start, content_end);		// process the content of element 'elem_tag'
            *prcss_cntnt = 1;										// we need to let the previous process know there was no tag and needs processed
            }
         }
      }
   }
else
   {
   if (content_start == (char *)NULL && content_end != (char *)NULL)					// found an end tag without a start tag
      printf("No element start tag found. (<%s>).\n", elem_tag);
   if (content_start != (char *)NULL && content_end == (char *)NULL)					// found a start tag without an end tag.
      printf("No matching element end tag found. (</%s>)\n", elem_tag) ;
   }

if (elem_data != (char *)NULL)										// if elem_data is not pointing "ins blaue hinein"
   free(elem_data);											// free it.

if (nest_elem == 1 && task == CREATE_STRUCT) {
   setIndent(r_lvl);
   sprintf(hf_line, "%s}\n", indent);
   stripTrailingWhiteSpace(hf_line);
   strcat(hf_line, ";");
#ifdef DEBUG
printf("ATH-3: %s\n", hf_line);
#endif
   addToHeaderFile(r_lvl, hf_line, "-3-",  base);							// was a typo,  this was r_lvl-1   in the closing bracket section !!!!
   resetIndent(r_lvl);
   }

// decrease recursionlevel
r_lvl--;

return next_content;
}



#ifdef DATAPARSE
int parseConfigFile(char *cfg_file, int task, struct line *base, struct line *fld_nm_lst, struct config *config, int verbose)		// parsing config file, in data parse 'mode'
#else
int parseConfigFile(char *cfg_file, int task, struct line *base, struct line *fld_nm_lst, int verbose)					// parsing config file when not in data parse 'mode'
#endif
{
struct line *new;
char *txt_config, *struct_h, *end_ptr, *root_data, config_tag[16], conf_path[] = "config";
int prc_cnt, lst_sz;

root_data = (char *)NULL;										// is recursively used by parseElemContent, not here.
struct_h  = (char *) NULL;										// buffer for structire header file

strcpy(config_tag, "config");		//     !!!!!  Do NOT change !!! 				// A configuration is ALWAYS in an element named this.
txt_config = readConfigFile(cfg_file);									// read in the config file
if (txt_config !=  (char *)NULL) {
   end_ptr = txt_config;										// also point at the last byte ...
   end_ptr += strlen(txt_config);									//                        ... of this buffer



if (task == CREATE_STRUCT_PATHS)
   struct_h = readConfigFile("./parse-inc/config-structs.h");

#ifdef DATAPARSE
   parseElementContent(txt_config, end_ptr, config_tag, txt_config, &root_data,				// parse the content of an element from txt_config to end_ptr in data parse 'mode'
                       &prc_cnt, task, base, struct_h, fld_nm_lst, conf_path, config, verbose);
#else
   if (task == CREATE_STRUCT_PATHS)
      addToNamesList("config", fld_nm_lst, TYPE_STRUCT);
   parseElementContent(txt_config, end_ptr, config_tag, txt_config, &root_data,				// parse the content of an element from txt_config to end_ptr
                       &prc_cnt, task, base, struct_h, fld_nm_lst, conf_path, verbose);
#endif

   free(txt_config);											// free the config txt buffer
   if (struct_h != (char *)NULL)
      free(struct_h);											// free structures header file buffer
   if (root_data != (char *) NULL)
      free(root_data);
   }
else
   return -1;

return 0;
}
