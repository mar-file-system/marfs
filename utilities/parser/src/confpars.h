/*
 * 
 *   confpar.h
 *
 *   config file parser header
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

#define TMP_BUFF_SZ	128
#define EL_NAME_SIZE	 64						// max length of the element/tag name
#define RSRV_LST         33						// length of reserved words list


char *stripLeadingWhiteSpace(char *);
char *stripTrailingWhiteSpace(char *);
char *stripWhiteSpace(char *);
struct line *findClosingStructBracket(struct line *);
int checkElementName(char *, char *);
char *readConfigFile(char *);
int  findLineNumber(char *, char *);
int  findElement(char *, char *, char *, char *);
int  strPrintTag(char*, char *);
int  printContent(char *, char *);
char *processElementContent(char *, char *, char *);
char *elemValPtr(char *);
void setIndent(int);
void resetIndent(int);
struct line *findNextLine(struct line *, char *, char *);
struct line *findNextOccurence(struct line *, char *);
struct line *findNextStruct(struct line *);
struct line *findEmptyStruct(struct line *);
int removeEmptyStruct(struct line *, struct line *);
void removeStruct(struct line *, struct line *);
struct line *findStructMember(struct line *, char *);
int removeStructField(struct line *, char *);
int countStructMembers(struct line *);
int countStructFieldsOccurences(struct line *, char *);
struct line *collateFields(struct line *, struct line *);
struct line *collateStructures(struct line *);
char *getStructName(char *);
struct line *listHeaderFile(struct line *, int);
void freeHeaderFile(struct line *);
struct line *addToHeaderFile(int, char *, char *, struct line *);
int checkForList(char *, char *, char *);
int checkForStruct(char *, char *s);
int checkElemCnt(char *, struct elmPathCnt *);
int countPaths(struct line *);
void *addToNamesList(char *, struct line *, int);
#ifdef DATAPARSE
struct line *freeConfigStructContent(struct config *);
char *parseElementContent(char *, char *, char *, char *, char **, int *, int, struct line *, char *, struct line *, char *, struct config *, int);
int  parseConfigFile(char *, int, struct line *, struct line *, struct config *, int);
#else
char *parseElementContent(char *, char *, char *, char *, char **, int *, int, struct line *, char *, struct line *, char *, int);
int  parseConfigFile(char *, int, struct line *, struct line *, int);
#endif
