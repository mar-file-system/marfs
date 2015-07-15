int configIdx(char *);
void *memberSwitchParse(char *, struct config *, int);
void *getMemberPtr(char *, struct config *);
void *fMemberPtr(char *, struct config *);
struct line *freeConfigStructContent(struct config *);
void *listObjByName(char *, struct config *);
