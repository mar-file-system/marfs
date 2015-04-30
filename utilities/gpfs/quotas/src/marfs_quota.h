#ifndef MARFS_QUOTA_H
#define MARFS_QUOTA_H


#define MAX_XATTR_VAL_LEN 64
#define MAX_XATTR_NAME_LEN 32

#define SMALL_FILE_MAX 4096
#define MEDIUM_FILE_MAX 1048576
#define MAX_MARFS_XATTR 3

struct histogram {
  unsigned long long int small_count;
  unsigned long long int medium_count;
  unsigned long long int large_count;
};
struct marfs_xattr {
  char xattr_name[MAX_XATTR_NAME_LEN];
  char xattr_value[MAX_XATTR_VAL_LEN];
};
#endif

