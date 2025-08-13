#ifndef _CHANGE_USER_H
#define _CHANGE_USER_H
/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */


#include <linux/limits.h>

typedef struct user_ctxt_struct
{
  int entered;
  int entered_groups;
  gid_t groups[NGROUPS_MAX];
  int group_ct;
} * user_ctxt;

int enter_user(user_ctxt ctxt, uid_t new_euid, gid_t new_egid, int enter_group);
int exit_user(user_ctxt ctxt);

#endif // _CHANGE_USER_H

