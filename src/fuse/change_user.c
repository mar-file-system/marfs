/**
 * Copyright 2015. Triad National Security, LLC. All rights reserved.
 *
 * Full details and licensing terms can be found in the License file in the main development branch
 * of the repository.
 *
 * MarFS was reviewed and released by LANL under Los Alamos Computer Code identifier: LA-CC-15-039.
 */

#define _GNU_SOURCE

#include "marfs_auto_config.h"
#ifdef DEBUG_FUSE
#define DEBUG DEBUG_FUSE
#elif (defined DEBUG_ALL)
#define DEBUG DEBUG_ALL
#endif
#define LOG_PREFIX "fuse_changeuser"
#include "logging/logging.h"

#include "change_user.h"

#include <unistd.h>
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/syscall.h>

int enter_groups(user_ctxt ctxt, uid_t uid, gid_t gid)
{
  if (ctxt->entered_groups)
  {
    LOG(LOG_ERR, "double-enter (groups) -> %u\n", uid);
    errno = EPERM;
    return -1;
  }

  ctxt->group_ct = getgroups(sizeof(ctxt->groups) / sizeof(gid_t), ctxt->groups);
  if (ctxt->group_ct < 0)
  {
    LOG(LOG_ERR, "getgroups() failed\n");
    return -1;
  }

  struct passwd pwd;
  struct passwd *result;
  const size_t STR_BUF_LEN = 1024;
  char str_buf[STR_BUF_LEN];
  if (getpwuid_r(uid, &pwd, str_buf, STR_BUF_LEN, &result))
  {
    LOG(LOG_ERR, "getpwuid_r() failed: %s\n", strerror(errno));
    errno = EINVAL;
    return -1;
  }
  else if (result == NULL)
  {
    LOG(LOG_ERR, "No passwd entries found, for uid %u\n", uid);
    errno = EINVAL;
    return -1;
  }
  LOG(LOG_INFO, "uid %u = user '%s'\n", uid, result->pw_name);

  gid_t groups[NGROUPS_MAX + 1];
  int ngroups = NGROUPS_MAX + 1;
  int group_ct = getgrouplist(result->pw_name, gid, groups, &ngroups);
  if (group_ct < 0)
  {
    LOG(LOG_ERR, "No passwd entries found, for user '%s'\n", result->pw_name);
    return -1;
  }

  int i;
  for (i = 0; i < group_ct; ++i)
  {
    LOG(LOG_INFO, "group = %u\n", groups[i]);
  }

  if (syscall(SYS_setgroups, ngroups, groups))
  {
    LOG( LOG_ERR, "Setgroups failure\n" );
    return -1;
  }

  ctxt->entered_groups = 1;
  return 0;
}

int enter_user(user_ctxt ctxt, uid_t new_euid, gid_t new_egid, int enter_group)
{
  if (ctxt->entered)
  {
    LOG(LOG_ERR, "double-enter -> %u\n", new_euid);
    errno = EPERM;
    return -1;
  }

  if (enter_group && enter_groups(ctxt, new_euid, new_egid))
  {
    return -1;
  }

  gid_t old_rgid;
  gid_t old_egid;
  gid_t old_sgid;

  if (syscall(SYS_getresgid, &old_rgid, &old_egid, &old_sgid))
  {
    LOG(LOG_ERR, "getresgid() failed\n");
    exit(EXIT_FAILURE);
  }

  LOG(LOG_INFO, "gid %u(%u) -> (%u)\n", old_rgid, old_egid, new_egid);
  if (syscall(SYS_setresgid, -1, new_egid, old_egid) == -1 && !((errno = EACCES) && ((new_egid == old_egid) || (new_egid == old_rgid))))
  {
    LOG(LOG_ERR, "failed!\n");
    return -1;
  }

  uid_t old_ruid;
  uid_t old_euid;
  uid_t old_suid;

  if (syscall(SYS_getresuid, &old_ruid, &old_euid, &old_suid))
  {
    LOG(LOG_ERR, "getresuid() failed\n");
    exit(EXIT_FAILURE);
  }

  LOG(LOG_INFO, "uid %u(%u) -> (%u)\n", old_ruid, old_euid, new_euid);
  if (syscall(SYS_setresuid, -1, new_euid, old_euid) == -1)
  {
    if (syscall(SYS_setresgid, -1, old_egid, -1))
    {
      LOG(LOG_ERR, "failed -- couldn't restore egid %d!\n", old_egid);
      exit(EXIT_FAILURE);
    }
    else
    {
      LOG(LOG_ERR, "failed!\n");
      return -1;
    }
  }

  ctxt->entered = 1;

  return 0;
}

int exit_groups(user_ctxt ctxt)
{
  int i;
  for (i = 0; i < ctxt->group_ct; ++i)
  {
    LOG(LOG_INFO, "group = %u\n", ctxt->groups[i]);
  }

  if (syscall(SYS_setgroups,ctxt->group_ct, ctxt->groups))
  {
    LOG( LOG_ERR, "Setgroups failure\n" );
    return -1;
  }

  ctxt->entered_groups = 0;
  return 0;
}

int exit_user(user_ctxt ctxt)
{
  uid_t old_ruid;
  uid_t old_euid;
  uid_t old_suid;

  if (syscall(SYS_getresuid, &old_ruid, &old_euid, &old_suid))
  {
    LOG(LOG_ERR, "getresuid() failed\n");
    exit(EXIT_FAILURE);
  }

  LOG(LOG_INFO, "uid (%u) <- %u(%u)\n", old_suid, old_ruid, old_euid);
  if (syscall(SYS_setresuid, -1, old_suid, -1) == -1 && !((errno == EACCES) && ((old_suid == old_ruid) || (old_suid == old_euid))))
  {
    LOG(LOG_ERR, "failed\n");
    exit(EXIT_FAILURE);
  }

  gid_t old_rgid;
  gid_t old_egid;
  gid_t old_sgid;

  if (syscall(SYS_getresgid, &old_rgid, &old_egid, &old_sgid))
  {
    LOG(LOG_ERR, "getresgid() failed\n");
    exit(EXIT_FAILURE);
  }

  LOG(LOG_INFO, "gid (%u) <- %u(%u)\n", old_sgid, old_rgid, old_euid);
  if (syscall(SYS_setresgid, -1, old_sgid, -1) == -1 && !((errno == EACCES) && ((old_sgid == old_rgid) || (old_sgid == old_egid))))
  {
    LOG(LOG_ERR, "failed!\n");
    exit(EXIT_FAILURE);
  }

  if (ctxt->entered_groups && exit_groups(ctxt))
  {
    return -1;
  }

  ctxt->entered = 0;
  return 0;
}
